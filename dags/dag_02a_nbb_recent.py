"""
DAG 02a — NBB Comptes annuels RÉCENTS → Bronze HDFS + MongoDB  [PRIORITAIRE]

Fenêtre fiscale : 2020 → 2025 (6 années, ~5 dépôts/entreprise maximum)
Objectif        : terminer en ~3 jours avant le DAG historique

Priorité Airflow :
  • priority_weight = 10  (vs 1 pour le DAG historique)
  • schedule @daily       (vs @weekly pour l'historique)
  • max_active_runs = 2   (vs 1 pour l'historique)

Idempotent : la State DB (bce_state_db) garantit qu'un dépôt déjà téléchargé
par n'importe quel autre run / DAG est automatiquement sauté.

Source State DB : "nbb_csv" / "nbb_pdf"  (partagées avec le DAG historique —
le deposit_id suffit à distinguer les années, pas besoin de source différente).
"""
import csv
import io
import logging
import re
import time
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_LIMIT: int | None = 1_000   # None = run complet de production

PATTERN_CODE = re.compile(r"^\d+(/\d+)?[A-Z]*P?$")


@dag(
    dag_id="dag_02a_nbb_recent",
    description="NBB 2020-2025 → HDFS + MongoDB [PRIORITAIRE]",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["bce", "nbb", "recent", "bronze", "prioritaire", "layer-1"],
    default_args={"retries": 2, "retry_delay": 300},
)
def dag_nbb_recent():

    @task(priority_weight=10)
    def check_ready() -> int:
        from ingestion.mongo_client import col, COL_ENTERPRISES
        n = col(COL_ENTERPRISES).count_documents({"Status": "AC"})
        if n == 0:
            raise RuntimeError("MongoDB vide — lancer dag_01_kbo_to_mongo d'abord")
        log.info("[nbb-recent] %d entreprises actives disponibles", n)
        return n

    @task(priority_weight=10, execution_timeout=None)
    def ingest_recent(n_companies: int) -> dict:
        from ingestion.config import (
            HDFS_BRONZE_NBB_CSVS, HDFS_BRONZE_NBB_PDFS,
            BATCH_LOG_EVERY, CBSO_DELAY,
            ANNEE_MIN_RECENT, ANNEE_MAX_RECENT,
            NBB_HEADERS,
        )
        from ingestion.mongo_client import (
            iter_active_companies, is_done, mark_pending, mark_done, mark_error,
            upsert_nbb_account,
        )
        from ingestion.nbb_api import iter_deposits_to_ingest, download_csv, download_pdf
        from ingestion.hdfs_utils import upload_bytes_retry
        from ingestion.tor_session import TorSession

        log.info(
            "=== [dag_02a] ingest_recent — années %d→%d | limit=%s | %d entreprises ===",
            ANNEE_MIN_RECENT, ANNEE_MAX_RECENT, _LIMIT, n_companies,
        )
        t0  = time.time()
        tor = TorSession(headers=NBB_HEADERS)

        stats = dict(companies=0, no_deposits=0, already=0,
                     csv_ok=0, csv_fail=0, pdf_ok=0, pdf_fail=0, errors=0)
        limit = _LIMIT

        for company in iter_active_companies():
            if limit is not None and stats["companies"] >= limit:
                log.info("[nbb-recent] Limite %d atteinte — arrêt", limit)
                break

            bce   = company["bce_num"]
            bce_c = company.get("bce_num_clean", bce.replace(".", ""))
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                _log_progress("[nbb-recent]", stats, n_companies, t0)

            try:
                deposits = list(iter_deposits_to_ingest(
                    bce_c, session=tor,
                    annee_min=ANNEE_MIN_RECENT,
                    annee_max=ANNEE_MAX_RECENT,
                ))
            except Exception as exc:
                log.warning("[nbb-recent] [%s] get_deposits échoué : %s", bce, exc)
                stats["errors"] += 1
                continue

            if not deposits:
                stats["no_deposits"] += 1
                continue

            for deposit_id, year, model_name in deposits:
                _process_deposit(
                    bce, bce_c, deposit_id, year, model_name,
                    tor, stats,
                    hdfs_csv_tpl=HDFS_BRONZE_NBB_CSVS,
                    hdfs_pdf_tpl=HDFS_BRONZE_NBB_PDFS,
                )
                time.sleep(CBSO_DELAY)

        _log_progress("[nbb-recent] FINAL", stats, n_companies, t0)
        stats["sec"] = round(time.time() - t0, 1)
        return stats

    @task(priority_weight=10)
    def report(stats: dict) -> None:
        from ingestion.mongo_client import count_state, get_db, COL_NBB_ACCOUNTS
        from ingestion.config import ANNEE_MIN_RECENT, ANNEE_MAX_RECENT
        n_acc = get_db()[COL_NBB_ACCOUNTS].count_documents({})
        log.info(
            "=== DAG 02a RAPPORT (années %d-%d) ===\n"
            "  Entreprises      : %d (sans dépôts: %d)\n"
            "  CSV → HDFS       : %d  (✗ %d)\n"
            "  PDF → HDFS       : %d  (✗ %d)\n"
            "  Déjà présents    : %d  |  Erreurs : %d\n"
            "  nbb_accounts     : %d docs MongoDB\n"
            "  State nbb_csv    : %d done\n"
            "  State nbb_pdf    : %d done\n"
            "  Durée            : %.0f s",
            ANNEE_MIN_RECENT, ANNEE_MAX_RECENT,
            stats["companies"], stats["no_deposits"],
            stats["csv_ok"], stats["csv_fail"],
            stats["pdf_ok"], stats["pdf_fail"],
            stats["already"], stats["errors"],
            n_acc,
            count_state("nbb_csv", "done"),
            count_state("nbb_pdf", "done"),
            stats["sec"],
        )

    n   = check_ready()
    res = ingest_recent(n)
    report(res)


dag_nbb_recent()


# ── Helpers partagés (réutilisés par dag_02b) ─────────────────────────────────

def _process_deposit(bce, bce_c, deposit_id, year, model_name,
                     tor, stats, hdfs_csv_tpl, hdfs_pdf_tpl):
    """Télécharge CSV + PDF d'un dépôt et met à jour la State DB + MongoDB."""
    from ingestion.mongo_client import is_done, mark_pending, mark_done, mark_error, upsert_nbb_account
    from ingestion.nbb_api import download_csv, download_pdf
    from ingestion.hdfs_utils import upload_bytes_retry

    # ── CSV ──────────────────────────────────────────────────────────────────
    if is_done(bce, "nbb_csv", deposit_id):
        stats["already"] += 1
    else:
        mark_pending(bce, "nbb_csv", deposit_id, year)
        csv_bytes = download_csv(deposit_id, tor)
        if csv_bytes:
            hdfs = f"{hdfs_csv_tpl.format(bce=bce_c)}/{deposit_id}.csv"
            try:
                sz = upload_bytes_retry(csv_bytes, hdfs)
                mark_done(bce, "nbb_csv", deposit_id, hdfs, sz)
                stats["csv_ok"] += 1
                _store_parsed(csv_bytes, bce, deposit_id, year, model_name)
            except Exception as exc:
                mark_error(bce, "nbb_csv", deposit_id, str(exc))
                stats["csv_fail"] += 1
                log.warning("[nbb] [%s] CSV upload %s : %s", bce, deposit_id, exc)
        else:
            mark_error(bce, "nbb_csv", deposit_id, "no_content")
            stats["csv_fail"] += 1

    # ── PDF ──────────────────────────────────────────────────────────────────
    # L'endpoint /deposits/pdf/ requiert un compte NBB (non open data → 403).
    # Désactivé par défaut via ENABLE_NBB_PDF = False dans config.py.
    from ingestion.config import ENABLE_NBB_PDF
    if ENABLE_NBB_PDF:
        if is_done(bce, "nbb_pdf", deposit_id):
            stats["already"] += 1
        else:
            mark_pending(bce, "nbb_pdf", deposit_id, year)
            pdf_bytes = download_pdf(deposit_id, tor)
            if pdf_bytes:
                hdfs = f"{hdfs_pdf_tpl.format(bce=bce_c)}/{deposit_id}.pdf"
                try:
                    sz = upload_bytes_retry(pdf_bytes, hdfs)
                    mark_done(bce, "nbb_pdf", deposit_id, hdfs, sz)
                    stats["pdf_ok"] += 1
                except Exception as exc:
                    mark_error(bce, "nbb_pdf", deposit_id, str(exc))
                    stats["pdf_fail"] += 1
            else:
                mark_error(bce, "nbb_pdf", deposit_id, "no_content_403")
                stats["pdf_fail"] += 1


def _store_parsed(csv_bytes, bce, deposit_id, year, model_name):
    """Parse le CSV NBB (paires code/valeur) et stocke dans MongoDB."""
    from ingestion.mongo_client import upsert_nbb_account
    try:
        text  = csv_bytes.decode("utf-8", errors="replace")
        codes: dict[str, float] = {}
        model_code = lang = ""
        for row in csv.reader(io.StringIO(text)):
            if len(row) != 2:
                continue
            k, v = row[0].strip('"'), row[1].strip('"')
            if k == "Model code":
                model_code = v
            elif k == "Language":
                lang = v
            elif PATTERN_CODE.match(k):
                try:
                    codes[k] = float(v)
                except ValueError:
                    pass
        if codes:
            upsert_nbb_account(bce, deposit_id, year,
                               model_code or model_name, lang, codes)
    except Exception as exc:
        log.warning("[nbb] [%s] parsing CSV %s : %s", bce, deposit_id, exc)


def _log_progress(tag, stats, total, t0):
    log.info(
        "%s %d/%d | CSV↓=%d(✗%d) | PDF↓=%d(✗%d) | skip=%d | err=%d | %.0f s",
        tag, stats["companies"], total,
        stats["csv_ok"], stats["csv_fail"],
        stats["pdf_ok"], stats["pdf_fail"],
        stats["already"], stats["errors"],
        time.time() - t0,
    )
