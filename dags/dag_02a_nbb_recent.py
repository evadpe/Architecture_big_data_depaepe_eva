"""
DAG 02a — NBB Comptes annuels RÉCENTS → Bronze HDFS + MongoDB  [PRIORITAIRE]

Fenêtre fiscale : 2020 → 2025
Traitement parallèle : les entreprises sont réparties en N shards par préfixe BCE.
Chaque shard tourne dans un task Airflow indépendant → N× plus rapide.

Priorité Airflow :
  • priority_weight = 10
  • schedule @daily
  • max_active_runs = 2
"""
import csv
import io
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_LIMIT: int | None = None   # production — toutes les entreprises

PATTERN_CODE = re.compile(r"^\d+(/\d+)?[A-Z]*P?$")

# Shards par plage de valeurs _id (BCE formaté "XXXX.XXX.XXX")
# Distribution réelle : tous en 0... (1.62M) et 1... (330K) → 0 après 2...
# Bornes calculées pour ~487K entreprises par shard
_SHARDS = [
    {"min": None,             "max": "0643.895.502"},  # shard 0 : ~487K
    {"min": "0643.895.502",   "max": "0770.981.536"},  # shard 1 : ~487K
    {"min": "0770.981.536",   "max": "0865.018.878"},  # shard 2 : ~487K
    {"min": "0865.018.878",   "max": None},             # shard 3 : ~487K
]


@dag(
    dag_id="dag_02a_nbb_recent",
    description="NBB 2020-2025 → HDFS + MongoDB [PRIORITAIRE, 4 shards parallèles]",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
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
        log.info("[nbb-recent] %d entreprises actives", n)
        return n

    @task(priority_weight=10, execution_timeout=None)
    def ingest_shard(shard_idx: int, n_companies: int) -> dict:
        """Traite un shard (sous-ensemble de préfixes BCE) en parallèle des autres."""
        from ingestion.config import (
            HDFS_BRONZE_NBB_CSVS, HDFS_BRONZE_NBB_PDFS,
            BATCH_LOG_EVERY, CBSO_DELAY,
            ANNEE_MIN_RECENT, ANNEE_MAX_RECENT,
            NBB_HEADERS,
        )
        from ingestion.mongo_client import (
            col, COL_ENTERPRISES,
            is_done, mark_pending, mark_done, mark_error,
            upsert_nbb_account,
        )
        from ingestion.nbb_api import iter_deposits_to_ingest, download_csv, download_pdf
        from ingestion.hdfs_utils import upload_bytes_retry
        from ingestion.tor_session import TorSession

        shard_range = _SHARDS[shard_idx]
        tag = f"[nbb-s{shard_idx} {shard_range['min'] or '0'}-{shard_range['max'] or 'fin'}]"

        log.info("=== %s démarrage — plage BCE: %s → %s ===", tag,
                 shard_range["min"] or "début", shard_range["max"] or "fin")
        t0  = time.time()
        tor = TorSession(headers=NBB_HEADERS)

        # Reprise : continuer depuis le dernier BCE traité si le task a été interrompu
        from ingestion.mongo_client import get_state_db
        state_col = get_state_db()["nbb_shard_cursor"]
        cursor_doc = state_col.find_one({"_id": f"shard_{shard_idx}"})
        last_bce = cursor_doc["last_bce"] if cursor_doc else None

        # ── Filtre MongoDB sur _id (indexé) ──────────────────────────────────────
        id_filter: dict = {}
        if last_bce:
            id_filter["$gt"] = last_bce
            log.info("%s reprise depuis BCE > %s", tag, last_bce)
        elif shard_range["min"] is not None:
            id_filter["$gte"] = shard_range["min"]
        if shard_range["max"] is not None:
            id_filter["$lt"] = shard_range["max"]

        query = {"Status": "AC"}
        if id_filter:
            query["_id"] = id_filter

        n_shard = col(COL_ENTERPRISES).count_documents(query)
        log.info("%s %d entreprises dans ce shard", tag, n_shard)

        stats = dict(companies=0, no_deposits=0, already=0,
                     csv_ok=0, csv_fail=0, pdf_ok=0, pdf_fail=0, errors=0)
        limit = _LIMIT
        last_processed_bce = last_bce

        for company in col(COL_ENTERPRISES).find(query, no_cursor_timeout=True).sort("_id", 1):
            if limit is not None and stats["companies"] >= limit:
                break

            bce   = company["_id"]   # _id == bce_num
            bce_c = bce.replace(".", "")
            last_processed_bce = bce
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                _log_progress(tag, stats, n_shard, t0)
                # Sauvegarde du curseur tous les 1000 pour reprise en cas de crash
                state_col.update_one(
                    {"_id": f"shard_{shard_idx}"},
                    {"$set": {"last_bce": last_processed_bce, "companies_done": stats["companies"]}},
                    upsert=True,
                )

            time.sleep(CBSO_DELAY)   # délai avant chaque appel listing (avec ou sans dépôt)
            try:
                deposits = list(iter_deposits_to_ingest(
                    bce_c, session=tor,
                    annee_min=ANNEE_MIN_RECENT,
                    annee_max=ANNEE_MAX_RECENT,
                ))
            except Exception as exc:
                log.warning("%s [%s] get_deposits : %s", tag, bce, exc)
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

        # Sauvegarder le curseur pour reprendre au prochain run
        if last_processed_bce:
            state_col.update_one(
                {"_id": f"shard_{shard_idx}"},
                {"$set": {"last_bce": last_processed_bce, "companies_done": stats["companies"]}},
                upsert=True,
            )
            log.info("%s curseur sauvegardé : last_bce=%s", tag, last_processed_bce)

        _log_progress(f"{tag} FINAL", stats, n_shard, t0)
        stats["shard"] = shard_idx
        stats["sec"]   = round(time.time() - t0, 1)
        return stats

    @task(priority_weight=10)
    def report(all_stats: list[dict]) -> None:
        from ingestion.mongo_client import count_state, get_db, COL_NBB_ACCOUNTS
        from ingestion.config import ANNEE_MIN_RECENT, ANNEE_MAX_RECENT
        total_csv = sum(s["csv_ok"]  for s in all_stats)
        total_pdf = sum(s["pdf_ok"]  for s in all_stats)
        total_ent = sum(s["companies"] for s in all_stats)
        n_acc = get_db()[COL_NBB_ACCOUNTS].count_documents({})
        log.info(
            "=== DAG 02a RAPPORT (années %d-%d) ===\n"
            "  Entreprises traitées : %d (4 shards parallèles)\n"
            "  CSV → HDFS           : %d\n"
            "  PDF → HDFS           : %d\n"
            "  nbb_accounts MongoDB : %d\n"
            "  State nbb_csv done   : %d\n"
            "  State nbb_pdf done   : %d",
            ANNEE_MIN_RECENT, ANNEE_MAX_RECENT,
            total_ent, total_csv, total_pdf, n_acc,
            count_state("nbb_csv", "done"),
            count_state("nbb_pdf", "done"),
        )
        for s in all_stats:
            log.info("  Shard %d : CSV↓=%d PDF↓=%d err=%d %.0fs",
                     s["shard"], s["csv_ok"], s["pdf_ok"], s["errors"], s["sec"])

    # ── Pipeline : 4 shards en parallèle ──────────────────────────────────────
    n = check_ready()

    shard_results = [ingest_shard.override(task_id=f"ingest_shard_{i}")(i, n)
                     for i in range(len(_SHARDS))]

    report(shard_results)


dag_nbb_recent()


# ── Helpers partagés ──────────────────────────────────────────────────────────

_pdf_session_direct = None


def _pdf_direct_session():
    global _pdf_session_direct
    if _pdf_session_direct is None:
        import requests as _req
        from ingestion.config import NBB_HEADERS
        _pdf_session_direct = _req.Session()
        _pdf_session_direct.headers.update(NBB_HEADERS)
    return _pdf_session_direct


def _process_deposit(bce, bce_c, deposit_id, year, model_name,
                     tor, stats, hdfs_csv_tpl, hdfs_pdf_tpl):
    from ingestion.mongo_client import is_done, mark_pending, mark_done, mark_error
    from ingestion.nbb_api import download_csv, download_pdf
    from ingestion.hdfs_utils import upload_bytes_retry

    # ── CSV (connexion directe — Tor bloqué sur /deposits/consult/csv/) ─────────
    if is_done(bce, "nbb_csv", deposit_id):
        stats["already"] += 1
    else:
        mark_pending(bce, "nbb_csv", deposit_id, year)
        csv_bytes = download_csv(deposit_id, _pdf_direct_session())
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
        else:
            # Pas de CSV (dépôt PDF-only) — on marque done pour ne pas retenter
            mark_done(bce, "nbb_csv", deposit_id, "no_csv", 0)
            stats["csv_fail"] += 1

    # ── PDF (connexion directe — Tor bloqué sur /deposits/pdf/) ───────────────
    from ingestion.config import ENABLE_NBB_PDF, PDF_USE_TOR
    if ENABLE_NBB_PDF:
        if is_done(bce, "nbb_pdf", deposit_id):
            stats["already"] += 1
        else:
            mark_pending(bce, "nbb_pdf", deposit_id, year)
            pdf_session = tor if PDF_USE_TOR else _pdf_direct_session()
            pdf_bytes = download_pdf(deposit_id, pdf_session)
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
                mark_error(bce, "nbb_pdf", deposit_id, "no_content")
                stats["pdf_fail"] += 1


def _store_parsed(csv_bytes, bce, deposit_id, year, model_name):
    from ingestion.mongo_client import upsert_nbb_account
    try:
        text = csv_bytes.decode("utf-8", errors="replace")
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
