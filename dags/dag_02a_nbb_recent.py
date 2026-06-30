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

# Shards par premier chiffre du numéro BCE (0-9)
# → 4 groupes traités en parallèle par 4 tasks Airflow
_SHARDS = [
    ["0", "1", "2"],   # shard 0
    ["3", "4", "5"],   # shard 1
    ["6", "7"],        # shard 2
    ["8", "9"],        # shard 3
]


@dag(
    dag_id="dag_02a_nbb_recent",
    description="NBB 2020-2025 → HDFS + MongoDB [PRIORITAIRE, 4 shards parallèles]",
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

        prefixes = _SHARDS[shard_idx]
        tag = f"[nbb-s{shard_idx} {'/'.join(prefixes)}]"

        log.info("=== %s démarrage — préfixes BCE: %s ===", tag, prefixes)
        t0  = time.time()
        tor = TorSession(headers=NBB_HEADERS)

        # Filtre MongoDB par préfixe du numéro BCE
        prefix_regex = "^(" + "|".join(prefixes) + ")"
        query = {"Status": "AC", "bce_num": {"$regex": prefix_regex}}
        n_shard = col(COL_ENTERPRISES).count_documents(query)
        log.info("%s %d entreprises dans ce shard", tag, n_shard)

        stats = dict(companies=0, no_deposits=0, already=0,
                     csv_ok=0, csv_fail=0, pdf_ok=0, pdf_fail=0, errors=0)
        limit = _LIMIT

        for company in col(COL_ENTERPRISES).find(query, batch_size=500):
            if limit is not None and stats["companies"] >= limit:
                break

            bce   = company["bce_num"]
            bce_c = company.get("bce_num_clean", bce.replace(".", ""))
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                _log_progress(tag, stats, n_shard, t0)

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

    # ── CSV ───────────────────────────────────────────────────────────────────
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
        else:
            mark_error(bce, "nbb_csv", deposit_id, "no_content")
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
