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

N_SHARDS = 4   # tâches Airflow parallèles

# NACE codes hôtellerie retenus (activité MAIN uniquement)
NACE_HEBERGEMENT = {"55100", "55201", "55202", "55203", "55204", "55209",
                    "55300", "55400", "55900"}

# Formes juridiques exclues (secteur public)
EXCLUDED_FORMS = {
    "110", "114", "116", "117",                                          # entités publiques
    "301", "302", "303",                                                  # services fédéraux
    "310", "320", "330", "340", "350",                                    # autorités régionales
    "400", "411", "412", "413", "414", "415",                            # communes / CPAS
    "416", "417", "418", "419", "420",                                    # intercommunales
}


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
            HDFS_SILVER_NBB_CSVS, HDFS_SILVER_NBB_PDFS,
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

        import socket
        socket.setdefaulttimeout(20)   # timeout global — évite le gel PySocks/SOCKS5

        tag = f"[nbb-s{shard_idx}]"
        log.info("=== %s démarrage — hébergements NACE 55.xx (sans formes sociales) ===", tag)
        t0  = time.time()
        tor = TorSession(headers=NBB_HEADERS)

        from ingestion.mongo_client import get_state_db, get_db
        db_ref    = get_db()
        state_col = get_state_db()["nbb_shard_cursor"]
        cursor_doc = state_col.find_one({"_id": f"shard_{shard_idx}"})

        # Reset curseur si scope précédent ≠ hébergement
        if cursor_doc and cursor_doc.get("scope") != "hebergement":
            state_col.delete_one({"_id": f"shard_{shard_idx}"})
            cursor_doc = None
        last_bce = cursor_doc["last_bce"] if cursor_doc else None

        # ── Liste cible : hôtellerie NACE MAIN, personne morale privée, hors public ─
        all_heberg = set(
            db_ref["kbo_activities"].distinct(
                "entity_number",
                {"nace_code": {"$in": list(NACE_HEBERGEMENT)}, "classification": "MAIN"},
            )
        )
        heberg_ids = sorted(
            doc["_id"]
            for doc in db_ref["kbo_enterprises"].find(
                {
                    "_id":              {"$in": list(all_heberg)},
                    "Status":           "AC",
                    "TypeOfEnterprise": "2",                        # personne morale privée
                    "JuridicalForm":    {"$nin": list(EXCLUDED_FORMS)},
                },
                {"_id": 1},
            )
        )

        # ── Chunk de ce shard ─────────────────────────────────────────────────────
        n_total    = len(heberg_ids)
        chunk_size = (n_total + N_SHARDS - 1) // N_SHARDS
        shard_ids  = heberg_ids[shard_idx * chunk_size : (shard_idx + 1) * chunk_size]
        if last_bce:
            shard_ids = [eid for eid in shard_ids if eid > last_bce]
            log.info("%s reprise depuis BCE > %s", tag, last_bce)

        n_shard = len(shard_ids)
        log.info("%s %d entreprises hébergement (shard %d/%d, total=%d)",
                 tag, n_shard, shard_idx + 1, N_SHARDS, n_total)

        stats = dict(companies=0, no_deposits=0, already=0,
                     csv_ok=0, csv_fail=0, pdf_ok=0, pdf_fail=0, errors=0)
        limit = _LIMIT
        last_processed_bce = last_bce

        for bce in shard_ids:
            if limit is not None and stats["companies"] >= limit:
                break

            bce_c = bce.replace(".", "")
            last_processed_bce = bce
            stats["companies"] += 1

            if stats["companies"] % 100 == 0:
                # Sauvegarde du curseur tous les 100 pour reprise rapide en cas de crash
                state_col.update_one(
                    {"_id": f"shard_{shard_idx}"},
                    {"$set": {"last_bce": last_processed_bce, "companies_done": stats["companies"],
                              "scope": "hebergement"}},
                    upsert=True,
                )
            if stats["companies"] % BATCH_LOG_EVERY == 0:
                _log_progress(tag, stats, n_shard, t0)

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
                    hdfs_csv_tpl=HDFS_SILVER_NBB_CSVS,
                    hdfs_pdf_tpl=HDFS_SILVER_NBB_PDFS,
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
                     for i in range(N_SHARDS)]

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

    # ── CSV via TorSession (rotation IP anti-429) ────────────────────────────────
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
            # Pas de CSV (dépôt PDF-only) — on marque done pour ne pas retenter
            mark_done(bce, "nbb_csv", deposit_id, "no_csv", 0)
            stats["csv_fail"] += 1

    # ── PDF via TorSession (rotation IP anti-429) ────────────────────────────────
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
