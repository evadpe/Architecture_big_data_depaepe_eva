"""
DAG 03 — Statuts Notaire (STRAPOR) → HDFS Bronze + MongoDB

  • HDFS   : PDFs → /bronze/strapor/{bce}/{deed_date}_{doc_id}.pdf
  • MongoDB: métadonnées → collection strapor_statutes (bce_db)
  • State DB (bce_state_db) : delta detection

Requêtes via pool Tor. Cookies anti-F5 gérés sur l'hôte (voir notaire_cookies.json).
"""
import logging
import time
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_LIMIT: int | None = None   # production — toutes les entreprises


@dag(
    dag_id="dag_03_strapor",
    description="Statuts Notaire (STRAPOR) → HDFS + MongoDB + State DB",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bce", "strapor", "notaire", "bronze", "layer-1"],
    default_args={"retries": 1, "retry_delay": 600},
)
def dag_strapor():

    @task
    def refresh_session() -> str:
        from ingestion.strapor_api import get_session, _session_valid
        log.info("=== TASK: refresh_session ===")
        sess = get_session()
        if not _session_valid(sess):
            raise RuntimeError(
                "Session Strapor invalide. "
                "Exécuter `python strapor.py` sur l'hôte pour régénérer notaire_cookies.json."
            )
        log.info("[strapor] Session valide")
        return "ok"

    @task(execution_timeout=None)
    def ingest_strapor(session_status: str) -> dict:
        from ingestion.config import HDFS_BRONZE_STRAPOR, BATCH_LOG_EVERY
        from ingestion.mongo_client import (
            iter_active_companies, is_done, mark_pending, mark_done, mark_error,
            upsert_strapor_statute,
        )
        from ingestion.strapor_api import (
            get_session, needs_notaire_check, get_statutes, download_statute_bytes,
        )
        from ingestion.hdfs_utils import upload_bytes_retry

        log.info("=== TASK: ingest_strapor ===")
        t0   = time.time()
        sess = get_session()

        stats = dict(companies=0, skipped=0, already=0, ok=0, fail=0, errors=0)
        limit = _LIMIT

        for company in iter_active_companies():
            if limit is not None and stats["companies"] >= limit:
                break
            bce   = company["bce_num"]
            bce_c = company.get("bce_num_clean", bce.replace(".", ""))
            form  = company.get("JuridicalForm", "")
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                log.info("[strapor] %d | PDF↓=%d(✗%d) | skip=%d | already=%d | %.0f s",
                         stats["companies"], stats["ok"], stats["fail"],
                         stats["skipped"], stats["already"], time.time() - t0)

            if not needs_notaire_check(form):
                stats["skipped"] += 1
                continue

            try:
                for statute in get_statutes(sess, bce_c):
                    doc_id    = statute["documentId"]
                    deed_date = statute.get("deedDate", "unknown").replace("-", "")
                    title     = statute.get("documentTitle", "")

                    if is_done(bce, "strapor_pdf", doc_id):
                        stats["already"] += 1
                        continue

                    mark_pending(bce, "strapor_pdf", doc_id)
                    pdf_bytes, _, _ = download_statute_bytes(sess, bce_c, statute)

                    if pdf_bytes is None:
                        mark_error(bce, "strapor_pdf", doc_id, "no_content")
                        upsert_strapor_statute(bce, doc_id, deed_date, title, None)
                        stats["fail"] += 1
                        continue

                    filename  = f"{deed_date}_{doc_id}.pdf"
                    hdfs_path = f"{HDFS_BRONZE_STRAPOR.format(bce=bce_c)}/{filename}"
                    try:
                        sz = upload_bytes_retry(pdf_bytes, hdfs_path)
                        mark_done(bce, "strapor_pdf", doc_id, hdfs_path, sz)
                        upsert_strapor_statute(bce, doc_id, deed_date, title, hdfs_path)
                        stats["ok"] += 1
                        log.debug("[strapor] [%s] %s → HDFS (%d o)", bce, filename, sz)
                    except Exception as exc:
                        mark_error(bce, "strapor_pdf", doc_id, str(exc))
                        stats["fail"] += 1
                        log.warning("[strapor] [%s] upload %s échoué : %s", bce, doc_id, exc)

            except Exception as exc:
                log.error("[strapor] [%s] exception : %s", bce, exc, exc_info=True)
                stats["errors"] += 1

        stats["sec"] = round(time.time() - t0, 1)
        log.info("=== ingest_strapor TERMINÉ : %s ===", stats)
        return stats

    @task
    def report(stats: dict) -> None:
        from ingestion.mongo_client import count_state, get_db, COL_STRAPOR_STATUTES
        n_docs = get_db()[COL_STRAPOR_STATUTES].count_documents({})
        log.info(
            "=== DAG 03 RAPPORT ===\n"
            "  Entreprises       : %d (skip %d)\n"
            "  PDF → HDFS        : %d (✗ %d)\n"
            "  Déjà présents     : %d  |  Erreurs : %d\n"
            "  strapor_statutes  : %d docs MongoDB\n"
            "  State done        : %d  |  Durée : %.0f s",
            stats["companies"], stats["skipped"],
            stats["ok"], stats["fail"],
            stats["already"], stats["errors"],
            n_docs,
            count_state("strapor_pdf", "done"),
            stats["sec"],
        )

    sess = refresh_session()
    res  = ingest_strapor(sess)
    report(res)


dag_strapor()
