"""
DAG 04 — Publications eJustice → HDFS Bronze + MongoDB

  • HDFS   : PDFs → /bronze/ejustice/{bce}/{numac}.pdf
  • MongoDB: métadonnées → collection ejustice_publications (bce_db)
  • State DB (bce_state_db) : delta detection

Toutes les requêtes passent par le pool Tor.
"""
import logging
import time
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_LIMIT: int | None = 1_000  # None = run complet de production


@dag(
    dag_id="dag_04_ejustice",
    description="Publications eJustice → HDFS + MongoDB + State DB",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bce", "ejustice", "bronze", "layer-1"],
    default_args={"retries": 1, "retry_delay": 300},
)
def dag_ejustice():

    @task
    def check_ready() -> int:
        from ingestion.mongo_client import col, COL_ENTERPRISES
        n = col(COL_ENTERPRISES).count_documents({"Status": "AC"})
        if n == 0:
            raise RuntimeError("MongoDB vide — lancer dag_01 d'abord")
        log.info("[ejustice] %d entreprises actives", n)
        return n

    @task(execution_timeout=None)
    def ingest_ejustice(n_companies: int) -> dict:
        from ingestion.config import HDFS_BRONZE_EJUSTICE, BATCH_LOG_EVERY
        from ingestion.mongo_client import (
            iter_active_companies, is_done, mark_pending, mark_done, mark_error,
            upsert_ejustice_pub,
        )
        from ingestion.ejustice_api import get_publications, download_publication_pdf
        from ingestion.hdfs_utils import upload_bytes_retry
        from ingestion.tor_session import TorSession
        from ingestion.config import NBB_HEADERS

        log.info("=== TASK: ingest_ejustice — %d entreprises ===", n_companies)
        t0  = time.time()
        tor = TorSession(headers=NBB_HEADERS)

        stats = dict(companies=0, already=0, ok=0, no_link=0, fail=0, errors=0)
        limit = _LIMIT

        for company in iter_active_companies():
            if limit is not None and stats["companies"] >= limit:
                break
            bce   = company["bce_num"]
            bce_c = company.get("bce_num_clean", bce.replace(".", ""))
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                log.info("[ejustice] %d/%d | PDF↓=%d(✗%d) | skip=%d | nolink=%d | %.0f s",
                         stats["companies"], n_companies,
                         stats["ok"], stats["fail"],
                         stats["already"], stats["no_link"], time.time() - t0)

            try:
                for pub in get_publications(bce_c, session=tor):
                    numac    = pub["numac"]
                    lien     = pub.get("lien_pdf")
                    pub_date = pub.get("date", "")
                    pub_type = pub.get("type", "")

                    if is_done(bce, "ejustice_pdf", numac):
                        stats["already"] += 1
                        continue

                    mark_pending(bce, "ejustice_pdf", numac)

                    if not lien:
                        upsert_ejustice_pub(bce, numac, pub_date, pub_type, None)
                        stats["no_link"] += 1
                        continue

                    content = download_publication_pdf(lien, session=tor)
                    if content is None:
                        mark_error(bce, "ejustice_pdf", numac, "download_failed")
                        upsert_ejustice_pub(bce, numac, pub_date, pub_type, None)
                        stats["fail"] += 1
                        continue

                    hdfs_path = f"{HDFS_BRONZE_EJUSTICE.format(bce=bce_c)}/{numac}.pdf"
                    try:
                        sz = upload_bytes_retry(content, hdfs_path)
                        mark_done(bce, "ejustice_pdf", numac, hdfs_path, sz)
                        upsert_ejustice_pub(bce, numac, pub_date, pub_type, hdfs_path)
                        stats["ok"] += 1
                        log.debug("[ejustice] [%s] %s → HDFS (%d o)", bce, numac, sz)
                    except Exception as exc:
                        mark_error(bce, "ejustice_pdf", numac, str(exc))
                        stats["fail"] += 1
                        log.warning("[ejustice] [%s] upload %s échoué : %s", bce, numac, exc)

            except Exception as exc:
                log.error("[ejustice] [%s] : %s", bce, exc, exc_info=True)
                stats["errors"] += 1

        stats["sec"] = round(time.time() - t0, 1)
        log.info("=== ingest_ejustice TERMINÉ : %s ===", stats)
        return stats

    @task
    def report(stats: dict) -> None:
        from ingestion.mongo_client import count_state, get_db, COL_EJUSTICE_PUBS
        n_docs = get_db()[COL_EJUSTICE_PUBS].count_documents({})
        log.info(
            "=== DAG 04 RAPPORT ===\n"
            "  Entreprises          : %d\n"
            "  PDF → HDFS           : %d (✗ %d)\n"
            "  Sans lien PDF        : %d  |  Déjà présents : %d\n"
            "  ejustice_publications: %d docs MongoDB\n"
            "  State done           : %d  |  Erreurs : %d  |  Durée : %.0f s",
            stats["companies"],
            stats["ok"], stats["fail"],
            stats["no_link"], stats["already"],
            n_docs,
            count_state("ejustice_pdf", "done"),
            stats["errors"], stats["sec"],
        )

    n   = check_ready()
    res = ingest_ejustice(n)
    report(res)


dag_ejustice()
