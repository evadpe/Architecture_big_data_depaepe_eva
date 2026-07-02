"""
DAG 04b — Publications eJustice → HDFS Silver  [Hébergement uniquement]

Périmètre : entreprises NACE 55.xxx (MAIN), personne morale privée (TypeOfEnterprise=2),
            statut AC, hors formes juridiques publiques.

  • HDFS   : PDFs → /silver/ejustice/hebergement/{bce}/{numac}.pdf
  • MongoDB: métadonnées → collection ejustice_publications (bce_db)
  • State DB (bce_state_db) : delta detection
"""
import logging
import time
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

NACE_HEBERGEMENT = {
    "55100", "55201", "55202", "55203", "55204", "55209",
    "55300", "55400", "55900",
}
EXCLUDED_FORMS = {
    "110", "114", "116", "117",
    "301", "302", "303",
    "310", "320", "330", "340", "350",
    "400", "411", "412", "413", "414", "415", "416", "417", "418", "419", "420",
}


def _get_hebergement_ids(db) -> list[str]:
    """Retourne les bce_num des entreprises hébergement éligibles, triés."""
    all_heberg = set(
        db["kbo_activities"].distinct(
            "entity_number",
            {"nace_code": {"$in": list(NACE_HEBERGEMENT)}, "classification": "MAIN"},
        )
    )
    return sorted(
        doc["_id"]
        for doc in db["kbo_enterprises"].find(
            {
                "_id":              {"$in": list(all_heberg)},
                "Status":           "AC",
                "TypeOfEnterprise": "2",
                "JuridicalForm":    {"$nin": list(EXCLUDED_FORMS)},
            },
            {"_id": 1},
        )
    )


@dag(
    dag_id="dag_04b_ejustice_hebergement",
    description="Publications eJustice hébergement → HDFS Silver",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bce", "ejustice", "silver", "hebergement", "layer-2"],
    default_args={"retries": 1, "retry_delay": 300},
)
def dag_ejustice_hebergement():

    @task
    def check_ready() -> int:
        from ingestion.mongo_client import get_db
        db = get_db()
        ids = _get_hebergement_ids(db)
        n = len(ids)
        if n == 0:
            raise RuntimeError("Aucune entreprise hébergement trouvée — vérifier dag_01")
        log.info("[ejustice-heberg] %d entreprises hébergement", n)
        return n

    @task(execution_timeout=None)
    def ingest_ejustice(n_companies: int) -> dict:
        import socket
        socket.setdefaulttimeout(20)

        from ingestion.config import HDFS_SILVER_EJUSTICE_HEBERG, BATCH_LOG_EVERY
        from ingestion.mongo_client import (
            get_db, is_done, mark_pending, mark_done, mark_error,
            upsert_ejustice_pub,
        )
        from ingestion.ejustice_api import get_publications, download_publication_pdf
        from ingestion.hdfs_utils import upload_bytes_retry
        from ingestion.tor_session import TorSession
        from ingestion.config import NBB_HEADERS

        db  = get_db()
        tor = TorSession(headers=NBB_HEADERS)
        t0  = time.time()

        heberg_ids = _get_hebergement_ids(db)
        log.info("[ejustice-heberg] %d entreprises hébergement à traiter", len(heberg_ids))

        stats = dict(companies=0, already=0, ok=0, no_link=0, fail=0, errors=0)

        for bce in heberg_ids:
            bce_c = bce.replace(".", "")
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                log.info("[ejustice-heberg] %d/%d | PDF↓=%d(✗%d) | already=%d | nolink=%d | %.0f s",
                         stats["companies"], len(heberg_ids),
                         stats["ok"], stats["fail"],
                         stats["already"], stats["no_link"], time.time() - t0)

            try:
                for pub in get_publications(bce_c, session=tor):
                    numac    = pub["numac"]
                    lien     = pub.get("lien_pdf")
                    pub_date = pub.get("date", "")
                    pub_type = pub.get("type", "")

                    if is_done(bce, "ejustice_pdf_heberg", numac):
                        stats["already"] += 1
                        continue

                    mark_pending(bce, "ejustice_pdf_heberg", numac)

                    if not lien:
                        upsert_ejustice_pub(bce, numac, pub_date, pub_type, None)
                        stats["no_link"] += 1
                        continue

                    content = download_publication_pdf(lien, session=tor)
                    if content is None:
                        mark_error(bce, "ejustice_pdf_heberg", numac, "download_failed")
                        upsert_ejustice_pub(bce, numac, pub_date, pub_type, None)
                        stats["fail"] += 1
                        continue

                    hdfs_path = f"{HDFS_SILVER_EJUSTICE_HEBERG.format(bce=bce_c)}/{numac}.pdf"
                    try:
                        sz = upload_bytes_retry(content, hdfs_path)
                        mark_done(bce, "ejustice_pdf_heberg", numac, hdfs_path, sz)
                        upsert_ejustice_pub(bce, numac, pub_date, pub_type, hdfs_path)
                        stats["ok"] += 1
                    except Exception as exc:
                        mark_error(bce, "ejustice_pdf_heberg", numac, str(exc))
                        stats["fail"] += 1
                        log.warning("[ejustice-heberg] [%s] upload %s échoué : %s", bce, numac, exc)

            except Exception as exc:
                log.error("[ejustice-heberg] [%s] : %s", bce, exc, exc_info=True)
                stats["errors"] += 1

        stats["sec"] = round(time.time() - t0, 1)
        log.info("=== ejustice-heberg TERMINÉ : %s ===", stats)
        return stats

    @task
    def report(stats: dict) -> None:
        from ingestion.mongo_client import count_state, get_db, COL_EJUSTICE_PUBS
        n_docs = get_db()[COL_EJUSTICE_PUBS].count_documents({})
        log.info(
            "=== DAG 04b RAPPORT ===\n"
            "  Entreprises hébergement  : %d\n"
            "  PDF → HDFS Silver        : %d (✗ %d)\n"
            "  Sans lien PDF : %d  |  Déjà présents : %d\n"
            "  ejustice_publications    : %d docs MongoDB\n"
            "  State done : %d  |  Erreurs : %d  |  Durée : %.0f s",
            stats["companies"],
            stats["ok"], stats["fail"],
            stats["no_link"], stats["already"],
            n_docs,
            count_state("ejustice_pdf_heberg", "done"),
            stats["errors"], stats["sec"],
        )

    n   = check_ready()
    res = ingest_ejustice(n)
    report(res)


dag_ejustice_hebergement()
