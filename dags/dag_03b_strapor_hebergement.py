"""
DAG 03b — Statuts Notaire (STRAPOR) → HDFS Silver  [Hébergement uniquement]

Périmètre : entreprises NACE 55.xxx (MAIN), personne morale privée (TypeOfEnterprise=2),
            statut AC, hors formes juridiques publiques.

  • HDFS   : PDFs → /silver/strapor/hebergement/{bce}/{deed_date}_{doc_id}.pdf
  • MongoDB: métadonnées → collection strapor_statutes (bce_db)
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
    dag_id="dag_03b_strapor_hebergement",
    description="Statuts Notaire (STRAPOR) hébergement → HDFS Silver",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bce", "strapor", "notaire", "silver", "hebergement", "layer-2"],
    default_args={"retries": 1, "retry_delay": 600},
)
def dag_strapor_hebergement():

    @task
    def setup_playwright() -> str:
        """Installe les binaires Chromium si absents (première exécution)."""
        import subprocess, sys, os
        browsers_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/airflow/pw-browsers")
        chromium_dir  = os.path.join(browsers_path, "chromium-*")
        import glob
        if glob.glob(chromium_dir):
            log.info("[strapor] Chromium déjà installé dans %s", browsers_path)
            return "cached"
        log.info("[strapor] Installation Chromium headless…")
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            log.warning("[strapor] playwright install stderr: %s", result.stderr[-500:])
            # Tentative sans --with-deps (si droits insuffisants pour les paquets système)
            result2 = subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                capture_output=True, text=True,
            )
            if result2.returncode != 0:
                raise RuntimeError(f"playwright install échoué : {result2.stderr[-300:]}")
        log.info("[strapor] Chromium installé")
        return "installed"

    @task
    def refresh_session(setup_status: str) -> str:
        from ingestion.strapor_api import get_session, _session_valid
        sess = get_session()
        if not _session_valid(sess):
            raise RuntimeError("Session Strapor invalide même après Playwright.")
        log.info("[strapor-heberg] Session valide")
        return "ok"

    @task(execution_timeout=None)
    def ingest_strapor(session_status: str) -> dict:
        import socket
        socket.setdefaulttimeout(20)

        from ingestion.config import HDFS_SILVER_STRAPOR_HEBERG, BATCH_LOG_EVERY
        from ingestion.mongo_client import (
            get_db, is_done, mark_pending, mark_done, mark_error,
            upsert_strapor_statute,
        )
        from ingestion.strapor_api import (
            get_session, needs_notaire_check, get_statutes, download_statute_bytes,
        )
        from ingestion.hdfs_utils import upload_bytes_retry

        db   = get_db()
        sess = get_session()
        t0   = time.time()

        heberg_ids = _get_hebergement_ids(db)
        log.info("[strapor-heberg] %d entreprises hébergement", len(heberg_ids))

        stats = dict(companies=0, skipped=0, already=0, ok=0, fail=0, errors=0)

        for bce in heberg_ids:
            bce_c = bce.replace(".", "")
            form  = db["kbo_enterprises"].find_one({"_id": bce}, {"JuridicalForm": 1}) or {}
            form  = form.get("JuridicalForm", "")
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                log.info("[strapor-heberg] %d/%d | PDF↓=%d(✗%d) | skip=%d | already=%d | %.0f s",
                         stats["companies"], len(heberg_ids),
                         stats["ok"], stats["fail"],
                         stats["skipped"], stats["already"], time.time() - t0)

            if not needs_notaire_check(form):
                stats["skipped"] += 1
                continue

            try:
                for statute in get_statutes(sess, bce_c):
                    doc_id    = statute["documentId"]
                    deed_date = statute.get("deedDate", "unknown").replace("-", "")
                    title     = statute.get("documentTitle", "")

                    if is_done(bce, "strapor_pdf_heberg", doc_id):
                        stats["already"] += 1
                        continue

                    mark_pending(bce, "strapor_pdf_heberg", doc_id)
                    pdf_bytes, _, _ = download_statute_bytes(sess, bce_c, statute)

                    if pdf_bytes is None:
                        mark_error(bce, "strapor_pdf_heberg", doc_id, "no_content")
                        upsert_strapor_statute(bce, doc_id, deed_date, title, None)
                        stats["fail"] += 1
                        continue

                    filename  = f"{deed_date}_{doc_id}.pdf"
                    hdfs_path = f"{HDFS_SILVER_STRAPOR_HEBERG.format(bce=bce_c)}/{filename}"
                    try:
                        sz = upload_bytes_retry(pdf_bytes, hdfs_path)
                        mark_done(bce, "strapor_pdf_heberg", doc_id, hdfs_path, sz)
                        upsert_strapor_statute(bce, doc_id, deed_date, title, hdfs_path)
                        stats["ok"] += 1
                    except Exception as exc:
                        mark_error(bce, "strapor_pdf_heberg", doc_id, str(exc))
                        stats["fail"] += 1
                        log.warning("[strapor-heberg] [%s] upload %s échoué : %s", bce, doc_id, exc)

            except Exception as exc:
                log.error("[strapor-heberg] [%s] exception : %s", bce, exc, exc_info=True)
                stats["errors"] += 1

        stats["sec"] = round(time.time() - t0, 1)
        log.info("=== strapor-heberg TERMINÉ : %s ===", stats)
        return stats

    @task
    def report(stats: dict) -> None:
        from ingestion.mongo_client import count_state, get_db, COL_STRAPOR_STATUTES
        n_docs = get_db()[COL_STRAPOR_STATUTES].count_documents({})
        log.info(
            "=== DAG 03b RAPPORT ===\n"
            "  Entreprises hébergement : %d (skip notaire=%d)\n"
            "  PDF → HDFS Silver       : %d (✗ %d)\n"
            "  Déjà présents : %d  |  Erreurs : %d\n"
            "  strapor_statutes MongoDB: %d docs  |  Durée : %.0f s",
            stats["companies"], stats["skipped"],
            stats["ok"], stats["fail"],
            stats["already"], stats["errors"],
            n_docs, stats["sec"],
        )

    setup = setup_playwright()
    sess  = refresh_session(setup)
    res   = ingest_strapor(sess)
    report(res)


dag_strapor_hebergement()
