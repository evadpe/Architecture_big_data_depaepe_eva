"""
DAG 02b — NBB Comptes annuels HISTORIQUES → Bronze HDFS + MongoDB  [NON-PRIORITAIRE]

Fenêtre fiscale : tout ce qui est ≤ 2019  (aucune borne inférieure)
Objectif        : récupérer l'historique complet APRÈS que le DAG récent
                  (dag_02a) a terminé les années 2020-2025.

Priorité Airflow :
  • priority_weight = 1   (le planificateur préfère dag_02a en cas de concurrence)
  • schedule @weekly      (moins fréquent que dag_02a)
  • max_active_runs = 1   (ne monopolise pas les ressources)

La State DB partagée garantit que ce DAG ne re-télécharge jamais un dépôt
déjà traité par dag_02a (même source "nbb_csv"/"nbb_pdf", deposit_id commun).
"""
import logging
import time
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_LIMIT: int | None = 1_000   # None = run complet de production


@dag(
    dag_id="dag_02b_nbb_historic",
    description="NBB ≤ 2019 → HDFS + MongoDB [NON-PRIORITAIRE]",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bce", "nbb", "historic", "bronze", "layer-1"],
    default_args={"retries": 2, "retry_delay": 600},
)
def dag_nbb_historic():

    @task(priority_weight=1)
    def check_ready() -> int:
        from ingestion.mongo_client import col, COL_ENTERPRISES
        n = col(COL_ENTERPRISES).count_documents({"Status": "AC"})
        if n == 0:
            raise RuntimeError("MongoDB vide — lancer dag_01_kbo_to_mongo d'abord")
        log.info("[nbb-hist] %d entreprises actives disponibles", n)
        return n

    @task(priority_weight=1, execution_timeout=None)
    def ingest_historic(n_companies: int) -> dict:
        from ingestion.config import (
            HDFS_BRONZE_NBB_CSVS, HDFS_BRONZE_NBB_PDFS,
            BATCH_LOG_EVERY, CBSO_DELAY,
            ANNEE_MAX_HISTORIC,
            NBB_HEADERS,
        )
        from ingestion.mongo_client import iter_active_companies
        from ingestion.nbb_api import iter_deposits_to_ingest
        from ingestion.tor_session import TorSession

        # Import des helpers partagés depuis dag_02a
        from dags.dag_02a_nbb_recent import _process_deposit, _log_progress

        log.info(
            "=== [dag_02b] ingest_historic — années ≤ %d | limit=%s | %d entreprises ===",
            ANNEE_MAX_HISTORIC, _LIMIT, n_companies,
        )
        t0  = time.time()
        tor = TorSession(headers=NBB_HEADERS)

        stats = dict(companies=0, no_deposits=0, already=0,
                     csv_ok=0, csv_fail=0, pdf_ok=0, pdf_fail=0, errors=0)
        limit = _LIMIT

        for company in iter_active_companies():
            if limit is not None and stats["companies"] >= limit:
                log.info("[nbb-hist] Limite %d atteinte — arrêt", limit)
                break

            bce   = company["bce_num"]
            bce_c = company.get("bce_num_clean", bce.replace(".", ""))
            stats["companies"] += 1

            if stats["companies"] % BATCH_LOG_EVERY == 0:
                _log_progress("[nbb-hist]", stats, n_companies, t0)

            try:
                deposits = list(iter_deposits_to_ingest(
                    bce_c, session=tor,
                    annee_min=None,              # pas de borne inférieure
                    annee_max=ANNEE_MAX_HISTORIC,
                ))
            except Exception as exc:
                log.warning("[nbb-hist] [%s] get_deposits échoué : %s", bce, exc)
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

        _log_progress("[nbb-hist] FINAL", stats, n_companies, t0)
        stats["sec"] = round(time.time() - t0, 1)
        return stats

    @task(priority_weight=1)
    def report(stats: dict) -> None:
        from ingestion.mongo_client import count_state, get_db, COL_NBB_ACCOUNTS
        from ingestion.config import ANNEE_MAX_HISTORIC
        n_acc  = get_db()[COL_NBB_ACCOUNTS].count_documents({})
        n_hist = get_db()[COL_NBB_ACCOUNTS].count_documents({"year": {"$lte": ANNEE_MAX_HISTORIC}})
        log.info(
            "=== DAG 02b RAPPORT (années ≤ %d) ===\n"
            "  Entreprises          : %d (sans dépôts historiques: %d)\n"
            "  CSV → HDFS           : %d  (✗ %d)\n"
            "  PDF → HDFS           : %d  (✗ %d)\n"
            "  Déjà présents        : %d  |  Erreurs : %d\n"
            "  nbb_accounts total   : %d docs MongoDB\n"
            "  nbb_accounts ≤ %d    : %d docs\n"
            "  State nbb_csv done   : %d\n"
            "  State nbb_pdf done   : %d\n"
            "  Durée                : %.0f s",
            ANNEE_MAX_HISTORIC,
            stats["companies"], stats["no_deposits"],
            stats["csv_ok"], stats["csv_fail"],
            stats["pdf_ok"], stats["pdf_fail"],
            stats["already"], stats["errors"],
            n_acc,
            ANNEE_MAX_HISTORIC, n_hist,
            count_state("nbb_csv", "done"),
            count_state("nbb_pdf", "done"),
            stats["sec"],
        )

    n   = check_ready()
    res = ingest_historic(n)
    report(res)


dag_nbb_historic()
