"""
DAG 07 — Gold Layer : nbb_accounts → hotel_gold  [via PySpark]

Le calcul des ratios financiers est délégué à spark_jobs/gold_hotels.py.
Spark groupe par bce_num et calcule les ratios sur les workers.

Source : nbb_accounts  (Bronze — non modifiée)
Cible  : hotel_gold    (Gold layer)
"""
import logging
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="dag_07_gold_hotels",
    description="nbb_accounts → hotel_gold via PySpark (ratios financiers hébergement)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bce", "gold", "spark", "layer-3"],
)
def dag_gold_hotels():

    @task(execution_timeout=None)
    def build_gold() -> dict:
        from spark_jobs.gold_hotels import main
        stats = main()
        log.info("[dag07] gold terminé : %s", stats)
        return stats

    build_gold()


dag_gold_hotels()
