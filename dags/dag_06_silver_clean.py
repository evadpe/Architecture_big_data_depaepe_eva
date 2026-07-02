"""
DAG 06 — Silver Layer : enterprises_full → enterprise_silver  [via PySpark]

Le traitement est délégué à spark_jobs/silver_clean.py.
Spark distribue les transformations sur les workers (spark-worker).

Transformations :
  1. StartDate   : DD-MM-YYYY → YYYY-MM-DD
  2. activities  : dédoublonnage (nace_code, classification)
  3. addresses   : garder uniquement type_of_address = "REGO"
  4. denominations : type "001" en premier
  5. Labels FR   : JuridicalFormLabel, StatusLabel, NaceLabel

Source : enterprises_full  (Bronze — non modifiée)
Cible  : enterprise_silver  (recréée à chaque run)
"""
import logging
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="dag_06_silver_clean",
    description="enterprises_full → enterprise_silver (PySpark — Silver layer)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bce", "silver", "spark", "layer-2"],
)
def dag_silver_clean():

    @task(execution_timeout=None)
    def build_silver() -> dict:
        from spark_jobs.silver_clean import main
        stats = main()
        log.info("[dag06] silver terminé : %s", stats)
        return stats

    build_silver()


dag_silver_clean()
