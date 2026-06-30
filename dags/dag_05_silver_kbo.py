"""
DAG 05 — Silver Layer : dénormalisation KBO + comptes NBB

Fusionne toutes les collections KBO (Bronze) en un seul document par entreprise
dans la collection `enterprises_full` (Silver).

Structure résultante :
{
  "_id": "0878.065.378",
  "bce_num": "...",
  "name": "Google Belgium",
  "Status": "AC",
  "JuridicalForm": "...",
  "StartDate": "...",

  "denominations": [{"language":..., "type":..., "denomination":...}, ...],
  "addresses":     [{"type_of_address":..., "zipcode":..., "street_fr":..., ...}, ...],
  "activities":    [{"nace_version":..., "nace_code":..., "classification":..., ...}, ...],
  "contacts":      [{"contact_type":..., "value":...}, ...],
  "establishments":[{"establishment_num":..., "start_date":...}, ...],

  "nbb_accounts":  [{"year":2024, "model_code":"...", "codes":{...}}, ...],
  "strapor":       [{"doc_id":..., "deed_date":..., "title":..., "hdfs_path":...}],
  "ejustice":      [{"numac":..., "date":..., "type":..., "hdfs_path":...}]
}

Permet les jointures directement sans $lookup.
Schedule : @daily (après les DAGs d'ingestion)
"""
import logging
import time
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_LIMIT: int | None = None   # production — toutes les entreprises


@dag(
    dag_id="dag_05_silver_kbo",
    description="Silver Layer — dénormalisation KBO + comptes NBB en un doc/entreprise",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bce", "silver", "kbo", "denorm"],
    default_args={"retries": 1, "retry_delay": 120},
)
def dag_silver_kbo():

    @task
    def ensure_indexes() -> None:
        """Index sur les collections source (lookup) et enterprises_full (requêtes)."""
        from ingestion.mongo_client import get_db
        from pymongo import ASCENDING, TEXT
        db = get_db()

        # ── Index de lookup sur les collections source ─────────────────────────
        # Sans ces index, 7 full-scans × 1.95M entreprises = plusieurs jours
        db["kbo_denominations"].create_index([("entity_number", ASCENDING)], background=True)
        db["kbo_addresses"].create_index([("entity_number", ASCENDING)], background=True)
        db["kbo_activities"].create_index([("entity_number", ASCENDING)], background=True)
        db["kbo_contacts"].create_index([("entity_number", ASCENDING)], background=True)
        db["kbo_establishments"].create_index([("enterprise_number", ASCENDING)], background=True)
        log.info("Index collections source OK")

        # ── Index de requête sur enterprises_full ─────────────────────────────
        col = db["enterprises_full"]
        col.create_index([("Status", ASCENDING)], background=True)
        col.create_index([("JuridicalForm", ASCENDING)], background=True)
        col.create_index([("name", TEXT)], background=True, name="idx_name_text")
        col.create_index([("activities.nace_code", ASCENDING)], background=True)
        log.info("Index enterprises_full OK")

    @task(execution_timeout=None)
    def denormalize(n: None) -> dict:
        """
        Join côté serveur via $lookup + $out — aucun aller-retour Python.
        MongoDB fait tout en une passe en utilisant les index sur entity_number.
        """
        from ingestion.mongo_client import get_db

        db = get_db()
        t0 = time.time()
        total = db["kbo_enterprises"].count_documents({})
        log.info("=== dag_05 denormalize — %d entreprises (pipeline $lookup) ===", total)

        pipeline = [
            {"$lookup": {
                "from": "kbo_denominations", "localField": "_id",
                "foreignField": "entity_number", "as": "denominations",
            }},
            {"$lookup": {
                "from": "kbo_addresses", "localField": "_id",
                "foreignField": "entity_number", "as": "addresses",
            }},
            {"$lookup": {
                "from": "kbo_activities", "localField": "_id",
                "foreignField": "entity_number", "as": "activities",
            }},
            {"$lookup": {
                "from": "kbo_contacts", "localField": "_id",
                "foreignField": "entity_number", "as": "contacts",
            }},
            {"$lookup": {
                "from": "kbo_establishments", "localField": "_id",
                "foreignField": "enterprise_number", "as": "establishments",
            }},
            {"$addFields": {"enriched_at": "$$NOW"}},
            {"$out": "enterprises_full"},
        ]

        db["kbo_enterprises"].aggregate(pipeline, allowDiskUse=True)

        sec = round(time.time() - t0, 1)
        n   = db["enterprises_full"].count_documents({})
        log.info("=== dag_05 TERMINÉ : %d docs en %.0f s ===", n, sec)
        return {"total": n, "errors": 0, "sec": sec}

    @task
    def report(result: dict) -> None:
        from ingestion.mongo_client import get_db
        db  = get_db()
        col = db["enterprises_full"]
        n   = col.count_documents({})
        ex  = col.find_one({"nbb_accounts.0": {"$exists": True}})
        log.info(
            "=== DAG 05 RAPPORT ===\n"
            "  enterprises_full : %d documents\n"
            "  Avec comptes NBB : %d\n"
            "  Durée            : %.0f s\n"
            "  Exemple          : %s — %s — %d années de comptes",
            n,
            col.count_documents({"nbb_accounts.0": {"$exists": True}}),
            result["sec"],
            ex["bce_num"] if ex else "—",
            ex.get("name", "—") if ex else "—",
            len(ex.get("nbb_accounts", [])) if ex else 0,
        )

    idx = ensure_indexes()
    res = denormalize(idx)
    report(res)


dag_silver_kbo()
