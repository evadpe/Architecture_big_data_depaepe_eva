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
        """Index sur enterprises_full pour les requêtes fréquentes."""
        from ingestion.mongo_client import get_db
        from pymongo import ASCENDING, TEXT
        col = get_db()["enterprises_full"]
        col.create_index([("Status", ASCENDING)], background=True)
        col.create_index([("JuridicalForm", ASCENDING)], background=True)
        col.create_index([("name", TEXT)], background=True, name="idx_name_text")
        col.create_index([("activities.nace_code", ASCENDING)], background=True)
        col.create_index([("nbb_accounts.year", ASCENDING)], background=True)
        log.info("Index enterprises_full OK")

    @task(execution_timeout=None)
    def denormalize(n: None) -> dict:
        """
        Pour chaque entreprise : agrège toutes les collections liées
        et écrit le document dans enterprises_full (upsert).
        """
        from ingestion.mongo_client import get_db
        from ingestion.config import BATCH_LOG_EVERY
        from pymongo import UpdateOne
        from pymongo.errors import BulkWriteError

        db    = get_db()
        src   = db["kbo_enterprises"]
        dst   = db["enterprises_full"]
        limit = _LIMIT

        total   = src.count_documents({})
        log.info("=== dag_05 denormalize — %d entreprises (limit=%s) ===", total, limit)
        t0      = time.time()
        done    = 0
        errors  = 0
        batch   = []
        BATCH_SIZE = 200

        cursor = src.find({}, batch_size=500)
        if limit:
            cursor = cursor.limit(limit)

        for ent in cursor:
            bce = ent["_id"]

            try:
                doc = {
                    "_id":            bce,
                    "bce_num":        ent.get("bce_num", bce),
                    "bce_num_clean":  ent.get("bce_num_clean", bce.replace(".", "")),
                    "name":           ent.get("name", ""),
                    "Status":         ent.get("Status", ""),
                    "JuridicalSituation": ent.get("JuridicalSituation", ""),
                    "TypeOfEnterprise":   ent.get("TypeOfEnterprise", ""),
                    "JuridicalForm":      ent.get("JuridicalForm", ""),
                    "StartDate":          ent.get("StartDate", ""),
                    "snapshot_date":      ent.get("snapshot_date", ""),

                    # ── Collections liées ─────────────────────────────────────
                    "denominations": list(db["kbo_denominations"].find(
                        {"entity_number": bce},
                        {"_id": 0, "entity_number": 0},
                    )),
                    "addresses": list(db["kbo_addresses"].find(
                        {"entity_number": bce},
                        {"_id": 0, "entity_number": 0},
                    )),
                    "activities": list(db["kbo_activities"].find(
                        {"entity_number": bce},
                        {"_id": 0, "entity_number": 0},
                    )),
                    "contacts": list(db["kbo_contacts"].find(
                        {"entity_number": bce},
                        {"_id": 0, "entity_number": 0},
                    )),
                    "establishments": list(db["kbo_establishments"].find(
                        {"enterprise_number": bce},
                        {"_id": 0, "enterprise_number": 0},
                    )),

                    # ── Données financières ───────────────────────────────────
                    "nbb_accounts": list(db["nbb_accounts"].find(
                        {"bce_num": bce},
                        {"_id": 0, "bce_num": 0},
                    )),
                    "strapor": list(db["strapor_statutes"].find(
                        {"bce_num": bce},
                        {"_id": 0, "bce_num": 0},
                    )),
                    "ejustice": list(db["ejustice_publications"].find(
                        {"bce_num": bce},
                        {"_id": 0, "bce_num": 0},
                    )),

                    "enriched_at": datetime.utcnow(),
                }

                batch.append(UpdateOne(
                    {"_id": bce},
                    {"$set": doc},
                    upsert=True,
                ))
                done += 1

            except Exception as exc:
                log.warning("[silver] [%s] erreur : %s", bce, exc)
                errors += 1

            # Flush par batch
            if len(batch) >= BATCH_SIZE:
                try:
                    dst.bulk_write(batch, ordered=False)
                except BulkWriteError:
                    pass
                batch.clear()

            if done % BATCH_LOG_EVERY == 0:
                log.info("[silver] %d/%d entreprises enrichies | %.0f s",
                         done, total, time.time() - t0)

        # Dernier batch
        if batch:
            try:
                dst.bulk_write(batch, ordered=False)
            except BulkWriteError:
                pass

        result = {
            "total":   done,
            "errors":  errors,
            "sec":     round(time.time() - t0, 1),
        }
        log.info("=== dag_05 TERMINÉ : %s ===", result)
        return result

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
