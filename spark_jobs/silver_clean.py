"""
PySpark Job — Silver Layer : enterprises_full → enterprise_silver

Exécuté depuis dag_06 via SparkSession connecté à spark://spark-master:7077.
Le driver tourne dans le container Airflow ; les executors sur spark-worker.

Transformations :
  1. StartDate   : DD-MM-YYYY → YYYY-MM-DD
  2. activities  : dédoublonnage (nace_code, classification)
  3. addresses   : garder uniquement type_of_address = "REGO"
  4. denominations : type "001" en premier
  5. Labels FR   : JuridicalFormLabel, StatusLabel, NaceLabel
"""
import os
from pyspark.sql import SparkSession

MONGO_URI   = os.getenv("MONGO_URI",        "mongodb://mongo:27017/")
MONGO_DB    = os.getenv("MONGO_DB",         "bce_db")
SPARK_URL   = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
BATCH_SIZE  = 10_000   # docs envoyés à MongoDB par lot


# ── Fonctions de transformation (exécutées sur les workers Spark) ─────────────

def _normalize_date(d):
    if not d or len(d) != 10 or d[2] != "-" or d[5] != "-":
        return d
    return f"{d[6:10]}-{d[3:5]}-{d[0:2]}"


def _dedup_activities(activities):
    seen, result = set(), []
    for act in (activities or []):
        key = (act.get("nace_code"), act.get("classification"))
        if key not in seen:
            seen.add(key)
            result.append(act)
    return result


def _transform(doc, code_map):
    import copy

    def label(cat, code, lang="FR"):
        return code_map.get((cat, str(code) if code else "", lang))

    out = copy.deepcopy(doc)
    out.pop("_id", None)

    out["StartDate"] = _normalize_date(out.get("StartDate"))

    activities = _dedup_activities(out.get("activities") or [])
    out["addresses"] = [
        a for a in (out.get("addresses") or [])
        if a.get("type_of_address") == "REGO"
    ]
    out["denominations"] = sorted(
        out.get("denominations") or [],
        key=lambda d: (d.get("type", "") != "001", d.get("type") or ""),
    )
    out["JuridicalFormLabel"] = label("JuridicalForm", out.get("JuridicalForm"))
    out["StatusLabel"]        = label("Status",        out.get("Status"))

    for act in activities:
        nv = act.get("nace_version") or "2025"
        act["NaceLabel"] = label(f"Nace{nv}", act.get("nace_code"))
    out["activities"] = activities

    return out


def _transform_partition(partition, bc_code_map):
    cm = bc_code_map.value
    for doc in partition:
        try:
            yield _transform(doc, cm)
        except Exception:
            pass


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main() -> dict:
    import time
    from pymongo import MongoClient

    spark = (
        SparkSession.builder
        .appName("BCE-Silver-Clean")
        .master(SPARK_URL)
        .config("spark.pyspark.python", "python3")
        .config("spark.pyspark.driver.python", "python3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    t0 = time.time()

    # ── Charger kbo_codes en mémoire sur le driver (~21K docs) ───────────────
    code_map = {
        (c["category"], str(c.get("code", "")), c.get("language", "")): c.get("description", "")
        for c in db["kbo_codes"].find(
            {}, {"_id": 0, "category": 1, "code": 1, "language": 1, "description": 1}
        )
    }
    bc_code_map = sc.broadcast(code_map)
    print(f"[silver] {len(code_map)} codes de référence broadcastés")

    # ── Lire enterprises_full depuis MongoDB ──────────────────────────────────
    n_total = db["enterprises_full"].count_documents({})
    print(f"[silver] {n_total} documents à transformer")

    # ── Recréer enterprise_silver ─────────────────────────────────────────────
    db["enterprise_silver"].drop()

    # ── Traitement par lots avec Spark ────────────────────────────────────────
    processed = errors = 0
    cursor = db["enterprises_full"].find({}, no_cursor_timeout=True).batch_size(BATCH_SIZE)

    batch = []
    try:
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                rdd = sc.parallelize(batch, numSlices=4)
                transformed = (
                    rdd.mapPartitions(lambda p: _transform_partition(p, bc_code_map))
                       .collect()
                )
                db["enterprise_silver"].insert_many(transformed, ordered=False)
                processed += len(transformed)
                errors    += len(batch) - len(transformed)
                print(f"[silver] {processed}/{n_total} — {time.time()-t0:.0f}s")
                batch.clear()
    finally:
        cursor.close()

    # Dernier lot
    if batch:
        rdd = sc.parallelize(batch, numSlices=4)
        transformed = (
            rdd.mapPartitions(lambda p: _transform_partition(p, bc_code_map))
               .collect()
        )
        db["enterprise_silver"].insert_many(transformed, ordered=False)
        processed += len(transformed)

    elapsed = round(time.time() - t0, 1)
    print(f"[silver] TERMINÉ — {processed} docs insérés, {errors} erreurs, {elapsed}s")

    client.close()
    spark.stop()

    return {"processed": processed, "errors": errors, "sec": elapsed}


if __name__ == "__main__":
    main()
