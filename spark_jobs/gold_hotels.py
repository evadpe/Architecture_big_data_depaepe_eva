"""
PySpark Job — Gold Layer : nbb_accounts → hotel_gold

Exécuté depuis dag_07 via SparkSession connecté à spark://spark-master:7077.

Pour chaque entreprise hébergement :
  - Regroupe les exercices (groupByKey sur bce_num)
  - Extrait les codes PCMN et calcule les ratios financiers
  - Upsert dans hotel_gold
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession

MONGO_URI  = os.getenv("MONGO_URI",        "mongodb://mongo:27017/")
MONGO_DB   = os.getenv("MONGO_DB",         "bce_db")
SPARK_URL  = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# ── Mapping PCMN ──────────────────────────────────────────────────────────────

PCMN_SINGLE = {
    "70":   "chiffre_affaires",
    "60":   "achats",
    "71":   "variation_stocks",
    "9901": "ebit",
    "9904": "resultat_net",
    "100":  "capital_souscrit",
}
PCMN_SUM = {
    "tresorerie":         ["54", "55"],
    "dettes_financieres": ["17", "43"],
    "fonds_propres":      ["10", "11", "12", "13", "14", "15"],
}
SCHEMA_MAP = {"f": "full", "a": "abrege", "m": "micro"}


# ── Fonctions métier (exécutées sur les workers Spark) ────────────────────────

def _safe_div(a, b):
    if a is None or b is None or b == 0:
        return None
    return round(a / b * 100, 2)


def _extract_financials(codes):
    out = {}
    for code, field in PCMN_SINGLE.items():
        v = codes.get(code)
        out[field] = float(v) if v is not None else None
    for field, code_list in PCMN_SUM.items():
        vals = [codes.get(c) for c in code_list if codes.get(c) is not None]
        out[field] = round(sum(float(v) for v in vals), 2) if vals else None
    return out


def _calc_ratios(f):
    ca   = f.get("chiffre_affaires")
    ach  = f.get("achats")
    var  = f.get("variation_stocks") or 0
    rnet = f.get("resultat_net")
    fp   = f.get("fonds_propres")
    tres = f.get("tresorerie")
    det  = f.get("dettes_financieres")

    marge_brute = round(ca - ach + var, 2) if ca is not None and ach is not None else None

    return {
        "marge_brute":          marge_brute,
        "marge_nette_pct":      _safe_div(rnet, ca),
        "roe_pct":              _safe_div(rnet, fp),
        "ratio_liquidite":      round(tres / det, 4) if tres and det else None,
        "taux_endettement_pct": _safe_div(det, fp),
    }


def _process_group(kv):
    """Calcule le document gold pour une entreprise (exécuté sur un worker)."""
    bce, entries = kv[0], list(kv[1])
    years = []
    schema_type = "unknown"

    for entry in sorted(entries, key=lambda x: x.get("year", 0)):
        year        = entry.get("year")
        model_code  = entry.get("model_code", "")
        codes       = entry.get("codes", {})
        last        = model_code.split("-")[-1].lower() if model_code else ""
        schema_type = SCHEMA_MAP.get(last, "unknown")

        fin    = _extract_financials(codes)
        ratios = _calc_ratios(fin)
        years.append({"year": year, **fin, "ratios": ratios, "model_code": model_code})

    return {
        "enterprise_number": bce,
        "years":             years,
        "schema_type":       schema_type,
        "last_updated":      datetime.utcnow().isoformat(),
    }


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main() -> dict:
    import time
    from pymongo import MongoClient, UpdateOne

    spark = (
        SparkSession.builder
        .appName("BCE-Gold-Hotels")
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

    # ── Charger nbb_accounts depuis MongoDB ───────────────────────────────────
    docs = list(db["nbb_accounts"].find(
        {}, {"bce_num": 1, "year": 1, "model_code": 1, "codes": 1, "_id": 0}
    ))
    print(f"[gold] {len(docs)} entrées nbb_accounts chargées")

    # ── Spark : groupByKey par bce_num → calcul ratios ────────────────────────
    rdd = sc.parallelize(docs, numSlices=4)

    gold_docs = (
        rdd
        .map(lambda d: (d["bce_num"], d))
        .groupByKey()
        .map(_process_group)
        .collect()
    )
    print(f"[gold] {len(gold_docs)} entreprises calculées par Spark")

    # ── Upsert dans hotel_gold ────────────────────────────────────────────────
    db["hotel_gold"].create_index("enterprise_number", unique=True, background=True)

    ops = [
        UpdateOne(
            {"enterprise_number": doc["enterprise_number"]},
            {"$set": doc},
            upsert=True,
        )
        for doc in gold_docs
    ]
    result = db["hotel_gold"].bulk_write(ops, ordered=False) if ops else None

    elapsed = round(time.time() - t0, 1)
    upserted = result.upserted_count if result else 0
    modified = result.modified_count  if result else 0

    print(f"[gold] upserted={upserted} modified={modified} — {elapsed}s")
    print(f"[gold] hotel_gold: {db['hotel_gold'].count_documents({})} docs")

    client.close()
    spark.stop()

    return {"processed": len(gold_docs), "upserted": upserted, "modified": modified, "sec": elapsed}


if __name__ == "__main__":
    main()
