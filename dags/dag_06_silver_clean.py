"""
DAG 06 — Silver Layer MongoDB : enterprises_full → enterprise_silver

Transformations appliquées :
  1. StartDate   : DD-MM-YYYY → YYYY-MM-DD
  2. activities  : dédoublonnage (nace_code + classification) uniques
  3. addresses   : garder uniquement type_of_address = "REGO"
  4. denominations : type "001" (dénomination officielle) en premier
  5. Labels FR   : JuridicalFormLabel, StatusLabel, NaceLabel par activité

Source : enterprises_full  (Bronze — non modifiée)
Cible  : enterprise_silver  (recréée à chaque run)
"""
import logging
import time
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

BATCH_SIZE = 2_000   # docs insérés par batch (insert_many)


@dag(
    dag_id="dag_06_silver_clean",
    description="enterprises_full → enterprise_silver (Silver layer MongoDB — nettoyage + labels FR)",
    schedule=None,        # déclenchement manuel uniquement
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bce", "silver", "mongo", "layer-2"],
)
def dag_silver_clean():

    @task(execution_timeout=None)
    def build_silver() -> dict:
        from ingestion.mongo_client import get_db

        db  = get_db()
        src = db["enterprises_full"]
        dst = db["enterprise_silver"]

        # ── Charger les codes de référence en mémoire (21 K docs, ~5 MB) ─────
        log.info("[silver] Chargement kbo_codes en mémoire…")
        code_map: dict[tuple, str] = {}
        for c in db["kbo_codes"].find(
            {},
            {"_id": 0, "category": 1, "code": 1, "language": 1, "description": 1},
        ):
            key = (c["category"], str(c.get("code", "")), c.get("language", ""))
            code_map[key] = c.get("description", "")
        log.info("[silver] %d codes chargés", len(code_map))

        def label(category: str, code: str | None, lang: str = "FR") -> str | None:
            if not code:
                return None
            return code_map.get((category, str(code), lang)) or None

        # ── Recréer la collection cible ───────────────────────────────────────
        dst.drop()
        log.info("[silver] collection enterprise_silver remise à zéro")

        # ── Traitement par lots ───────────────────────────────────────────────
        t0      = time.time()
        n_total = src.count_documents({})
        log.info("[silver] %d documents à transformer depuis enterprises_full", n_total)

        processed = 0
        errors    = 0
        bulk: list[dict] = []

        cursor = src.find({}, no_cursor_timeout=True).batch_size(BATCH_SIZE)
        try:
            for doc in cursor:
                try:
                    bulk.append(_transform(doc, label))
                except Exception as exc:
                    log.warning("[silver] [%s] erreur transform : %s", doc.get("_id"), exc)
                    errors += 1
                    continue

                if len(bulk) >= BATCH_SIZE:
                    dst.insert_many(bulk, ordered=False)
                    processed += len(bulk)
                    bulk.clear()
                    log.info("[silver] %d/%d — %.1f s", processed, n_total, time.time() - t0)
        finally:
            cursor.close()

        if bulk:
            dst.insert_many(bulk, ordered=False)
            processed += len(bulk)

        # Index sur _id créé automatiquement par MongoDB à l'insert
        log.info(
            "[silver] TERMINÉ — %d documents insérés, %d erreurs, %.1f s",
            processed, errors, time.time() - t0,
        )
        return {"processed": processed, "errors": errors, "sec": round(time.time() - t0, 1)}

    build_silver()


dag_silver_clean()


# ── Helpers de transformation (module-level pour éviter la sérialisation Airflow) ──

def _normalize_date(d: str | None) -> str | None:
    """DD-MM-YYYY → YYYY-MM-DD. Retourne inchangé si format différent ou None."""
    if not d or len(d) != 10 or d[2] != "-" or d[5] != "-":
        return d
    # "09-08-1960"  →  "1960-08-09"
    return f"{d[6:10]}-{d[3:5]}-{d[0:2]}"


def _dedup_activities(activities: list[dict]) -> list[dict]:
    """Une seule activité par couple (nace_code, classification)."""
    seen: set[tuple] = set()
    result = []
    for act in (activities or []):
        key = (act.get("nace_code"), act.get("classification"))
        if key in seen:
            continue
        seen.add(key)
        result.append(act)
    return result


def _transform(doc: dict, label_fn) -> dict:
    """Retourne une copie transformée du document enterprises_full."""
    import copy
    out = copy.deepcopy(doc)

    # 1. Normalisation date
    out["StartDate"] = _normalize_date(out.get("StartDate"))

    # 2. Dédoublonnage activities
    activities = _dedup_activities(out.get("activities") or [])

    # 3. Garder uniquement l'adresse REGO
    out["addresses"] = [
        a for a in (out.get("addresses") or [])
        if a.get("type_of_address") == "REGO"
    ]

    # 4. Denominations : type "001" en premier, puis ordre alphabétique du code
    out["denominations"] = sorted(
        out.get("denominations") or [],
        key=lambda d: (d.get("type", "") != "001", d.get("type") or ""),
    )

    # 5. Labels FR au niveau entreprise
    out["JuridicalFormLabel"] = label_fn("JuridicalForm", out.get("JuridicalForm"))
    out["StatusLabel"]        = label_fn("Status",        out.get("Status"))

    # NaceLabel sur chaque activité (Nace2025 / Nace2008 / Nace2003)
    for act in activities:
        nv = act.get("nace_version") or "2025"
        act["NaceLabel"] = label_fn(f"Nace{nv}", act.get("nace_code"))
    out["activities"] = activities

    return out
