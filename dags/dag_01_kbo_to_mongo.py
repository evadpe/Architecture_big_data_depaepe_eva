"""
DAG 01 — KBO CSV → MongoDB (toutes les tables)

Charge les 8 fichiers CSV KBO en MongoDB bce_db, un chunk à la fois.
Idempotent (upsert). Crée aussi les index de la State DB isolée.

Tables chargées :
  enterprise.csv   →  kbo_enterprises    (~1 M lignes,  87 MB)
  denomination.csv →  kbo_denominations  (~5 M lignes, 148 MB)
  address.csv      →  kbo_addresses      (~3 M lignes, 291 MB)
  activity.csv     →  kbo_activities     (~15M lignes, 1.5 GB)  ← long !
  contact.csv      →  kbo_contacts       (~1 M lignes,  33 MB)
  establishment.csv→  kbo_establishments (~1 M lignes,  68 MB)
  code.csv         →  kbo_codes          (petit,         2 MB)
  branch.csv       →  kbo_branches       (petit,       301 KB)

Schedule : @once (relancer manuellement à chaque nouvel export KBO)
"""
import logging
import re
import time
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# ── Paramètres de batch ───────────────────────────────────────────────────────
# Nombre de lignes VALIDES chargées par table et par exécution.
# Mettre None pour charger la totalité (run de production).
_LIMIT_ROWS: int | None = 1_000

# Numéro BCE belge : 4 chiffres . 3 chiffres . 3 chiffres  ex. 0878.065.378
_BCE_RE = re.compile(r"^\d{4}\.\d{3}\.\d{3}$")


@dag(
    dag_id="dag_01_kbo_to_mongo",
    description="KBO CSV → MongoDB (toutes les tables) + init State DB",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bce", "kbo", "ingestion", "layer-1"],
    default_args={"retries": 1, "retry_delay": 120},
)
def dag_kbo_to_mongo():

    # ── 0. Init ─────────────────────────────────────────────────────────────

    @task
    def init_indexes() -> str:
        from ingestion.mongo_client import ensure_indexes
        log.info("=== TASK: init_indexes ===")
        ensure_indexes()
        return "ok"

    # ── 1. Entreprises (avec nom principal déduit des denominations) ─────────

    @task
    def build_name_lookup() -> int:
        """
        Lit denomination.csv une seule fois pour construire un mapping
        {EntityNumber → nom principal} stocké dans une collection temporaire.
        Préférence FR (Language=1) > NL (Language=2) > autres, TypeOfDenomination=001.
        """
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import get_db

        log.info("=== TASK: build_name_lookup ===")
        t0 = time.time()
        LANG_PRIO = {"1": 0, "2": 1}
        lookup: dict[str, tuple[int, str]] = {}
        total = 0

        for chunk in pd.read_csv(
            f"{KBO_DATA_DIR}/denomination.csv", dtype=str, chunksize=KBO_CHUNK_SIZE
        ):
            sub = chunk[chunk["TypeOfDenomination"] == "001"].dropna(subset=["Denomination"])
            for _, row in sub.iterrows():
                eid  = str(row["EntityNumber"]).strip()
                lang = str(row["Language"]).strip()
                name = str(row["Denomination"]).strip()
                prio = LANG_PRIO.get(lang, 99)
                cur  = lookup.get(eid)
                if cur is None or prio < cur[0]:
                    lookup[eid] = (prio, name)
            total += len(chunk)
            if total % 1_000_000 == 0:
                log.info("  denomination.csv — %d lignes lues, %d entités", total, len(lookup))

        log.info("Lookup : %d entités en %.1f s", len(lookup), time.time() - t0)

        tmp = get_db()["_tmp_name_lookup"]
        tmp.drop()
        BATCH = 10_000
        keys  = list(lookup)
        for i in range(0, len(keys), BATCH):
            batch = keys[i:i + BATCH]
            tmp.insert_many([{"_id": k, "name": lookup[k][1]} for k in batch])
        tmp.create_index("_id")
        log.info("Lookup sauvegardé → _tmp_name_lookup (%d docs)", len(lookup))
        return len(lookup)

    @task
    def ingest_enterprises(n_denom: int) -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE, BATCH_LOG_EVERY
        from ingestion.mongo_client import get_db, upsert_enterprises

        log.info("=== TASK: ingest_enterprises (%d noms disponibles) ===", n_denom)
        t0 = time.time()

        lookup = {d["_id"]: d["name"]
                  for d in get_db()["_tmp_name_lookup"].find({}, {"name": 1})}
        snapshot = _snapshot_date(KBO_DATA_DIR)
        loaded   = datetime.now(tz=timezone.utc)

        ins = mod = rows = skipped = chunk_n = 0
        limit = _LIMIT_ROWS
        done  = False

        for chunk in pd.read_csv(
            f"{KBO_DATA_DIR}/enterprise.csv", dtype=str, chunksize=KBO_CHUNK_SIZE
        ):
            if done:
                break
            chunk_n += 1
            docs = []
            for _, r in chunk.iterrows():
                bce = str(r["EnterpriseNumber"]).strip()
                # Filtre : numéro BCE belge ****.***.***
                if not _BCE_RE.match(bce):
                    skipped += 1
                    continue
                docs.append({
                    "_id": bce,
                    "bce_num": bce,
                    "bce_num_clean": bce.replace(".", ""),
                    "name": lookup.get(bce, ""),
                    "Status": _s(r, "Status"),
                    "JuridicalSituation": _s(r, "JuridicalSituation"),
                    "TypeOfEnterprise": _s(r, "TypeOfEnterprise"),
                    "JuridicalForm": _s(r, "JuridicalForm"),
                    "JuridicalFormCAC": _s(r, "JuridicalFormCAC"),
                    "StartDate": _s(r, "StartDate"),
                    "snapshot_date": snapshot,
                    "loaded_at": loaded,
                })
                if limit is not None and (rows + len(docs)) >= limit:
                    done = True
                    break

            i, m = upsert_enterprises(docs)
            ins += i; mod += m; rows += len(docs)
            if chunk_n % 5 == 0 or done:
                log.info("  enterprise — chunk %d | %d valides | %d hors-format | +%d ins | ~%d mod | %.0f s",
                         chunk_n, rows, skipped, ins, mod, time.time() - t0)

        if limit is not None:
            log.info("  enterprise — limite de %d lignes atteinte (skip hors-format: %d)", limit, skipped)

        get_db()["_tmp_name_lookup"].drop()
        log.info("_tmp_name_lookup supprimé")
        result = {"rows": rows, "skipped": skipped, "inserted": ins, "modified": mod,
                  "sec": round(time.time()-t0, 1)}
        log.info("=== ingest_enterprises terminé : %s ===", result)
        return result

    # ── 2. Dénominations (toutes, pas seulement les primaires) ───────────────

    @task
    def ingest_denominations() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_denominations
        return _ingest_csv(
            path=f"{KBO_DATA_DIR}/denomination.csv",
            label="denominations",
            row_fn=lambda r: {
                "entity_number": _s(r, "EntityNumber"),
                "language":      _s(r, "Language"),
                "type":          _s(r, "TypeOfDenomination"),
                "denomination":  _s(r, "Denomination"),
            },
            upsert_fn=upsert_denominations,
            chunk_size=KBO_CHUNK_SIZE,
            bce_key="EntityNumber",
            limit=_LIMIT_ROWS,
        )

    # ── 3. Adresses ──────────────────────────────────────────────────────────

    @task
    def ingest_addresses() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_addresses
        return _ingest_csv(
            path=f"{KBO_DATA_DIR}/address.csv",
            label="addresses",
            row_fn=lambda r: {
                "entity_number":     _s(r, "EntityNumber"),
                "type_of_address":   _s(r, "TypeOfAddress"),
                "zipcode":           _s(r, "Zipcode"),
                "municipality_fr":   _s(r, "MunicipalityFR"),
                "municipality_nl":   _s(r, "MunicipalityNL"),
                "street_fr":         _s(r, "StreetFR"),
                "street_nl":         _s(r, "StreetNL"),
                "house_number":      _s(r, "HouseNumber"),
                "box":               _s(r, "Box"),
                "country_fr":        _s(r, "CountryFR"),
                "country_nl":        _s(r, "CountryNL"),
                "date_striking_off": _s(r, "DateStrikingOff"),
            },
            upsert_fn=upsert_addresses,
            chunk_size=KBO_CHUNK_SIZE,
            bce_key="EntityNumber",
            limit=_LIMIT_ROWS,
        )

    # ── 4. Activités (1.5 GB — tâche longue) ─────────────────────────────────

    @task(execution_timeout=None)
    def ingest_activities() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_activities
        return _ingest_csv(
            path=f"{KBO_DATA_DIR}/activity.csv",
            label="activities",
            row_fn=lambda r: {
                "entity_number":  _s(r, "EntityNumber"),
                "activity_group": _s(r, "ActivityGroup"),
                "nace_version":   _s(r, "NaceVersion"),
                "nace_code":      _s(r, "NaceCode"),
                "classification": _s(r, "Classification"),
            },
            upsert_fn=upsert_activities,
            chunk_size=KBO_CHUNK_SIZE,
            bce_key="EntityNumber",
            limit=_LIMIT_ROWS,
            log_every=500_000,
        )

    # ── 5. Contacts ───────────────────────────────────────────────────────────

    @task
    def ingest_contacts() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_contacts
        return _ingest_csv(
            path=f"{KBO_DATA_DIR}/contact.csv",
            label="contacts",
            row_fn=lambda r: {
                "entity_number":  _s(r, "EntityNumber"),
                "entity_contact": _s(r, "EntityContact"),
                "contact_type":   _s(r, "ContactType"),
                "value":          _s(r, "Value"),
            },
            upsert_fn=upsert_contacts,
            chunk_size=KBO_CHUNK_SIZE,
            bce_key="EntityNumber",
            limit=_LIMIT_ROWS,
        )

    # ── 6. Établissements ─────────────────────────────────────────────────────

    @task
    def ingest_establishments() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_establishments
        return _ingest_csv(
            path=f"{KBO_DATA_DIR}/establishment.csv",
            label="establishments",
            row_fn=lambda r: {
                "_id":               _s(r, "EstablishmentNumber"),
                "establishment_num": _s(r, "EstablishmentNumber"),
                "enterprise_number": _s(r, "EnterpriseNumber"),
                "start_date":        _s(r, "StartDate"),
            },
            upsert_fn=upsert_establishments,
            chunk_size=KBO_CHUNK_SIZE,
            bce_key="EnterpriseNumber",   # filtre sur le numéro de l'entreprise parente
            limit=_LIMIT_ROWS,
        )

    # ── 7. Codes (petite table — pas de filtre BCE, codes internes KBO) ───────

    @task
    def ingest_codes() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_codes
        return _ingest_csv(
            path=f"{KBO_DATA_DIR}/code.csv",
            label="codes",
            row_fn=lambda r: {
                "category":    _s(r, "Category"),
                "code":        _s(r, "Code"),
                "language":    _s(r, "Language"),
                "description": _s(r, "Description"),
            },
            upsert_fn=upsert_codes,
            chunk_size=KBO_CHUNK_SIZE,
            # Pas de bce_key ni de limit : petite table, pas de numéros BCE
        )

    # ── 8. Branches ───────────────────────────────────────────────────────────

    @task
    def ingest_branches() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_branches
        return _ingest_csv(
            path=f"{KBO_DATA_DIR}/branch.csv",
            label="branches",
            row_fn=lambda r: {
                "_id":               _s(r, "Id"),
                "branch_id":         _s(r, "Id"),
                "enterprise_number": _s(r, "EnterpriseNumber"),
                "start_date":        _s(r, "StartDate"),
            },
            upsert_fn=upsert_branches,
            chunk_size=KBO_CHUNK_SIZE,
            bce_key="EnterpriseNumber",
            limit=_LIMIT_ROWS,
        )

    # ── Rapport final ─────────────────────────────────────────────────────────

    @task
    def report(results: list) -> None:
        from ingestion.mongo_client import get_db, get_state_db, COL_STATE
        db   = get_db()
        cols = [
            "kbo_enterprises", "kbo_denominations", "kbo_addresses",
            "kbo_activities", "kbo_contacts", "kbo_establishments",
            "kbo_codes", "kbo_branches",
        ]
        log.info("=== DAG 01 — RAPPORT FINAL ===")
        for c in cols:
            n = db[c].count_documents({})
            log.info("  %-25s : %d documents", c, n)
        n_state = get_state_db()[COL_STATE].count_documents({})
        log.info("  State DB (bce_state_db)   : %d entrées", n_state)

    # ── Pipeline ──────────────────────────────────────────────────────────────
    idx    = init_indexes()
    lookup = build_name_lookup()
    ent    = ingest_enterprises(lookup)

    # Tables indépendantes — peuvent tourner en parallèle avec enterprises
    den  = ingest_denominations()
    addr = ingest_addresses()
    act  = ingest_activities()
    con  = ingest_contacts()
    est  = ingest_establishments()
    cod  = ingest_codes()
    bra  = ingest_branches()

    report([ent, den, addr, act, con, est, cod, bra])

    # Ordre : index d'abord, lookup avant enterprises
    idx >> lookup >> ent
    idx >> [den, addr, act, con, est, cod, bra]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _s(row, col: str) -> str:
    v = row.get(col)
    return "" if (v is None or (isinstance(v, float) and str(v) == "nan")) else str(v).strip()


def _snapshot_date(data_dir: str) -> str:
    try:
        meta = pd.read_csv(f"{data_dir}/meta.csv", dtype=str)
        row  = meta[meta["Variable"] == "SnapshotDate"]
        return row.iloc[0]["Value"] if not row.empty else ""
    except Exception:
        return ""


def _ingest_csv(path: str, label: str, row_fn, upsert_fn,
                chunk_size: int,
                bce_key: str | None = None,
                limit: int | None = None,
                log_every: int = 200_000) -> dict:
    """
    Générique : lit un CSV en chunks, filtre et upsert dans MongoDB.

    bce_key  : nom de la colonne contenant le numéro BCE à valider.
               Si fourni, les lignes dont le numéro ne correspond pas à
               ^\d{4}\.\d{3}\.\d{3}$ sont ignorées (ex. entités étrangères).
    limit    : nombre maximum de lignes VALIDES à charger (None = tout).
               Permet d'arrêter après N lignes pour les tests batch.
    """
    log.info("=== TASK: ingest_%s (bce_key=%s, limit=%s) ===", label, bce_key, limit)
    t0 = time.time()
    ins = mod = rows = skipped = chunk_n = 0
    done = False

    for chunk in pd.read_csv(path, dtype=str, chunksize=chunk_size):
        if done:
            break
        chunk_n += 1
        docs = []

        for _, r in chunk.iterrows():
            # ── Validation du numéro BCE si demandée ──────────────────────
            if bce_key is not None:
                bce_val = str(r.get(bce_key, "")).strip()
                if not _BCE_RE.match(bce_val):
                    skipped += 1
                    continue

            doc = row_fn(r)
            if not doc:
                continue
            docs.append(doc)

            # ── Limite de batch ───────────────────────────────────────────
            if limit is not None and (rows + len(docs)) >= limit:
                done = True
                break

        if docs:
            i, m = upsert_fn(docs)
            ins += i; mod += m; rows += len(docs)

        if rows % log_every < chunk_size or done:
            log.info("  %s — chunk %d | %d valides | %d hors-format | +%d ins | ~%d mod | %.0f s",
                     label, chunk_n, rows, skipped, ins, mod, time.time() - t0)

    if limit is not None and done:
        log.info("  %s — limite de %d lignes valides atteinte", label, limit)

    result = {"table": label, "rows": rows, "skipped_invalid_bce": skipped,
              "inserted": ins, "modified": mod, "sec": round(time.time() - t0, 1)}
    log.info("=== ingest_%s terminé : %s ===", label, result)
    return result


dag_kbo_to_mongo()
