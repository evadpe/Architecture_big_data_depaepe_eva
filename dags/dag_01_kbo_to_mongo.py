"""
DAG 01 — KBO CSV → MongoDB (toutes les tables)

Optimisations :
  • Recherche binaire (O log n) pour positionner le curseur sur chaque shard BCE
  • Lecture vectorisée par chunk avec to_dict('records') (~10× > apply/iterrows)
  • Filtre regex BCE sur colonne entière (numpy str.match, pas ligne par ligne)
  • 4 shards parallèles pour address, activity, denomination, contacts, establishments

Schedule : @once
"""
import logging
import time
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_LIMIT_ROWS: int | None = None   # production — toutes les lignes

import re
_BCE_RE = re.compile(r"^\d{4}\.\d{3}\.\d{3}$")


@dag(
    dag_id="dag_01_kbo_to_mongo",
    description="KBO CSV → MongoDB (binaire + vectorisé) + init State DB",
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

    # ── 1. Entreprises ───────────────────────────────────────────────────────

    @task
    def build_name_lookup() -> int:
        """Lit denomination.csv vectorisé pour construire {EntityNumber → nom}."""
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
            sub = (chunk[chunk["TypeOfDenomination"] == "001"]
                   .dropna(subset=["Denomination"])
                   .copy())
            if not sub.empty:
                sub["_prio"] = sub["Language"].map(LANG_PRIO).fillna(99).astype(int)
                sub = sub.sort_values("_prio").drop_duplicates("EntityNumber", keep="first")
                for eid, prio, name in zip(sub["EntityNumber"], sub["_prio"], sub["Denomination"]):
                    eid = str(eid).strip()
                    prio = int(prio)
                    cur = lookup.get(eid)
                    if cur is None or prio < cur[0]:
                        lookup[eid] = (prio, str(name).strip())
            total += len(chunk)
            if total % 1_000_000 == 0:
                log.info("  denomination — %d lignes, %d entités, %.0f s", total, len(lookup), time.time()-t0)

        log.info("Lookup : %d entités en %.1f s", len(lookup), time.time() - t0)
        tmp = get_db()["_tmp_name_lookup"]
        tmp.drop()
        BATCH = 10_000
        keys = list(lookup)
        for i in range(0, len(keys), BATCH):
            tmp.insert_many([{"_id": k, "name": lookup[k][1]} for k in keys[i:i+BATCH]])
        tmp.create_index("_id")
        return len(lookup)

    @task
    def ingest_enterprises(n_denom: int) -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import get_db, upsert_enterprises

        log.info("=== TASK: ingest_enterprises ===")
        t0 = time.time()
        lookup = {d["_id"]: d["name"] for d in get_db()["_tmp_name_lookup"].find({}, {"name": 1})}
        snapshot = _snapshot_date(KBO_DATA_DIR)
        loaded   = datetime.now(tz=timezone.utc)

        ins = mod = rows = skipped = chunk_n = 0
        limit = _LIMIT_ROWS
        done  = False

        for chunk in pd.read_csv(
            f"{KBO_DATA_DIR}/enterprise.csv", dtype=str, chunksize=KBO_CHUNK_SIZE
        ):
            if done: break
            chunk_n += 1

            mask    = chunk["EnterpriseNumber"].fillna("").str.match(r"^\d{4}\.\d{3}\.\d{3}$")
            skipped += int((~mask).sum())
            chunk   = chunk[mask]
            if chunk.empty: continue

            if limit is not None:
                remaining = limit - rows
                if remaining <= 0: done = True; break
                if len(chunk) > remaining:
                    chunk = chunk.iloc[:remaining]
                    done = True

            # Vectorisé : renommage + fillna + to_dict
            chunk = chunk.rename(columns={
                "EnterpriseNumber": "bce_num",
                "Status": "Status",
                "JuridicalSituation": "JuridicalSituation",
                "TypeOfEnterprise": "TypeOfEnterprise",
                "JuridicalForm": "JuridicalForm",
                "JuridicalFormCAC": "JuridicalFormCAC",
                "StartDate": "StartDate",
            }).fillna("").replace("nan", "")

            docs = []
            for d in chunk.to_dict("records"):
                bce = d["bce_num"]
                d["_id"]           = bce
                d["bce_num_clean"] = bce.replace(".", "")
                d["name"]          = lookup.get(bce, "")
                d["snapshot_date"] = snapshot
                d["loaded_at"]     = loaded
                docs.append(d)

            i, m = upsert_enterprises(docs)
            ins += i; mod += m; rows += len(docs)
            if chunk_n % 10 == 0 or done:
                log.info("  enterprise — %d valides | %d hors-format | +%d ins | %.0f s",
                         rows, skipped, ins, time.time()-t0)

        get_db()["_tmp_name_lookup"].drop()
        result = {"rows": rows, "skipped": skipped, "inserted": ins, "modified": mod,
                  "sec": round(time.time()-t0, 1)}
        log.info("=== ingest_enterprises terminé : %s ===", result)
        return result

    # ── 2-7. Tables liées : 4 shards parallèles avec recherche binaire ────────

    @task
    def ingest_denominations(shard: int) -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_denominations
        from ingestion.csv_utils import read_csv_range, get_shard_range
        bce_start, bce_end = get_shard_range(shard)
        return _ingest_range(
            path=f"{KBO_DATA_DIR}/denomination.csv",
            col_mapping={"EntityNumber": "entity_number", "Language": "language",
                         "TypeOfDenomination": "type", "Denomination": "denomination"},
            bce_key_col="EntityNumber",
            upsert_fn=upsert_denominations,
            bce_start=bce_start, bce_end=bce_end,
            chunk_size=KBO_CHUNK_SIZE, label=f"denominations[shard {shard}]",
        )

    @task
    def ingest_addresses(shard: int) -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_addresses
        from ingestion.csv_utils import get_shard_range
        bce_start, bce_end = get_shard_range(shard)
        return _ingest_range(
            path=f"{KBO_DATA_DIR}/address.csv",
            col_mapping={
                "EntityNumber": "entity_number", "TypeOfAddress": "type_of_address",
                "Zipcode": "zipcode", "MunicipalityFR": "municipality_fr",
                "MunicipalityNL": "municipality_nl", "StreetFR": "street_fr",
                "StreetNL": "street_nl", "HouseNumber": "house_number",
                "Box": "box", "CountryFR": "country_fr", "CountryNL": "country_nl",
                "DateStrikingOff": "date_striking_off",
            },
            bce_key_col="EntityNumber",
            upsert_fn=upsert_addresses,
            bce_start=bce_start, bce_end=bce_end,
            chunk_size=KBO_CHUNK_SIZE, label=f"addresses[shard {shard}]",
        )

    @task(execution_timeout=None)
    def ingest_activities(shard: int) -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_activities
        from ingestion.csv_utils import get_shard_range
        bce_start, bce_end = get_shard_range(shard)
        return _ingest_range(
            path=f"{KBO_DATA_DIR}/activity.csv",
            col_mapping={
                "EntityNumber": "entity_number", "ActivityGroup": "activity_group",
                "NaceVersion": "nace_version", "NaceCode": "nace_code",
                "Classification": "classification",
            },
            bce_key_col="EntityNumber",
            upsert_fn=upsert_activities,
            bce_start=bce_start, bce_end=bce_end,
            chunk_size=KBO_CHUNK_SIZE, label=f"activities[shard {shard}]",
            log_every=500_000,
        )

    @task
    def ingest_contacts(shard: int) -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_contacts
        from ingestion.csv_utils import get_shard_range
        bce_start, bce_end = get_shard_range(shard)
        return _ingest_range(
            path=f"{KBO_DATA_DIR}/contact.csv",
            col_mapping={"EntityNumber": "entity_number", "EntityContact": "entity_contact",
                         "ContactType": "contact_type", "Value": "value"},
            bce_key_col="EntityNumber",
            upsert_fn=upsert_contacts,
            bce_start=bce_start, bce_end=bce_end,
            chunk_size=KBO_CHUNK_SIZE, label=f"contacts[shard {shard}]",
        )

    @task
    def ingest_establishments(shard: int) -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_establishments
        from ingestion.csv_utils import get_shard_range
        bce_start, bce_end = get_shard_range(shard)
        return _ingest_range(
            path=f"{KBO_DATA_DIR}/establishment.csv",
            col_mapping={"EstablishmentNumber": "_id", "EnterpriseNumber": "enterprise_number",
                         "StartDate": "start_date"},
            bce_key_col="EnterpriseNumber",
            upsert_fn=upsert_establishments,
            bce_start=bce_start, bce_end=bce_end,
            chunk_size=KBO_CHUNK_SIZE, label=f"establishments[shard {shard}]",
        )

    @task
    def ingest_codes() -> dict:
        """codes.csv — petite table, pas de sharding ni recherche binaire."""
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_codes
        log.info("=== TASK: ingest_codes ===")
        t0 = time.time(); ins = mod = rows = 0
        for chunk in pd.read_csv(f"{KBO_DATA_DIR}/code.csv", dtype=str, chunksize=KBO_CHUNK_SIZE):
            chunk = chunk.rename(columns={"Category": "category", "Code": "code",
                                          "Language": "language", "Description": "description"})
            chunk = chunk.fillna("").replace("nan", "")
            docs = chunk.to_dict("records")
            i, m = upsert_codes(docs); ins += i; mod += m; rows += len(docs)
        result = {"rows": rows, "inserted": ins, "modified": mod, "sec": round(time.time()-t0, 1)}
        log.info("=== ingest_codes terminé : %s ===", result)
        return result

    @task
    def ingest_branches() -> dict:
        from ingestion.config import KBO_DATA_DIR, KBO_CHUNK_SIZE
        from ingestion.mongo_client import upsert_branches
        log.info("=== TASK: ingest_branches ===")
        t0 = time.time(); ins = mod = rows = 0
        for chunk in pd.read_csv(f"{KBO_DATA_DIR}/branch.csv", dtype=str, chunksize=KBO_CHUNK_SIZE):
            chunk = chunk.rename(columns={"Id": "_id", "EnterpriseNumber": "enterprise_number",
                                          "StartDate": "start_date"})
            chunk = chunk.fillna("").replace("nan", "")
            docs = chunk.to_dict("records")
            i, m = upsert_branches(docs); ins += i; mod += m; rows += len(docs)
        result = {"rows": rows, "inserted": ins, "modified": mod, "sec": round(time.time()-t0, 1)}
        log.info("=== ingest_branches terminé : %s ===", result)
        return result

    # ── Rapport ──────────────────────────────────────────────────────────────

    @task
    def report(results: list) -> None:
        from ingestion.mongo_client import get_db, get_state_db
        from ingestion.config import MONGO_COL_STATE
        db = get_db()
        log.info("=== DAG 01 — RAPPORT FINAL ===")
        for c in ["kbo_enterprises","kbo_denominations","kbo_addresses",
                  "kbo_activities","kbo_contacts","kbo_establishments","kbo_codes","kbo_branches"]:
            log.info("  %-25s : %d", c, db[c].count_documents({}))
        log.info("  State DB : %d entrées", get_state_db()[MONGO_COL_STATE].count_documents({}))

    # ── Pipeline ──────────────────────────────────────────────────────────────
    idx    = init_indexes()
    lookup = build_name_lookup()
    ent    = ingest_enterprises(lookup)

    # 2 shards en parallèle (laptop : 4 tâches max simultanées)
    shards = list(range(2))

    den_tasks  = [ingest_denominations.override(task_id=f"ingest_denominations_{i}")(i) for i in shards]
    addr_tasks = [ingest_addresses.override(task_id=f"ingest_addresses_{i}")(i) for i in shards]
    act_tasks  = [ingest_activities.override(task_id=f"ingest_activities_{i}")(i) for i in shards]
    con_tasks  = [ingest_contacts.override(task_id=f"ingest_contacts_{i}")(i) for i in shards]
    est_tasks  = [ingest_establishments.override(task_id=f"ingest_establishments_{i}")(i) for i in shards]

    codes  = ingest_codes()
    branch = ingest_branches()

    all_results = [ent] + den_tasks + addr_tasks + act_tasks + con_tasks + est_tasks + [codes, branch]
    report(all_results)

    idx >> lookup >> ent
    idx >> den_tasks + addr_tasks + act_tasks + con_tasks + est_tasks + [codes, branch]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ingest_range(path, col_mapping, bce_key_col, upsert_fn,
                  bce_start, bce_end, chunk_size, label,
                  log_every=200_000) -> dict:
    """
    Ingestion d'une plage BCE via recherche binaire + to_dict vectorisé.
    """
    from ingestion.csv_utils import read_csv_range
    from pymongo.errors import BulkWriteError

    log.info("=== TASK: %s [%s → %s] ===", label, bce_start or "début", bce_end or "fin")
    t0 = time.time()
    ins = mod = rows = 0

    for docs in read_csv_range(
        filepath=path,
        col_mapping=col_mapping,
        bce_key_col=bce_key_col,
        bce_start=bce_start,
        bce_end=bce_end,
        chunk_size=chunk_size,
    ):
        if _LIMIT_ROWS is not None and rows >= _LIMIT_ROWS:
            break

        i, m = upsert_fn(docs)
        ins += i; mod += m; rows += len(docs)

        if rows % log_every < chunk_size:
            log.info("  %s — %d lignes | +%d ins | ~%d mod | %.0f s",
                     label, rows, ins, mod, time.time()-t0)

    result = {"table": label, "rows": rows, "inserted": ins, "modified": mod,
              "sec": round(time.time()-t0, 1)}
    log.info("=== %s terminé : %s ===", label, result)
    return result


def _snapshot_date(data_dir: str) -> str:
    try:
        meta = pd.read_csv(f"{data_dir}/meta.csv", dtype=str)
        row  = meta[meta["Variable"] == "SnapshotDate"]
        return row.iloc[0]["Value"] if not row.empty else ""
    except Exception:
        return ""


def _s(row, col: str) -> str:
    v = row.get(col)
    return "" if (v is None or (isinstance(v, float) and str(v) == "nan")) else str(v).strip()


dag_kbo_to_mongo()
