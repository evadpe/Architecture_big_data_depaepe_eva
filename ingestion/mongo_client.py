"""
Couche d'accès MongoDB — deux instances séparées :

  • mongo       (bce_db)       : toutes les données structurées
                                 KBO, comptes NBB, métadonnées Strapor/eJustice
  • mongo_state (bce_state_db) : State DB isolée (download_state uniquement)
                                 Garantit l'idempotence de l'ingestion

State DB — cycle de vie d'un téléchargement :
  pending  →  done   (succès)
  pending  →  error  (échec, éligible au retry)
  L'index unique (bce_num, source, deposit_id) empêche tout doublon.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from ingestion.config import (
    # Connexions
    MONGO_URI, MONGO_DB,
    MONGO_STATE_URI, MONGO_STATE_DB, COL_STATE,
    # Collections données
    COL_ENTERPRISES, COL_DENOMINATIONS, COL_ADDRESSES,
    COL_ACTIVITIES, COL_CONTACTS, COL_ESTABLISHMENTS,
    COL_CODES, COL_BRANCHES,
    COL_NBB_ACCOUNTS, COL_STRAPOR_STATUTES, COL_EJUSTICE_PUBS,
)

log = logging.getLogger(__name__)

# ── Singletons ─────────────────────────────────────────────────────────────────

_client_main:  Optional[MongoClient] = None
_client_state: Optional[MongoClient] = None


def _get_main() -> MongoClient:
    global _client_main
    if _client_main is None:
        log.info("[mongo] Connexion bce_db → %s", MONGO_URI)
        _client_main = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
        _client_main.admin.command("ping")
        log.info("[mongo] bce_db OK")
    return _client_main


def _get_state() -> MongoClient:
    global _client_state
    if _client_state is None:
        log.info("[mongo] Connexion bce_state_db → %s", MONGO_STATE_URI)
        _client_state = MongoClient(MONGO_STATE_URI, serverSelectionTimeoutMS=10_000)
        _client_state.admin.command("ping")
        log.info("[mongo] bce_state_db OK")
    return _client_state


def get_db() -> Database:
    return _get_main()[MONGO_DB]


def get_state_db() -> Database:
    return _get_state()[MONGO_STATE_DB]


def col(name: str) -> Collection:
    return get_db()[name]


def state_col() -> Collection:
    return get_state_db()[COL_STATE]


# ── Initialisation des index ───────────────────────────────────────────────────

def ensure_indexes() -> None:
    """Crée tous les index (idempotent)."""
    db = get_db()

    # KBO
    db[COL_ENTERPRISES].create_index([("Status", ASCENDING)], background=True)
    db[COL_ENTERPRISES].create_index([("JuridicalForm", ASCENDING)], background=True)

    db[COL_DENOMINATIONS].create_index([("entity_number", ASCENDING)], background=True)
    db[COL_DENOMINATIONS].create_index(
        [("entity_number", ASCENDING), ("language", ASCENDING), ("type", ASCENDING)],
        unique=True, background=True, name="idx_denom_unique",
    )

    db[COL_ADDRESSES].create_index([("entity_number", ASCENDING)], background=True)
    db[COL_ADDRESSES].create_index(
        [("entity_number", ASCENDING), ("type_of_address", ASCENDING)],
        background=True,
    )

    db[COL_ACTIVITIES].create_index([("entity_number", ASCENDING)], background=True)
    db[COL_ACTIVITIES].create_index([("nace_code", ASCENDING)], background=True)

    db[COL_CONTACTS].create_index([("entity_number", ASCENDING)], background=True)
    db[COL_ESTABLISHMENTS].create_index([("enterprise_number", ASCENDING)], background=True)
    db[COL_CODES].create_index(
        [("category", ASCENDING), ("code", ASCENDING), ("language", ASCENDING)],
        unique=True, background=True, name="idx_codes_unique",
    )

    # Données téléchargées
    db[COL_NBB_ACCOUNTS].create_index([("bce_num", ASCENDING)], background=True)
    db[COL_NBB_ACCOUNTS].create_index(
        [("bce_num", ASCENDING), ("deposit_id", ASCENDING)],
        unique=True, background=True, name="idx_nbb_unique",
    )
    db[COL_NBB_ACCOUNTS].create_index([("year", ASCENDING)], background=True)

    db[COL_STRAPOR_STATUTES].create_index([("bce_num", ASCENDING)], background=True)
    db[COL_STRAPOR_STATUTES].create_index(
        [("bce_num", ASCENDING), ("doc_id", ASCENDING)],
        unique=True, background=True, name="idx_strapor_unique",
    )

    db[COL_EJUSTICE_PUBS].create_index([("bce_num", ASCENDING)], background=True)
    db[COL_EJUSTICE_PUBS].create_index(
        [("bce_num", ASCENDING), ("numac", ASCENDING)],
        unique=True, background=True, name="idx_ejustice_unique",
    )

    log.info("[mongo] Index bce_db OK")

    # State DB — index UNIQUE (bce_num, source, deposit_id)
    sdb = get_state_db()
    sdb[COL_STATE].create_index(
        [("bce_num", ASCENDING), ("source", ASCENDING), ("deposit_id", ASCENDING)],
        unique=True, background=True, name="idx_state_unique",
    )
    sdb[COL_STATE].create_index([("status", ASCENDING)], background=True)
    sdb[COL_STATE].create_index([("source", ASCENDING)], background=True)
    log.info("[mongo] Index bce_state_db OK")


# ── Upserts génériques ─────────────────────────────────────────────────────────

def _bulk_upsert(collection_name: str, ops: list, ordered: bool = False) -> tuple[int, int]:
    """Exécute un bulk_write de UpdateOne et retourne (upserted, modified)."""
    if not ops:
        return 0, 0
    try:
        r = col(collection_name).bulk_write(ops, ordered=ordered)
        return r.upserted_count, r.modified_count
    except BulkWriteError as e:
        n_err = len(e.details.get("writeErrors", []))
        log.warning("[mongo] bulk_write %s — %d erreurs partielles", collection_name, n_err)
        return 0, 0


# ── KBO — upserts par table ────────────────────────────────────────────────────

def upsert_enterprises(rows: list[dict]) -> tuple[int, int]:
    ops = [
        UpdateOne({"_id": r["_id"]},
                  {"$set": r, "$setOnInsert": {"created_at": _now()}},
                  upsert=True)
        for r in rows
    ]
    return _bulk_upsert(COL_ENTERPRISES, ops)


def upsert_denominations(rows: list[dict]) -> tuple[int, int]:
    """Clé unique : (entity_number, language, type)."""
    ops = [
        UpdateOne(
            {"entity_number": r["entity_number"],
             "language": r["language"],
             "type": r["type"]},
            {"$set": r, "$setOnInsert": {"created_at": _now()}},
            upsert=True,
        )
        for r in rows
    ]
    return _bulk_upsert(COL_DENOMINATIONS, ops)


def upsert_addresses(rows: list[dict]) -> tuple[int, int]:
    ops = [
        UpdateOne(
            {"entity_number": r["entity_number"], "type_of_address": r["type_of_address"]},
            {"$set": r, "$setOnInsert": {"created_at": _now()}},
            upsert=True,
        )
        for r in rows
    ]
    return _bulk_upsert(COL_ADDRESSES, ops)


def upsert_activities(rows: list[dict]) -> tuple[int, int]:
    ops = [
        UpdateOne(
            {"entity_number": r["entity_number"],
             "nace_version": r["nace_version"],
             "nace_code": r["nace_code"],
             "classification": r["classification"]},
            {"$set": r, "$setOnInsert": {"created_at": _now()}},
            upsert=True,
        )
        for r in rows
    ]
    return _bulk_upsert(COL_ACTIVITIES, ops)


def upsert_contacts(rows: list[dict]) -> tuple[int, int]:
    ops = [
        UpdateOne(
            {"entity_number": r["entity_number"],
             "contact_type": r["contact_type"],
             "value": r["value"]},
            {"$set": r, "$setOnInsert": {"created_at": _now()}},
            upsert=True,
        )
        for r in rows
    ]
    return _bulk_upsert(COL_CONTACTS, ops)


def upsert_establishments(rows: list[dict]) -> tuple[int, int]:
    ops = [
        UpdateOne({"_id": r["_id"]},
                  {"$set": r, "$setOnInsert": {"created_at": _now()}},
                  upsert=True)
        for r in rows
    ]
    return _bulk_upsert(COL_ESTABLISHMENTS, ops)


def upsert_codes(rows: list[dict]) -> tuple[int, int]:
    ops = [
        UpdateOne(
            {"category": r["category"], "code": r["code"], "language": r["language"]},
            {"$set": r, "$setOnInsert": {"created_at": _now()}},
            upsert=True,
        )
        for r in rows
    ]
    return _bulk_upsert(COL_CODES, ops)


def upsert_branches(rows: list[dict]) -> tuple[int, int]:
    ops = [
        UpdateOne({"_id": r["_id"]},
                  {"$set": r, "$setOnInsert": {"created_at": _now()}},
                  upsert=True)
        for r in rows
    ]
    return _bulk_upsert(COL_BRANCHES, ops)


# ── Données téléchargées ───────────────────────────────────────────────────────

def upsert_nbb_account(bce_num: str, deposit_id: str, year: int,
                        model_code: str, language: str,
                        codes: dict[str, float]) -> None:
    """Stocke les codes comptables parsés d'un dépôt NBB."""
    col(COL_NBB_ACCOUNTS).update_one(
        {"bce_num": bce_num, "deposit_id": deposit_id},
        {"$set": {
            "bce_num":    bce_num,
            "deposit_id": deposit_id,
            "year":       year,
            "model_code": model_code,
            "language":   language,
            "codes":      codes,
            "stored_at":  _now(),
        }, "$setOnInsert": {"created_at": _now()}},
        upsert=True,
    )


def upsert_strapor_statute(bce_num: str, doc_id: str, deed_date: str,
                            title: str, hdfs_path: str | None) -> None:
    col(COL_STRAPOR_STATUTES).update_one(
        {"bce_num": bce_num, "doc_id": doc_id},
        {"$set": {
            "bce_num":   bce_num,
            "doc_id":    doc_id,
            "deed_date": deed_date,
            "title":     title,
            "hdfs_path": hdfs_path,
            "stored_at": _now(),
        }, "$setOnInsert": {"created_at": _now()}},
        upsert=True,
    )


def upsert_ejustice_pub(bce_num: str, numac: str, date: str,
                         pub_type: str, hdfs_path: str | None) -> None:
    col(COL_EJUSTICE_PUBS).update_one(
        {"bce_num": bce_num, "numac": numac},
        {"$set": {
            "bce_num":   bce_num,
            "numac":     numac,
            "date":      date,
            "type":      pub_type,
            "hdfs_path": hdfs_path,
            "stored_at": _now(),
        }, "$setOnInsert": {"created_at": _now()}},
        upsert=True,
    )


# ── State DB ───────────────────────────────────────────────────────────────────

def is_done(bce_num: str, source: str, deposit_id: str) -> bool:
    """True si ce téléchargement est déjà réussi → skip."""
    return state_col().find_one(
        {"bce_num": bce_num, "source": source, "deposit_id": deposit_id, "status": "done"},
        {"_id": 1},
    ) is not None


def mark_pending(bce_num: str, source: str, deposit_id: str, year: int | None = None) -> None:
    state_col().update_one(
        {"bce_num": bce_num, "source": source, "deposit_id": deposit_id},
        {"$setOnInsert": {
            "bce_num": bce_num, "source": source, "deposit_id": deposit_id,
            "year": year, "status": "pending",
            "hdfs_path": None, "file_size_bytes": None,
            "error_msg": None, "retry_count": 0,
            "created_at": _now(), "updated_at": _now(),
        }},
        upsert=True,
    )


def mark_done(bce_num: str, source: str, deposit_id: str,
              hdfs_path: str, file_size_bytes: int) -> None:
    state_col().update_one(
        {"bce_num": bce_num, "source": source, "deposit_id": deposit_id},
        {"$set": {
            "status": "done", "hdfs_path": hdfs_path,
            "file_size_bytes": file_size_bytes, "error_msg": None,
            "downloaded_at": _now(), "updated_at": _now(),
        }},
    )


def mark_error(bce_num: str, source: str, deposit_id: str, error_msg: str) -> None:
    state_col().update_one(
        {"bce_num": bce_num, "source": source, "deposit_id": deposit_id},
        {"$set": {
            "status": "error",
            "error_msg": str(error_msg)[:500],
            "updated_at": _now(),
        }, "$inc": {"retry_count": 1}},
    )


# ── Requêtes utilitaires ───────────────────────────────────────────────────────

def iter_active_companies(batch_size: int = 500):
    """Générateur : itère sur les entreprises actives (cursor MongoDB)."""
    total = col(COL_ENTERPRISES).count_documents({"Status": "AC"})
    log.info("[mongo] iter_active_companies — %d actives", total)
    yield from col(COL_ENTERPRISES).find({"Status": "AC"}, batch_size=batch_size)


def count_state(source: str, status: str) -> int:
    return state_col().count_documents({"source": source, "status": status})


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)
