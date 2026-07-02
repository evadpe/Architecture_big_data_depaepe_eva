"""
Backend FastAPI — BCE Hébergement Gold Layer

Endpoints :
  GET  /search?q=...            Recherche par nom ou numéro BCE
  GET  /enterprise/{bce}        Fiche complète (Silver + Gold)
  GET  /enterprise/{bce}/statutes  Scraping notaire en SSE
  GET  /enterprise/{bce}/dirigeants  Dirigeants (kbopub)
"""
import asyncio
import json
import logging
import os
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pymongo import MongoClient

log = logging.getLogger("bce_api")

app = FastAPI(title="BCE Hébergement API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── MongoDB ───────────────────────────────────────────────────────────────────

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB  = os.getenv("MONGO_DB",  "bce_db")

_client: MongoClient | None = None

def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client[MONGO_DB]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean(obj):
    """Convertit récursivement les ObjectId et datetime en types JSON-sérialisables."""
    from bson import ObjectId
    from datetime import datetime
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items() if k != "_id"}
    if isinstance(obj, list):
        return [_clean(i) for i in obj]
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _bce_fmt(bce_raw: str) -> str:
    """Normalise le BCE : ajoute les points si absent (0123456789 → 0123.456.789)."""
    clean = bce_raw.replace(".", "")
    if len(clean) == 10:
        return f"{clean[:4]}.{clean[4:7]}.{clean[7:]}"
    return bce_raw


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/search")
def search(q: str = Query(..., min_length=2)):
    """
    Recherche une entreprise par nom (dénomination) ou numéro BCE.
    Retourne les 20 premiers résultats.
    """
    db = get_db()
    results = []

    # Recherche par BCE si ressemble à un numéro
    q_stripped = q.replace(".", "").replace(" ", "")
    if q_stripped.isdigit():
        bce = _bce_fmt(q_stripped)
        doc = db["enterprise_silver"].find_one({"_id": bce})
        if doc:
            results.append(_format_search_result(doc))
        return results

    # Recherche textuelle sur les dénominations
    pipeline = [
        {"$match": {"denominations.denomination": {"$regex": q, "$options": "i"}}},
        {"$limit": 20},
        {"$project": {"_id": 1, "Status": 1, "JuridicalFormLabel": 1,
                      "StatusLabel": 1, "denominations": {"$slice": ["$denominations", 1]}}},
    ]
    for doc in db["enterprise_silver"].aggregate(pipeline):
        results.append(_format_search_result(doc))

    return results


def _format_search_result(doc: dict) -> dict:
    denoms = doc.get("denominations") or []
    name   = denoms[0].get("denomination", "") if denoms else ""
    return {
        "bce":          doc["_id"],
        "name":         name,
        "status":       doc.get("StatusLabel") or doc.get("Status"),
        "form":         doc.get("JuridicalFormLabel"),
    }


@app.get("/enterprise/{bce}")
def get_enterprise(bce: str):
    """Fiche complète : Silver (infos, activités, adresse) + Gold (ratios financiers)."""
    db      = get_db()
    bce_fmt = _bce_fmt(bce)

    silver = db["enterprise_silver"].find_one({"_id": bce_fmt})
    if not silver:
        raise HTTPException(status_code=404, detail=f"Entreprise {bce_fmt} introuvable")
    silver = _clean(silver)

    gold = db["hotel_gold"].find_one({"enterprise_number": bce_fmt})
    gold = _clean(gold) if gold else None

    return {"silver": silver, "gold": gold}


@app.get("/enterprise/{bce}/statutes")
async def stream_statutes(bce: str):
    """
    SSE : scrape les statuts notaire en temps réel et les diffuse au frontend.
    Chaque événement SSE contient un document statut en JSON.
    """
    bce_c = _bce_fmt(bce).replace(".", "")

    async def event_generator() -> AsyncGenerator[str, None]:
        db = get_db()

        # Vérifier le cache MongoDB d'abord
        cached = list(db["strapor_statutes"].find(
            {"bce_num": _bce_fmt(bce)},
            {"_id": 0, "bce_num": 1, "doc_id": 1, "deed_date": 1,
             "title": 1, "hdfs_path": 1}
        ))
        if cached:
            for doc in cached:
                yield f"data: {json.dumps(doc, default=str)}\n\n"
                await asyncio.sleep(0.05)
            yield "data: {\"__done__\": true}\n\n"
            return

        # Scraping en temps réel
        try:
            from ingestion.strapor_api import get_session, get_statutes
            loop = asyncio.get_event_loop()
            sess = await loop.run_in_executor(None, get_session)

            statutes = await loop.run_in_executor(None, get_statutes, sess, bce_c)
            for s in statutes:
                payload = {
                    "bce_num":   _bce_fmt(bce),
                    "doc_id":    s.get("documentId"),
                    "deed_date": s.get("deedDate"),
                    "title":     s.get("documentTitle"),
                }
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(0.1)
        except Exception as exc:
            log.warning("[sse/statutes] [%s] %s", bce, exc)
            yield f"data: {{\"__error__\": \"{exc}\"}}\n\n"

        yield "data: {\"__done__\": true}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/enterprise/{bce}/dirigeants")
def get_dirigeants(bce: str):
    """Retourne les dirigeants depuis la collection kbopub_dirigeants (si dispo)."""
    db      = get_db()
    bce_fmt = _bce_fmt(bce)

    docs = list(db["kbopub_dirigeants"].find(
        {"bce_num": bce_fmt}, {"_id": 0}
    ))
    return docs


@app.get("/health")
def health():
    return {"status": "ok"}
