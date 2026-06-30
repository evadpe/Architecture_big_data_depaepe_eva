"""
Client NBB / CBSO — liste des dépôts + téléchargement CSV / PDF.
Accepte un TorSession ou une requests.Session standard.
"""
import logging
import time
from typing import Iterator

import requests

from ingestion.config import (
    CBSO_API, CBSO_PAGE_SIZE, CBSO_DELAY,
    CBSO_TIMEOUT_CSV, CBSO_TIMEOUT_PDF,
    NBB_HEADERS,
    ANNEE_MIN_RECENT, ANNEE_MAX_RECENT,  # valeurs par défaut
)

log = logging.getLogger(__name__)

_MOTS_CONSOLIDE = ("consolid", "geconsolideerd")

_Session = object  # TorSession | requests.Session


def _default_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(NBB_HEADERS)
    return s


# ── Dépôts ────────────────────────────────────────────────────────────────────

def get_deposits(bce_num_clean: str, session: _Session | None = None) -> list[dict]:
    """Récupère tous les dépôts publiés (toutes pages) pour un numéro BCE sans points."""
    sess = session or _default_session()
    deposits, page = [], 0
    log.debug("[nbb] get_deposits [%s]", bce_num_clean)

    while True:
        try:
            r = sess.get(
                f"{CBSO_API}/rs-consult/published-deposits",
                params={
                    "page":             page,
                    "size":             CBSO_PAGE_SIZE,
                    "enterpriseNumber": bce_num_clean.zfill(10),
                    "sort":             "depositDate,desc",
                },
                timeout=15,
            )
            r.raise_for_status()
        except requests.RequestException as exc:
            log.warning("[nbb] [%s] page %d → %s", bce_num_clean, page, exc)
            break

        data        = r.json()
        batch       = data.get("content", [])
        total_pages = data.get("totalPages", 1)
        if not batch:
            break
        deposits.extend(batch)
        log.debug("  [%s] page %d/%d — %d dépôts", bce_num_clean, page+1, total_pages, len(deposits))
        if page + 1 >= total_pages:
            break
        page += 1
        time.sleep(CBSO_DELAY)

    return deposits


def select_one_per_year(
    deposits: list[dict],
    annee_min: int | None = ANNEE_MIN_RECENT,
    annee_max: int | None = ANNEE_MAX_RECENT,
) -> dict[int, dict]:
    """
    Exclut les comptes consolidés, garde 1 dépôt par année.

    annee_min : borne inférieure incluse (None = pas de borne basse).
    annee_max : borne supérieure incluse (None = pas de borne haute).
    Préférence FR > NL, puis dépôt le plus récent en cas d'ex-aequo.
    """
    par_annee: dict[int, dict] = {}
    for d in deposits:
        if any(m in (d.get("modelName") or "").lower() for m in _MOTS_CONSOLIDE):
            continue
        annee = d.get("periodEndDateYear")
        if annee is None:
            continue
        annee = int(annee)
        if annee_min is not None and annee < annee_min:
            continue
        if annee_max is not None and annee > annee_max:
            continue
        cur = par_annee.get(annee)
        if cur is None:
            par_annee[annee] = d
        elif d.get("language") == "FR" and cur.get("language") != "FR":
            par_annee[annee] = d
        elif (d.get("language") == cur.get("language")
              and d.get("depositDate", "") > cur.get("depositDate", "")):
            par_annee[annee] = d
    return par_annee


def iter_deposits_to_ingest(
    bce_num_clean: str,
    session: _Session | None = None,
    annee_min: int | None = ANNEE_MIN_RECENT,
    annee_max: int | None = ANNEE_MAX_RECENT,
) -> Iterator[tuple[str, int, str]]:
    """
    Yield (deposit_id, year, model_name) pour chaque dépôt éligible.
    annee_min / annee_max : bornes inclusives (None = illimité).
    """
    raw = get_deposits(bce_num_clean, session)
    for year, depot in select_one_per_year(raw, annee_min, annee_max).items():
        yield depot["id"], year, depot.get("modelName", "")
        time.sleep(CBSO_DELAY)


# ── Téléchargement ────────────────────────────────────────────────────────────

def download_csv(deposit_id: str, session: _Session | None = None) -> bytes | None:
    """Télécharge le CSV de consultation NBB. Retourne bytes ou None."""
    sess = session or _default_session()
    url  = f"{CBSO_API}/external/broker/public/deposits/consult/csv/{deposit_id}"
    log.debug("[nbb] download_csv [%s]", deposit_id)
    try:
        r = sess.get(url, timeout=CBSO_TIMEOUT_CSV)
    except requests.RequestException as exc:
        log.warning("[nbb] download_csv [%s] : %s", deposit_id, exc)
        return None
    if r.status_code == 404:
        return None
    if not r.ok:
        log.warning("[nbb] download_csv [%s] HTTP %s", deposit_id, r.status_code)
        return None
    if len(r.content) < 50:
        return None
    log.debug("[nbb] download_csv [%s] → %d o", deposit_id, len(r.content))
    return r.content


def download_pdf(deposit_id: str, session: _Session | None = None) -> bytes | None:
    """Télécharge le PDF des comptes annuels NBB. Retourne bytes ou None."""
    sess = session or _default_session()
    url  = f"{CBSO_API}/external/broker/public/deposits/pdf/{deposit_id}"
    log.debug("[nbb] download_pdf [%s]", deposit_id)
    try:
        r = sess.get(url, timeout=CBSO_TIMEOUT_PDF)
    except requests.RequestException as exc:
        log.warning("[nbb] download_pdf [%s] : %s", deposit_id, exc)
        return None
    if r.status_code == 404:
        return None
    if not r.ok:
        log.warning("[nbb] download_pdf [%s] HTTP %s", deposit_id, r.status_code)
        return None
    if "pdf" not in r.headers.get("content-type", "").lower() or len(r.content) < 1_000:
        return None
    log.debug("[nbb] download_pdf [%s] → %d o", deposit_id, len(r.content))
    return r.content
