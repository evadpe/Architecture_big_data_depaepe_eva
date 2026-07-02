"""
Client eJustice — scraping des publications légales (Annexes Personnes Morales).
Adapté du notebook BCE section 2.9.
"""
import logging
import re
import time
from typing import Iterator

import requests

from ingestion.config import (
    EJUSTICE_URL, EJUSTICE_DELAY, EJUSTICE_TIMEOUT,
    NBB_HEADERS,
)

log = logging.getLogger(__name__)

_PATTERN_DATE_NUMAC = re.compile(
    r"(?P<date>\d{4}(?:-\d{2}-\d{2})?)\s*/\s*(?P<numac>[\d.\-]+)\s*"
    r"(?:<font color=blue>(?:&nbsp;)*(?:<a href=\"(?P<lien>[^\"]+)\"[^>]*>IMAGE</a>)?</font>)?\s*$"
)
_PATTERN_NOM_ENTITE = re.compile(r"^<font color=blue>.+</font>")

_default_session: requests.Session | None = None

_Session = object  # TorSession | requests.Session


def _get_default() -> requests.Session:
    global _default_session
    if _default_session is None:
        _default_session = requests.Session()
        _default_session.headers.update(NBB_HEADERS)
    return _default_session


# ── Récupération des publications ─────────────────────────────────────────────

def get_publications(bce_num_clean: str, session: _Session | None = None) -> list[dict]:
    """
    Retourne la liste des publications légales eJustice pour un numéro BCE (sans points).
    Chaque entrée : {date, numac, type, lien_pdf}.
    """
    log.debug("[ejustice] get_publications [%s]", bce_num_clean)
    sess = session or _get_default()

    # eJustice attend le numéro BCE sans zéro(s) initial/initiaux
    btw = bce_num_clean.lstrip("0") or "0"
    try:
        r = sess.get(
            EJUSTICE_URL,
            params={
                "language":   "fr",
                "btw_search": btw,
                "btw":        btw,
                "page":       1,
                "la_search":  "f",
                "caller":     "list",
            },
            timeout=EJUSTICE_TIMEOUT,
        )
        r.raise_for_status()
    except requests.RequestException as exc:
        log.warning("[ejustice] [%s] erreur réseau : %s", bce_num_clean, exc)
        return []

    r.encoding = "iso-8859-1"
    html = r.text

    pubs = _parse_html(html)
    log.info("[ejustice] [%s] → %d publication(s)", bce_num_clean, len(pubs))
    return pubs


def _parse_html(html: str) -> list[dict]:
    """Parse l'historique de publications eJustice (séparés par <hr>)."""
    main_match = re.search(r"<main.*?>(.*?)</main>", html, re.S)
    if not main_match:
        return []

    publications = []
    for bloc in main_match.group(1).split("<hr>"):
        lignes = bloc.split("<br>")
        idx = next(
            (i for i in range(len(lignes) - 1, -1, -1)
             if _PATTERN_DATE_NUMAC.search(lignes[i].strip())),
            None,
        )
        if idx is None:
            continue
        match = _PATTERN_DATE_NUMAC.search(lignes[idx].strip())
        if not match:
            continue

        type_pub = "—"
        for ligne in lignes[idx - 1::-1]:
            if _PATTERN_NOM_ENTITE.match(ligne.strip()):
                break
            texte = re.sub(r"<[^>]+>", "", ligne).replace("\xa0", " ").replace("&nbsp;", " ").strip()
            if texte:
                type_pub = texte
                break

        lien = match.group("lien")
        lien_full = f"https://www.ejustice.just.fgov.be{lien}" if lien else None

        publications.append({
            "date":      match.group("date"),
            "numac":     match.group("numac").strip().replace(".", ""),
            "type":      type_pub,
            "lien_pdf":  lien_full,
        })

    return publications


# ── Téléchargement ────────────────────────────────────────────────────────────

def download_publication_pdf(lien_pdf: str, session: _Session | None = None) -> bytes | None:
    """
    Télécharge le PDF d'une publication eJustice depuis son lien IMAGE.
    Retourne les bytes ou None.
    """
    if not lien_pdf:
        return None

    log.debug("[ejustice] download_publication_pdf → %s", lien_pdf[:80])
    sess = session or _get_default()

    try:
        r = sess.get(lien_pdf, timeout=EJUSTICE_TIMEOUT * 2)
    except requests.RequestException as exc:
        log.warning("[ejustice] download_pdf → exception : %s", exc)
        return None

    if not r.ok:
        log.debug("[ejustice] download_pdf → HTTP %s", r.status_code)
        return None

    ct = r.headers.get("content-type", "")
    if "pdf" not in ct.lower() and len(r.content) < 500:
        log.debug("[ejustice] download_pdf → contenu invalide (ct=%s, %d o)", ct, len(r.content))
        return None

    log.debug("[ejustice] download_pdf → %d o", len(r.content))
    return r.content


# ── Itérateur haut niveau ─────────────────────────────────────────────────────

def iter_publications(bce_num_clean: str, session: _Session | None = None) -> Iterator[dict]:
    """Générateur : yield chaque publication avec son contenu bytes ou None."""
    pubs = get_publications(bce_num_clean, session=session)
    for pub in pubs:
        if pub.get("lien_pdf"):
            content = download_publication_pdf(pub["lien_pdf"], session=session)
            time.sleep(EJUSTICE_DELAY)
        else:
            content = None
        yield {**pub, "content": content}
