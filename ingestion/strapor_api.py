"""
Client Strapor (statuts.notaire.be) — adapté de strapor.py pour le pipeline Airflow.

Différences vs strapor.py :
  • download_statute_pdf() retourne des bytes (→ HDFS) au lieu d'un Path local
  • COOKIE_FILE / TMP_PDFS proviennent de config.py
  • get_all_statutes() est un générateur (yield par statut → streaming HDFS)
  • Pas de bloc __main__ ; Playwright n'est appelé que si les cookies ont expiré
    (dans un Airflow Dockerisé sans Chrome, cette étape doit être faite sur l'hôte
     et les cookies montés via le volume notaire_cookies.json)
"""
import json
import logging
import time
from pathlib import Path
from typing import Iterator

import requests

from ingestion.config import (
    STRAPOR_BASE, STRAPOR_COOKIE_FILE,
    STRAPOR_PAGE_SIZE, STRAPOR_DELAY, STRAPOR_TIMEOUT,
    STRAPOR_NO_NOTAIRE,
)

log = logging.getLogger(__name__)

HEADERS_API = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
}

# BCE de référence pour valider la session
_SEED_BCE = "0836157420"


# ── Session / cookies ─────────────────────────────────────────────────────────

def _fetch_cookies_via_playwright() -> list[dict]:
    """
    Ouvre Chrome (visible ~3 s) pour obtenir les cookies anti-bot F5.
    Requiert Playwright + Chrome installés sur la machine hôte.
    Dans un conteneur Airflow headless, monter notaire_cookies.json manuellement.
    """
    from playwright.sync_api import sync_playwright

    seed_url = (
        f"{STRAPOR_BASE}/enterprise/{_SEED_BCE}/statutes"
        f"?enterpriseNumber={_SEED_BCE}&statuteStart=0&statuteCount=5"
    )
    log.info("[strapor] Ouverture Chrome pour renouveler les cookies F5…")

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=False)
        except Exception:
            log.warning("[strapor] Chrome introuvable — fallback Chromium")
            browser = p.chromium.launch(headless=False)

        ctx = browser.new_context(
            locale="fr-BE",
            user_agent=HEADERS_API["User-Agent"],
        )
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page.goto("https://statuts.notaire.be/", wait_until="load", timeout=20_000)
        page.wait_for_timeout(2_000)
        page.goto(seed_url, wait_until="load", timeout=30_000)

        for i in range(40):
            names = {c["name"] for c in ctx.cookies()}
            if "OClmoOot" in names and "Lyp1CWKh" in names:
                log.info("[strapor] Cookies F5 obtenus (%d ms)", i * 500)
                break
            page.wait_for_timeout(500)
        else:
            log.warning("[strapor] Timeout cookies — présents : %s",
                        [c["name"] for c in ctx.cookies()])

        cookies = ctx.cookies()
        browser.close()

    return cookies


def _build_session(cookies: list[dict]) -> requests.Session:
    sess = requests.Session()
    sess.headers.update(HEADERS_API)
    for c in cookies:
        sess.cookies.set(c["name"], c["value"], domain=c["domain"])
    return sess


def _session_valid(sess: requests.Session) -> bool:
    """Ping rapide sur un exemple connu — True si JSON reçu."""
    try:
        r = sess.get(
            f"{STRAPOR_BASE}/api/enterprises/{_SEED_BCE}/statutes",
            params={"offset": 0, "limit": 1},
            timeout=10,
        )
        return "application/json" in r.headers.get("content-type", "")
    except Exception:
        return False


def get_session(cookie_file: str | None = None) -> requests.Session:
    """
    Retourne une session requests valide pour l'API Strapor.
    Lit d'abord le fichier de cookies ; si invalide/absent, lance Playwright.
    """
    cookie_path = Path(cookie_file or STRAPOR_COOKIE_FILE)

    if cookie_path.exists():
        try:
            cookies = json.loads(cookie_path.read_text(encoding="utf-8"))
            sess = _build_session(cookies)
            if _session_valid(sess):
                log.info("[strapor] Session OK (cookies en cache : %s)", cookie_path)
                return sess
            log.info("[strapor] Cookies expirés — renouvellement Playwright…")
        except Exception as exc:
            log.warning("[strapor] Lecture cookies échouée : %s — renouvellement", exc)
    else:
        log.info("[strapor] Pas de fichier cookies (%s) — lancement Playwright", cookie_path)

    cookies = _fetch_cookies_via_playwright()
    cookie_path.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
    log.info("[strapor] Cookies sauvegardés → %s", cookie_path)
    return _build_session(cookies)


# ── Récupération des statuts ──────────────────────────────────────────────────

def get_statutes(sess: requests.Session, enterprise_number: str) -> list[dict]:
    """
    Récupère tous les statuts DONE pour un numéro d'entreprise (sans points).
    """
    url    = f"{STRAPOR_BASE}/api/enterprises/{enterprise_number}/statutes"
    referer = (
        f"{STRAPOR_BASE}/enterprise/{enterprise_number}/statutes"
        f"?enterpriseNumber={enterprise_number}&statuteStart=0&statuteCount=5"
    )
    sess.headers["Referer"] = referer

    all_statutes, offset = [], 0
    log.debug("[strapor] get_statutes [%s]", enterprise_number)

    while True:
        try:
            r = sess.get(
                url,
                params={"deedDate": "", "offset": offset, "limit": STRAPOR_PAGE_SIZE},
                timeout=STRAPOR_TIMEOUT,
            )
            r.raise_for_status()
        except requests.RequestException as exc:
            log.warning("[strapor] [%s] offset=%d — erreur réseau : %s", enterprise_number, offset, exc)
            break

        if "application/json" not in r.headers.get("content-type", ""):
            log.error("[strapor] [%s] Réponse non-JSON — session expirée mid-run", enterprise_number)
            break

        data  = r.json()
        batch = data.get("statutes", [])
        total = data.get("totalItems", 0)
        all_statutes.extend(batch)
        log.debug("  [%s] offset=%d — %d/%d statuts", enterprise_number, offset, len(all_statutes), total)

        if not batch or len(all_statutes) >= total:
            break
        offset += STRAPOR_PAGE_SIZE
        time.sleep(STRAPOR_DELAY)

    done = [s for s in all_statutes if s.get("documentStatus") == "DONE"]
    log.info("[strapor] [%s] → %d statuts DONE (sur %d)", enterprise_number, len(done), len(all_statutes))
    return done


def download_statute_bytes(sess: requests.Session,
                            enterprise_number: str,
                            statute: dict) -> tuple[bytes | None, str, str]:
    """
    Télécharge un statut PDF et retourne (contenu_bytes, doc_id, deed_date).
    Retourne (None, doc_id, deed_date) si indisponible.
    Contrairement à strapor.py, ici on retourne des bytes (→ upload HDFS direct).
    """
    doc_id    = statute["documentId"]
    deed_date = statute.get("deedDate", "unknown").replace("-", "")

    url = f"{STRAPOR_BASE}/api/enterprises/{enterprise_number}/statutes/non-certified/{doc_id}"
    log.debug("[strapor] [%s] téléchargement doc_id=%s (date=%s)", enterprise_number, doc_id, deed_date)

    try:
        r = sess.get(url, timeout=STRAPOR_TIMEOUT)
    except requests.RequestException as exc:
        log.warning("[strapor] [%s] doc_id=%s — exception réseau : %s", enterprise_number, doc_id, exc)
        return None, doc_id, deed_date

    if r.status_code == 404:
        log.debug("[strapor] [%s] doc_id=%s → 404", enterprise_number, doc_id)
        return None, doc_id, deed_date

    if not r.ok:
        log.warning("[strapor] [%s] doc_id=%s → HTTP %s", enterprise_number, doc_id, r.status_code)
        return None, doc_id, deed_date

    if "pdf" not in r.headers.get("content-type", "").lower() or len(r.content) < 1_000:
        log.debug("[strapor] [%s] doc_id=%s → pas un PDF valide (%d o)", enterprise_number, doc_id, len(r.content))
        return None, doc_id, deed_date

    log.debug("[strapor] [%s] doc_id=%s → %d o", enterprise_number, doc_id, len(r.content))
    return r.content, doc_id, deed_date


# ── Itérateur haut niveau ─────────────────────────────────────────────────────

def iter_statutes(sess: requests.Session,
                  enterprise_number: str) -> Iterator[tuple[bytes | None, str, str]]:
    """
    Générateur : pour chaque statut DONE d'une entreprise, yield
    (contenu_bytes_ou_None, doc_id, deed_date).
    """
    statutes = get_statutes(sess, enterprise_number)
    for statute in statutes:
        yield download_statute_bytes(sess, enterprise_number, statute)
        time.sleep(STRAPOR_DELAY)


# ── Filtrage ──────────────────────────────────────────────────────────────────

def needs_notaire_check(juridical_form: str) -> bool:
    """Retourne True si la forme juridique implique un passage chez notaire."""
    return str(juridical_form) not in STRAPOR_NO_NOTAIRE
