"""
Pool de proxies Tor avec rotation automatique anti-429.

Stratégie :
  • Round-robin entre tor1 / tor2 / tor3 sur chaque requête
  • Sur HTTP 429 ou connexion refusée :
      1. Renouvellement de circuit (SIGNAL NEWNYM sur le proxy courant)
      2. Rotation immédiate vers le proxy suivant
      3. Backoff exponentiel si plusieurs 429 consécutifs
  • Thread-safe (Lock par proxy pour le renouvellement de circuit)

Usage :
    from ingestion.tor_session import TorSession
    sess = TorSession()
    resp = sess.get("https://example.com", timeout=15)
"""
import logging
import socket
import threading
import time
from itertools import cycle
from typing import Any

import requests

from ingestion.config import (
    TOR_PROXIES, TOR_CONTROL_PASSWORD,
    TOR_NEWNYM_WAIT, TOR_MAX_RETRIES, TOR_BACKOFF_BASE,
)

log = logging.getLogger(__name__)


class TorSession:
    """
    Session requests utilisant un pool de proxies Tor avec rotation.
    Hérite du comportement de requests.Session (headers, cookies, etc.)
    via composition (délégation vers l'instance active).
    """

    def __init__(self, headers: dict | None = None):
        self._proxies   = TOR_PROXIES
        self._cycle     = cycle(range(len(self._proxies)))
        self._current   = next(self._cycle)
        self._locks     = [threading.Lock() for _ in self._proxies]
        self._sessions  = [self._make_session(p, headers) for p in self._proxies]
        log.info("[tor] Pool initialisé — %d proxies", len(self._proxies))

    # ── Interface publique (get / post) ───────────────────────────────────────

    def get(self, url: str, **kwargs) -> requests.Response:
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        return self._request("POST", url, **kwargs)

    def update_headers(self, headers: dict) -> None:
        for sess in self._sessions:
            sess.headers.update(headers)

    # ── Logique de retry / rotation ───────────────────────────────────────────

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        backoff = TOR_BACKOFF_BASE

        for attempt in range(1, TOR_MAX_RETRIES + 1):
            proxy_cfg = self._proxies[self._current]
            sess      = self._sessions[self._current]
            proxy_tag = f"tor{self._current + 1}"

            try:
                resp = sess.request(method, url, **kwargs)

                if resp.status_code == 429:
                    log.warning(
                        "[tor] %s 429 sur %s (tentative %d/%d) — rotation + NEWNYM",
                        proxy_tag, url[:60], attempt, TOR_MAX_RETRIES,
                    )
                    self._renew_circuit(self._current)
                    self._rotate()
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 120)
                    continue

                # Succès ou erreur HTTP normale (4xx/5xx non-429)
                log.debug("[tor] %s %s %s → HTTP %s", proxy_tag, method, url[:60], resp.status_code)
                return resp

            except (requests.ConnectionError, requests.Timeout) as exc:
                log.warning(
                    "[tor] %s connexion échouée vers %s (tentative %d/%d) : %s",
                    proxy_tag, url[:60], attempt, TOR_MAX_RETRIES, exc,
                )
                self._rotate()
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)

        raise RuntimeError(
            f"[tor] {TOR_MAX_RETRIES} tentatives épuisées pour {url[:80]}"
        )

    def _rotate(self) -> None:
        """Passe au proxy suivant dans le cycle."""
        self._current = next(self._cycle)
        log.debug("[tor] Rotation → tor%d", self._current + 1)

    def _renew_circuit(self, proxy_idx: int) -> None:
        """
        Envoie SIGNAL NEWNYM au port de contrôle Tor pour obtenir un nouveau circuit.
        Attend TOR_NEWNYM_WAIT secondes (délai minimum Tor entre deux NEWNYM).
        Thread-safe via lock par proxy.
        """
        proxy = self._proxies[proxy_idx]
        host  = proxy["control_host"]
        port  = proxy["control_port"]

        if not self._locks[proxy_idx].acquire(blocking=False):
            # Un autre thread renouvelle déjà ce circuit — on attend juste
            log.debug("[tor] tor%d NEWNYM déjà en cours — skip", proxy_idx + 1)
            return

        try:
            log.info("[tor] tor%d SIGNAL NEWNYM → %s:%d", proxy_idx + 1, host, port)
            with socket.create_connection((host, port), timeout=8) as s:
                s.sendall(f'AUTHENTICATE "{TOR_CONTROL_PASSWORD}"\r\n'.encode())
                resp = s.recv(256)
                if b"250" not in resp:
                    log.warning("[tor] tor%d AUTH échoué : %s", proxy_idx + 1, resp[:80])
                    return
                s.sendall(b"SIGNAL NEWNYM\r\n")
                resp = s.recv(256)
                if b"250" in resp:
                    log.info("[tor] tor%d nouveau circuit demandé — attente %ds", proxy_idx + 1, TOR_NEWNYM_WAIT)
                    time.sleep(TOR_NEWNYM_WAIT)
                else:
                    log.warning("[tor] tor%d NEWNYM réponse inattendue : %s", proxy_idx + 1, resp[:80])
        except Exception as exc:
            log.warning("[tor] tor%d renew_circuit échoué : %s", proxy_idx + 1, exc)
        finally:
            self._locks[proxy_idx].release()

    # ── Création de sessions ──────────────────────────────────────────────────

    @staticmethod
    def _make_session(proxy_cfg: dict, headers: dict | None) -> requests.Session:
        sess = requests.Session()
        proxy_url = proxy_cfg["socks"]
        sess.proxies = {"http": proxy_url, "https": proxy_url}
        if headers:
            sess.headers.update(headers)
        return sess
