"""
Utilitaires HDFS via WebHDFS REST (pas de dépendance au client Java).
Utilise requests directement sur l'API WebHDFS du NameNode.
"""
import logging
import time
from urllib.parse import quote as urlquote

import requests

from ingestion.config import HDFS_URL, HDFS_USER

log = logging.getLogger(__name__)

_WEBHDFS = f"{HDFS_URL}/webhdfs/v1"
_TIMEOUT_META  = 15
_TIMEOUT_DATA  = 120


def _params(**extra) -> dict:
    return {"user.name": HDFS_USER, **extra}


# ── Vérification / création de répertoire ─────────────────────────────────────

def makedirs(hdfs_dir: str) -> None:
    """Crée un répertoire HDFS (équivalent mkdir -p). Idempotent."""
    url = f"{_WEBHDFS}{hdfs_dir}"
    r = requests.put(url, params=_params(op="MKDIRS", permission="755"), timeout=_TIMEOUT_META)
    if not r.ok:
        log.warning("makedirs %s → HTTP %s: %s", hdfs_dir, r.status_code, r.text[:200])
    else:
        log.debug("makedirs OK: %s", hdfs_dir)


def exists(hdfs_path: str) -> bool:
    """Retourne True si le fichier/répertoire existe sur HDFS."""
    url = f"{_WEBHDFS}{hdfs_path}"
    r = requests.get(url, params=_params(op="GETFILESTATUS"), timeout=_TIMEOUT_META)
    return r.status_code == 200


def file_size(hdfs_path: str) -> int | None:
    """Retourne la taille en octets d'un fichier HDFS, ou None s'il n'existe pas."""
    url = f"{_WEBHDFS}{hdfs_path}"
    r = requests.get(url, params=_params(op="GETFILESTATUS"), timeout=_TIMEOUT_META)
    if r.ok:
        return r.json()["FileStatus"]["length"]
    return None


# ── Écriture ──────────────────────────────────────────────────────────────────

def upload_bytes(content: bytes, hdfs_path: str, overwrite: bool = False) -> int:
    """
    Écrit `content` sur HDFS à `hdfs_path`.
    Utilise le flux de redirection WebHDFS (PUT → redirect → DataNode).
    Retourne le nombre d'octets écrits.
    Lève requests.HTTPError en cas d'échec.
    """
    if not overwrite and exists(hdfs_path):
        sz = file_size(hdfs_path) or 0
        log.debug("upload_bytes — fichier déjà présent (%d o) : %s", sz, hdfs_path)
        return sz

    # Étape 1 : créer le répertoire parent
    parent = "/".join(hdfs_path.split("/")[:-1])
    makedirs(parent)

    # Étape 2 : initier le CREATE (NameNode renvoie une URL DataNode)
    url = f"{_WEBHDFS}{hdfs_path}"
    params = _params(op="CREATE", overwrite="true" if overwrite else "false", replication=1)
    r1 = requests.put(url, params=params, allow_redirects=False, timeout=_TIMEOUT_META)

    if r1.status_code == 307:
        # Redirection vers DataNode
        datanode_url = r1.headers["Location"]
        r2 = requests.put(datanode_url, data=content, timeout=_TIMEOUT_DATA)
        r2.raise_for_status()
    elif r1.status_code == 201:
        pass  # Certaines configs acceptent la requête directe
    else:
        r1.raise_for_status()

    n = len(content)
    log.debug("upload_bytes OK — %d o → %s", n, hdfs_path)
    return n


def upload_bytes_retry(content: bytes, hdfs_path: str, retries: int = 3, delay: float = 2.0) -> int:
    """Wrapper upload_bytes avec retry exponentiel."""
    for attempt in range(1, retries + 1):
        try:
            return upload_bytes(content, hdfs_path, overwrite=True)
        except Exception as exc:
            if attempt == retries:
                raise
            log.warning("upload_bytes tentative %d/%d échouée (%s) — retry dans %.0fs",
                        attempt, retries, exc, delay)
            time.sleep(delay)
            delay *= 2
    return 0  # jamais atteint


# ── Lecture (pour diagnostics) ────────────────────────────────────────────────

def read_bytes(hdfs_path: str) -> bytes:
    """Lit un fichier HDFS et retourne son contenu brut."""
    url = f"{_WEBHDFS}{hdfs_path}"
    r = requests.get(url, params=_params(op="OPEN"), allow_redirects=True, timeout=_TIMEOUT_DATA)
    r.raise_for_status()
    return r.content


def list_dir(hdfs_dir: str) -> list[str]:
    """Retourne la liste des noms de fichiers dans un répertoire HDFS."""
    url = f"{_WEBHDFS}{hdfs_dir}"
    r = requests.get(url, params=_params(op="LISTSTATUS"), timeout=_TIMEOUT_META)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return [fs["pathSuffix"] for fs in r.json()["FileStatuses"]["FileStatus"]]
