"""Centralise tous les paramètres de connexion et chemins du pipeline BCE."""
import os

# ── MongoDB — données structurées (bce_db) ────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB  = os.getenv("MONGO_DB",  "bce_db")

# Collections KBO (une par fichier source)
COL_ENTERPRISES    = "kbo_enterprises"
COL_DENOMINATIONS  = "kbo_denominations"
COL_ADDRESSES      = "kbo_addresses"
COL_ACTIVITIES     = "kbo_activities"
COL_CONTACTS       = "kbo_contacts"
COL_ESTABLISHMENTS = "kbo_establishments"
COL_CODES          = "kbo_codes"
COL_BRANCHES       = "kbo_branches"

# Collections données téléchargées
COL_NBB_ACCOUNTS        = "nbb_accounts"         # comptes annuels parsés (codes→valeurs)
COL_STRAPOR_STATUTES    = "strapor_statutes"      # métadonnées statuts notaire
COL_EJUSTICE_PUBS       = "ejustice_publications" # métadonnées publications légales

# ── MongoDB — State DB isolée (bce_state_db) ─────────────────────────────────
MONGO_STATE_URI = os.getenv("MONGO_STATE_URI", "mongodb://mongo_state:27017/")
MONGO_STATE_DB  = os.getenv("MONGO_STATE_DB",  "bce_state_db")
COL_STATE       = "download_state"

# ── HDFS (WebHDFS REST) ───────────────────────────────────────────────────────
HDFS_URL  = os.getenv("HDFS_URL",  "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

HDFS_BRONZE_NBB_CSVS = "/bronze/nbb/csvs/{bce}"
HDFS_BRONZE_NBB_PDFS = "/bronze/nbb/pdfs/{bce}"
HDFS_SILVER_NBB_CSVS = "/silver/nbb/hebergement/csvs/{bce}"
HDFS_SILVER_NBB_PDFS = "/silver/nbb/hebergement/pdfs/{bce}"
HDFS_BRONZE_STRAPOR          = "/bronze/strapor/{bce}"
HDFS_BRONZE_EJUSTICE         = "/bronze/ejustice/{bce}"
HDFS_SILVER_STRAPOR_HEBERG   = "/silver/strapor/hebergement/{bce}"
HDFS_SILVER_EJUSTICE_HEBERG  = "/silver/ejustice/hebergement/{bce}"

# ── KBO CSV local ─────────────────────────────────────────────────────────────
KBO_DATA_DIR   = os.getenv("KBO_DATA_DIR", "/opt/airflow/données")
KBO_CHUNK_SIZE = 50_000

# ── Tor pool (proxies SOCKS5 anti-429) ───────────────────────────────────────
# Adresses vues depuis le réseau Docker bce_network
TOR_PROXIES = [
    {"socks": "socks5h://tor1:9050", "control_host": "tor1", "control_port": 9051},
    {"socks": "socks5h://tor2:9050", "control_host": "tor2", "control_port": 9051},
    {"socks": "socks5h://tor3:9050", "control_host": "tor3", "control_port": 9051},
    {"socks": "socks5h://tor4:9050", "control_host": "tor4", "control_port": 9051},
    {"socks": "socks5h://tor5:9050", "control_host": "tor5", "control_port": 9051},
    {"socks": "socks5h://tor6:9050", "control_host": "tor6", "control_port": 9051},
]
TOR_CONTROL_PASSWORD = os.getenv("TOR_CONTROL_PASSWORD", "mypass")
TOR_NEWNYM_WAIT      = 10    # secondes d'attente après SIGNAL NEWNYM
TOR_MAX_RETRIES      = 6    # tentatives max avant abandon (2 tours complets)
TOR_BACKOFF_BASE     = 5    # secondes de base entre deux retries

# ── NBB / CBSO ────────────────────────────────────────────────────────────────
CBSO_API         = "https://consult.cbso.nbb.be/api"
CBSO_PAGE_SIZE   = 50
CBSO_DELAY       = 1.0   # 1s entre requêtes — 4 shards × 6 proxies → ~40 req/min/IP
CBSO_TIMEOUT_CSV = 60
CBSO_TIMEOUT_PDF = 90

# PDFs accessibles publiquement MAIS l'endpoint /deposits/pdf/ bloque les Tor exit nodes.
# → CSVs via Tor (anti-429), PDFs via connexion directe (Origin/Referer suffisent).
ENABLE_NBB_PDF     = False  # NBB PDF servers en erreur 500/502 — réactiver quand stable
PDF_USE_TOR        = False   # False = connexion directe pour les PDFs

NBB_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "Origin":          "https://consult.cbso.nbb.be",
    "Referer":         "https://consult.cbso.nbb.be/",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
}

# ── eJustice ──────────────────────────────────────────────────────────────────
EJUSTICE_URL     = "https://www.ejustice.just.fgov.be/cgi_tsv/article.pl"
EJUSTICE_DELAY   = 0.8
EJUSTICE_TIMEOUT = 25

# ── STRAPOR (Notaire) ─────────────────────────────────────────────────────────
STRAPOR_BASE         = "https://statuts.notaire.be/stapor_v1"
STRAPOR_COOKIE_FILE  = os.getenv("STRAPOR_COOKIE_FILE", "/opt/airflow/notaire_cookies.json")
STRAPOR_PAGE_SIZE    = 20
STRAPOR_DELAY        = 0.4
STRAPOR_TIMEOUT      = 30
STRAPOR_NO_NOTAIRE   = {"009", "017", "018", "025", "026", "027", "051", "052"}

# ── Fenêtres fiscales NBB ────────────────────────────────────────────────────
# DAG prioritaire  : 2020 → 2025  (5 dernières années, à finir en ~3 jours)
ANNEE_MIN_RECENT  = 2021
ANNEE_MAX_RECENT  = 2025
# DAG non-prioritaire : tout ce qui est < 2020  (aucune borne inférieure)
ANNEE_MAX_HISTORIC = 2019   # inclus ; pas de borne basse (on prend tout)

# ── Logs progressifs ──────────────────────────────────────────────────────────
BATCH_LOG_EVERY = 1_000
