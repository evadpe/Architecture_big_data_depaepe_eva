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
HDFS_BRONZE_STRAPOR  = "/bronze/strapor/{bce}"
HDFS_BRONZE_EJUSTICE = "/bronze/ejustice/{bce}"

# ── KBO CSV local ─────────────────────────────────────────────────────────────
KBO_DATA_DIR   = os.getenv("KBO_DATA_DIR", "/opt/airflow/données")
KBO_CHUNK_SIZE = 50_000

# ── Tor pool (proxies SOCKS5 anti-429) ───────────────────────────────────────
# Adresses vues depuis le réseau Docker bce_network
TOR_PROXIES = [
    {"socks": "socks5h://tor1:9050", "control_host": "tor1", "control_port": 9051},
    {"socks": "socks5h://tor2:9050", "control_host": "tor2", "control_port": 9051},
    {"socks": "socks5h://tor3:9050", "control_host": "tor3", "control_port": 9051},
]
TOR_CONTROL_PASSWORD = os.getenv("TOR_CONTROL_PASSWORD", "mypass")
TOR_NEWNYM_WAIT      = 10    # secondes d'attente après SIGNAL NEWNYM
TOR_MAX_RETRIES      = 6    # tentatives max avant abandon (2 tours complets)
TOR_BACKOFF_BASE     = 5    # secondes de base entre deux retries

# ── NBB / CBSO ────────────────────────────────────────────────────────────────
CBSO_API         = "https://consult.cbso.nbb.be/api"
CBSO_PAGE_SIZE   = 50
CBSO_DELAY       = 2.0   # 2s entre requêtes — évite le rate limit NBB (0.5s le déclenchait)
CBSO_TIMEOUT_CSV = 60
CBSO_TIMEOUT_PDF = 90

# PDFs accessibles publiquement — le notebook les a téléchargés sans auth.
# Le 403 actuel = rate limit temporaire (trop de requêtes en rafale), pas un blocage permanent.
ENABLE_NBB_PDF   = True

NBB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BCE-pipeline/1.0; research)",
    "Accept":     "application/json, */*",
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
ANNEE_MIN_RECENT  = 2020
ANNEE_MAX_RECENT  = 2025
# DAG non-prioritaire : tout ce qui est < 2020  (aucune borne inférieure)
ANNEE_MAX_HISTORIC = 2019   # inclus ; pas de borne basse (on prend tout)

# ── Logs progressifs ──────────────────────────────────────────────────────────
BATCH_LOG_EVERY = 1_000
