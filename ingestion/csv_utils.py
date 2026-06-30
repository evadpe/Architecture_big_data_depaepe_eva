"""
Utilitaires CSV haute performance pour les fichiers KBO triés.

• bce_bisect()   : recherche binaire O(log n) sur un CSV trié par numéro BCE
• ChunkReader    : lecture vectorisée avec to_dict('records') — ~10× plus vite qu'apply(axis=1)

Les CSVs KBO (enterprise, denomination, address, activity, contact, establishment)
sont tous triés par EntityNumber / EnterpriseNumber → recherche binaire applicable.
"""
import csv
import io
import logging
import os
from typing import Iterator

import pandas as pd

log = logging.getLogger(__name__)


# ── Recherche binaire ──────────────────────────────────────────────────────────

def bce_bisect(filepath: str, target: str, key_col: int = 0,
               encoding: str = "utf-8") -> int:
    """
    Recherche binaire dans un CSV trié par numéro BCE.
    Retourne le byte offset du début de la première ligne dont la clé >= target.
    Complexité : O(log(taille_fichier)) — indépendant du nombre de lignes.

    Args:
        filepath  : chemin du fichier CSV
        target    : numéro BCE recherché (ex. "0400.000.000")
        key_col   : index de la colonne-clé (0 = première colonne)
        encoding  : encodage du fichier

    Returns:
        offset en octets à passer à seek() avant pd.read_csv()
    """
    file_size = os.path.getsize(filepath)

    with open(filepath, "rb") as f:
        # Sauter l'en-tête (1ère ligne)
        header_end = len(f.readline())
        lo, hi = header_end, file_size

        iterations = 0
        while lo < hi:
            mid = (lo + hi) // 2
            f.seek(mid)

            if mid > header_end:
                f.readline()   # aligner sur le début de la prochaine ligne

            pos  = f.tell()
            line = f.readline()

            if not line or pos >= file_size:
                hi = mid
                continue

            key = _csv_key(line.decode(encoding, errors="replace"), key_col)
            iterations += 1

            if key < target:
                lo = pos
            else:
                hi = mid

    log.debug("bce_bisect(%s, %s) → offset=%d (%d itérations)", filepath, target, lo, iterations)
    return lo


def bce_bisect_end(filepath: str, target_exclusive: str, key_col: int = 0,
                   encoding: str = "utf-8") -> int:
    """
    Retourne le byte offset de la première ligne dont la clé >= target_exclusive.
    Utile pour définir la fin d'un shard : lire de bisect(start) à bisect(end).
    """
    return bce_bisect(filepath, target_exclusive, key_col, encoding)


def _csv_key(line: str, col: int) -> str:
    """Extrait la valeur de la colonne `col` d'une ligne CSV (gère les guillemets)."""
    try:
        row = next(csv.reader(io.StringIO(line.strip())))
        return row[col].strip('"') if col < len(row) else ""
    except StopIteration:
        return ""


# ── Lecture vectorisée avec recherche binaire ──────────────────────────────────

def read_csv_range(
    filepath: str,
    col_mapping: dict[str, str],
    bce_key_col: str,
    bce_start: str | None = None,
    bce_end: str | None = None,
    chunk_size: int = 50_000,
    extra_cols: dict | None = None,
) -> Iterator[list[dict]]:
    """
    Lit un CSV KBO trié par chunks vectorisés, en utilisant la recherche binaire
    pour positionner le curseur directement sur la plage BCE [bce_start, bce_end[.

    Args:
        filepath    : chemin du CSV
        col_mapping : {nom_colonne_source: nom_champ_mongo}
                      ex. {"EntityNumber": "entity_number", "NaceCode": "nace_code"}
        bce_key_col : nom de la colonne clé BCE dans le CSV (pour le filtre de range)
        bce_start   : premier numéro BCE à inclure (None = depuis le début)
        bce_end     : premier numéro BCE à exclure (None = jusqu'à la fin)
        chunk_size  : nombre de lignes par chunk
        extra_cols  : {nom_col_source: valeur_fixe} — colonnes ajoutées à chaque doc

    Yields:
        liste de dicts prêts pour MongoDB bulk_write (1 yield = 1 chunk)
    """
    # ── 1. Calculer les offsets via recherche binaire ──────────────────────────
    key_col_idx = list(col_mapping.keys()).index(bce_key_col) if bce_key_col in col_mapping else 0

    skipbytes = 0
    if bce_start is not None:
        skipbytes = bce_bisect(filepath, bce_start, key_col_idx)
        log.info("  bce_bisect → offset %d pour BCE >= %s", skipbytes, bce_start)

    # ── 2. Lire le CSV depuis l'offset trouvé ─────────────────────────────────
    # pd.read_csv n'accepte pas skipbytes directement → on ouvre le fichier,
    # on seek, et on passe le file object à pd.read_csv
    src_cols  = list(col_mapping.keys())
    dest_cols = list(col_mapping.values())
    nan_str   = {c: "" for c in dest_cols}   # remplace NaN par ""

    with open(filepath, "rb") as raw:
        # Lire l'en-tête séparément pour pandas
        header = raw.readline()
        # csv.reader gère correctement les guillemets et le \n final
        header_cols = next(csv.reader(io.StringIO(header.decode("utf-8"))))

        if skipbytes > len(header):
            raw.seek(skipbytes)

        reader = pd.read_csv(
            raw,
            dtype=str,
            names=header_cols,    # on fournit les noms de colonnes
            header=None,          # pas d'en-tête dans le flux restant
            chunksize=chunk_size,
            usecols=src_cols,
            engine="c",
            na_filter=True,
        )

        for chunk in reader:
            # ── Filtre plage BCE [bce_start, bce_end[ ─────────────────────
            # La bisect peut atterrir légèrement avant la frontière → post-filtre
            if bce_start is not None:
                chunk = chunk[chunk[bce_key_col] >= bce_start]
            if bce_end is not None:
                chunk = chunk[chunk[bce_key_col] < bce_end]
                if chunk.empty:
                    break

            # ── Filtre regex BCE (vectorisé) ───────────────────────────────
            mask  = chunk[bce_key_col].fillna("").str.match(r"^\d{4}\.\d{3}\.\d{3}$")
            chunk = chunk[mask]
            if chunk.empty:
                continue

            # ── Renommage + nettoyage vectorisé ───────────────────────────
            chunk = chunk.rename(columns=col_mapping).fillna(nan_str)
            # Remplacer les strings "nan" résiduelles
            chunk = chunk.replace("nan", "")

            # ── Ajout de colonnes fixes ────────────────────────────────────
            if extra_cols:
                for col, val in extra_cols.items():
                    chunk[col] = val

            # ── Conversion en dicts (C-level, ~10× plus rapide qu'apply) ──
            docs = chunk.to_dict("records")

            if docs:
                yield docs


# ── Shards BCE par préfixe ────────────────────────────────────────────────────

# Découpage en 4 shards couvrant 0-9 de façon équilibrée
# (les numéros BCE belges commencent presque tous par 0, 1 ou 2)
BCE_SHARDS = [
    ("0000.000.000", "0700.000.000"),   # shard 0 : ~900k entreprises
    ("0700.000.000", None),             # shard 1 : ~1M entreprises
    # Shards 2 et 3 non utilisés en mode laptop (2 shards max)
    ("0000.000.000", "0400.000.000"),
    ("0400.000.000", "0700.000.000"),
]


def get_shard_range(shard_idx: int) -> tuple[str | None, str | None]:
    """Retourne (bce_start, bce_end) pour un shard donné."""
    return BCE_SHARDS[shard_idx]
