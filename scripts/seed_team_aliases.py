"""
seed_team_aliases.py
────────────────────
Erzeugt Standard-Aliase für vorhandene Teams in public.teams
und schreibt sie idempotent nach public.team_aliases.

Ziele:
- Self alias: kanonischer Name selbst
- generische Kurzformen:
  - Entfernen von Präfixen wie FC, AFC, CF, RC, SSC, US, GD, ACF, SV, SC
  - Entfernen führender Nummern wie "1. FC", "1. FSV"
  - Entfernen von Jahreszahlen wie 1909 / 1910 / 1846
- einige wenige fußballspezifische Sonderfälle:
  - FC Internazionale Milano -> Inter Milan / Inter
  - Athletic Club -> Athletic Bilbao
  - RC Celta de Vigo -> Celta Vigo / Celta
  - Real Betis Balompié -> Real Betis
  - GD Estoril Praia -> Estoril
  - Sporting Clube de Braga -> Braga
  - US Lecce -> Lecce
  - TSG 1899 Hoffenheim -> Hoffenheim
  - etc.

Environment:
  DATABASE_URL

Usage:
  python scripts/seed_team_aliases.py
  python scripts/seed_team_aliases.py --dry-run
"""

import argparse
import logging
import os
import re
import unicodedata
from typing import Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ["DATABASE_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("seed_team_aliases")


# ─────────────────────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────────────────────

GENERIC_REPLACEMENTS = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
}


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""

    value = value.strip().lower()
    for src, dst in GENERIC_REPLACEMENTS.items():
        value = value.replace(src, dst)

    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.replace("/", " ").replace("-", " ")
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


# ─────────────────────────────────────────────────────────────
# TEAM NAME RULES
# ─────────────────────────────────────────────────────────────

LEADING_PREFIX_PATTERNS = [
    r"^1\s+fc\s+",
    r"^1\s+fsv\s+",
    r"^fc\s+",
    r"^afc\s+",
    r"^cf\s+",
    r"^rc\s+",
    r"^acf\s+",
    r"^gd\s+",
    r"^us\s+",
    r"^ssc\s+",
    r"^sv\s+",
    r"^sc\s+",
    r"^fk\s+",
    r"^sk\s+",
    r"^tsg\s+",
    r"^vfl\s+",
    r"^vfb\s+",
]

TRAILING_SUFFIX_PATTERNS = [
    r"\s+fc$",
    r"\s+afc$",
    r"\s+cf$",
    r"\s+ac$",
    r"\s+bc$",
    r"\s+sk$",
    r"\s+fk$",
]

YEAR_PATTERNS = [
    r"\b18\d{2}\b",
    r"\b19\d{2}\b",
    r"\b20\d{2}\b",
]

# Kleine, fokussierte Sonderfallliste für echte Fußballvarianten
SPECIAL_ALIASES: Dict[str, List[str]] = {
    "fc internazionale milano": ["inter milan", "inter"],
    "internazionale milano": ["inter milan", "inter"],
    "athletic club": ["athletic bilbao"],
    "rc celta de vigo": ["celta vigo", "celta"],
    "real betis balompie": ["real betis"],
    "gd estoril praia": ["estoril"],
    "sporting clube de braga": ["braga", "sporting braga"],
    "acf fiorentina": ["fiorentina"],
    "us lecce": ["lecce"],
    "tsg 1899 hoffenheim": ["hoffenheim", "tsg hoffenheim"],
    "1 fc heidenheim 1846": ["heidenheim"],
    "1 fsv mainz 05": ["mainz 05", "fsv mainz 05", "mainz"],
    "fc st pauli 1910": ["st pauli", "fc st pauli"],
    "borussia monchengladbach": ["gladbach", "monchengladbach"],
    "bayer 04 leverkusen": ["bayer leverkusen", "leverkusen"],
    "sport lisboa e benfica": ["benfica"],
    "fc porto": ["porto"],
    "vitoria sc": ["vitoria guimaraes"],
    "fc famalicao": ["famalicao"],
    "sbv excelsior": ["excelsior"],
    "pec zwolle": ["zwolle", "fc zwolle"],
    "afc ajax": ["ajax"],
    "psv eindhoven": ["psv"],
    "fc twente enschede": ["twente", "fc twente 65"],
    "racing club de lens": ["rc lens", "lens"],
    "angers sco": ["angers"],
    "olympique lyonnais": ["lyon"],
    "olympique de marseille": ["marseille"],
    "stade rennais fc 1901": ["rennes"],
    "ogc nice": ["nice"],
    "rc strasbourg alsace": ["strasbourg"],
    "as monaco fc": ["monaco", "as monaco"],
    "le havre ac": ["le havre"],
    "sporting clube de portugal": ["sporting cp", "sporting", "sporting lisbon"],
    "fk bodo glimt": ["bodo glimt", "bodo glimt"],
    "paris saint germain fc": ["paris saint germain", "psg"],
    "club atletico de madrid": ["atletico madrid", "atletico de madrid"],
}


def clean_years(value: str) -> str:
    result = value
    for pattern in YEAR_PATTERNS:
        result = re.sub(pattern, " ", result)
    result = re.sub(r"\s+", " ", result).strip()
    return result


def remove_leading_prefixes(value: str) -> str:
    result = value
    changed = True
    while changed:
        changed = False
        for pattern in LEADING_PREFIX_PATTERNS:
            new_value = re.sub(pattern, "", result).strip()
            if new_value != result:
                result = new_value
                changed = True
    return re.sub(r"\s+", " ", result).strip()


def remove_trailing_suffixes(value: str) -> str:
    result = value
    changed = True
    while changed:
        changed = False
        for pattern in TRAILING_SUFFIX_PATTERNS:
            new_value = re.sub(pattern, "", result).strip()
            if new_value != result:
                result = new_value
                changed = True
    return re.sub(r"\s+", " ", result).strip()


def candidate_aliases(canonical_name: str) -> Set[str]:
    raw_norm = normalize_text(canonical_name)
    aliases: Set[str] = set()

    if not raw_norm:
        return aliases

    # 1) Self alias
    aliases.add(raw_norm)

    # 2) Jahreszahlen entfernen
    no_years = clean_years(raw_norm)
    if no_years:
        aliases.add(no_years)

    # 3) Präfixe/Suffixe entfernen
    no_prefix = remove_leading_prefixes(raw_norm)
    if no_prefix:
        aliases.add(no_prefix)

    no_suffix = remove_trailing_suffixes(raw_norm)
    if no_suffix:
        aliases.add(no_suffix)

    no_prefix_no_suffix = remove_trailing_suffixes(no_prefix)
    if no_prefix_no_suffix:
        aliases.add(no_prefix_no_suffix)

    no_years_no_prefix = remove_leading_prefixes(no_years)
    if no_years_no_prefix:
        aliases.add(no_years_no_prefix)

    no_years_no_prefix_no_suffix = remove_trailing_suffixes(no_years_no_prefix)
    if no_years_no_prefix_no_suffix:
        aliases.add(no_years_no_prefix_no_suffix)

    # 4) Teilvarianten aus rechten Tokens
    parts = raw_norm.split()
    if len(parts) >= 2:
        aliases.add(" ".join(parts[-2:]))
    if len(parts) >= 3:
        aliases.add(" ".join(parts[-3:]))

    # 5) Sonderfälle
    if raw_norm in SPECIAL_ALIASES:
        for alias in SPECIAL_ALIASES[raw_norm]:
            alias_norm = normalize_text(alias)
            if alias_norm:
                aliases.add(alias_norm)

    # 6) Offensichtliche Restbereinigung
    cleaned: Set[str] = set()
    for alias in aliases:
        alias = clean_years(alias)
        alias = remove_leading_prefixes(alias)
        alias = remove_trailing_suffixes(alias)
        alias = re.sub(r"\s+", " ", alias).strip()

        if not alias:
            continue
        if len(alias) < 3:
            continue

        cleaned.add(alias)

    return cleaned


# ─────────────────────────────────────────────────────────────
# DB ACCESS
# ─────────────────────────────────────────────────────────────

def load_teams(conn) -> List[Dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select
              id,
              canonical_name,
              normalized_name
            from public.teams
            order by canonical_name asc
            """
        )
        return cur.fetchall()


def alias_exists(
    conn,
    *,
    team_id: int,
    alias_normalized: str,
    source_system: Optional[str] = None,
    competition_code: Optional[str] = None,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select 1
            from public.team_aliases
            where team_id = %s
              and alias_normalized = %s
              and coalesce(source_system, '') = coalesce(%s, '')
              and coalesce(competition_code, '') = coalesce(%s, '')
            limit 1
            """,
            (team_id, alias_normalized, source_system, competition_code),
        )
        return cur.fetchone() is not None


def insert_alias(
    conn,
    *,
    team_id: int,
    alias_name: str,
    alias_normalized: str,
    source_system: Optional[str] = None,
    competition_code: Optional[str] = None,
    status: str = "approved",
    decision_source: str = "seed",
    confidence: float = 0.95,
    explanation: str = "generated seed alias",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.team_aliases (
              team_id,
              source_system,
              competition_code,
              alias_name,
              alias_normalized,
              status,
              decision_source,
              confidence,
              explanation,
              usage_count,
              last_seen_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, now())
            """,
            (
                team_id,
                source_system,
                competition_code,
                alias_name,
                alias_normalized,
                status,
                decision_source,
                confidence,
                explanation,
            ),
        )


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seed standard team aliases from public.teams")
    parser.add_argument("--dry-run", action="store_true", help="Do not write aliases")
    return parser


def run(dry_run: bool) -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    inserted = 0
    skipped = 0

    try:
        teams = load_teams(conn)
        logger.info("Teams loaded: %s", len(teams))

        for team in teams:
            team_id = team["id"]
            canonical_name = team["canonical_name"]
            canonical_norm = team["normalized_name"]

            aliases = candidate_aliases(canonical_name)

            # Self alias immer sicherstellen
            aliases.add(canonical_norm)

            # Alias-Namen hübsch ableiten:
            # Wir speichern den Alias_name hier pragmatisch in der gleichen Textform wie normalized,
            # weil die operative Wahrheit auf alias_normalized basiert.
            for alias_norm in sorted(aliases):
                if not alias_norm:
                    skipped += 1
                    continue

                if alias_exists(
                    conn,
                    team_id=team_id,
                    alias_normalized=alias_norm,
                    source_system=None,
                    competition_code=None,
                ):
                    skipped += 1
                    continue

                if dry_run:
                    logger.info(
                        "[DRY RUN] would insert alias | team=%s | alias=%s",
                        canonical_name,
                        alias_norm,
                    )
                    inserted += 1
                    continue

                insert_alias(
                    conn,
                    team_id=team_id,
                    alias_name=alias_norm,
                    alias_normalized=alias_norm,
                    source_system=None,
                    competition_code=None,
                    status="approved",
                    decision_source="seed",
                    confidence=0.95 if alias_norm != canonical_norm else 1.0,
                    explanation="generated by seed_team_aliases.py",
                )
                inserted += 1

        if dry_run:
            conn.rollback()
            logger.info("DRY RUN complete.")
        else:
            conn.commit()
            logger.info("Committed alias seed run.")

        logger.info("══════════════════════════════════════")
        logger.info("SEED TEAM ALIASES SUMMARY")
        logger.info("  inserted : %s", inserted)
        logger.info("  skipped  : %s", skipped)
        logger.info("══════════════════════════════════════")

    except Exception:
        conn.rollback()
        logger.exception("seed_team_aliases.py failed")
        raise
    finally:
        conn.close()


def main() -> None:
    args = build_arg_parser().parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()