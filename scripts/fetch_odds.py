import os
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values
import requests


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

DATABASE_URL = os.environ["DATABASE_URL"]
ODDS_API_KEY = os.environ["ODDS_API_KEY"]

# Optional overrides
ODDS_LOOKAHEAD_DAYS = int(os.getenv("ODDS_LOOKAHEAD_DAYS", "3"))
ODDS_REGIONS = os.getenv("ODDS_REGIONS", "eu,uk")
ODDS_MARKETS = os.getenv("ODDS_MARKETS", "h2h")
ODDS_PROVIDER_KEY = os.getenv("ODDS_PROVIDER_KEY", "the_odds_api")
REQUEST_TIMEOUT = int(os.getenv("ODDS_REQUEST_TIMEOUT", "30"))

# Mapping deiner competition_code-Werte auf The Odds API sport keys
SPORT_KEY_MAP: Dict[str, str] = {
    "BL1": "soccer_germany_bundesliga",
    "PL": "soccer_epl",
    "PD": "soccer_spain_la_liga",
    "SA": "soccer_italy_serie_a",
    "FL1": "soccer_france_ligue_one",
}

# Teamnamen-Aliase für häufige Abweichungen.
# Links: deine DB-Namen normalisiert
# Rechts: mögliche API-Namen normalisiert
TEAM_ALIASES: Dict[str, List[str]] = {
    "bayern munchen": ["bayern munich", "fc bayern munich"],
    "fc bayern munchen": ["bayern munich", "fc bayern munich"],
    "koln": ["fc koln", "1 fc koln", "cologne"],
    "fc koln": ["koln", "1 fc koln", "cologne"],
    "borussia monchengladbach": ["monchengladbach", "borussia mgladbach"],
    "mainz 05": ["mainz", "1 fsv mainz 05"],
    "freiburg": ["sc freiburg"],
    "leverkusen": ["bayer leverkusen"],
    "rb leipzig": ["leipzig", "rasenballsport leipzig"],
    "eintracht frankfurt": ["frankfurt"],
    "st pauli": ["fc st pauli"],
}


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("fetch_odds")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_team_name(name: Optional[str]) -> str:
    if not name:
        return ""

    value = name.strip().lower()

    # Umlaute / Sonderzeichen normalisieren
    value = (
        value.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )

    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))

    # Störwörter entfernen
    value = re.sub(r"\bfc\b", " ", value)
    value = re.sub(r"\bsc\b", " ", value)
    value = re.sub(r"\bsv\b", " ", value)
    value = re.sub(r"\bev\b", " ", value)
    value = re.sub(r"\bbor\.\b", "borussia ", value)

    # Satzzeichen entfernen
    value = re.sub(r"[^a-z0-9 ]+", " ", value)

    # Mehrfachspaces glätten
    value = re.sub(r"\s+", " ", value).strip()
    return value


def team_name_candidates(name: str) -> List[str]:
    norm = normalize_team_name(name)
    candidates = {norm}

    if norm in TEAM_ALIASES:
        candidates.update(TEAM_ALIASES[norm])

    # Rückwärts-Aliase mit aufnehmen
    for base_name, aliases in TEAM_ALIASES.items():
        if norm in aliases:
            candidates.add(base_name)
            candidates.update(aliases)

    return [c for c in candidates if c]


def parse_iso_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


# -----------------------------------------------------------------------------
# DB
# -----------------------------------------------------------------------------

def get_upcoming_matches(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              id,
              external_id,
              kickoff_at,
              competition_code,
              home_team,
              away_team
            from public.matches
            where kickoff_at >= now() - interval '2 hours'
              and kickoff_at <= now() + (%s || ' days')::interval
            order by kickoff_at asc
            """,
            (ODDS_LOOKAHEAD_DAYS,),
        )
        rows = cur.fetchall()

    matches: List[Dict[str, Any]] = []
    for row in rows:
        competition_code = row[3]
        sport_key = SPORT_KEY_MAP.get(competition_code)
        if not sport_key:
            continue

        matches.append(
            {
                "id": row[0],
                "external_id": row[1],
                "kickoff_at": row[2],
                "competition_code": competition_code,
                "sport_key": sport_key,
                "home_team": row[4],
                "away_team": row[5],
            }
        )

    logger.info("Gefundene kommende Matches für Odds: %s", len(matches))
    return matches


def insert_snapshots(conn, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into public.odds_snapshots (
              match_id,
              provider_key,
              bookmaker_key,
              bookmaker_name,
              market_key,
              outcome_scope,
              snapshot_ts,
              source_last_update_ts,
              home_odds,
              draw_odds,
              away_odds,
              payload
            ) values %s
            """,
            rows,
            page_size=500,
        )

    return len(rows)


def refresh_matches_from_consensus(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.matches m
            set
              market_home_win_odds = c.median_home_odds,
              market_draw_odds = c.median_draw_odds,
              market_away_odds = c.median_away_odds,
              market_odds_bookmaker_count = c.bookmaker_count,
              market_odds_provider = 'consensus',
              market_odds_fetched_at = now(),
              market_odds_updated_at = now(),
              updated_at = now()
            from public.v_match_market_consensus c
            where c.match_id = m.id
              and (
                m.market_home_win_odds is distinct from c.median_home_odds or
                m.market_draw_odds is distinct from c.median_draw_odds or
                m.market_away_odds is distinct from c.median_away_odds or
                m.market_odds_bookmaker_count is distinct from c.bookmaker_count or
                m.market_odds_provider is distinct from 'consensus'
              )
            """
        )
        updated = cur.rowcount

    return updated


# -----------------------------------------------------------------------------
# The Odds API
# -----------------------------------------------------------------------------

def fetch_odds_for_sport(sport_key: str) -> List[Dict[str, Any]]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
    }

    logger.info("Hole Odds für %s", sport_key)
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    # Praktisch für Monitoring des Free-Plans
    remaining = response.headers.get("x-requests-remaining")
    used = response.headers.get("x-requests-used")
    if remaining or used:
        logger.info("Odds API Credits | used=%s remaining=%s", used, remaining)

    data = response.json()
    logger.info("Events von API für %s: %s", sport_key, len(data))
    return data


# -----------------------------------------------------------------------------
# Matching
# -----------------------------------------------------------------------------

def build_event_lookup(events: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for event in events:
        home = normalize_team_name(event.get("home_team"))
        away = normalize_team_name(event.get("away_team"))
        if not home or not away:
            continue

        lookup[(home, away)] = event

    return lookup


def find_matching_event(
    match: Dict[str, Any],
    events: List[Dict[str, Any]],
    event_lookup: Dict[Tuple[str, str], Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    # 1) exakter Treffer nach normalisierten Namen
    home_candidates = team_name_candidates(match["home_team"])
    away_candidates = team_name_candidates(match["away_team"])

    for h in home_candidates:
        for a in away_candidates:
            event = event_lookup.get((h, a))
            if event:
                return event

    # 2) Fallback: gleiche Paarung unabhängig von kleinen Alias-Abweichungen
    match_home_set = set(home_candidates)
    match_away_set = set(away_candidates)

    for event in events:
        api_home_candidates = set(team_name_candidates(str(event.get("home_team", ""))))
        api_away_candidates = set(team_name_candidates(str(event.get("away_team", ""))))

        if match_home_set & api_home_candidates and match_away_set & api_away_candidates:
            return event

    return None


# -----------------------------------------------------------------------------
# Extraction
# -----------------------------------------------------------------------------

def extract_snapshot_rows(
    matches: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
) -> Tuple[List[Tuple[Any, ...]], List[Dict[str, str]]]:
    event_lookup = build_event_lookup(events)
    snapshot_ts = utcnow()

    rows: List[Tuple[Any, ...]] = []
    unmatched: List[Dict[str, str]] = []

    for match in matches:
        event = find_matching_event(match, events, event_lookup)

        if not event:
            unmatched.append(
                {
                    "competition_code": match["competition_code"],
                    "home_team": match["home_team"],
                    "away_team": match["away_team"],
                }
            )
            continue

        event_home = event.get("home_team")
        event_away = event.get("away_team")

        for bookmaker in event.get("bookmakers", []):
            home_odds: Optional[float] = None
            draw_odds: Optional[float] = None
            away_odds: Optional[float] = None

            for market in bookmaker.get("markets", []):
                if market.get("key") != "h2h":
                    continue

                for outcome in market.get("outcomes", []):
                    name = str(outcome.get("name", "")).strip()
                    price = outcome.get("price")

                    if price is None:
                        continue

                    try:
                        price_value = float(price)
                    except (TypeError, ValueError):
                        continue

                    if name == event_home:
                        home_odds = price_value
                    elif name == event_away:
                        away_odds = price_value
                    elif name.lower() == "draw":
                        draw_odds = price_value

            if home_odds and draw_odds and away_odds:
                rows.append(
                    (
                        match["id"],
                        ODDS_PROVIDER_KEY,
                        bookmaker.get("key"),
                        bookmaker.get("title"),
                        "1x2",
                        "ft",
                        snapshot_ts,
                        parse_iso_ts(bookmaker.get("last_update")),
                        home_odds,
                        draw_odds,
                        away_odds,
                        json.dumps(bookmaker),
                    )
                )

    return rows, unmatched


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        matches = get_upcoming_matches(conn)
        if not matches:
            logger.info("Keine relevanten Matches gefunden. Ende.")
            return

        matches_by_sport: Dict[str, List[Dict[str, Any]]] = {}
        for match in matches:
            matches_by_sport.setdefault(match["sport_key"], []).append(match)

        total_inserted = 0
        total_unmatched: List[Dict[str, str]] = []

        for sport_key, sport_matches in matches_by_sport.items():
            logger.info(
                "Verarbeite Sport-Key %s mit %s Matches",
                sport_key,
                len(sport_matches),
            )

            events = fetch_odds_for_sport(sport_key)
            rows, unmatched = extract_snapshot_rows(sport_matches, events)

            inserted = insert_snapshots(conn, rows)
            total_inserted += inserted
            total_unmatched.extend(unmatched)

            logger.info(
                "Sport %s | Snapshots inserted=%s | unmatched=%s",
                sport_key,
                inserted,
                len(unmatched),
            )

        updated_matches = refresh_matches_from_consensus(conn)
        conn.commit()

        logger.info("Gesamt inserted snapshots: %s", total_inserted)
        logger.info("Matches mit aktualisierten Marktquoten: %s", updated_matches)

        if total_unmatched:
            logger.warning("Nicht gematchte Spiele: %s", len(total_unmatched))
            for item in total_unmatched[:20]:
                logger.warning(
                    "UNMATCHED | %s | %s vs %s",
                    item["competition_code"],
                    item["home_team"],
                    item["away_team"],
                )

    except Exception:
        conn.rollback()
        logger.exception("fetch_odds.py fehlgeschlagen")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()