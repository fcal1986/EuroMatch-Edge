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

ODDS_LOOKAHEAD_DAYS = int(os.getenv("ODDS_LOOKAHEAD_DAYS", "14"))
ODDS_REGIONS = os.getenv("ODDS_REGIONS", "eu,uk")
ODDS_MARKETS = os.getenv("ODDS_MARKETS", "h2h")
ODDS_PROVIDER_KEY = os.getenv("ODDS_PROVIDER_KEY", "the_odds_api")
REQUEST_TIMEOUT = int(os.getenv("ODDS_REQUEST_TIMEOUT", "30"))

SPORTS_URL = "https://api.the-odds-api.com/v4/sports"
ODDS_URL_TEMPLATE = "https://api.the-odds-api.com/v4/sports/{sport_key}/odds"


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


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""

    value = value.strip().lower()
    value = (
        value.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )

    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))

    value = re.sub(r"[^a-z0-9 /-]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_team_name(name: Optional[str]) -> str:
    if not name:
        return ""

    value = name.lower().strip()

    # Umlaute / Sonderzeichen
    value = (
        value.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )

    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))

    # Slash / Bindestrich vereinheitlichen
    value = value.replace("/", " ")
    value = value.replace("-", " ")

    # Satzzeichen raus
    value = re.sub(r"[^a-z0-9 ]+", " ", value)

    # Club-/Suffix-Wörter entfernen
    stopwords = {
        "fc", "sc", "sv", "ev", "afc", "bc", "cf",
        "sk", "fk", "club", "clube"
    }
    parts = [p for p in value.split() if p not in stopwords]
    value = " ".join(parts)

    # Harte Synonym-Normalisierung
    replacements = {
        "munich": "munchen",
        "muenchen": "munchen",
        "bayern munich": "bayern munchen",
        "atalanta bc": "atalanta",
        "atalanta bergamasca calcio": "atalanta",
        "sporting cp": "sporting",
        "sporting lisbon": "sporting",
        "sporting portugal": "sporting",
        "bodoe": "bodo",
        "bodo glimt": "bodo glimt",
    }

    for src, dst in replacements.items():
        value = value.replace(src, dst)

    value = re.sub(r"\s+", " ", value).strip()
    return value


TEAM_ALIASES: Dict[str, List[str]] = {
    "bayern munchen": [
        "bayern",
        "bayern munich",
    ],
    "atalanta": [
        "atalanta bc",
    ],
    "sporting": [
        "sporting clube de portugal",
        "sporting cp",
        "sporting lisbon",
        "sporting portugal",
    ],
    "bodo glimt": [
        "fk bodo glimt",
        "bodo glimt",
    ],
    "paris saint germain": [
        "psg",
        "paris saint germain fc",
    ],
    "real madrid": [
        "real madrid cf",
    ],
    "barcelona": [
        "fc barcelona",
    ],
    "atletico madrid": [
        "club atletico de madrid",
        "atletico de madrid",
    ],
    "watford": [
        "watford fc",
    ],
    "wrexham": [
        "wrexham afc",
    ],
    "southampton": [
        "southampton fc",
    ],
    "norwich city": [
        "norwich city fc",
    ],
    "manchester city": [
        "manchester city fc",
    ],
    "chelsea": [
        "chelsea fc",
    ],
    "arsenal": [
        "arsenal fc",
    ],
    "newcastle united": [
        "newcastle united fc",
    ],
    "tottenham hotspur": [
        "tottenham hotspur fc",
        "spurs",
    ],
    "liverpool": [
        "liverpool fc",
    ],
    "galatasaray": [
        "galatasaray sk",
    ],
}


def team_name_candidates(name: Optional[str]) -> List[str]:
    norm = normalize_team_name(name)
    candidates = {norm}

    if norm in TEAM_ALIASES:
        candidates.update(normalize_team_name(x) for x in TEAM_ALIASES[norm])

    for base, aliases in TEAM_ALIASES.items():
        alias_norms = {normalize_team_name(a) for a in aliases}
        if norm in alias_norms:
            candidates.add(normalize_team_name(base))
            candidates.update(alias_norms)

    return [c for c in candidates if c]


def parse_iso_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Odds API discovery
# -----------------------------------------------------------------------------

def fetch_available_sports() -> List[Dict[str, Any]]:
    params = {"apiKey": ODDS_API_KEY}
    response = requests.get(SPORTS_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    sports = response.json()
    logger.info("Verfügbare Sports von Odds API geladen: %s", len(sports))
    return sports


def build_sport_lookup(available_sports: List[Dict[str, Any]]) -> Dict[str, str]:
    alias_to_key: Dict[str, str] = {}

    for sport in available_sports:
        key = sport.get("key")
        title = sport.get("title")
        group = sport.get("group")
        active = sport.get("active", True)

        if not key or not title or not active:
            continue

        group_norm = normalize_text(group)
        title_norm = normalize_text(title)
        key_norm = normalize_text(key)

        if "soccer" not in group_norm and "soccer" not in key_norm:
            continue

        aliases = {
            title_norm,
            key_norm,
        }

        if "bundesliga" in title_norm or "bundesliga" in key_norm:
            aliases.update({"bundesliga", "germany bundesliga"})
        if "premier league" in title_norm or key == "soccer_epl":
            aliases.update({"premier league", "epl", "england premier league"})
        if "la liga" in title_norm:
            aliases.update({"la liga", "spanien la liga", "spain la liga"})
        if "serie a" in title_norm:
            aliases.update({"serie a", "italien serie a", "italy serie a"})
        if "ligue 1" in title_norm:
            aliases.update({"ligue 1", "frankreich ligue 1", "france ligue 1"})
        if "eredivisie" in title_norm:
            aliases.update({"eredivisie", "niederlande eredivisie", "netherlands eredivisie"})
        if "primeira liga" in title_norm:
            aliases.update({"primeira liga", "portugal primeira liga"})
        if "champions league" in title_norm:
            aliases.update({"champions league", "uefa champions league"})
        if "championship" in title_norm:
            aliases.update({"championship", "efl championship", "english championship"})

        for alias in aliases:
            alias_to_key[alias] = key

    logger.info("Soccer-Sport-Lookup gebaut: %s Einträge", len(alias_to_key))
    return alias_to_key


def resolve_sport_key(
    competition_code: str,
    competition_name: str,
    country: Optional[str],
    sport_lookup: Dict[str, str],
) -> Optional[str]:
    code_norm = normalize_text(competition_code)
    name_norm = normalize_text(competition_name)
    country_norm = normalize_text(country)

    wanted_aliases = []

    if code_norm == "bundesliga":
        wanted_aliases += ["bundesliga", "germany bundesliga"]
    elif code_norm == "premier_league":
        wanted_aliases += ["premier league", "epl", "england premier league"]
    elif code_norm == "la_liga":
        wanted_aliases += ["la liga", "spain la liga", "spanien la liga"]
    elif code_norm == "serie_a":
        wanted_aliases += ["serie a", "italy serie a", "italien serie a"]
    elif code_norm == "ligue_1":
        wanted_aliases += ["ligue 1", "france ligue 1", "frankreich ligue 1"]
    elif code_norm == "eredivisie":
        wanted_aliases += ["eredivisie", "netherlands eredivisie", "niederlande eredivisie"]
    elif code_norm == "primeira_liga":
        wanted_aliases += ["primeira liga", "portugal primeira liga"]
    elif code_norm == "champions_league":
        wanted_aliases += ["champions league", "uefa champions league"]
    elif code_norm == "championship":
        wanted_aliases += ["championship", "efl championship", "english championship"]

    wanted_aliases += [
        name_norm,
        f"{country_norm} {name_norm}".strip(),
        code_norm,
    ]

    for alias in wanted_aliases:
        if alias in sport_lookup:
            return sport_lookup[alias]

    return None


# -----------------------------------------------------------------------------
# DB
# -----------------------------------------------------------------------------

def get_upcoming_matches(conn, sport_lookup: Dict[str, str]) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              id,
              external_id,
              kickoff_at,
              competition_code,
              competition_name,
              country,
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

    logger.info("Matches im Lookahead-Fenster: %s", len(rows))

    unresolved = []
    matches: List[Dict[str, Any]] = []

    for row in rows:
        sport_key = resolve_sport_key(
            competition_code=row[3],
            competition_name=row[4],
            country=row[5],
            sport_lookup=sport_lookup,
        )

        if not sport_key:
            unresolved.append(
                {
                    "competition_code": row[3],
                    "competition_name": row[4],
                    "country": row[5],
                }
            )
            continue

        matches.append(
            {
                "id": row[0],
                "external_id": row[1],
                "kickoff_at": row[2],
                "competition_code": row[3],
                "competition_name": row[4],
                "country": row[5],
                "sport_key": sport_key,
                "home_team": row[6],
                "away_team": row[7],
            }
        )

    if unresolved:
        logger.warning("Nicht auflösbare Competitions:")
        seen = set()
        for item in unresolved:
            key = (item["competition_code"], item["competition_name"], item["country"])
            if key in seen:
                continue
            seen.add(key)
            logger.warning(
                "  %s | %s | %s",
                item["competition_code"],
                item["competition_name"],
                item["country"],
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
        return cur.rowcount


# -----------------------------------------------------------------------------
# Odds fetching
# -----------------------------------------------------------------------------

def fetch_odds_for_sport(sport_key: str) -> List[Dict[str, Any]]:
    url = ODDS_URL_TEMPLATE.format(sport_key=sport_key)
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
    }

    logger.info("Hole Odds für sport_key=%s", sport_key)
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    remaining = response.headers.get("x-requests-remaining")
    used = response.headers.get("x-requests-used")
    if remaining or used:
        logger.info("Odds API Credits | used=%s remaining=%s", used, remaining)

    data = response.json()
    logger.info("Events geladen für %s: %s", sport_key, len(data))
    return data


# -----------------------------------------------------------------------------
# Matching
# -----------------------------------------------------------------------------

def build_event_lookup(events: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for event in events:
        home = normalize_team_name(event.get("home_team"))
        away = normalize_team_name(event.get("away_team"))
        if home and away:
            lookup[(home, away)] = event

    return lookup


def kickoff_close(db_kickoff: datetime, api_commence: Optional[str], max_minutes_diff: int = 180) -> bool:
    api_ts = parse_iso_ts(api_commence)
    if not api_ts:
        return True
    diff_minutes = abs((db_kickoff - api_ts).total_seconds()) / 60.0
    return diff_minutes <= max_minutes_diff


def find_matching_event(
    match: Dict[str, Any],
    events: List[Dict[str, Any]],
    event_lookup: Dict[Tuple[str, str], Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    home_candidates = team_name_candidates(match["home_team"])
    away_candidates = team_name_candidates(match["away_team"])

    # 1) Direkter Lookup
    for h in home_candidates:
        for a in away_candidates:
            event = event_lookup.get((h, a))
            if event and kickoff_close(match["kickoff_at"], event.get("commence_time")):
                return event

    # 2) Fallback über Mengen-Schnitt
    home_set = set(home_candidates)
    away_set = set(away_candidates)

    for event in events:
        event_home_set = set(team_name_candidates(event.get("home_team")))
        event_away_set = set(team_name_candidates(event.get("away_team")))

        if (home_set & event_home_set) and (away_set & event_away_set):
            if kickoff_close(match["kickoff_at"], event.get("commence_time")):
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
            logger.warning(
                "UNMATCHED DB MATCH | %s | %s vs %s",
                match["competition_code"],
                match["home_team"],
                match["away_team"],
            )
            logger.warning(
                "DB normalized      | home=%s | away=%s",
                normalize_team_name(match["home_team"]),
                normalize_team_name(match["away_team"]),
            )
            logger.warning(
                "API candidates     | %s",
                [
                    {
                        "home": e.get("home_team"),
                        "away": e.get("away_team"),
                        "home_norm": normalize_team_name(e.get("home_team")),
                        "away_norm": normalize_team_name(e.get("away_team")),
                    }
                    for e in events[:20]
                ],
            )

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
            home_odds = None
            draw_odds = None
            away_odds = None

            for market in bookmaker.get("markets", []):
                if market.get("key") != "h2h":
                    continue

                for outcome in market.get("outcomes", []):
                    name = str(outcome.get("name", "")).strip()
                    price = outcome.get("price")

                    if price is None:
                        continue

                    try:
                        price_val = float(price)
                    except (TypeError, ValueError):
                        continue

                    if name == event_home:
                        home_odds = price_val
                    elif name == event_away:
                        away_odds = price_val
                    elif name.lower() == "draw":
                        draw_odds = price_val

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
        available_sports = fetch_available_sports()
        sport_lookup = build_sport_lookup(available_sports)

        matches = get_upcoming_matches(conn, sport_lookup)
        if not matches:
            logger.info("Keine relevanten Matches gefunden. Ende.")
            return

        matches_by_sport: Dict[str, List[Dict[str, Any]]] = {}
        for match in matches:
            matches_by_sport.setdefault(match["sport_key"], []).append(match)

        total_inserted = 0
        total_unmatched: List[Dict[str, str]] = []

        logger.info("Sport-Keys für diesen Lauf: %s", list(matches_by_sport.keys()))

        for sport_key, sport_matches in matches_by_sport.items():
            logger.info(
                "Verarbeite sport_key=%s | matches=%s",
                sport_key,
                len(sport_matches),
            )

            events = fetch_odds_for_sport(sport_key)
            rows, unmatched = extract_snapshot_rows(sport_matches, events)

            inserted = insert_snapshots(conn, rows)
            total_inserted += inserted
            total_unmatched.extend(unmatched)

            logger.info(
                "sport_key=%s | inserted=%s | unmatched=%s",
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
            for item in total_unmatched[:50]:
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