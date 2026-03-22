import json
import logging
import os
import re
import unicodedata
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
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
ODDS_SOURCE_SYSTEM = os.getenv("ODDS_SOURCE_SYSTEM", "odds_api")

MAX_RETRIES = int(os.getenv("ODDS_MAX_RETRIES", "3"))
INITIAL_BACKOFF_SECONDS = int(os.getenv("ODDS_INITIAL_BACKOFF_SECONDS", "2"))

SPORTS_URL = "https://api.the-odds-api.com/v4/sports"
ODDS_URL_TEMPLATE = "https://api.the-odds-api.com/v4/sports/{sport_key}/odds"

# Mindestquote für gültige Decimal Odds
MIN_VALID_ODDS = float(os.getenv("MIN_VALID_ODDS", "1.01"))


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("fetch_odds")


# -----------------------------------------------------------------------------
# Technische String-Normalisierung
# Fachliche Wahrheit liegt in der DB, nicht hier.
# -----------------------------------------------------------------------------

STOPWORDS = {
    "fc", "sc", "sv", "ev", "afc", "bc", "cf",
    "sk", "fk", "club", "clube", "ac", "as", "ss",
}

GENERIC_REPLACEMENTS = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def normalize_team_name(value: Optional[str]) -> str:
    norm = normalize_text(value)
    if not norm:
        return ""

    parts = [p for p in norm.split() if p not in STOPWORDS]
    norm = " ".join(parts)
    norm = re.sub(r"\s+", " ", norm).strip()
    return norm


def parse_iso_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def is_valid_odds(value: Optional[float]) -> bool:
    try:
        if value is None:
            return False
        return float(value) > MIN_VALID_ODDS
    except (TypeError, ValueError):
        return False


# -----------------------------------------------------------------------------
# DB: Competition- und Alias-Lookups
# -----------------------------------------------------------------------------

def load_active_odds_mappings(conn) -> Dict[str, Dict[str, Any]]:
    """
    Lädt alle aktiven Odds-Competition-Mappings aus der DB.

    Rückgabe:
      {
        "BUNDESLIGA": {
          "competition_code": ...,
          "competition_name": ...,
          "sport_key": "soccer_germany_bundesliga",
          ...
        },
        ...
      }
    """
    sql = """
        select
          competition_code,
          competition_name,
          league_abbr,
          competition_type,
          country,
          flag,
          external_code as sport_key,
          external_name
        from public.v_competition_external_map_active
        where source_system = %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (ODDS_SOURCE_SYSTEM,))
        rows = cur.fetchall()

    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        result[row["competition_code"]] = dict(row)

    logger.info("Aktive Odds-Competition-Mappings geladen: %s", len(result))
    return result


def load_active_team_aliases(conn) -> Dict[str, List[Dict[str, Any]]]:
    """
    Lädt alle aktiven Team-Aliase aus der DB.
    """
    sql = """
        select
          id,
          source_system,
          competition_code,
          alias_name,
          alias_normalized,
          status,
          confidence,
          decision_source,
          explanation,
          team_id,
          canonical_name,
          canonical_normalized_name
        from public.v_team_aliases_active
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    by_alias: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_alias[row["alias_normalized"]].append(dict(row))

    logger.info("Aktive Team-Aliase geladen: %s Alias-Normalformen", len(by_alias))
    return dict(by_alias)


def get_upcoming_matches(conn, competition_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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

    unresolved: List[Dict[str, Any]] = []
    matches: List[Dict[str, Any]] = []

    for row in rows:
        comp = competition_map.get(row["competition_code"])
        if not comp:
            unresolved.append(
                {
                    "competition_code": row["competition_code"],
                    "competition_name": row["competition_name"],
                    "country": row["country"],
                }
            )
            continue

        match = dict(row)
        match["sport_key"] = comp["sport_key"]
        matches.append(match)

    if unresolved:
        logger.warning("Keine aktiven Odds-Mappings für folgende Competitions:")
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


# -----------------------------------------------------------------------------
# Odds API discovery / fetching
# -----------------------------------------------------------------------------

def fetch_available_sports() -> List[Dict[str, Any]]:
    params = {"apiKey": ODDS_API_KEY}
    response = requests.get(SPORTS_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    sports = response.json()
    logger.info("Verfügbare Sports von Odds API geladen: %s", len(sports))
    return sports


def fetch_odds_for_sport(sport_key: str, max_retries: int = MAX_RETRIES) -> List[Dict[str, Any]]:
    url = ODDS_URL_TEMPLATE.format(sport_key=sport_key)
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
    }

    logger.info("Hole Odds für sport_key=%s", sport_key)
    backoff = INITIAL_BACKOFF_SECONDS

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

            remaining = response.headers.get("x-requests-remaining")
            used = response.headers.get("x-requests-used")
            if remaining or used:
                logger.info("Odds API Credits | used=%s remaining=%s", used, remaining)

            if response.status_code == 429:
                logger.warning(
                    "429 Rate Limit | sport_key=%s | attempt=%s/%s | wait=%ss",
                    sport_key, attempt, max_retries, backoff
                )
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                logger.error("429 bleibt bestehen → skip sport_key=%s", sport_key)
                return []

            response.raise_for_status()
            data = response.json()
            logger.info("Events geladen für %s: %s", sport_key, len(data))
            return data

        except requests.RequestException as exc:
            logger.warning(
                "Request Fehler | sport_key=%s | attempt=%s/%s | error=%s",
                sport_key, attempt, max_retries, exc
            )
            if attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.error("Request endgültig fehlgeschlagen → skip sport_key=%s", sport_key)
            return []

    return []


# -----------------------------------------------------------------------------
# Matching Helpers
# -----------------------------------------------------------------------------

def kickoff_close(db_kickoff: datetime, api_commence: Optional[str], max_minutes_diff: int = 180) -> bool:
    api_ts = parse_iso_ts(api_commence)
    if not api_ts:
        return True
    diff_minutes = abs((db_kickoff - api_ts).total_seconds()) / 60.0
    return diff_minutes <= max_minutes_diff


def build_match_candidate_set(
    team_name: str,
    competition_code: str,
    alias_lookup: Dict[str, List[Dict[str, Any]]],
    source_system: str = ODDS_SOURCE_SYSTEM,
) -> List[str]:
    base_norm = normalize_team_name(team_name)
    candidates = {base_norm}

    alias_rows = alias_lookup.get(base_norm, [])
    for row in alias_rows:
        row_source = row.get("source_system")
        row_comp = row.get("competition_code")

        source_ok = row_source is None or row_source == source_system
        comp_ok = row_comp is None or row_comp == competition_code

        if source_ok and comp_ok:
            candidates.add(row["alias_normalized"])
            candidates.add(row["canonical_normalized_name"])

    return sorted(c for c in candidates if c)


def build_event_candidate_set(
    api_team_name: str,
    competition_code: str,
    alias_lookup: Dict[str, List[Dict[str, Any]]],
    source_system: str = ODDS_SOURCE_SYSTEM,
) -> List[str]:
    base_norm = normalize_team_name(api_team_name)
    candidates = {base_norm}

    alias_rows = alias_lookup.get(base_norm, [])
    for row in alias_rows:
        row_source = row.get("source_system")
        row_comp = row.get("competition_code")

        source_ok = row_source is None or row_source == source_system
        comp_ok = row_comp is None or row_comp == competition_code

        if source_ok and comp_ok:
            candidates.add(row["alias_normalized"])
            candidates.add(row["canonical_normalized_name"])

    return sorted(c for c in candidates if c)


def find_matching_event(
    match: Dict[str, Any],
    events: List[Dict[str, Any]],
    alias_lookup: Dict[str, List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    db_home_candidates = set(
        build_match_candidate_set(
            team_name=match["home_team"],
            competition_code=match["competition_code"],
            alias_lookup=alias_lookup,
        )
    )
    db_away_candidates = set(
        build_match_candidate_set(
            team_name=match["away_team"],
            competition_code=match["competition_code"],
            alias_lookup=alias_lookup,
        )
    )

    for event in events:
        event_home_candidates = set(
            build_event_candidate_set(
                api_team_name=event.get("home_team"),
                competition_code=match["competition_code"],
                alias_lookup=alias_lookup,
            )
        )
        event_away_candidates = set(
            build_event_candidate_set(
                api_team_name=event.get("away_team"),
                competition_code=match["competition_code"],
                alias_lookup=alias_lookup,
            )
        )

        if (db_home_candidates & event_home_candidates) and (db_away_candidates & event_away_candidates):
            if kickoff_close(match["kickoff_at"], event.get("commence_time")):
                return event

    db_home_norm = normalize_team_name(match["home_team"])
    db_away_norm = normalize_team_name(match["away_team"])

    for event in events:
        event_home_norm = normalize_team_name(event.get("home_team"))
        event_away_norm = normalize_team_name(event.get("away_team"))

        if db_home_norm == event_home_norm and db_away_norm == event_away_norm:
            if kickoff_close(match["kickoff_at"], event.get("commence_time")):
                return event

    return None


# -----------------------------------------------------------------------------
# Event / Alias Logging
# -----------------------------------------------------------------------------

def insert_team_alias_events(conn, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into public.team_alias_events (
              source_system,
              competition_code,
              raw_name,
              raw_name_normalized,
              home_or_away,
              context,
              status
            ) values %s
            """,
            rows,
            page_size=500,
        )
    return len(rows)


def build_alias_event_rows_for_unmatched(
    match: Dict[str, Any],
    events: List[Dict[str, Any]],
    alias_lookup: Dict[str, List[Dict[str, Any]]],
) -> List[Tuple[Any, ...]]:
    rows: List[Tuple[Any, ...]] = []

    known_alias_norms = set(alias_lookup.keys())

    for event in events:
        for side, raw_name in (("home", event.get("home_team")), ("away", event.get("away_team"))):
            raw_norm = normalize_team_name(raw_name)
            if not raw_norm:
                continue

            if raw_norm in known_alias_norms:
                continue

            context = {
                "match_competition_code": match["competition_code"],
                "db_home_team": match["home_team"],
                "db_away_team": match["away_team"],
                "api_home_team": event.get("home_team"),
                "api_away_team": event.get("away_team"),
                "api_commence_time": event.get("commence_time"),
                "sport_key": match["sport_key"],
            }

            rows.append(
                (
                    ODDS_SOURCE_SYSTEM,
                    match["competition_code"],
                    raw_name,
                    raw_norm,
                    side,
                    json.dumps(context, ensure_ascii=False),
                    "new",
                )
            )

    dedup = {}
    for row in rows:
        key = (row[0], row[1], row[3], row[4])
        dedup[key] = row

    return list(dedup.values())


# -----------------------------------------------------------------------------
# DB snapshots / market consensus update
# -----------------------------------------------------------------------------

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
# Extraction
# -----------------------------------------------------------------------------

def extract_snapshot_rows(
    matches: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
    alias_lookup: Dict[str, List[Dict[str, Any]]],
) -> Tuple[List[Tuple[Any, ...]], List[Dict[str, str]], List[Tuple[Any, ...]]]:
    snapshot_ts = utcnow()

    rows: List[Tuple[Any, ...]] = []
    unmatched: List[Dict[str, str]] = []
    alias_event_rows: List[Tuple[Any, ...]] = []

    for match in matches:
        event = find_matching_event(match, events, alias_lookup)

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

            alias_event_rows.extend(
                build_alias_event_rows_for_unmatched(
                    match=match,
                    events=events[:20],
                    alias_lookup=alias_lookup,
                )
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

            valid_home = is_valid_odds(home_odds)
            valid_draw = is_valid_odds(draw_odds)
            valid_away = is_valid_odds(away_odds)

            if valid_home and valid_draw and valid_away:
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
                        float(home_odds),
                        float(draw_odds),
                        float(away_odds),
                        json.dumps(bookmaker, ensure_ascii=False),
                    )
                )
            else:
                if any(v is not None for v in (home_odds, draw_odds, away_odds)):
                    logger.warning(
                        "Ungültige Odds verworfen | bookmaker=%s | match=%s vs %s | home=%s draw=%s away=%s",
                        bookmaker.get("title"),
                        match["home_team"],
                        match["away_team"],
                        home_odds,
                        draw_odds,
                        away_odds,
                    )

    dedup = {}
    for row in alias_event_rows:
        key = (row[0], row[1], row[3], row[4])
        dedup[key] = row

    return rows, unmatched, list(dedup.values())


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        available_sports = fetch_available_sports()
        active_sport_keys = {s.get("key") for s in available_sports if s.get("active", True)}

        competition_map = load_active_odds_mappings(conn)
        alias_lookup = load_active_team_aliases(conn)

        matches = get_upcoming_matches(conn, competition_map)
        if not matches:
            logger.info("Keine relevanten Matches gefunden. Ende.")
            return

        matches_by_sport: Dict[str, List[Dict[str, Any]]] = {}
        for match in matches:
            if match["sport_key"] not in active_sport_keys:
                logger.warning(
                    "Sport-Key nicht aktiv/verfügbar laut Odds API: %s | competition=%s",
                    match["sport_key"],
                    match["competition_code"],
                )
                continue
            matches_by_sport.setdefault(match["sport_key"], []).append(match)

        total_inserted = 0
        total_unmatched: List[Dict[str, str]] = []
        total_alias_events = 0

        logger.info("Sport-Keys für diesen Lauf: %s", list(matches_by_sport.keys()))

        for sport_key, sport_matches in matches_by_sport.items():
            logger.info(
                "Verarbeite sport_key=%s | matches=%s",
                sport_key,
                len(sport_matches),
            )

            try:
                events = fetch_odds_for_sport(sport_key)
            except Exception:
                logger.exception("sport_key komplett fehlgeschlagen: %s", sport_key)
                continue

            if not events:
                logger.warning("Keine Events für sport_key=%s", sport_key)
                continue

            rows, unmatched, alias_event_rows = extract_snapshot_rows(
                matches=sport_matches,
                events=events,
                alias_lookup=alias_lookup,
            )

            inserted = insert_snapshots(conn, rows)
            inserted_alias_events = insert_team_alias_events(conn, alias_event_rows)

            total_inserted += inserted
            total_alias_events += inserted_alias_events
            total_unmatched.extend(unmatched)

            logger.info(
                "sport_key=%s | inserted=%s | unmatched=%s | alias_events=%s",
                sport_key,
                inserted,
                len(unmatched),
                inserted_alias_events,
            )

        updated_matches = refresh_matches_from_consensus(conn)
        conn.commit()

        logger.info("Gesamt inserted snapshots: %s", total_inserted)
        logger.info("Matches mit aktualisierten Marktquoten: %s", updated_matches)
        logger.info("Neu geschriebene team_alias_events: %s", total_alias_events)

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