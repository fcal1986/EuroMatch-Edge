import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime
from hashlib import sha256
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import requests


DATABASE_URL = os.environ["DATABASE_URL"]
ODDS_API_KEY = os.environ["ODDS_API_KEY"]

LOOKAHEAD_DAYS = int(os.getenv("ODDS_LOOKAHEAD_DAYS", "14"))
ODDS_REGIONS = os.getenv("ODDS_REGIONS", "eu,uk")
ODDS_MARKETS = os.getenv("ODDS_MARKETS", "h2h")
REQUEST_TIMEOUT = int(os.getenv("ODDS_REQUEST_TIMEOUT", "30"))
ODDS_SOURCE_SYSTEM = os.getenv("ODDS_SOURCE_SYSTEM", "odds_api")

MAX_RETRIES = int(os.getenv("ODDS_MAX_RETRIES", "3"))
INITIAL_BACKOFF_SECONDS = int(os.getenv("ODDS_INITIAL_BACKOFF_SECONDS", "2"))

SPORTS_URL = "https://api.the-odds-api.com/v4/sports"
ODDS_URL_TEMPLATE = "https://api.the-odds-api.com/v4/sports/{sport_key}/odds"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("detect_team_alias_candidates")


# -----------------------------------------------------------------------------
# Technische Normalisierung
# Fachliche Wahrheit liegt in der DB, nicht im Skript.
# -----------------------------------------------------------------------------

STOPWORDS = {
    "fc", "sc", "sv", "ev", "afc", "bc", "cf",
    "sk", "fk", "club", "clube",
    "de", "da", "do", "del",
}

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


def normalize_team_name(name: Optional[str]) -> str:
    norm = normalize_text(name)
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


def token_set(value: str) -> set[str]:
    return set(value.split()) if value else set()


def similarity_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0

    set_a = token_set(a)
    set_b = token_set(b)
    if not set_a or not set_b:
        return 0.0

    overlap = len(set_a & set_b)
    union = len(set_a | set_b)
    return overlap / union if union else 0.0


def is_substringish(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return a in b or b in a


def is_trivial_name_difference(a: str, b: str) -> bool:
    """
    Filtert kosmetische Unterschiede raus:
    - gleiche Normalform
    - gleiche Tokenmenge
    - sehr hoher Token-Overlap
    - Teilstring + hohe Überlappung
    """
    if not a or not b:
        return False

    if a == b:
        return True

    tokens_a = token_set(a)
    tokens_b = token_set(b)

    if tokens_a == tokens_b:
        return True

    score = similarity_score(a, b)

    if score >= 0.90:
        return True

    if is_substringish(a, b) and score >= 0.70:
        return True

    return False


def kickoff_close(db_kickoff: datetime, api_commence: Optional[str], max_minutes_diff: int = 180) -> bool:
    api_ts = parse_iso_ts(api_commence)
    if not api_ts:
        return True
    diff_minutes = abs((db_kickoff - api_ts).total_seconds()) / 60.0
    return diff_minutes <= max_minutes_diff


def build_dedup_key(
    match_id: str,
    db_home_team: Optional[str],
    db_away_team: Optional[str],
    api_home_team: Optional[str],
    api_away_team: Optional[str],
) -> str:
    raw = "||".join([
        str(match_id or ""),
        str(db_home_team or ""),
        str(db_away_team or ""),
        str(api_home_team or ""),
        str(api_away_team or ""),
    ])
    return sha256(raw.encode("utf-8")).hexdigest()


# -----------------------------------------------------------------------------
# DB Lookups
# -----------------------------------------------------------------------------

def load_active_odds_mappings(conn) -> Dict[str, Dict[str, Any]]:
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

    by_alias: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_alias.setdefault(row["alias_normalized"], []).append(dict(row))

    logger.info("Aktive Team-Aliase geladen: %s Alias-Normalformen", len(by_alias))
    return by_alias


def get_upcoming_matches(conn, competition_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select
              id as match_id,
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
            (LOOKAHEAD_DAYS,),
        )
        rows = cur.fetchall()

    matches: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []

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

        item = dict(row)
        item["sport_key"] = comp["sport_key"]
        matches.append(item)

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

    logger.info("Relevante Matches im Lookahead: %s", len(matches))
    return matches


# -----------------------------------------------------------------------------
# Odds API
# -----------------------------------------------------------------------------

def fetch_available_sports() -> List[Dict[str, Any]]:
    params = {"apiKey": ODDS_API_KEY}
    response = requests.get(SPORTS_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    sports = response.json()
    logger.info("Verfügbare Sports geladen: %s", len(sports))
    return sports


def fetch_odds_for_sport(sport_key: str, max_retries: int = MAX_RETRIES) -> List[Dict[str, Any]]:
    backoff = INITIAL_BACKOFF_SECONDS

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                ODDS_URL_TEMPLATE.format(sport_key=sport_key),
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions": ODDS_REGIONS,
                    "markets": ODDS_MARKETS,
                    "oddsFormat": "decimal",
                },
                timeout=REQUEST_TIMEOUT,
            )

            used = response.headers.get("x-requests-used")
            remaining = response.headers.get("x-requests-remaining")
            if used or remaining:
                logger.info("Odds API Credits | used=%s remaining=%s", used, remaining)

            if response.status_code == 429:
                logger.warning(
                    "429 Rate Limit | sport_key=%s | attempt=%s/%s | wait=%ss",
                    sport_key, attempt, max_retries, backoff,
                )
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                logger.error("429 bleibt bestehen → skip sport_key=%s", sport_key)
                return []

            response.raise_for_status()
            events = response.json()
            logger.info("sport_key=%s | API events=%s", sport_key, len(events))
            return events

        except requests.RequestException as exc:
            logger.warning(
                "Request Fehler | sport_key=%s | attempt=%s/%s | error=%s",
                sport_key, attempt, max_retries, exc,
            )
            if attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.error("Request endgültig fehlgeschlagen → skip sport_key=%s", sport_key)
            return []

    return []


# -----------------------------------------------------------------------------
# Kandidatensuche mit DB-Aliaswissen
# -----------------------------------------------------------------------------

def build_db_candidate_set(
    team_name: str,
    competition_code: str,
    alias_lookup: Dict[str, List[Dict[str, Any]]],
) -> set[str]:
    """
    Kandidaten für DB-Teamnamen:
    - technische Normalform
    - bekannte Alias-/Kanonikformen aus der DB
    """
    base_norm = normalize_team_name(team_name)
    candidates = {base_norm}

    alias_rows = alias_lookup.get(base_norm, [])
    for row in alias_rows:
        row_source = row.get("source_system")
        row_comp = row.get("competition_code")

        source_ok = row_source is None or row_source == ODDS_SOURCE_SYSTEM
        comp_ok = row_comp is None or row_comp == competition_code

        if source_ok and comp_ok:
            candidates.add(row["alias_normalized"])
            candidates.add(row["canonical_normalized_name"])

    return {c for c in candidates if c}


def build_api_candidate_set(
    api_name: str,
    competition_code: str,
    alias_lookup: Dict[str, List[Dict[str, Any]]],
) -> set[str]:
    """
    Kandidaten für API-Teamnamen:
    - technische Normalform
    - falls bereits bekannter Alias/Kanonik, auch kanonische DB-Form
    """
    base_norm = normalize_team_name(api_name)
    candidates = {base_norm}

    alias_rows = alias_lookup.get(base_norm, [])
    for row in alias_rows:
        row_source = row.get("source_system")
        row_comp = row.get("competition_code")

        source_ok = row_source is None or row_source == ODDS_SOURCE_SYSTEM
        comp_ok = row_comp is None or row_comp == competition_code

        if source_ok and comp_ok:
            candidates.add(row["alias_normalized"])
            candidates.add(row["canonical_normalized_name"])

    return {c for c in candidates if c}


def find_best_candidate(
    match: Dict[str, Any],
    events: List[Dict[str, Any]],
    alias_lookup: Dict[str, List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    db_home_norm = normalize_team_name(match["home_team"])
    db_away_norm = normalize_team_name(match["away_team"])

    db_home_candidates = build_db_candidate_set(
        team_name=match["home_team"],
        competition_code=match["competition_code"],
        alias_lookup=alias_lookup,
    )
    db_away_candidates = build_db_candidate_set(
        team_name=match["away_team"],
        competition_code=match["competition_code"],
        alias_lookup=alias_lookup,
    )

    best: Optional[Dict[str, Any]] = None
    best_score = -1.0

    for event in events:
        api_home = event.get("home_team")
        api_away = event.get("away_team")
        api_home_norm = normalize_team_name(api_home)
        api_away_norm = normalize_team_name(api_away)

        if not kickoff_close(match["kickoff_at"], event.get("commence_time")):
            continue

        api_home_candidates = build_api_candidate_set(
            api_name=str(api_home or ""),
            competition_code=match["competition_code"],
            alias_lookup=alias_lookup,
        )
        api_away_candidates = build_api_candidate_set(
            api_name=str(api_away or ""),
            competition_code=match["competition_code"],
            alias_lookup=alias_lookup,
        )

        home_score = 0.0
        away_score = 0.0

        if db_home_candidates & api_home_candidates:
            home_score = 1.0
        else:
            home_score = max(
                [similarity_score(db_home_norm, c) for c in api_home_candidates] or [0.0]
            )

        if db_away_candidates & api_away_candidates:
            away_score = 1.0
        else:
            away_score = max(
                [similarity_score(db_away_norm, c) for c in api_away_candidates] or [0.0]
            )

        total_score = round((home_score + away_score) / 2.0, 4)

        if total_score > best_score:
            best_score = total_score
            best = {
                "api_home_team": api_home,
                "api_away_team": api_away,
                "api_home_team_normalized": api_home_norm,
                "api_away_team_normalized": api_away_norm,
                "api_commence_time": parse_iso_ts(event.get("commence_time")),
                "candidate_score": total_score,
            }

    return best


def should_store_candidate(
    db_home_norm: str,
    db_away_norm: str,
    api_home_norm: str,
    api_away_norm: str,
    candidate_score: float,
) -> bool:
    """
    Speichert nur echte Problemfälle.
    Filtert kosmetische Unterschiede raus.
    """
    if not api_home_norm or not api_away_norm:
        return False

    if db_home_norm == api_home_norm and db_away_norm == api_away_norm:
        return False

    if (
        is_trivial_name_difference(db_home_norm, api_home_norm)
        and is_trivial_name_difference(db_away_norm, api_away_norm)
    ):
        return False

    if candidate_score >= 0.95:
        return False

    if candidate_score < 0.35:
        return False

    return True


# -----------------------------------------------------------------------------
# Inserts
# -----------------------------------------------------------------------------

def insert_candidates(conn, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into public.team_alias_candidates (
              competition_code,
              competition_name,
              country,
              match_id,
              db_home_team,
              db_away_team,
              db_home_team_normalized,
              db_away_team_normalized,
              api_home_team,
              api_away_team,
              api_home_team_normalized,
              api_away_team_normalized,
              sport_key,
              api_commence_time,
              candidate_score,
              status,
              note,
              dedup_key
            ) values %s
            on conflict (dedup_key) do nothing
            """,
            rows,
            page_size=500,
        )

    return len(rows)


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


def build_alias_event_rows_from_candidate(
    match: Dict[str, Any],
    best: Dict[str, Any],
    alias_lookup: Dict[str, List[Dict[str, Any]]],
) -> List[Tuple[Any, ...]]:
    rows: List[Tuple[Any, ...]] = []

    for side, raw_name in (
        ("home", best.get("api_home_team")),
        ("away", best.get("api_away_team")),
    ):
        raw_norm = normalize_team_name(raw_name)
        if not raw_norm:
            continue

        if raw_norm in alias_lookup:
            continue

        context = {
            "match_id": str(match["match_id"]),
            "db_home_team": match["home_team"],
            "db_away_team": match["away_team"],
            "api_home_team": best.get("api_home_team"),
            "api_away_team": best.get("api_away_team"),
            "api_commence_time": best.get("api_commence_time").isoformat() if best.get("api_commence_time") else None,
            "candidate_score": best.get("candidate_score"),
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
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        sports = fetch_available_sports()
        active_sport_keys = {s.get("key") for s in sports if s.get("active", True)}

        competition_map = load_active_odds_mappings(conn)
        alias_lookup = load_active_team_aliases(conn)
        matches = get_upcoming_matches(conn, competition_map)

        if not matches:
            logger.info("Keine relevanten Matches gefunden.")
            return

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for match in matches:
            if match["sport_key"] not in active_sport_keys:
                logger.warning(
                    "Sport-Key nicht aktiv/verfügbar laut Odds API: %s | competition=%s",
                    match["sport_key"],
                    match["competition_code"],
                )
                continue
            grouped.setdefault(match["sport_key"], []).append(match)

        candidate_rows: List[Tuple[Any, ...]] = []
        alias_event_rows: List[Tuple[Any, ...]] = []

        for sport_key, sport_matches in grouped.items():
            logger.info("Prüfe sport_key=%s | matches=%s", sport_key, len(sport_matches))
            events = fetch_odds_for_sport(sport_key)

            if not events:
                logger.warning("Keine API-Events für sport_key=%s", sport_key)
                continue

            for match in sport_matches:
                best = find_best_candidate(match, events, alias_lookup)
                if not best:
                    continue

                db_home_norm = normalize_team_name(match["home_team"])
                db_away_norm = normalize_team_name(match["away_team"])

                api_home_norm = best["api_home_team_normalized"]
                api_away_norm = best["api_away_team_normalized"]
                candidate_score = float(best["candidate_score"])

                if not should_store_candidate(
                    db_home_norm=db_home_norm,
                    db_away_norm=db_away_norm,
                    api_home_norm=api_home_norm,
                    api_away_norm=api_away_norm,
                    candidate_score=candidate_score,
                ):
                    continue

                dedup_key = build_dedup_key(
                    match_id=str(match["match_id"]),
                    db_home_team=match["home_team"],
                    db_away_team=match["away_team"],
                    api_home_team=best["api_home_team"],
                    api_away_team=best["api_away_team"],
                )

                candidate_rows.append((
                    match["competition_code"],
                    match["competition_name"],
                    match["country"],
                    match["match_id"],
                    match["home_team"],
                    match["away_team"],
                    db_home_norm,
                    db_away_norm,
                    best["api_home_team"],
                    best["api_away_team"],
                    api_home_norm,
                    api_away_norm,
                    sport_key,
                    best["api_commence_time"],
                    candidate_score,
                    "pending_review",
                    "auto-detected alias candidate",
                    dedup_key,
                ))

                alias_event_rows.extend(
                    build_alias_event_rows_from_candidate(
                        match=match,
                        best=best,
                        alias_lookup=alias_lookup,
                    )
                )

        inserted = insert_candidates(conn, candidate_rows)
        inserted_events = insert_team_alias_events(conn, alias_event_rows)
        conn.commit()

        logger.info("Alias-Kandidaten eingefügt: %s", inserted)
        logger.info("team_alias_events eingefügt: %s", inserted_events)

    except Exception:
        conn.rollback()
        logger.exception("detect_team_alias_candidates.py fehlgeschlagen")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()