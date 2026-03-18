import os
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values
import requests


DATABASE_URL = os.environ["DATABASE_URL"]
ODDS_API_KEY = os.environ["ODDS_API_KEY"]

LOOKAHEAD_DAYS = int(os.getenv("ODDS_LOOKAHEAD_DAYS", "14"))
REQUEST_TIMEOUT = int(os.getenv("ODDS_REQUEST_TIMEOUT", "30"))

SPORTS_URL = "https://api.the-odds-api.com/v4/sports"
ODDS_URL_TEMPLATE = "https://api.the-odds-api.com/v4/sports/{sport_key}/odds"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("detect_team_alias_candidates")


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
    value = (
        value.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )

    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = value.replace("/", " ").replace("-", " ")
    value = re.sub(r"[^a-z0-9 ]+", " ", value)

    stopwords = {
        "fc", "sc", "sv", "ev", "afc", "bc", "cf",
        "sk", "fk", "club", "clube"
    }
    parts = [p for p in value.split() if p not in stopwords]
    value = " ".join(parts)

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
    "bayern munchen": ["bayern", "bayern munich"],
    "atalanta": ["atalanta bc"],
    "sporting": ["sporting clube de portugal", "sporting cp", "sporting lisbon", "sporting portugal"],
    "bodo glimt": ["fk bodo glimt", "bodo glimt"],
    "paris saint germain": ["psg", "paris saint germain fc"],
    "real madrid": ["real madrid cf"],
    "barcelona": ["fc barcelona"],
    "atletico madrid": ["club atletico de madrid", "atletico de madrid"],
    "watford": ["watford fc"],
    "wrexham": ["wrexham afc"],
    "southampton": ["southampton fc"],
    "norwich city": ["norwich city fc"],
    "manchester city": ["manchester city fc"],
    "chelsea": ["chelsea fc"],
    "arsenal": ["arsenal fc"],
    "newcastle united": ["newcastle united fc"],
    "tottenham hotspur": ["tottenham hotspur fc", "spurs"],
    "liverpool": ["liverpool fc"],
    "galatasaray": ["galatasaray sk"],
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


def similarity_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0

    set_a = set(a.split())
    set_b = set(b.split())
    if not set_a or not set_b:
        return 0.0

    overlap = len(set_a & set_b)
    union = len(set_a | set_b)
    return overlap / union if union else 0.0


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


def fetch_available_sports() -> List[Dict[str, Any]]:
    response = requests.get(
        SPORTS_URL,
        params={"apiKey": ODDS_API_KEY},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    sports = response.json()
    logger.info("Verfügbare Sports geladen: %s", len(sports))
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

        aliases = {title_norm, key_norm}

        if "bundesliga" in title_norm or "bundesliga" in key_norm:
            aliases.update({"bundesliga", "germany bundesliga"})
        if "premier league" in title_norm or key == "soccer_epl":
            aliases.update({"premier league", "epl", "england premier league"})
        if "la liga" in title_norm:
            aliases.update({"la liga", "spain la liga", "spanien la liga"})
        if "serie a" in title_norm:
            aliases.update({"serie a", "italy serie a", "italien serie a"})
        if "ligue 1" in title_norm:
            aliases.update({"ligue 1", "france ligue 1", "frankreich ligue 1"})
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

    wanted_aliases += [name_norm, f"{country_norm} {name_norm}".strip(), code_norm]

    for alias in wanted_aliases:
        if alias in sport_lookup:
            return sport_lookup[alias]

    return None


def get_upcoming_matches(conn, sport_lookup: Dict[str, str]) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              id,
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

    matches = []
    for row in rows:
        sport_key = resolve_sport_key(
            competition_code=row[2],
            competition_name=row[3],
            country=row[4],
            sport_lookup=sport_lookup,
        )
        if not sport_key:
            continue

        matches.append({
            "match_id": row[0],
            "kickoff_at": row[1],
            "competition_code": row[2],
            "competition_name": row[3],
            "country": row[4],
            "home_team": row[5],
            "away_team": row[6],
            "sport_key": sport_key,
        })

    logger.info("Relevante Matches im Lookahead: %s", len(matches))
    return matches


def fetch_odds_for_sport(sport_key: str) -> List[Dict[str, Any]]:
    response = requests.get(
        ODDS_URL_TEMPLATE.format(sport_key=sport_key),
        params={
            "apiKey": ODDS_API_KEY,
            "regions": "eu,uk",
            "markets": "h2h",
            "oddsFormat": "decimal",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    events = response.json()
    logger.info("sport_key=%s | API events=%s", sport_key, len(events))
    return events


def kickoff_close(db_kickoff: datetime, api_commence: Optional[str], max_minutes_diff: int = 180) -> bool:
    api_ts = parse_iso_ts(api_commence)
    if not api_ts:
        return True
    diff_minutes = abs((db_kickoff - api_ts).total_seconds()) / 60.0
    return diff_minutes <= max_minutes_diff


def find_best_candidate(match: Dict[str, Any], events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    db_home_norm = normalize_team_name(match["home_team"])
    db_away_norm = normalize_team_name(match["away_team"])

    db_home_candidates = set(team_name_candidates(match["home_team"]))
    db_away_candidates = set(team_name_candidates(match["away_team"]))

    best = None
    best_score = -1.0

    for event in events:
        api_home = event.get("home_team")
        api_away = event.get("away_team")

        api_home_norm = normalize_team_name(api_home)
        api_away_norm = normalize_team_name(api_away)

        if not kickoff_close(match["kickoff_at"], event.get("commence_time")):
            continue

        home_score = max(
            [similarity_score(c, api_home_norm) for c in db_home_candidates] +
            [similarity_score(db_home_norm, api_home_norm)]
        )
        away_score = max(
            [similarity_score(c, api_away_norm) for c in db_away_candidates] +
            [similarity_score(db_away_norm, api_away_norm)]
        )

        total_score = (home_score + away_score) / 2.0

        if total_score > best_score:
            best_score = total_score
            best = {
                "api_home_team": api_home,
                "api_away_team": api_away,
                "api_home_team_normalized": api_home_norm,
                "api_away_team_normalized": api_away_norm,
                "api_commence_time": parse_iso_ts(event.get("commence_time")),
                "candidate_score": round(total_score, 4),
            }

    return best


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


def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        sports = fetch_available_sports()
        sport_lookup = build_sport_lookup(sports)
        matches = get_upcoming_matches(conn, sport_lookup)

        if not matches:
            logger.info("Keine relevanten Matches gefunden.")
            return

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for match in matches:
            grouped.setdefault(match["sport_key"], []).append(match)

        candidate_rows: List[Tuple[Any, ...]] = []

        for sport_key, sport_matches in grouped.items():
            logger.info("Prüfe sport_key=%s | matches=%s", sport_key, len(sport_matches))
            events = fetch_odds_for_sport(sport_key)

            for match in sport_matches:
                best = find_best_candidate(match, events)
                if not best:
                    continue

                db_home_norm = normalize_team_name(match["home_team"])
                db_away_norm = normalize_team_name(match["away_team"])

                exact_match = (
                    db_home_norm == best["api_home_team_normalized"] and
                    db_away_norm == best["api_away_team_normalized"]
                )

                if exact_match:
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
                    best["api_home_team_normalized"],
                    best["api_away_team_normalized"],
                    sport_key,
                    best["api_commence_time"],
                    best["candidate_score"],
                    "pending_review",
                    "auto-detected alias candidate",
                    dedup_key,
                ))

        inserted = insert_candidates(conn, candidate_rows)
        conn.commit()

        logger.info("Alias-Kandidaten eingefügt: %s", inserted)

    except Exception:
        conn.rollback()
        logger.exception("detect_team_alias_candidates.py fehlgeschlagen")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()