"""
enrich_matches.py
─────────────────
EuroMatch Edge — Data Pipeline Step 2

Reads today's/upcoming matches from Supabase, fetches standings and recent results
from football-data.org, and PATCHes each match with:
  - home_strength / away_strength   (0–100, derived from league table or fallback)
  - form_home    / form_away        (last 5 results from team perspective)

Must run AFTER fetch_matches.py and BEFORE compute_predictions.py.

Environment variables required:
  FOOTBALL_DATA_API_KEY      — football-data.org API key
  SUPABASE_URL               — https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  — Supabase service_role JWT
  DATABASE_URL               — Postgres connection string

Usage:
  python scripts/enrich_matches.py
  python scripts/enrich_matches.py --days-ahead 2
  python scripts/enrich_matches.py --dry-run
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
import requests


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
REQUEST_DELAY_SECONDS = 7   # free tier: 10 req/min

# Competitions with usable /standings on football-data.org
STANDINGS_CAPABLE: set[str] = {
    "PL", "BL1", "PD", "SA", "FL1", "DED", "PPL", "ELC",
}

FORM_LIMIT = 5


# ─────────────────────────────────────────────────────────────
# DB Competition Mapping
# ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CompetitionMapping:
    source_code: str
    competition_code: str
    competition_name: str
    competition_type: str
    country: str
    flag: Optional[str]
    league_abbr: str


def load_active_football_data_mappings(database_url: str) -> Dict[str, CompetitionMapping]:
    """
    Lädt aktive football_data-Mappings aus der DB.
    Rückgabe keyed by internal competition_code.
    """
    sql = """
        select
          external_code as source_code,
          competition_code,
          competition_name,
          competition_type,
          country,
          flag,
          league_abbr
        from public.v_competition_external_map_active
        where source_system = 'football_data'
        order by competition_code
    """

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()

    result: Dict[str, CompetitionMapping] = {}
    for row in rows:
        result[row["competition_code"]] = CompetitionMapping(
            source_code=row["source_code"],
            competition_code=row["competition_code"],
            competition_name=row["competition_name"],
            competition_type=row["competition_type"],
            country=row["country"],
            flag=row["flag"],
            league_abbr=row["league_abbr"],
        )

    log.info("Loaded %d active football_data competition mappings from DB.", len(result))
    return result


# ─────────────────────────────────────────────────────────────
# FOOTBALL-DATA CLIENT
# ─────────────────────────────────────────────────────────────

class FootballDataClient:
    """Thin wrapper around football-data.org v4 REST API."""

    def __init__(self, api_key: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "X-Auth-Token": api_key,
            "Accept": "application/json",
        })

    def _get(self, path: str, params: dict | None = None) -> Any:
        """GET helper with rate-limit retry. Raises HTTPError on failure."""
        url = f"{FOOTBALL_DATA_BASE}{path}"
        response = self.session.get(url, params=params, timeout=20)

        if response.status_code == 429:
            log.warning("  Rate limited (429) — waiting 60 s.")
            time.sleep(60)
            response = self.session.get(url, params=params, timeout=20)

        response.raise_for_status()
        return response.json()

    def get_standings(self, source_code: str) -> list[dict]:
        """
        Fetch standings table for a competition source code, e.g. BL1 / PL.
        Returns the first table if available, else [].
        """
        data = self._get(f"/competitions/{source_code}/standings")
        standings = data.get("standings", []) or []

        # football-data can return multiple tables (e.g. home/away/overall).
        # We prefer TOTAL if available, else first table.
        for block in standings:
            if block.get("type") == "TOTAL":
                table = block.get("table", []) or []
                log.info("  %s standings rows: %d (TOTAL)", source_code, len(table))
                return table

        if standings:
            table = standings[0].get("table", []) or []
            log.info("  %s standings rows: %d (fallback first table)", source_code, len(table))
            return table

        log.info("  %s standings rows: 0", source_code)
        return []

    def get_team_recent_matches(self, team_id: int, limit: int = FORM_LIMIT) -> list[dict]:
        """
        Fetch finished matches for one team.
        We ask for more than needed because some feeds can contain postponed/no-score artifacts.
        """
        data = self._get(
            f"/teams/{team_id}/matches",
            params={"status": "FINISHED", "limit": max(limit * 3, 15)},
        )
        matches = data.get("matches", []) or []
        log.info("    team %s recent matches fetched: %d", team_id, len(matches))
        return matches


# ─────────────────────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────────────────────

class SupabaseClient:
    """Minimal Supabase REST client."""

    def __init__(self, url: str, service_role_key: str) -> None:
        self.base_url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        })

    def fetch_today_matches(self, date_from: str, date_to: str) -> list[dict]:
        """
        Load upcoming matches in the window.
        Must include team source ids because enrich step uses football-data team endpoints.
        """
        url = f"{self.base_url}/rest/v1/matches"
        params = {
            "select": ",".join([
                "id",
                "external_id",
                "kickoff_at",
                "competition_code",
                "competition_name",
                "home_team",
                "away_team",
                "home_team_source_id",
                "away_team_source_id",
                "home_strength",
                "away_strength",
                "form_home",
                "form_away",
                "updated_at",
            ]),
            "kickoff_at": f"gte.{date_from}",
            "kickoff_at": f"lt.{date_to}",  # overwritten? no; use tuple list below instead
            "order": "kickoff_at.asc",
        }

        # requests can't handle duplicate keys via dict cleanly; build manually
        response = self.session.get(
            url,
            params=[
                ("select", ",".join([
                    "id",
                    "external_id",
                    "kickoff_at",
                    "competition_code",
                    "competition_name",
                    "home_team",
                    "away_team",
                    "home_team_source_id",
                    "away_team_source_id",
                    "home_strength",
                    "away_strength",
                    "form_home",
                    "form_away",
                    "updated_at",
                ])),
                ("kickoff_at", f"gte.{date_from}"),
                ("kickoff_at", f"lt.{date_to}"),
                ("order", "kickoff_at.asc"),
            ],
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()
        log.info("  → %d match(es) found.", len(rows))
        return rows

    def patch_match(self, match_id: str, payload: dict) -> None:
        url = f"{self.base_url}/rest/v1/matches"
        response = self.session.patch(
            url,
            params={"id": f"eq.{match_id}"},
            data=json.dumps(payload),
            headers={"Prefer": "return=minimal"},
            timeout=30,
        )
        if not response.ok:
            log.error(
                "  Supabase patch failed: %d %s\n  URL : %s\n  Body: %s",
                response.status_code,
                response.reason,
                response.url,
                response.text[:500],
            )
            response.raise_for_status()


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def result_symbol_from_match(match: dict, team_id: int) -> Optional[str]:
    """
    Convert one finished football-data match into team-perspective form symbol:
      W / D / L
    """
    full_time = (match.get("score") or {}).get("fullTime") or {}
    home_goals = full_time.get("home")
    away_goals = full_time.get("away")
    if home_goals is None or away_goals is None:
        return None

    home_team = (match.get("homeTeam") or {}).get("id")
    away_team = (match.get("awayTeam") or {}).get("id")
    if home_team != team_id and away_team != team_id:
        return None

    if home_goals == away_goals:
        return "D"

    won = (
        (home_team == team_id and home_goals > away_goals) or
        (away_team == team_id and away_goals > home_goals)
    )
    return "W" if won else "L"


def compute_form(recent_matches: list[dict], team_id: int, limit: int = FORM_LIMIT) -> list[str]:
    """
    Most recent first from football-data; we return at most the last `limit` symbols.
    """
    symbols: list[str] = []

    # football-data often returns newest first already, but we sort defensively
    sorted_matches = sorted(
        recent_matches,
        key=lambda m: m.get("utcDate") or "",
        reverse=True,
    )

    for match in sorted_matches:
        symbol = result_symbol_from_match(match, team_id)
        if symbol:
            symbols.append(symbol)
        if len(symbols) >= limit:
            break

    return symbols


def form_score(form: list[str]) -> int:
    """
    Convert W/D/L list into 0..100 score.
      W = 3, D = 1, L = 0
    """
    if not form:
        return 50

    points = 0
    for symbol in form:
        if symbol == "W":
            points += 3
        elif symbol == "D":
            points += 1
        elif symbol == "L":
            points += 0

    max_points = len(form) * 3
    if max_points == 0:
        return 50

    return round((points / max_points) * 100)


def compute_strength_from_standings(table: list[dict]) -> dict[int, int]:
    """
    Convert standings table rows into 0..100 strength.
    Best team ≈ high score, worst team ≈ low score.
    """
    if not table:
        return {}

    positions = []
    for row in table:
        team = row.get("team") or {}
        team_id = team.get("id")
        position = row.get("position")
        points = row.get("points", 0)

        if team_id is None or position is None:
            continue

        positions.append({
            "team_id": int(team_id),
            "position": int(position),
            "points": int(points or 0),
        })

    if not positions:
        return {}

    max_position = max(r["position"] for r in positions)
    min_position = min(r["position"] for r in positions)

    if max_position == min_position:
        return {r["team_id"]: 50 for r in positions}

    result: dict[int, int] = {}
    for row in positions:
        # Higher rank -> higher score.
        rank_component = (max_position - row["position"]) / (max_position - min_position)
        score = 35 + rank_component * 55

        # Small points bonus for teams level on table area.
        score += min(row["points"] / 100.0 * 10.0, 10.0)

        result[row["team_id"]] = max(0, min(100, round(score)))

    return result


def fallback_strength_from_recent_matches(fd: FootballDataClient, team_id: int) -> tuple[int, list[str]]:
    recent = fd.get_team_recent_matches(team_id, limit=FORM_LIMIT)
    form = compute_form(recent, team_id)

    if not form:
        return 50, []

    score = form_score(form)
    return score, form


# ─────────────────────────────────────────────────────────────
# CLI / ENV
# ─────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enrich upcoming matches with strength and form using football-data.org.",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=2,
        help="Window: now → +N days (default: 2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute enrichment but do not PATCH Supabase.",
    )
    return parser


def load_env() -> tuple[str, str, str, str]:
    api_key = os.environ.get("FOOTBALL_DATA_API_KEY", "").strip()
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    database_url = os.environ.get("DATABASE_URL", "").strip()

    missing = [
        name for name, val in [
            ("FOOTBALL_DATA_API_KEY", api_key),
            ("SUPABASE_URL", supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", service_key),
            ("DATABASE_URL", database_url),
        ]
        if not val
    ]

    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    return api_key, supabase_url, service_key, database_url


# ─────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────

def run(days_ahead: int, dry_run: bool) -> None:
    now = utc_now()
    date_from = iso_utc(now)
    date_to = iso_utc(now + timedelta(days=days_ahead))

    log.info("═══════════════════════════════════════════════")
    log.info("EuroMatch Edge — enrich_matches.py")
    log.info("ENRICH_MATCHES_VERSION = db_competition_registry_v1")
    log.info("Window   : now → +%d day(s)", days_ahead)
    log.info("From     : %s", date_from)
    log.info("To       : %s", date_to)
    log.info("Dry run  : %s", dry_run)
    log.info("═══════════════════════════════════════════════")

    api_key, supabase_url, service_key, database_url = load_env()

    competition_map = load_active_football_data_mappings(database_url)

    fd = FootballDataClient(api_key)
    sb = SupabaseClient(supabase_url, service_key)

    # ── Step 1: load today's matches from Supabase ────────────
    log.info("─── Step 1: loading matches from Supabase")
    try:
        matches = sb.fetch_today_matches(date_from, date_to)
    except requests.RequestException as exc:
        log.error("Failed to load matches: %s", exc)
        sys.exit(1)

    if not matches:
        log.info("No enrichable matches found. Nothing to do.")
        log.info("Status: SUCCESS (no-op)")
        return

    # ── Step 2: group team IDs by football-data source competition code ─────
    teams_by_source: dict[str, set[int]] = {}

    for m in matches:
        comp = competition_map.get(m["competition_code"])
        if not comp:
            log.warning(
                "  No football_data mapping for competition_code=%s — skipping strength.",
                m["competition_code"],
            )
            continue

        source_code = comp.source_code
        teams_by_source.setdefault(source_code, set())

        if m.get("home_team_source_id"):
            teams_by_source[source_code].add(int(m["home_team_source_id"]))
        if m.get("away_team_source_id"):
            teams_by_source[source_code].add(int(m["away_team_source_id"]))

    log.info("  Competitions with games today: %s", list(teams_by_source.keys()))

    # ── Step 3: fetch standings and compute strength scores ───
    log.info("─── Step 3: fetching standings")

    team_strength: dict[int, int] = {}
    team_form_cache: dict[int, list[str]] = {}

    for source_code, team_ids in teams_by_source.items():
        if source_code not in STANDINGS_CAPABLE:
            log.info("  %s — no standings, using recent-matches fallback for strength.", source_code)

            for team_id in team_ids:
                strength, form = fallback_strength_from_recent_matches(fd, team_id)
                team_strength[team_id] = strength
                team_form_cache[team_id] = form

                if form:
                    log.info(
                        "    fallback strength team %s -> %s (form=%s)",
                        team_id, strength, "".join(form),
                    )
                else:
                    log.info(
                        "    fallback strength team %s -> 50 (no recent form)",
                        team_id,
                    )
                time.sleep(REQUEST_DELAY_SECONDS)
            continue

        try:
            table = fd.get_standings(source_code)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            log.warning("  %s standings failed (HTTP %s) — fallback to recent matches.", source_code, status)
            table = []

        standings_strength = compute_strength_from_standings(table)

        for team_id in team_ids:
            if team_id in standings_strength:
                team_strength[team_id] = standings_strength[team_id]
                log.info(
                    "    standings strength team %s -> %s",
                    team_id, team_strength[team_id],
                )
            else:
                strength, form = fallback_strength_from_recent_matches(fd, team_id)
                team_strength[team_id] = strength
                team_form_cache[team_id] = form

                if form:
                    log.info(
                        "    fallback strength team %s -> %s (form=%s)",
                        team_id, strength, "".join(form),
                    )
                else:
                    log.info(
                        "    fallback strength team %s -> 50 (no recent form)",
                        team_id,
                    )

            time.sleep(REQUEST_DELAY_SECONDS)

    # ── Step 4: ensure form for all teams ─────────────────────
    log.info("─── Step 4: fetching recent form")

    all_team_ids: set[int] = set()
    for m in matches:
        if m.get("home_team_source_id"):
            all_team_ids.add(int(m["home_team_source_id"]))
        if m.get("away_team_source_id"):
            all_team_ids.add(int(m["away_team_source_id"]))

    for team_id in sorted(all_team_ids):
        if team_id in team_form_cache:
            continue

        recent = fd.get_team_recent_matches(team_id, limit=FORM_LIMIT)
        form = compute_form(recent, team_id)
        team_form_cache[team_id] = form
        log.info("    form team %s -> %s", team_id, "".join(form) if form else "[]")
        time.sleep(REQUEST_DELAY_SECONDS)

    # ── Step 5: patch matches ─────────────────────────────────
    log.info("─── Step 5: patching matches")

    patched = 0
    skipped = 0

    for m in matches:
        home_team_id = int(m["home_team_source_id"]) if m.get("home_team_source_id") else None
        away_team_id = int(m["away_team_source_id"]) if m.get("away_team_source_id") else None

        home_strength = team_strength.get(home_team_id) if home_team_id else 50
        away_strength = team_strength.get(away_team_id) if away_team_id else 50
        form_home = team_form_cache.get(home_team_id, []) if home_team_id else []
        form_away = team_form_cache.get(away_team_id, []) if away_team_id else []

        payload = {
            "home_strength": int(home_strength) if home_strength is not None else None,
            "away_strength": int(away_strength) if away_strength is not None else None,
            "form_home": form_home,
            "form_away": form_away,
        }

        unchanged = (
            m.get("home_strength") == payload["home_strength"] and
            m.get("away_strength") == payload["away_strength"] and
            (m.get("form_home") or []) == payload["form_home"] and
            (m.get("form_away") or []) == payload["form_away"]
        )

        if unchanged:
            skipped += 1
            log.info(
                "  = %s vs %s — unchanged",
                m["home_team"], m["away_team"],
            )
            continue

        if dry_run:
            log.info(
                "  [DRY RUN] %s vs %s -> H:%s A:%s | form %s / %s",
                m["home_team"],
                m["away_team"],
                payload["home_strength"],
                payload["away_strength"],
                "".join(payload["form_home"]),
                "".join(payload["form_away"]),
            )
            patched += 1
            continue

        try:
            sb.patch_match(m["id"], payload)
            patched += 1
            log.info(
                "  ✓ %s vs %s -> H:%s A:%s | form %s / %s",
                m["home_team"],
                m["away_team"],
                payload["home_strength"],
                payload["away_strength"],
                "".join(payload["form_home"]),
                "".join(payload["form_away"]),
            )
        except requests.RequestException as exc:
            log.error(
                "  Patch failed for %s vs %s: %s",
                m["home_team"],
                m["away_team"],
                exc,
            )

    log.info("═══════════════════════════════════════════════")
    log.info("SUMMARY")
    log.info("  Matches loaded : %d", len(matches))
    log.info("  Patched        : %d", patched)
    log.info("  Skipped        : %d", skipped)
    log.info("  Status         : SUCCESS")
    log.info("═══════════════════════════════════════════════")


def main() -> None:
    args = build_arg_parser().parse_args()
    run(days_ahead=args.days_ahead, dry_run=args.dry_run)


if __name__ == "__main__":
    main()