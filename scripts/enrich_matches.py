"""
enrich_matches.py
─────────────────
EuroMatch Edge — Data Pipeline Step 2

Reads today's matches from Supabase, fetches standings and recent results
from football-data.org, and PATCHes each match with:
  - home_strength / away_strength   (0–100, derived from league table)
  - form_home    / form_away        (last 5 results from team perspective)

Must run AFTER fetch_matches.py and BEFORE compute_predictions.py.

Environment variables required:
  FOOTBALL_DATA_API_KEY      — football-data.org API key
  SUPABASE_URL               — https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  — Supabase service_role JWT

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
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from config import ACTIVE_SOURCE_CODES, COMPETITION_BY_SOURCE

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

FOOTBALL_DATA_BASE    = "https://api.football-data.org/v4"
REQUEST_DELAY_SECONDS = 7   # free tier: 10 req/min

# Competitions that have a usable /standings endpoint on football-data.org.
# International cup competitions (CL) use a group/knockout format — no single table.
STANDINGS_CAPABLE: set[str] = {
    "PL", "BL1", "PD", "SA", "FL1", "DED", "PPL", "ELC",
    # TSL and EL currently return 403 on free tier — excluded via config.py active=False
}


# ─────────────────────────────────────────────────────────────
# FOOTBALL-DATA CLIENT
# ─────────────────────────────────────────────────────────────

class FootballDataClient:
    """Thin wrapper around football-data.org v4 REST API."""

    def __init__(self, api_key: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "X-Auth-Token": api_key,
            "Accept":       "application/json",
        })

    def _get(self, path: str, params: dict | None = None) -> Any:
        """GET helper with rate-limit retry. Raises HTTPError on failure."""
        url      = f"{FOOTBALL_DATA_BASE}{path}"
        response = self.session.get(url, params=params, timeout=20)

        if response.status_code == 429:
            log.warning("  Rate limited (429) — waiting 60 s.")
            time.sleep(60)
            response = self.session.get(url, params=params, timeout=20)

        response.raise_for_status()
        return response.json()

    def get_standings(self, source_code: str) -> list[dict[str, Any]]:
        """
        Fetch current standings for a competition.
        Returns a flat list of standing entries (one per team).
        Returns [] if the competition has no standings (e.g. CL group stage).
        """
        try:
            data = self._get(f"/competitions/{source_code}/standings")
        except requests.HTTPError as exc:
            log.warning("  Standings unavailable for %s: %s", source_code, exc)
            return []

        # football-data wraps standings in a list of stage objects
        # For a single-table league the first entry's "table" is what we need
        standings = data.get("standings", [])
        if not standings:
            return []

        # TOTAL type is the overall league table (vs HOME/AWAY splits)
        for s in standings:
            if s.get("type") == "TOTAL":
                return s.get("table", [])

        # Fallback: just return the first table
        return standings[0].get("table", [])

    def get_team_recent_matches(self, team_id: int, limit: int = 5) -> list[dict[str, Any]]:
        """
        Fetch the most recent FINISHED matches for a team.
        Returns up to `limit` matches, newest first (as the API delivers them).
        """
        try:
            data = self._get(
                f"/teams/{team_id}/matches",
                params={"status": "FINISHED", "limit": limit},
            )
            return data.get("matches", [])
        except requests.HTTPError as exc:
            log.warning("  Recent matches unavailable for team %s: %s", team_id, exc)
            return []


# ─────────────────────────────────────────────────────────────
# MODEL HELPERS
# ─────────────────────────────────────────────────────────────

def compute_strength(table: list[dict[str, Any]]) -> dict[int, int]:
    """
    Derive a 0–100 strength score for each team from a league standings table.

    Formula (all values normalised against the league maximum):
      raw = points * 0.50 + goalDifference * 0.30 + won * 0.20
      strength = round(raw * 100)

    Returns { team_id: strength_score }.
    Teams with negative goalDifference are floored at 0 before normalising.
    """
    if not table:
        return {}

    # Extract raw values, floor negative goal differences at 0 for normalisation
    rows = []
    for entry in table:
        team_id = entry.get("team", {}).get("id")
        if team_id is None:
            continue
        rows.append({
            "id":  team_id,
            "pts": entry.get("points", 0) or 0,
            "gd":  max(entry.get("goalDifference", 0) or 0, 0),
            "won": entry.get("won", 0) or 0,
        })

    if not rows:
        return {}

    max_pts = max(r["pts"] for r in rows) or 1
    max_gd  = max(r["gd"]  for r in rows) or 1
    max_won = max(r["won"] for r in rows) or 1

    result: dict[int, int] = {}
    for r in rows:
        raw = (
            (r["pts"] / max_pts) * 0.50 +
            (r["gd"]  / max_gd)  * 0.30 +
            (r["won"] / max_won) * 0.20
        )
        result[r["id"]] = max(5, round(raw * 100))   # floor at 5 — no team gets 0

    return result


def compute_form(matches: list[dict[str, Any]], team_id: int) -> list[str]:
    """
    Derive a form array from the last 5 finished matches of a team.

    Returns ['W','D','L','W','W'] with the OLDEST result first.
    The API returns matches newest-first, so we reverse at the end.
    Skips matches with missing score data.
    """
    form: list[str] = []

    for m in matches:
        score = m.get("score", {}).get("fullTime", {})
        home_g = score.get("home")
        away_g = score.get("away")

        if home_g is None or away_g is None:
            continue   # abandoned match or missing score — skip

        is_home = m.get("homeTeam", {}).get("id") == team_id

        if is_home:
            result = "W" if home_g > away_g else ("D" if home_g == away_g else "L")
        else:
            result = "W" if away_g > home_g else ("D" if home_g == away_g else "L")

        form.append(result)
        if len(form) == 5:
            break

    return list(reversed(form))   # oldest first


# ─────────────────────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────────────────────

class SupabaseClient:
    """Minimal Supabase REST client (service_role)."""

    def __init__(self, url: str, service_role_key: str) -> None:
        self.base_url = url.rstrip("/")
        self._headers = {
            "apikey":        service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type":  "application/json",
            "Prefer":        "return=minimal",
        }

    def fetch_today_matches(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        """
        Fetch matches in the time window that have source IDs set.
        Only retrieves the fields needed for enrichment.
        """
        params = [
            ("kickoff_at",            f"gte.{date_from}"),
            ("kickoff_at",            f"lt.{date_to}"),
            ("home_team_source_id",   "not.is.null"),
            ("away_team_source_id",   "not.is.null"),
            ("order",                 "kickoff_at.asc"),
            ("select",                "id,external_id,competition_code,home_team,away_team,home_team_source_id,away_team_source_id"),
        ]
        url = f"{self.base_url}/rest/v1/matches"
        response = requests.get(
            url,
            params=params,
            headers={**self._headers, "Range": "0-999"},
            timeout=20,
        )
        if not response.ok:
            log.error(
                "  Supabase fetch failed: %d %s\n  URL : %s\n  Body: %s",
                response.status_code, response.reason,
                response.url, response.text[:500],
            )
            response.raise_for_status()
        rows = response.json()
        log.info("  → %d match(es) with source IDs found.", len(rows))
        return rows

    def patch_match(self, external_id: str, data: dict[str, Any]) -> None:
        """PATCH a single match row identified by external_id."""
        url = f"{self.base_url}/rest/v1/matches"
        response = requests.patch(
            url,
            params={"external_id": f"eq.{external_id}"},
            headers=self._headers,
            data=json.dumps(data),
            timeout=20,
        )
        if not response.ok:
            log.error(
                "  PATCH failed for %s: %d %s\n  URL : %s\n  Body: %s",
                external_id,
                response.status_code, response.reason,
                response.url, response.text[:300],
            )
            response.raise_for_status()


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enrich matches with strength scores and form arrays."
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=2,
        help="How many days ahead of now to include (default: 2).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute enrichment data but do NOT write to Supabase.",
    )
    return parser


def load_env() -> tuple[str, str, str]:
    api_key      = os.environ.get("FOOTBALL_DATA_API_KEY",     "").strip()
    supabase_url = os.environ.get("SUPABASE_URL",              "").strip()
    service_key  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    missing = [
        name for name, val in [
            ("FOOTBALL_DATA_API_KEY",     api_key),
            ("SUPABASE_URL",              supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", service_key),
        ]
        if not val
    ]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    return api_key, supabase_url, service_key


# ─────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────

def run(days_ahead: int, dry_run: bool) -> None:
    now       = datetime.now(timezone.utc)
    date_from = now.replace(microsecond=0).isoformat()
    date_to   = (now + timedelta(days=days_ahead)).replace(microsecond=0).isoformat()

    log.info("═══════════════════════════════════════════════")
    log.info("EuroMatch Edge — enrich_matches.py")
    log.info("Window   : now → +%d day(s)", days_ahead)
    log.info("From     : %s", date_from)
    log.info("To       : %s", date_to)
    log.info("Dry run  : %s", dry_run)
    log.info("═══════════════════════════════════════════════")

    api_key, supabase_url, service_key = load_env()

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

    # ── Step 2: group team IDs by source competition code ─────
    # We need the football-data source_code (e.g. "BL1") for the standings call,
    # not our internal competition_code (e.g. "BUNDESLIGA").
    # Build a reverse lookup from config.
    internal_to_source: dict[str, str] = {
        c.competition_code: c.source_code
        for c in COMPETITION_BY_SOURCE.values()
    }

    # { source_code: set of team_ids that play today in this competition }
    teams_by_source: dict[str, set[int]] = {}

    for m in matches:
        source_code = internal_to_source.get(m["competition_code"])
        if not source_code:
            log.warning("  No source_code for competition_code=%s — skipping strength.", m["competition_code"])
            continue
        teams_by_source.setdefault(source_code, set())
        if m["home_team_source_id"]:
            teams_by_source[source_code].add(int(m["home_team_source_id"]))
        if m["away_team_source_id"]:
            teams_by_source[source_code].add(int(m["away_team_source_id"]))

    log.info(
        "  Competitions with games today: %s",
        list(teams_by_source.keys()),
    )

    # ── Step 3: fetch standings and compute strength scores ───
    log.info("─── Step 3: fetching standings")

    # { team_id: strength_score }
    team_strength: dict[int, int] = {}

    for source_code, team_ids in teams_by_source.items():
     if source_code not in STANDINGS_CAPABLE:
     log.info("  %s — no standings, using form fallback.", source_code)

    # 🔥 FIX: fallback strength aus Form
    for team_id in team_ids:
        form = team_form.get(team_id, [])

        if not form:
            team_strength[team_id] = 50
            continue

        pts = sum({"W":3,"D":1,"L":0}.get(x,0) for x in form)
        max_pts = len(form) * 3

        score = int((pts / max_pts) * 100)

        team_strength[team_id] = max(25, min(85, score))

    time.sleep(REQUEST_DELAY_SECONDS)
    continue

      
        log.info("  GET standings: %s", source_code)
        table = fd.get_standings(source_code)
        if not table:
            log.warning("  Empty standings for %s.", source_code)
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        scores = compute_strength(table)
        found  = {tid: scores[tid] for tid in team_ids if tid in scores}
        team_strength.update(found)

        log.info(
            "  %s → %d teams scored (of %d playing today)",
            source_code, len(found), len(team_ids),
        )
        time.sleep(REQUEST_DELAY_SECONDS)

    # ── Step 4: fetch recent matches and compute form ─────────
    log.info("─── Step 4: fetching form (recent matches per team)")

    # Collect all unique team IDs across all competitions
    all_team_ids: set[int] = set()
    for ids in teams_by_source.values():
        all_team_ids.update(ids)

    # { team_id: form_array }
    team_form: dict[int, list[str]] = {}

    for team_id in sorted(all_team_ids):
        log.info("  GET recent matches: team %s", team_id)
        recent = fd.get_team_recent_matches(team_id, limit=5)
        form   = compute_form(recent, team_id)
        team_form[team_id] = form
        log.info("  → form: %s", form or "(empty)")
        time.sleep(REQUEST_DELAY_SECONDS)

    # ── Step 5: build and apply enrichment patches ────────────
    log.info("─── Step 5: patching matches")

    enriched = 0
    skipped  = 0
    errors: list[str] = []

    for m in matches:
        external_id    = m["external_id"]
        home_id        = int(m["home_team_source_id"]) if m["home_team_source_id"] else None
        away_id        = int(m["away_team_source_id"]) if m["away_team_source_id"] else None
        home_team_name = m.get("home_team", "?")
        away_team_name = m.get("away_team", "?")

        home_strength = team_strength.get(home_id) if home_id else None
        away_strength = team_strength.get(away_id) if away_id else None
        form_home     = team_form.get(home_id, []) if home_id else []
        form_away     = team_form.get(away_id, []) if away_id else []

        payload = {
            "home_strength": home_strength,
            "away_strength": away_strength,
            "form_home":     form_home,
            "form_away":     form_away,
            "updated_at":    now.replace(microsecond=0).isoformat(),
        }

        has_strength = home_strength is not None and away_strength is not None
        has_form     = bool(form_home) and bool(form_away)

        log.info(
            "  %s vs %s  strength=%s/%s  form=%s/%s",
            home_team_name, away_team_name,
            home_strength, away_strength,
            form_home or "[]", form_away or "[]",
        )

        if dry_run:
            log.info("  [DRY RUN] Would PATCH %s", external_id)
            enriched += 1
            continue

        try:
            sb.patch_match(external_id, payload)
            enriched += 1
        except requests.HTTPError as exc:
            err = f"{external_id}: PATCH failed — {exc}"
            log.error("  %s", err)
            errors.append(err)
            skipped += 1

    # ── Summary ───────────────────────────────────────────────
    log.info("═══════════════════════════════════════════════")
    log.info("SUMMARY")
    log.info("  Matches total      : %d", len(matches))
    log.info("  Enriched           : %d", enriched)
    log.info("  Skipped/errors     : %d", skipped)
    log.info("  Teams with strength: %d", len(team_strength))
    log.info("  Teams with form    : %d", len(team_form))

    if errors:
        log.warning("  Errors (%d):", len(errors))
        for err in errors:
            log.warning("    - %s", err)

    if enriched == 0:
        log.error("  No matches enriched — marking pipeline as failed.")
        log.info("═══════════════════════════════════════════════")
        sys.exit(1)

    log.info("  Status             : SUCCESS")
    log.info("═══════════════════════════════════════════════")


def main() -> None:
    args = build_arg_parser().parse_args()
    run(days_ahead=args.days_ahead, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
