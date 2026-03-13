"""
fetch_matches.py
────────────────
EuroMatch Edge — Data Pipeline Step 1
Fetches upcoming/scheduled matches from football-data.org
and upserts them into the Supabase `matches` table.

Environment variables required:
  FOOTBALL_DATA_API_KEY      — football-data.org API key
  SUPABASE_URL               — https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  — Supabase service_role JWT

Usage:
  python scripts/fetch_matches.py
  python scripts/fetch_matches.py --date-from 2025-06-10 --date-to 2025-06-12
  python scripts/fetch_matches.py --dry-run
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, timedelta
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
SUPABASE_TABLE        = "matches"
ACCEPTED_STATUSES     = {"TIMED", "SCHEDULED"}

# football-data.org free tier: 10 requests/minute → 7 s gap is safe
REQUEST_DELAY_SECONDS = 7


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

    def get_matches(
        self,
        source_code: str,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        """
        Fetch matches for a single competition within a date range.
        Returns the raw list of match objects from the API.
        Raises requests.HTTPError on non-2xx responses (except 404).
        """
        url    = f"{FOOTBALL_DATA_BASE}/competitions/{source_code}/matches"
        params = {
            "dateFrom": date_from,
            "dateTo":   date_to,
            "status":   ",".join(ACCEPTED_STATUSES),
        }

        log.info("  GET %s  [%s → %s]", url, date_from, date_to)
        response = self.session.get(url, params=params, timeout=20)

        if response.status_code == 404:
            log.warning("  %s — not found (404), skipping.", source_code)
            return []

        if response.status_code == 429:
            log.warning("  Rate limited (429) — waiting 60 s before retry.")
            time.sleep(60)
            response = self.session.get(url, params=params, timeout=20)

        response.raise_for_status()

        matches = response.json().get("matches", [])
        log.info("  → %d match(es) found.", len(matches))
        return matches


# ─────────────────────────────────────────────────────────────
# MAPPER — football-data payload → Supabase `matches` row
# ─────────────────────────────────────────────────────────────

def map_match(raw: dict[str, Any], source_code: str) -> dict[str, Any] | None:
    """
    Map a single raw football-data match object to our Supabase `matches` schema.
    Returns None if the match should be skipped.

    This is the only function that knows about the external API format.
    If football-data.org changes its payload structure, only this function changes.
    """
    # Belt-and-suspenders status guard (API filter already applied, but be safe)
    if raw.get("status") not in ACCEPTED_STATUSES:
        return None

    match_id = raw.get("id")
    utc_date = raw.get("utcDate")   # ISO 8601 e.g. "2025-06-10T19:45:00Z"

    if not match_id or not utc_date:
        log.warning("  Skipping match — missing id or utcDate: %s", raw)
        return None

    home_team_raw = raw.get("homeTeam", {})
    away_team_raw = raw.get("awayTeam", {})
    home_name     = home_team_raw.get("name") or home_team_raw.get("shortName")
    away_name     = away_team_raw.get("name") or away_team_raw.get("shortName")

    if not home_name or not away_name:
        log.warning("  Skipping match %s — missing team name(s).", match_id)
        return None

    comp = COMPETITION_BY_SOURCE[source_code]   # always present: we only iterate active codes

    return {
        # ── Identifikation
        "external_id":           f"football_data_{match_id}",

        # ── Zeitplanung
        "kickoff_at":            utc_date,

        # ── Wettbewerb  (sourced from config.py — single source of truth)
        "competition_code":      comp.competition_code,
        "competition_name":      comp.competition_name,
        "competition_type":      comp.competition_type,
        "country":               comp.country,
        "flag":                  comp.flag,     # nullable in DB — fine
        "league_abbr":           comp.league_abbr,

        # ── Teams
        "home_team":             home_name,
        "away_team":             away_name,

        # ── Modell-Eingangsdaten: MVP defaults
        # Enriched later by compute_predictions.py or a dedicated enrichment job.
        "form_home":             [],
        "form_away":             [],
        "home_strength":         None,
        "away_strength":         None,
        "injuries_home":         None,
        "injuries_away":         None,
        "load_home":             None,
        "load_away":             None,
        "motivation_home":       None,
        "motivation_away":       None,
        "market_home_win_odds":  None,
        "weather":               None,

        # ── Herkunft
        "source":                "football_data",
    }


# ─────────────────────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────────────────────

class SupabaseClient:
    """
    Minimal Supabase REST client using the service_role key.
    Avoids the supabase-py SDK intentionally: fewer dependencies,
    and the UPSERT behaviour is fully explicit via HTTP headers.
    """

    def __init__(self, url: str, service_role_key: str) -> None:
        self.base_url = url.rstrip("/")
        self.session  = requests.Session()
        self.session.headers.update({
            "apikey":        service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type":  "application/json",
            # merge-duplicates = UPSERT on the table's UNIQUE constraint (external_id)
            "Prefer":        "resolution=merge-duplicates,return=minimal",
        })

    def upsert(self, table: str, rows: list[dict[str, Any]]) -> None:
        """UPSERT rows into the given table. Raises on HTTP errors."""
        if not rows:
            return

        url      = f"{self.base_url}/rest/v1/{table}"
        response = self.session.post(url, data=json.dumps(rows), timeout=30)

        if not response.ok:
            log.error(
                "  Supabase upsert failed: %d %s\n  Body: %s",
                response.status_code,
                response.reason,
                response.text[:400],
            )
            response.raise_for_status()


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    today    = date.today()
    tomorrow = today + timedelta(days=1)

    parser = argparse.ArgumentParser(
        description="Fetch matches from football-data.org and upsert into Supabase."
    )
    parser.add_argument(
        "--date-from",
        default=today.isoformat(),
        help=f"Start date YYYY-MM-DD (default: today = {today})",
    )
    parser.add_argument(
        "--date-to",
        default=tomorrow.isoformat(),
        help=f"End date YYYY-MM-DD (default: tomorrow = {tomorrow})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and map data but do NOT write to Supabase.",
    )
    return parser


def load_env() -> tuple[str, str, str]:
    """Load and validate required environment variables. Exits on missing values."""
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

def run(date_from: str, date_to: str, dry_run: bool) -> None:
    log.info("═══════════════════════════════════════════════")
    log.info("EuroMatch Edge — fetch_matches.py")
    log.info("Date range  : %s → %s", date_from, date_to)
    log.info("Competitions: %d active", len(ACTIVE_SOURCE_CODES))
    log.info("Dry run     : %s", dry_run)
    log.info("═══════════════════════════════════════════════")

    api_key, supabase_url, service_key = load_env()

    fd_client = FootballDataClient(api_key)
    sb_client = SupabaseClient(supabase_url, service_key) if not dry_run else None

    total_fetched    = 0
    total_mapped     = 0
    total_skipped    = 0
    total_upserted   = 0
    succeeded_codes: list[str] = []
    failed_codes:    list[str] = []
    errors:          list[str] = []

    for source_code in ACTIVE_SOURCE_CODES:
        log.info("─── %s", source_code)

        # ── Fetch
        try:
            raw_matches = fd_client.get_matches(source_code, date_from, date_to)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            err = f"{source_code}: HTTP {status} — skipping"
            log.warning("  %s", err)
            errors.append(err)
            failed_codes.append(source_code)
            time.sleep(REQUEST_DELAY_SECONDS)
            continue
        except requests.RequestException as exc:
            err = f"{source_code}: Network error — {exc}"
            log.warning("  %s", err)
            errors.append(err)
            failed_codes.append(source_code)
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        total_fetched += len(raw_matches)

        # ── Map
        rows: list[dict[str, Any]] = []
        for raw in raw_matches:
            mapped = map_match(raw, source_code)
            if mapped:
                rows.append(mapped)
                total_mapped += 1
            else:
                total_skipped += 1

        # ── Upsert or dry-run report
        if dry_run:
            log.info("  [DRY RUN] Would upsert %d row(s).", len(rows))
            if rows:
                log.info("  Sample row:\n%s", json.dumps(rows[0], indent=2, ensure_ascii=False))
            succeeded_codes.append(source_code)
        elif rows:
            try:
                sb_client.upsert(SUPABASE_TABLE, rows)
                total_upserted += len(rows)
                log.info("  ✓ Upserted %d row(s).", len(rows))
                succeeded_codes.append(source_code)
            except requests.HTTPError as exc:
                err = f"{source_code}: Supabase upsert failed — {exc}"
                log.error("  %s", err)
                errors.append(err)
                failed_codes.append(source_code)
        else:
            # No matches found is not an error — competition simply has no games today
            log.info("  No matches in window.")
            succeeded_codes.append(source_code)

        time.sleep(REQUEST_DELAY_SECONDS)

    # ── Summary
    log.info("═══════════════════════════════════════════════")
    log.info("SUMMARY")
    log.info("  Competitions : %d active", len(ACTIVE_SOURCE_CODES))
    log.info("  Succeeded    : %d  %s", len(succeeded_codes), succeeded_codes)
    log.info("  Failed       : %d  %s", len(failed_codes),   failed_codes)
    log.info("  Fetched      : %d", total_fetched)
    log.info("  Mapped       : %d", total_mapped)
    log.info("  Skipped      : %d", total_skipped)
    log.info("  Upserted     : %d", total_upserted)

    if errors:
        log.warning("  Errors (%d):", len(errors))
        for err in errors:
            log.warning("    - %s", err)

    # Exit 1 only when every competition failed — partial success is still success.
    if len(succeeded_codes) == 0:
        log.error("  No competitions succeeded — marking pipeline as failed.")
        log.info("═══════════════════════════════════════════════")
        sys.exit(1)

    if failed_codes:
        log.warning("  Status : PARTIAL SUCCESS (%d competition(s) failed, pipeline continues)", len(failed_codes))
    else:
        log.info("  Status : SUCCESS")
    log.info("═══════════════════════════════════════════════")


def main() -> None:
    args = build_arg_parser().parse_args()
    run(date_from=args.date_from, date_to=args.date_to, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
