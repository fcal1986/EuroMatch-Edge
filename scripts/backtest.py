#!/usr/bin/env python3
"""
EuroMatch Edge — Serverseitiger Backtest-Runner
================================================
Läuft als GitHub Actions Step (oder lokal) nach Spieltagen.
Berechnet Modell-Prognosen für abgeschlossene Spiele und schreibt
Ergebnisse mit dem SERVICE_ROLE_KEY nach Supabase `model_results`.

ARCHITEKTUR-HINWEIS — warum dieser Ansatz:
───────────────────────────────────────────
Die Supabase `matches`-Tabelle speichert ausschließlich bevorstehende
Spiele (Status TIMED/SCHEDULED aus fetch_matches.py). Sie hat KEINE
Spalten für Spielergebnisse wie actual_home_goals o.ä. — diese existieren
im Schema schlicht nicht. Ein Filter wie

    actual_home_goals=not.is.null

erzeugt deshalb HTTP 400 von PostgREST, weil die Spalte unbekannt ist.

Lösung:
  Abgeschlossene Spiele (FINISHED) werden direkt von football-data.org
  geholt. Die Modell-Eingangsdaten (Stärke, Form, Quoten) werden aus
  Supabase `matches` per external_id beigemischt.

DATENFLUSS:
  football-data.org (FINISHED) ──┐
                                  ├─ merge per external_id ──► Modelle ──► model_results
  Supabase matches (enrichment) ─┘

Umgebungsvariablen (GitHub Actions Secrets):
  SUPABASE_URL               z.B. https://xxx.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  langer JWT — NICHT der anon key
  FOOTBALL_DATA_API_KEY      football-data.org API key (free tier reicht)

Aufruf:
  python backtest.py
  python backtest.py --season 2024 --competition BUNDESLIGA
  python backtest.py --dry-run
"""

# ARCHITEKTUR-HINWEIS:
# model_results ist die Evaluation-/Backtest-Tabelle dieses Projekts.
# Sie speichert Predictions GEGEN tatsächliche Ergebnisse und ist damit
# das Äquivalent zu "prediction_evaluations" aus anderen Systemen.
# Eine separate prediction_evaluations-Tabelle existiert nicht.

import os
import sys
import json
import argparse
import logging
import math
import time
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

# Runtime Metadata Layer
try:
    from utils.runtime_metadata import (
        get_system_version, start_pipeline_run, finish_pipeline_run,
        register_system_release,
    )
except ImportError:
    def get_system_version(): return "dev"
    def start_pipeline_run(j, **kw): return "no-run-id"
    def finish_pipeline_run(r, **kw): pass
    def register_system_release(**kw): pass

load_dotenv()

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backtest")

# ── Config from environment ───────────────────────────────────
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SERVICE_ROLE_KEY  = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
FOOTBALL_DATA_KEY = os.environ.get("FOOTBALL_DATA_API_KEY", "")

TABLE = "model_results"

# ── Modell-Versionen ─────────────────────────────────────────
# Bump these when the corresponding logic changes.
# Must match what compute_predictions.py uses when possible.
# Can be overridden via ENV for controlled rollouts.
MODEL_VERSION       = os.environ.get("MODEL_VERSION",       "1.0")
CALIBRATION_VERSION = os.environ.get("CALIBRATION_VERSION", "1.0")
WEIGHTS_VERSION     = os.environ.get("WEIGHTS_VERSION",     "1.0")

HEADERS_READ = {
    "apikey":        SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "Accept":        "application/json",
}
HEADERS_WRITE = {
    **HEADERS_READ,
    "Content-Type": "application/json",
    "Prefer":       "resolution=merge-duplicates",
}

# ── football-data.org settings ────────────────────────────────
FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
REQUEST_DELAY_S    = 7     # free tier: 10 req/min — 7 s gap is safe

# Internal competition_code → football-data.org source_code
# Mirrors config.py so backtest.py has no import dependency on it.
COMP_CODE_TO_SOURCE = {
    "PREMIER_LEAGUE":  "PL",
    "BUNDESLIGA":      "BL1",
    "LA_LIGA":         "PD",
    "SERIE_A":         "SA",
    "LIGUE_1":         "FL1",
    "EREDIVISIE":      "DED",
    "PRIMEIRA_LIGA":   "PPL",
    "CHAMPIONSHIP":    "ELC",
    "CHAMPIONS_LEAGUE":"CL",
    # SUPER_LIG (TSL) and EUROPA_LEAGUE (EL) are inactive on the free tier.
    # Add them here when re-enabled in config.py.
}


# ═══════════════════════════════════════════════════════════════
# KICKTIPP SCORING
# ═══════════════════════════════════════════════════════════════

def calc_points(pred: str, actual_home: int, actual_away: int) -> int:
    """4 = exakter Score, 2 = richtige Tendenz (H/D/A), 0 = falsch."""
    if not pred:
        return 0
    parts = pred.split(":")
    if len(parts) != 2:
        return 0
    try:
        ph, pa = int(parts[0]), int(parts[1])
    except ValueError:
        return 0
    if ph == actual_home and pa == actual_away:
        return 4
    pred_t = "H" if ph > pa else ("A" if ph < pa else "D")
    act_t  = "H" if actual_home > actual_away else ("A" if actual_home < actual_away else "D")
    return 2 if pred_t == act_t else 0


def calc_outcome_label(pred: str, actual_home: int, actual_away: int) -> str:
    """
    4-stufige Bewertungskategorie für die Historien-Seite.

    exact     — Exakter Score getroffen (z.B. 2:1 getippt, 2:1 gespielt)
    goal_diff — Richtige Tordifferenz, aber falscher Score (z.B. 2:1 → 3:2)
    tendency  — Richtige Tendenz (Heimsieg/Remis/Auswärtssieg), falsche Tordiff
    wrong     — Komplett falsch

    Punkte:  exact=4, goal_diff=2, tendency=2, wrong=0
    (goal_diff und tendency ergeben beide 2 Kicktipp-Punkte, aber sind
    unterschiedliche Qualitäten — goal_diff ist präziser)
    """
    if not pred:
        return "wrong"
    parts = pred.split(":")
    if len(parts) != 2:
        return "wrong"
    try:
        ph, pa = int(parts[0]), int(parts[1])
    except ValueError:
        return "wrong"

    # Exakter Score
    if ph == actual_home and pa == actual_away:
        return "exact"

    pred_diff = ph - pa
    act_diff  = actual_home - actual_away

    pred_t = "H" if ph > pa else ("A" if ph < pa else "D")
    act_t  = "H" if actual_home > actual_away else ("A" if actual_home < actual_away else "D")

    # Richtige Tordifferenz (aber anderer Score, z.B. 2:1 vs 3:2)
    if pred_diff == act_diff and pred_t == act_t:
        return "goal_diff"

    # Richtige Tendenz (Heimsieg/Remis/Auswärtssieg)
    if pred_t == act_t:
        return "tendency"

    return "wrong"


# ═══════════════════════════════════════════════════════════════
# PURE MATH HELPERS (mirror of js/models/shared.js)
# ═══════════════════════════════════════════════════════════════

def form_score(form: list) -> float:
    """W=3, D=1, L=0 → normalised 0–1."""
    if not form:
        return 0.5
    pts = sum(3 if r == "W" else 1 if r == "D" else 0 for r in form)
    return pts / (len(form) * 3)


def strength_diff(m: dict) -> float:
    """(homeStrength - awayStrength) / 100.  Range: −1…+1."""
    h = m.get("home_strength")
    a = m.get("away_strength")
    if h is None or a is None:
        return 0.0
    return (h - a) / 100.0


def pois(k: int, lam: float) -> float:
    """P(X = k) for Poisson(λ=lam)."""
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def score_grid(lambda_h: float, lambda_a: float, max_g: int = 6) -> dict:
    """Full (max_g+1)² score probability grid."""
    return {
        f"{h}:{a}": pois(h, lambda_h) * pois(a, lambda_a)
        for h in range(max_g + 1)
        for a in range(max_g + 1)
    }


def apply_dixon_coles(grid: dict, lh: float, la: float, rho: float = 0.13) -> dict:
    """Dixon–Coles low-score correction on an existing score grid."""
    def tau(h: int, a: int) -> float:
        if h == 0 and a == 0: return 1 - lh * la * rho
        if h == 1 and a == 0: return 1 + la * rho
        if h == 0 and a == 1: return 1 + lh * rho
        if h == 1 and a == 1: return 1 - rho
        return 1.0

    corrected = dict(grid)
    for h, a in [(0, 0), (1, 0), (0, 1), (1, 1)]:
        key = f"{h}:{a}"
        if key in corrected:
            corrected[key] *= tau(h, a)
    total = sum(corrected.values())
    if total > 0:
        corrected = {k: v / total for k, v in corrected.items()}
    return corrected


def top_scores(grid: dict, n: int = 2) -> list:
    return sorted(grid.items(), key=lambda x: x[1], reverse=True)[:n]


def winner_from_grid(grid: dict) -> str:
    p_home = p_away = p_draw = 0.0
    for score, prob in grid.items():
        h, a = map(int, score.split(":"))
        if h > a:   p_home += prob
        elif a > h: p_away += prob
        else:       p_draw += prob
    if p_home >= p_away and p_home >= p_draw: return "home"
    if p_away >= p_home and p_away >= p_draw: return "away"
    return "draw"


# ═══════════════════════════════════════════════════════════════
# MODEL IMPLEMENTATIONS (mirror of js/models/*.js)
# ═══════════════════════════════════════════════════════════════

MODELS: dict = {}

def register_model(fn):
    MODELS[fn.__name__] = fn
    return fn


@register_model
def ensemble(m: dict) -> dict:
    diff     = strength_diff(m)
    form_h   = form_score(m.get("form_home") or [])
    form_a   = form_score(m.get("form_away") or [])
    form_adv = form_h - form_a
    raw      = (m.get("win_probability", 50) / 100) * 0.6 + (0.5 + diff * 0.25 + form_adv * 0.15)
    home_p   = min(0.95, max(0.05, raw / 1.6))
    winner   = "home" if home_p > 0.55 else ("away" if home_p < 0.40 else "draw")
    conf     = int(50 + abs(home_p - 0.5) * 80)
    hg       = 1.2 + diff * 1.5 + form_adv * 0.5
    ag       = 0.7 - diff * 0.8 + (1 - form_adv) * 0.3
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": min(99, conf)}


@register_model
def poisson(m: dict) -> dict:
    diff   = strength_diff(m)
    form_h = form_score(m.get("form_home") or [])
    form_a = form_score(m.get("form_away") or [])
    lh     = max(0.3, 1.4 + diff * 1.2 + (form_h - 0.5) * 0.6)
    la     = max(0.3, 1.1 - diff * 0.8 + (form_a - 0.5) * 0.4)
    grid   = score_grid(lh, la)
    winner = winner_from_grid(grid)
    top    = top_scores(grid, 1)
    main_tip = top[0][0] if top else f"{round(lh)}:{round(la)}"
    conf   = int(min(85, top[0][1] * 100 * 2.5)) if top else 50
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": conf}


@register_model
def dc(m: dict) -> dict:
    diff   = strength_diff(m)
    form_h = form_score(m.get("form_home") or [])
    form_a = form_score(m.get("form_away") or [])
    lh     = max(0.3, 1.3 + diff * 1.0 + (form_h - 0.5) * 0.5)
    la     = max(0.3, 1.0 - diff * 0.7 + (form_a - 0.5) * 0.4)
    raw    = score_grid(lh, la)
    grid   = apply_dixon_coles(raw, lh, la)
    winner = winner_from_grid(grid)
    top    = top_scores(grid, 1)
    main_tip = top[0][0] if top else f"{round(lh)}:{round(la)}"
    conf   = int(min(85, top[0][1] * 100 * 2.5)) if top else 50
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": conf}


@register_model
def elo(m: dict) -> dict:
    diff   = strength_diff(m)
    home_p = 1 / (1 + 10 ** (-(diff * 4 + 0.1)))
    winner = "home" if home_p > 0.58 else ("away" if home_p < 0.38 else "draw")
    conf   = int(45 + abs(home_p - 0.5) * 90)
    hg     = 1.0 + diff * 1.8
    ag     = 0.9 - diff * 0.9
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": min(99, conf)}


@register_model
def market(m: dict) -> dict:
    # Column in Supabase is market_home_win_odds (set by fetch_matches.py)
    quote  = m.get("market_home_win_odds") or m.get("market_signal_home") or 2.0
    implied = min(0.92, max(0.08, (1 / quote) * 0.9))
    winner  = "home" if implied > 0.55 else ("away" if implied < 0.40 else "draw")
    conf    = int(40 + implied * 45)
    diff    = strength_diff(m)
    hg      = 0.9 + diff * 1.2
    ag      = 0.8 - diff * 0.6
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": min(90, conf)}


# ═══════════════════════════════════════════════════════════════
# SUPABASE HELPERS — reads only, full error context on failure
# ═══════════════════════════════════════════════════════════════

def _supabase_get(table: str, params: list) -> list:
    """
    GET from Supabase PostgREST.
    On any non-2xx response raises RuntimeError with URL + status + body.
    Never swallows errors silently.
    """
    url = f"{SUPABASE_URL}/rest/v1/{table}?{urlencode(params)}"
    try:
        r = requests.get(url, headers=HEADERS_READ, timeout=15)
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Supabase GET network error: {exc}") from exc

    if not r.ok:
        raise RuntimeError(
            f"Supabase GET failed\n"
            f"  Table  : {table}\n"
            f"  URL    : {r.url}\n"
            f"  Status : {r.status_code}\n"
            f"  Body   : {r.text[:600]}\n"
            f"\nHint: HTTP 400 usually means a column in the filter does not "
            f"exist in the table or view. Check the column names carefully."
        )

    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(
            f"Supabase GET returned unexpected type {type(data).__name__}. "
            f"Expected list. URL: {r.url}  Body: {r.text[:300]}"
        )
    return data


def fetch_supabase_enrichment(competition_code: str | None = None) -> dict:
    """
    Load model input data + display metadata from Supabase `matches`.

    Only queries columns that ACTUALLY EXIST in the schema.
    Returns dict: external_id → row  (e.g. "football_data_12345" → {...})
    """
    params = [
        # Request only known columns — avoids HTTP 400 from unknown columns
        ("select", "external_id,competition_code,league_abbr,flag,"
                   "home_team,away_team,"
                   "home_strength,away_strength,"
                   "form_home,form_away,"
                   "market_home_win_odds"),
        ("limit",  "5000"),
    ]
    if competition_code:
        params.append(("competition_code", f"eq.{competition_code}"))

    rows = _supabase_get("matches", params)
    log.info("Supabase enrichment: %d rows loaded", len(rows))
    return {row["external_id"]: row for row in rows if row.get("external_id")}


# ═══════════════════════════════════════════════════════════════
# FOOTBALL-DATA.ORG — fetch FINISHED matches with actual results
# ═══════════════════════════════════════════════════════════════

def _fd_get(path: str, params: dict) -> dict:
    """
    GET from football-data.org v4 API with full error context.
    Returns parsed JSON dict. Raises RuntimeError on failure.
    """
    if not FOOTBALL_DATA_KEY:
        raise RuntimeError(
            "FOOTBALL_DATA_API_KEY is not set.\n"
            "This variable is required to fetch finished match results.\n"
            "Add it to GitHub Secrets and expose it in backtest-pipeline.yml:\n"
            "  env:\n"
            "    FOOTBALL_DATA_API_KEY: ${{ secrets.FOOTBALL_DATA_API_KEY }}"
        )

    url = f"{FOOTBALL_DATA_BASE}/{path.lstrip('/')}"
    try:
        r = requests.get(
            url,
            headers={"X-Auth-Token": FOOTBALL_DATA_KEY},
            params=params,
            timeout=15,
        )
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"football-data.org network error: {exc}") from exc

    return r   # caller inspects status


def fetch_finished_from_api(
    competition_code: str | None = None,
    season: str | None = None,
    enrichment: dict | None = None,
) -> list:
    """
    Fetch FINISHED matches from football-data.org and merge enrichment data.

    Returns list of normalised match dicts, one per finished game, each
    containing actual_home_goals, actual_away_goals, and model inputs.

    Args:
        competition_code: Internal code, e.g. 'BUNDESLIGA'. None = all active.
        season: Year string, e.g. '2024' or '2024/25' (only year part is used).
        enrichment: Pre-fetched Supabase enrichment dict. Fetched if None.
    """
    if enrichment is None:
        enrichment = fetch_supabase_enrichment(competition_code)

    # Determine which competitions to request
    if competition_code:
        if competition_code not in COMP_CODE_TO_SOURCE:
            raise RuntimeError(
                f"Unknown competition_code '{competition_code}'.\n"
                f"Known codes: {sorted(COMP_CODE_TO_SOURCE)}"
            )
        comps = {competition_code: COMP_CODE_TO_SOURCE[competition_code]}
    else:
        comps = dict(COMP_CODE_TO_SOURCE)

    # Extract 4-digit season year if provided
    season_year: str | None = None
    if season:
        # Accept "2024/25" or "2024" or "24/25"
        year_part = season.split("/")[0].strip()
        if len(year_part) == 2:
            year_part = f"20{year_part}"
        season_year = year_part if year_part.isdigit() else None
        if not season_year:
            log.warning("Could not parse season year from '%s' — ignoring", season)

    all_matches: list[dict] = []

    for comp_code, fd_source in comps.items():
        params: dict = {"status": "FINISHED", "limit": 100}
        if season_year:
            params["season"] = season_year

        r = _fd_get(f"competitions/{fd_source}/matches", params)

        if r.status_code == 404:
            log.warning("%s (%s): 404 from football-data — skipping", comp_code, fd_source)
            time.sleep(REQUEST_DELAY_S)
            continue
        if r.status_code == 403:
            log.warning(
                "%s (%s): 403 Forbidden — competition not available on current API tier",
                comp_code, fd_source,
            )
            time.sleep(REQUEST_DELAY_S)
            continue
        if r.status_code == 429:
            log.warning("%s: rate-limited (429) — sleeping 60 s", comp_code)
            time.sleep(60)
            r = _fd_get(f"competitions/{fd_source}/matches", params)
            if not r.ok:
                log.error("%s: still failing after rate-limit retry — skipping", comp_code)
                continue

        if not r.ok:
            log.error(
                "%s: HTTP %d — %s — skipping",
                comp_code, r.status_code, r.text[:200],
            )
            time.sleep(REQUEST_DELAY_S)
            continue

        raw_matches = r.json().get("matches", [])
        log.info("%s: %d FINISHED matches from football-data", comp_code, len(raw_matches))

        for raw in raw_matches:
            # Parse actual goals from the football-data score object
            ft         = raw.get("score", {}).get("fullTime", {})
            home_goals = ft.get("home")
            away_goals = ft.get("away")

            if home_goals is None or away_goals is None:
                log.debug("Skipping match %s — fullTime score missing", raw.get("id"))
                continue

            fd_match_id = raw.get("id")
            external_id = f"football_data_{fd_match_id}"
            enrich      = enrichment.get(external_id, {})

            if not enrich:
                log.debug(
                    "No Supabase enrichment for %s — model inputs will use defaults",
                    external_id,
                )

            # Extract team names from football-data.org (fallback when Supabase enrichment missing)
            raw_home = raw.get("homeTeam", {})
            raw_away = raw.get("awayTeam", {})
            home_name = enrich.get("home_team") or raw_home.get("name") or raw_home.get("shortName") or "?"
            away_name = enrich.get("away_team") or raw_away.get("name") or raw_away.get("shortName") or "?"

            all_matches.append({
                # ── Identity ──────────────────────────────────────
                # Use external_id as match_id so it matches the
                # external_id stored in the Supabase matches table.
                "id":                external_id,
                "external_id":       external_id,

                # ── Actual result (from football-data.org) ────────
                "actual_home_goals": int(home_goals),
                "actual_away_goals": int(away_goals),

                # ── Display metadata (from Supabase, falls back to API) ─
                "home_team":         home_name,
                "away_team":         away_name,
                "flag":              enrich.get("flag", ""),
                "league_abbr":       enrich.get("league_abbr", comp_code),

                # ── Model inputs (from Supabase enrichment) ───────
                # All may be None if the match was not in Supabase yet.
                "home_strength":     enrich.get("home_strength"),
                "away_strength":     enrich.get("away_strength"),
                "form_home":         enrich.get("form_home") or [],
                "form_away":         enrich.get("form_away") or [],
                # market_home_win_odds is the column name in Supabase;
                # the market model also accepts market_signal_home as fallback.
                "market_home_win_odds": enrich.get("market_home_win_odds"),

                # win_probability not available for historic matches —
                # ensemble model falls back to strength+form only.
                "win_probability":   50,

                # ── Context ───────────────────────────────────────
                "competition_code":  enrich.get("competition_code", comp_code),
                "season":            season_year or "",
                "kickoff_at":        raw.get("utcDate"),
            })

        time.sleep(REQUEST_DELAY_S)

    return all_matches


def fetch_finished_matches(
    season: str | None = None,
    competition: str | None = None,
) -> list:
    """
    Public entry point: load finished matches with actual results.

    Fetches results from football-data.org (not Supabase) — see module
    docstring for the full explanation of why.
    """
    # Pre-fetch enrichment once for all competitions
    enrichment = fetch_supabase_enrichment(competition)
    return fetch_finished_from_api(
        competition_code=competition,
        season=season,
        enrichment=enrichment,
    )


# ═══════════════════════════════════════════════════════════════
# SUPABASE WRITE — model_results
# ═══════════════════════════════════════════════════════════════

def upsert_results(rows: list) -> None:
    """
    Upsert backtest records into Supabase `model_results`.
    Conflict key: (match_id, model_key) — idempotent re-runs.
    Uses SERVICE_ROLE_KEY — never the anon key.
    """
    if not rows:
        log.info("No rows to write.")
        return

    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=match_id,model_key"
    try:
        r = requests.post(url, headers=HEADERS_WRITE, json=rows, timeout=30)
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Supabase upsert network error: {exc}") from exc

    if not r.ok:
        raise RuntimeError(
            f"Supabase upsert failed\n"
            f"  URL    : {r.url}\n"
            f"  Status : {r.status_code}\n"
            f"  Body   : {r.text[:600]}"
        )
    log.info("Wrote %d records to %s", len(rows), TABLE)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def run_backtest(
    season: str | None = None,
    competition: str | None = None,
    dry_run: bool = False,
) -> None:
    log.info("── Backtest start ──────────────────────────────────")
    log.info("  competition : %s", competition or "all active")
    log.info("  season      : %s", season or "latest")
    log.info("  dry_run     : %s", dry_run)

    _sys_version = get_system_version()
    _run_id      = start_pipeline_run(
        "backtest",
        system_version=_sys_version,
        metadata={"season": season, "competition": competition, "dry_run": dry_run},
    )
    register_system_release()
    log.info("  version     : %s", _sys_version)
    log.info("  run_id      : %s", _run_id)

    matches = fetch_finished_matches(season=season, competition=competition)
    log.info("Fetched %d finished matches", len(matches))

    if not matches:
        log.warning("No finished matches found — nothing to do.")
        return

    rows: list[dict] = []
    skipped = 0

    for m in matches:
        actual_h = m.get("actual_home_goals")
        actual_a = m.get("actual_away_goals")
        if actual_h is None or actual_a is None:
            skipped += 1
            continue

        actual_score  = f"{actual_h}:{actual_a}"
        actual_winner = (
            "home"  if actual_h > actual_a else
            "away"  if actual_a > actual_h else
            "draw"
        )

        for model_key, model_fn in MODELS.items():
            try:
                result = model_fn(m)
            except Exception as exc:
                log.warning("  [WARN] %s on %s failed: %s", model_key, m.get("id"), exc)
                continue

            pts     = calc_points(result["predicted_score"], actual_h, actual_a)
            outcome = calc_outcome_label(result["predicted_score"], actual_h, actual_a)
            rows.append({
                "match_id":           m["id"],
                "model_key":          model_key,
                "model_version":      MODEL_VERSION,
                "calibration_version": CALIBRATION_VERSION,
                "weights_version":     WEIGHTS_VERSION,
                # Prognose
                "predicted_score":    result["predicted_score"],
                "predicted_winner":   result["predicted_winner"],
                "confidence_score":   result["confidence_score"],
                # Ergebnis
                "actual_score":       actual_score,
                "actual_winner":      actual_winner,
                "actual_home_goals":  actual_h,
                "actual_away_goals":  actual_a,
                # Bewertung
                "points":             pts,
                "outcome_label":      outcome,
                # Spielkontext (denormalisiert für Frontend)
                "home_team":          m.get("home_team", ""),
                "away_team":          m.get("away_team", ""),
                "flag":               m.get("flag", ""),
                "league_abbr":        m.get("league_abbr", ""),
                "competition_code":   m.get("competition_code", ""),
                "season":             m.get("season", season or ""),
                "kickoff_at":         m.get("kickoff_at"),
                # Versioning — filled below after pipeline_run_id is known:
                "system_version":  None,
                "pipeline_run_id": None,
            })

    if skipped:
        log.warning("Skipped %d matches with missing actual goals", skipped)

    log.info("Computed %d predictions (%d matches × %d models)",
             len(rows), len(matches) - skipped, len(MODELS))

    # Inject versioning metadata
    for r in rows:
        r["system_version"]  = _sys_version
        r["pipeline_run_id"] = _run_id

    if dry_run:
        log.info("DRY RUN — no Supabase write")
        # Print first 3 rows for inspection
        sample = rows[:3]
        print(json.dumps(sample, indent=2, default=str))
        # Print per-model summary
        from collections import defaultdict
        by_model: dict = defaultdict(list)
        for r in rows:
            by_model[r["model_key"]].append(r["points"])
        print("\nPoints summary (dry run):")
        for mk, pts_list in sorted(by_model.items()):
            n     = len(pts_list)
            avg   = sum(pts_list) / n if n else 0
            exact = sum(1 for p in pts_list if p == 4)
            print(f"  {mk:12s}  n={n:4d}  avg={avg:.2f}  exact={exact}")
    else:
        upsert_results(rows)

    log.info("── Backtest done ───────────────────────────────────")
    finish_pipeline_run(
        _run_id,
        status="success" if not dry_run else "dry_run",
        metadata={"rows_written": len(rows), "skipped": skipped},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="EuroMatch Edge — Serverseitiger Backtest-Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python backtest.py --dry-run
  python backtest.py --competition BUNDESLIGA --season 2024
  python backtest.py --competition PREMIER_LEAGUE --dry-run

Bekannte competition-Codes:
  """ + "  ".join(sorted(COMP_CODE_TO_SOURCE))
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Saison-Jahr, z.B. '2024' oder '2024/25' (nur Jahr-Teil wird genutzt)",
    )
    parser.add_argument(
        "--competition",
        default=None,
        help="Interner competition_code, z.B. BUNDESLIGA (leer = alle aktiven)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Berechnen ohne Supabase-Write. Gibt Sample-Output und Punktzusammenfassung aus.",
    )
    args = parser.parse_args()
    run_backtest(season=args.season, competition=args.competition, dry_run=args.dry_run)
