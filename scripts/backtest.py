#!/usr/bin/env python3
"""
EuroMatch Edge — historischer Self-Contained Backtest-Runner
============================================================

Ziel:
- FINISHED Matches direkt von football-data.org laden
- historische Features OHNE Current-State-Supabase-Enrichment berechnen
- Modelle auf Basis echter VOR dem Match verfügbarer Historie ausführen
- Ergebnisse nach Supabase `model_results` schreiben

Wichtig:
- Kein Join auf Supabase `matches`
- Keine Abhängigkeit von aktuellen Prediction-/Enrichment-Rows
- Features werden pro Match strikt kausal aus früheren FINISHED Matches derselben Saison berechnet
"""

import argparse
import json
import logging
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backtest")

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

# Evaluation Metrics Layer
try:
    from utils.eval_metrics import (
        compute_eval_metrics,
        probs_from_score_grid,
        probs_from_home_probability,
    )
except ImportError:
    log.warning("utils.eval_metrics not found — extended metrics disabled.")
    def compute_eval_metrics(*a, **kw): return {}
    def probs_from_score_grid(g): return (None, None, None)
    def probs_from_home_probability(p): return (None, None, None)

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
FOOTBALL_DATA_KEY = os.environ.get("FOOTBALL_DATA_API_KEY", "")

TABLE = "model_results"
MODEL_VERSION = os.environ.get("MODEL_VERSION", "1.0")
CALIBRATION_VERSION = os.environ.get("CALIBRATION_VERSION", "1.0")
WEIGHTS_VERSION = os.environ.get("WEIGHTS_VERSION", "1.0")

HEADERS_READ = {
    "apikey": SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "Accept": "application/json",
}
HEADERS_WRITE = {
    **HEADERS_READ,
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
REQUEST_DELAY_S = 7
DEBUG_FEATURE_SAMPLE_LIMIT = int(os.environ.get("BACKTEST_DEBUG_FEATURE_SAMPLE_LIMIT", "5"))
MIN_HISTORY_MATCHES_FOR_STRENGTH = int(os.environ.get("BACKTEST_MIN_HISTORY_MATCHES_FOR_STRENGTH", "3"))
FORM_WINDOW = int(os.environ.get("BACKTEST_FORM_WINDOW", "5"))

COMP_CODE_TO_SOURCE = {
    "PREMIER_LEAGUE": "PL",
    "BUNDESLIGA": "BL1",
    "LA_LIGA": "PD",
    "SERIE_A": "SA",
    "LIGUE_1": "FL1",
    "EREDIVISIE": "DED",
    "PRIMEIRA_LIGA": "PPL",
    "CHAMPIONSHIP": "ELC",
    "CHAMPIONS_LEAGUE": "CL",
}


def parse_iso_dt(value: Optional[str]) -> datetime:
    if not value:
        return datetime.min
    s = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.min


# ═══════════════════════════════════════════════════════════════
# KICKTIPP SCORING
# ═══════════════════════════════════════════════════════════════

def calc_points(pred: str, actual_home: int, actual_away: int) -> int:
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
    act_t = "H" if actual_home > actual_away else ("A" if actual_home < actual_away else "D")
    return 2 if pred_t == act_t else 0


def calc_outcome_label(pred: str, actual_home: int, actual_away: int) -> str:
    if not pred:
        return "wrong"
    parts = pred.split(":")
    if len(parts) != 2:
        return "wrong"
    try:
        ph, pa = int(parts[0]), int(parts[1])
    except ValueError:
        return "wrong"

    if ph == actual_home and pa == actual_away:
        return "exact"

    pred_diff = ph - pa
    act_diff = actual_home - actual_away
    pred_t = "H" if ph > pa else ("A" if ph < pa else "D")
    act_t = "H" if actual_home > actual_away else ("A" if actual_home < actual_away else "D")

    if pred_diff == act_diff and pred_t == act_t:
        return "goal_diff"
    if pred_t == act_t:
        return "tendency"
    return "wrong"


# ═══════════════════════════════════════════════════════════════
# MATH HELPERS
# ═══════════════════════════════════════════════════════════════

def form_score(form: list) -> float:
    if not form:
        return 0.5
    pts = sum(3 if r == "W" else 1 if r == "D" else 0 for r in form)
    return pts / (len(form) * 3)


def strength_diff(m: dict) -> float:
    h = m.get("home_strength")
    a = m.get("away_strength")
    if h is None or a is None:
        return 0.0
    return (h - a) / 100.0


def pois(k: int, lam: float) -> float:
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def score_grid(lambda_h: float, lambda_a: float, max_g: int = 6) -> dict:
    return {
        f"{h}:{a}": pois(h, lambda_h) * pois(a, lambda_a)
        for h in range(max_g + 1)
        for a in range(max_g + 1)
    }


def apply_dixon_coles(grid: dict, lh: float, la: float, rho: float = 0.13) -> dict:
    def tau(h: int, a: int) -> float:
        if h == 0 and a == 0:
            return 1 - lh * la * rho
        if h == 1 and a == 0:
            return 1 + la * rho
        if h == 0 and a == 1:
            return 1 + lh * rho
        if h == 1 and a == 1:
            return 1 - rho
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
        if h > a:
            p_home += prob
        elif a > h:
            p_away += prob
        else:
            p_draw += prob
    if p_home >= p_away and p_home >= p_draw:
        return "home"
    if p_away >= p_home and p_away >= p_draw:
        return "away"
    return "draw"


# ═══════════════════════════════════════════════════════════════
# MODEL IMPLEMENTATIONS
# ═══════════════════════════════════════════════════════════════

MODELS: Dict[str, Any] = {}


def register_model(fn):
    MODELS[fn.__name__] = fn
    return fn


@register_model
def ensemble(m: dict) -> dict:
    diff = strength_diff(m)
    form_h = form_score(m.get("form_home") or [])
    form_a = form_score(m.get("form_away") or [])
    form_adv = form_h - form_a
    raw = (m.get("win_probability", 50) / 100) * 0.6 + (0.5 + diff * 0.25 + form_adv * 0.15)
    home_p = min(0.95, max(0.05, raw / 1.6))
    winner = "home" if home_p > 0.55 else ("away" if home_p < 0.40 else "draw")
    conf = int(50 + abs(home_p - 0.5) * 80)
    hg = 1.2 + diff * 1.5 + form_adv * 0.5
    ag = 0.7 - diff * 0.8 + (1 - form_adv) * 0.3
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    _ph, _pd, _pa = probs_from_home_probability(home_p)
    return {
        "predicted_winner": winner,
        "predicted_score": main_tip,
        "confidence_score": min(99, conf),
        "prob_home": _ph,
        "prob_draw": _pd,
        "prob_away": _pa,
    }


@register_model
def poisson(m: dict) -> dict:
    diff = strength_diff(m)
    form_h = form_score(m.get("form_home") or [])
    form_a = form_score(m.get("form_away") or [])
    lh = max(0.3, 1.4 + diff * 1.2 + (form_h - 0.5) * 0.6)
    la = max(0.3, 1.1 - diff * 0.8 + (form_a - 0.5) * 0.4)
    grid = score_grid(lh, la)
    winner = winner_from_grid(grid)
    top = top_scores(grid, 1)
    main_tip = top[0][0] if top else f"{round(lh)}:{round(la)}"
    conf = int(min(85, top[0][1] * 100 * 2.5)) if top else 50
    _ph, _pd, _pa = probs_from_score_grid(grid)
    return {
        "predicted_winner": winner,
        "predicted_score": main_tip,
        "confidence_score": conf,
        "prob_home": _ph,
        "prob_draw": _pd,
        "prob_away": _pa,
    }


@register_model
def dc(m: dict) -> dict:
    diff = strength_diff(m)
    form_h = form_score(m.get("form_home") or [])
    form_a = form_score(m.get("form_away") or [])
    lh = max(0.3, 1.3 + diff * 1.0 + (form_h - 0.5) * 0.5)
    la = max(0.3, 1.0 - diff * 0.7 + (form_a - 0.5) * 0.4)
    raw = score_grid(lh, la)
    grid = apply_dixon_coles(raw, lh, la)
    winner = winner_from_grid(grid)
    top = top_scores(grid, 1)
    main_tip = top[0][0] if top else f"{round(lh)}:{round(la)}"
    conf = int(min(85, top[0][1] * 100 * 2.5)) if top else 50
    _ph, _pd, _pa = probs_from_score_grid(grid)
    return {
        "predicted_winner": winner,
        "predicted_score": main_tip,
        "confidence_score": conf,
        "prob_home": _ph,
        "prob_draw": _pd,
        "prob_away": _pa,
    }


@register_model
def elo(m: dict) -> dict:
    diff = strength_diff(m)
    home_p = 1 / (1 + 10 ** (-(diff * 4 + 0.1)))
    winner = "home" if home_p > 0.58 else ("away" if home_p < 0.38 else "draw")
    conf = int(45 + abs(home_p - 0.5) * 90)
    hg = 1.0 + diff * 1.8
    ag = 0.9 - diff * 0.9
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    _ph, _pd, _pa = probs_from_home_probability(home_p)
    return {
        "predicted_winner": winner,
        "predicted_score": main_tip,
        "confidence_score": min(99, conf),
        "prob_home": _ph,
        "prob_draw": _pd,
        "prob_away": _pa,
    }


@register_model
def market(m: dict) -> dict:
    quote = m.get("market_home_win_odds") or m.get("market_signal_home") or 2.0
    implied = min(0.92, max(0.08, (1 / quote) * 0.9))
    winner = "home" if implied > 0.55 else ("away" if implied < 0.40 else "draw")
    conf = int(40 + implied * 45)
    diff = strength_diff(m)
    hg = 0.9 + diff * 1.2
    ag = 0.8 - diff * 0.6
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    _ph, _pd, _pa = probs_from_home_probability(implied)
    return {
        "predicted_winner": winner,
        "predicted_score": main_tip,
        "confidence_score": min(90, conf),
        "prob_home": _ph,
        "prob_draw": _pd,
        "prob_away": _pa,
    }


# ═══════════════════════════════════════════════════════════════
# FOOTBALL-DATA.ORG HELPERS
# ═══════════════════════════════════════════════════════════════

def _fd_get(path: str, params: dict) -> requests.Response:
    if not FOOTBALL_DATA_KEY:
        raise RuntimeError(
            "FOOTBALL_DATA_API_KEY is not set. Add it to GitHub Secrets and expose it in the workflow."
        )

    url = f"{FOOTBALL_DATA_BASE}/{path.lstrip('/')}"
    try:
        return requests.get(
            url,
            headers={"X-Auth-Token": FOOTBALL_DATA_KEY},
            params=params,
            timeout=20,
        )
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"football-data.org network error: {exc}") from exc


def parse_season_year(season: Optional[str]) -> Optional[str]:
    if not season:
        return None
    year_part = season.split("/")[0].strip()
    if len(year_part) == 2:
        year_part = f"20{year_part}"
    return year_part if year_part.isdigit() else None


def fetch_finished_matches(season: Optional[str] = None, competition: Optional[str] = None) -> List[Dict[str, Any]]:
    season_year = parse_season_year(season)

    if competition:
        if competition not in COMP_CODE_TO_SOURCE:
            raise RuntimeError(f"Unknown competition_code '{competition}'. Known: {sorted(COMP_CODE_TO_SOURCE)}")
        comps = {competition: COMP_CODE_TO_SOURCE[competition]}
    else:
        comps = dict(COMP_CODE_TO_SOURCE)

    all_matches: List[Dict[str, Any]] = []

    for comp_code, fd_source in comps.items():
        params: Dict[str, Any] = {"status": "FINISHED", "limit": 500}
        if season_year:
            params["season"] = season_year

        r = _fd_get(f"competitions/{fd_source}/matches", params)

        if r.status_code == 404:
            log.warning("%s (%s): 404 from football-data — skipping", comp_code, fd_source)
            time.sleep(REQUEST_DELAY_S)
            continue
        if r.status_code == 403:
            log.warning("%s (%s): 403 Forbidden — not available on current API tier", comp_code, fd_source)
            time.sleep(REQUEST_DELAY_S)
            continue
        if r.status_code == 429:
            log.warning("%s: rate-limited (429) — sleeping 60 s", comp_code)
            time.sleep(60)
            r = _fd_get(f"competitions/{fd_source}/matches", params)

        if not r.ok:
            log.error("%s: HTTP %d — %s — skipping", comp_code, r.status_code, r.text[:200])
            time.sleep(REQUEST_DELAY_S)
            continue

        payload = r.json()
        raw_matches = payload.get("matches", [])
        season_info = payload.get("filters", {}).get("season") or season_year or ""
        log.info("%s: %d FINISHED matches from football-data", comp_code, len(raw_matches))

        for raw in raw_matches:
            ft = raw.get("score", {}).get("fullTime", {})
            home_goals = ft.get("home")
            away_goals = ft.get("away")
            if home_goals is None or away_goals is None:
                continue

            home_team = raw.get("homeTeam", {})
            away_team = raw.get("awayTeam", {})
            match_id = f"football_data_{raw.get('id')}"

            all_matches.append({
                "id": match_id,
                "external_id": match_id,
                "competition_code": comp_code,
                "league_abbr": comp_code,
                "flag": "",
                "season": str(season_info or ""),
                "kickoff_at": raw.get("utcDate"),
                "home_team": home_team.get("name") or home_team.get("shortName") or "?",
                "away_team": away_team.get("name") or away_team.get("shortName") or "?",
                "home_team_id": home_team.get("id"),
                "away_team_id": away_team.get("id"),
                "actual_home_goals": int(home_goals),
                "actual_away_goals": int(away_goals),
            })

        time.sleep(REQUEST_DELAY_S)

    all_matches.sort(key=lambda m: (m.get("competition_code", ""), m.get("season", ""), parse_iso_dt(m.get("kickoff_at")), m.get("id", "")))
    return all_matches


# ═══════════════════════════════════════════════════════════════
# SELF-CONTAINED HISTORICAL FEATURE ENGINE
# ═══════════════════════════════════════════════════════════════

@dataclass
class TeamHistory:
    played: int = 0
    wins: int = 0
    draws: int = 0
    losses: int = 0
    goals_for: int = 0
    goals_against: int = 0
    results: deque = field(default_factory=lambda: deque(maxlen=FORM_WINDOW))


def result_code(goals_for: int, goals_against: int) -> str:
    if goals_for > goals_against:
        return "W"
    if goals_for == goals_against:
        return "D"
    return "L"


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def normalize_to_100(value: float, low: float, high: float) -> float:
    if high <= low:
        return 50.0
    x = (value - low) / (high - low)
    return clamp(x, 0.0, 1.0) * 100.0


def derive_team_strength(hist: TeamHistory) -> Optional[float]:
    if hist.played < MIN_HISTORY_MATCHES_FOR_STRENGTH:
        return None

    ppg = (hist.wins * 3 + hist.draws) / hist.played
    gd_pg = (hist.goals_for - hist.goals_against) / hist.played
    win_rate = hist.wins / hist.played
    recent = form_score(list(hist.results))

    ppg_score = normalize_to_100(ppg, 0.0, 3.0)
    gd_score = normalize_to_100(gd_pg, -2.0, 2.0)
    win_score = normalize_to_100(win_rate, 0.0, 1.0)
    recent_score = normalize_to_100(recent, 0.0, 1.0)

    strength = (
        ppg_score * 0.45 +
        gd_score * 0.20 +
        win_score * 0.15 +
        recent_score * 0.20
    )
    return round(clamp(strength, 0.0, 100.0), 2)


def derive_win_probability(home_strength: Optional[float], away_strength: Optional[float]) -> int:
    if home_strength is None or away_strength is None:
        return 50
    diff = (home_strength - away_strength) / 12.0
    home_p = 1.0 / (1.0 + math.exp(-diff))
    return int(round(clamp(home_p, 0.05, 0.95) * 100))


def annotate_historical_features(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    histories: Dict[tuple, Dict[Any, TeamHistory]] = defaultdict(dict)
    enriched_matches: List[Dict[str, Any]] = []

    for m in matches:
        scope = (m.get("competition_code", ""), m.get("season", ""))
        team_hist = histories[scope]

        home_id = m.get("home_team_id") or f"team:{m.get('home_team')}"
        away_id = m.get("away_team_id") or f"team:{m.get('away_team')}"

        if home_id not in team_hist:
            team_hist[home_id] = TeamHistory()
        if away_id not in team_hist:
            team_hist[away_id] = TeamHistory()

        home_hist = team_hist[home_id]
        away_hist = team_hist[away_id]

        form_home = list(home_hist.results)
        form_away = list(away_hist.results)
        home_strength = derive_team_strength(home_hist)
        away_strength = derive_team_strength(away_hist)
        win_probability = derive_win_probability(home_strength, away_strength)

        enriched = dict(m)
        enriched.update({
            "home_strength": home_strength,
            "away_strength": away_strength,
            "form_home": form_home,
            "form_away": form_away,
            "market_home_win_odds": None,
            "market_signal_home": None,
            "win_probability": win_probability,
            "history_home_matches": home_hist.played,
            "history_away_matches": away_hist.played,
            "feature_source": "historical_self_contained",
        })
        enriched_matches.append(enriched)

        actual_h = int(m["actual_home_goals"])
        actual_a = int(m["actual_away_goals"])

        home_hist.played += 1
        away_hist.played += 1
        home_hist.goals_for += actual_h
        home_hist.goals_against += actual_a
        away_hist.goals_for += actual_a
        away_hist.goals_against += actual_h

        home_result = result_code(actual_h, actual_a)
        away_result = result_code(actual_a, actual_h)

        if home_result == "W":
            home_hist.wins += 1
            away_hist.losses += 1
        elif home_result == "D":
            home_hist.draws += 1
            away_hist.draws += 1
        else:
            home_hist.losses += 1
            away_hist.wins += 1

        home_hist.results.append(home_result)
        away_hist.results.append(away_result)

    return enriched_matches


# ═══════════════════════════════════════════════════════════════
# FEATURE LOGGING
# ═══════════════════════════════════════════════════════════════

def compute_feature_coverage(matches: List[Dict[str, Any]]) -> Dict[str, float]:
    total = len(matches)
    if total == 0:
        return {
            "total": 0,
            "strengths": 0,
            "form": 0,
            "market_odds": 0,
            "strength_cov": 0.0,
            "form_cov": 0.0,
            "market_cov": 0.0,
        }

    strengths = sum(1 for m in matches if m.get("home_strength") is not None and m.get("away_strength") is not None)
    form = sum(1 for m in matches if bool(m.get("form_home")) and bool(m.get("form_away")))
    market = sum(1 for m in matches if m.get("market_home_win_odds") is not None)
    return {
        "total": total,
        "strengths": strengths,
        "form": form,
        "market_odds": market,
        "strength_cov": strengths / total,
        "form_cov": form / total,
        "market_cov": market / total,
    }


def log_feature_coverage(matches: List[Dict[str, Any]]) -> None:
    c = compute_feature_coverage(matches)
    if c["total"] == 0:
        return
    log.info(
        "Historical feature coverage: strengths=%d/%d (%.1f%%), form=%d/%d (%.1f%%), market_odds=%d/%d (%.1f%%)",
        c["strengths"], c["total"], c["strength_cov"] * 100.0,
        c["form"], c["total"], c["form_cov"] * 100.0,
        c["market_odds"], c["total"], c["market_cov"] * 100.0,
    )


def log_match_feature_samples(matches: List[Dict[str, Any]], limit: int = DEBUG_FEATURE_SAMPLE_LIMIT) -> None:
    if not matches or limit <= 0:
        return
    log.info("Historical feature samples (first %d matches)", min(limit, len(matches)))
    for i, m in enumerate(matches[:limit], start=1):
        snapshot = {
            "idx": i,
            "match_id": m.get("id"),
            "kickoff_at": m.get("kickoff_at"),
            "home_team": m.get("home_team"),
            "away_team": m.get("away_team"),
            "history_home_matches": m.get("history_home_matches"),
            "history_away_matches": m.get("history_away_matches"),
            "home_strength": m.get("home_strength"),
            "away_strength": m.get("away_strength"),
            "form_home": m.get("form_home"),
            "form_away": m.get("form_away"),
            "win_probability": m.get("win_probability"),
            "feature_source": m.get("feature_source"),
        }
        log.info("Feature sample %d: %s", i, json.dumps(snapshot, ensure_ascii=False, default=str))


def log_strength_distribution(matches: List[Dict[str, Any]]) -> None:
    strengths = [m["home_strength"] for m in matches if m.get("home_strength") is not None]
    strengths += [m["away_strength"] for m in matches if m.get("away_strength") is not None]
    if not strengths:
        log.warning("No derived strengths available yet. This can happen very early in a season.")
        return
    avg_strength = sum(strengths) / len(strengths)
    log.info(
        "Derived strength distribution: n=%d min=%.2f avg=%.2f max=%.2f",
        len(strengths), min(strengths), avg_strength, max(strengths)
    )


# ═══════════════════════════════════════════════════════════════
# SUPABASE WRITE
# ═══════════════════════════════════════════════════════════════

def upsert_results(rows: List[Dict[str, Any]]) -> None:
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
    season: Optional[str] = None,
    competition: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    log.info("── Backtest start ──────────────────────────────────")
    log.info("  competition : %s", competition or "all active")
    log.info("  season      : %s", season or "latest")
    log.info("  dry_run     : %s", dry_run)
    log.info("  feature_src : historical_self_contained")

    system_version = get_system_version()
    run_id = start_pipeline_run(
        "backtest",
        system_version=system_version,
        metadata={
            "season": season,
            "competition": competition,
            "dry_run": dry_run,
            "feature_source": "historical_self_contained",
        },
    )
    register_system_release()
    log.info("  version     : %s", system_version)
    log.info("  run_id      : %s", run_id)

    raw_matches = fetch_finished_matches(season=season, competition=competition)
    log.info("Fetched %d finished matches from football-data", len(raw_matches))

    matches = annotate_historical_features(raw_matches)
    log_feature_coverage(matches)
    log_strength_distribution(matches)
    log_match_feature_samples(matches)

    if not matches:
        log.warning("No finished matches found — nothing to do.")
        finish_pipeline_run(run_id, status="success", metadata={"rows_written": 0, "skipped": 0})
        return

    rows: List[Dict[str, Any]] = []
    skipped = 0

    for m in matches:
        actual_h = m.get("actual_home_goals")
        actual_a = m.get("actual_away_goals")
        if actual_h is None or actual_a is None:
            skipped += 1
            continue

        actual_score = f"{actual_h}:{actual_a}"
        actual_winner = "home" if actual_h > actual_a else ("away" if actual_a > actual_h else "draw")

        for model_key, model_fn in MODELS.items():
            try:
                result = model_fn(m)
            except Exception as exc:
                log.warning("  [WARN] %s on %s failed: %s", model_key, m.get("id"), exc)
                continue

            pts = calc_points(result["predicted_score"], actual_h, actual_a)
            outcome = calc_outcome_label(result["predicted_score"], actual_h, actual_a)

            metrics = compute_eval_metrics(
                predicted_score=result["predicted_score"],
                actual_home=actual_h,
                actual_away=actual_a,
                prob_home=result.get("prob_home"),
                prob_draw=result.get("prob_draw"),
                prob_away=result.get("prob_away"),
            )

            rows.append({
                "match_id": m["id"],
                "model_key": model_key,
                "model_version": MODEL_VERSION,
                "calibration_version": CALIBRATION_VERSION,
                "weights_version": WEIGHTS_VERSION,
                "predicted_score": result["predicted_score"],
                "predicted_winner": result["predicted_winner"],
                "confidence_score": result["confidence_score"],
                "actual_score": actual_score,
                "actual_winner": actual_winner,
                "actual_home_goals": actual_h,
                "actual_away_goals": actual_a,
                "points": pts,
                "outcome_label": outcome,
                "predicted_1x2": metrics.get("predicted_1x2"),
                "actual_1x2": metrics.get("actual_1x2"),
                "is_exact_hit": metrics.get("is_exact_hit"),
                "is_tendency_hit": metrics.get("is_tendency_hit"),
                "score_error_abs": metrics.get("score_error_abs"),
                "goal_diff_error": metrics.get("goal_diff_error"),
                "brier_score_1x2": metrics.get("brier_score_1x2"),
                "log_loss_1x2": metrics.get("log_loss_1x2"),
                "home_team": m.get("home_team", ""),
                "away_team": m.get("away_team", ""),
                "flag": m.get("flag", ""),
                "league_abbr": m.get("league_abbr", ""),
                "competition_code": m.get("competition_code", ""),
                "season": m.get("season", season or ""),
                "kickoff_at": m.get("kickoff_at"),
                "system_version": system_version,
                "pipeline_run_id": run_id,
            })

    if skipped:
        log.warning("Skipped %d matches with missing actual goals", skipped)

    log.info("Computed %d predictions (%d matches × %d models)", len(rows), len(matches) - skipped, len(MODELS))

    if dry_run:
        log.info("DRY RUN — no Supabase write")
        print(json.dumps(rows[:3], indent=2, default=str))

        by_model: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            by_model[r["model_key"]].append(r)

        print("\n── Dry-Run Model Summary ─────────────────────")
        hdr = f"  {'model':<12s}  {'n':>4}  {'avg_pts':>7}  {'tend%':>6}  {'exact%':>6}  {'avg_err':>7}  {'avg_brier':>9}"
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for mk, rlist in sorted(by_model.items()):
            n = len(rlist)
            avg_pts = sum(r["points"] for r in rlist) / n if n else 0.0
            tend_n = [r for r in rlist if r.get("is_tendency_hit") is not None]
            tend_pct = (sum(1 for r in tend_n if r["is_tendency_hit"]) / len(tend_n) * 100.0) if tend_n else float("nan")
            exact_n = [r for r in rlist if r.get("is_exact_hit") is not None]
            exact_pct = (sum(1 for r in exact_n if r["is_exact_hit"]) / len(exact_n) * 100.0) if exact_n else float("nan")
            err_vals = [r["score_error_abs"] for r in rlist if r.get("score_error_abs") is not None]
            avg_err = sum(err_vals) / len(err_vals) if err_vals else float("nan")
            brier_vals = [r["brier_score_1x2"] for r in rlist if r.get("brier_score_1x2") is not None]
            avg_brier = sum(brier_vals) / len(brier_vals) if brier_vals else None
            brier_str = f"{avg_brier:9.4f}" if avg_brier is not None else "      n/a"
            print(
                f"  {mk:<12s}  {n:>4d}  {avg_pts:7.2f}  "
                f"{tend_pct:6.1f}  {exact_pct:6.1f}  {avg_err:7.2f}  {brier_str}"
            )
    else:
        upsert_results(rows)

    log.info("── Backtest done ───────────────────────────────────")
    finish_pipeline_run(
        run_id,
        status="success" if not dry_run else "dry_run",
        metadata={
            "rows_written": len(rows),
            "skipped": skipped,
            "feature_source": "historical_self_contained",
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="EuroMatch Edge — historischer Self-Contained Backtest-Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python backtest.py --dry-run
  python backtest.py --competition BUNDESLIGA --season 2025
  python backtest.py --competition PREMIER_LEAGUE --dry-run

Bekannte competition-Codes:
  """ + "  ".join(sorted(COMP_CODE_TO_SOURCE))
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Saison-Jahr, z.B. '2025' oder '2025/26' (nur Jahr-Teil wird genutzt)",
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
