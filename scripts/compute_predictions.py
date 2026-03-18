"""
compute_predictions.py
──────────────────────
EuroMatch Edge — Data Pipeline Step 3

Reads upcoming matches from Supabase `matches`, computes predictions
using the MVP model, and upserts results into `predictions`.

Environment variables required:
  SUPABASE_URL               — https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  — Supabase service_role JWT

Usage:
  python scripts/compute_predictions.py
  python scripts/compute_predictions.py --days-ahead 3
  python scripts/compute_predictions.py --dry-run
  python scripts/compute_predictions.py --dirty-only
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

# Runtime Metadata Layer
try:
    from utils.runtime_metadata import (
        get_system_version,
        start_pipeline_run,
        finish_pipeline_run,
        register_system_release,
    )
    _METADATA_AVAILABLE = True
except ImportError:
    import logging as _log

    _log.getLogger(__name__).warning(
        "utils.runtime_metadata not found — versioning disabled."
    )

    def get_system_version() -> str:
        return "dev"

    def start_pipeline_run(job_name: str, **kwargs: Any) -> str:
        return "no-run-id"

    def finish_pipeline_run(run_id: str, **kwargs: Any) -> None:
        return None

    def register_system_release(**kwargs: Any) -> None:
        return None

    _METADATA_AVAILABLE = False


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
# MODEL CONFIGURATION
# ─────────────────────────────────────────────────────────────

MODEL_VERSION = os.environ.get("MODEL_VERSION", "1.0")
CALIBRATION_VERSION = os.environ.get("CALIBRATION_VERSION", "1.0")
WEIGHTS_VERSION = os.environ.get("WEIGHTS_VERSION", "1.0")

STRONG_TIP_THRESHOLD = 70

# Must sum to ~1.0
WEIGHTS = {
    "strength": 0.36,
    "form": 0.22,
    "home_adv": 0.08,
    "injuries": 0.12,
    "motivation": 0.10,
    "load": 0.08,
    "market": 0.04,
}

MAX_KNOWN_FACTORS = len(WEIGHTS)

LOAD_PENALTY: dict[str, float] = {
    "low": 0.00,
    "medium": 0.03,
    "high": 0.07,
}

MOTIVATION_HIGH = {
    "titel",
    "meister",
    "scudetto",
    "treble",
    "ungeschlagen",
    "abstieg",
    "relegation",
    "champions league",
    "championship",
    "cup final",
    "finale",
    "klassenerhalt",
}

# Value thresholds
MIN_EDGE_FOR_VALUE = 0.02
MIN_EV_FOR_VALUE = 0.03
MIN_BOOKMAKER_COUNT_FOR_VALUE = 3


# ─────────────────────────────────────────────────────────────
# MARKET HELPERS
# ─────────────────────────────────────────────────────────────

def market_odds_to_score(odds: float | None) -> float | None:
    """
    Convert decimal home-win odds to a [0,1] model score.
    Lower odds = stronger market signal for the home side.
    """
    if odds is None:
        return None
    if odds <= 1.10:
        return 0.90
    if odds <= 1.25:
        return 0.80
    if odds <= 1.50:
        return 0.65
    if odds <= 2.00:
        return 0.50
    if odds <= 2.50:
        return 0.35
    return 0.20


def derive_market_metrics(match: dict[str, Any]) -> dict[str, Any]:
    """
    Compute fair market probabilities, overround, edge, EV and value flags.

    Returns defaults (None / False) if full 1X2 odds are unavailable.
    """
    home_odds_raw = match.get("market_home_win_odds")
    draw_odds_raw = match.get("market_draw_odds")
    away_odds_raw = match.get("market_away_odds")
    bookmaker_count = match.get("market_odds_bookmaker_count")
    market_last_update_ts = match.get("market_odds_updated_at")

    result = {
        "market_home_prob_fair": None,
        "market_draw_prob_fair": None,
        "market_away_prob_fair": None,
        "market_overround": None,
        "market_bookmaker_count": bookmaker_count,
        "market_last_update_ts": market_last_update_ts,
        "edge_home": None,
        "edge_draw": None,
        "edge_away": None,
        "ev_home": None,
        "ev_draw": None,
        "ev_away": None,
        "value_home": False,
        "value_draw": False,
        "value_away": False,
    }

    if not home_odds_raw or not draw_odds_raw or not away_odds_raw:
        return result

    try:
        home_odds = float(home_odds_raw)
        draw_odds = float(draw_odds_raw)
        away_odds = float(away_odds_raw)
    except (TypeError, ValueError):
        return result

    if home_odds <= 1.0 or draw_odds <= 1.0 or away_odds <= 1.0:
        return result

    raw_home = 1.0 / home_odds
    raw_draw = 1.0 / draw_odds
    raw_away = 1.0 / away_odds
    overround = raw_home + raw_draw + raw_away

    if overround <= 0:
        return result

    fair_home = raw_home / overround
    fair_draw = raw_draw / overround
    fair_away = raw_away / overround

    result["market_home_prob_fair"] = round(fair_home, 6)
    result["market_draw_prob_fair"] = round(fair_draw, 6)
    result["market_away_prob_fair"] = round(fair_away, 6)
    result["market_overround"] = round(overround, 6)

    return result


# ─────────────────────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────────────────────

class SupabaseClient:
    """Minimal Supabase REST client (service_role)."""

    def __init__(self, url: str, service_role_key: str) -> None:
        self.base_url = url.rstrip("/")
        self._auth_headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }

    # ── READ ──────────────────────────────────────────────────

    def fetch_upcoming_matches(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}/rest/v1/matches"

        def _clean_ts(ts: str) -> str:
            try:
                dt = datetime.fromisoformat(ts)
                return dt.replace(microsecond=0).isoformat()
            except ValueError:
                return ts

        ts_from = _clean_ts(date_from)
        ts_to = _clean_ts(date_to)

        params = [
            ("kickoff_at", f"gte.{ts_from}"),
            ("kickoff_at", f"lt.{ts_to}"),
            ("order", "kickoff_at.asc"),
            ("select", "*"),
        ]

        log.info("  Fetching matches: %s → %s", ts_from, ts_to)

        response = requests.get(
            url,
            params=params,
            headers={**self._auth_headers, "Range": "0-999"},
            timeout=20,
        )

        if not response.ok:
            log.error(
                "  Supabase fetch failed: %d %s\n  URL : %s\n  Body: %s",
                response.status_code,
                response.reason,
                response.url,
                response.text[:500],
            )
            response.raise_for_status()

        rows = response.json()
        log.info("  → %d match(es) found.  (url: %s)", len(rows), response.url)
        return rows

    def fetch_dirty_matches(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        dirty_url = f"{self.base_url}/rest/v1/v_dirty_predictions"

        def _clean_ts(ts: str) -> str:
            try:
                dt = datetime.fromisoformat(ts)
                return dt.replace(microsecond=0).isoformat()
            except ValueError:
                return ts

        ts_from = _clean_ts(date_from)
        ts_to = _clean_ts(date_to)

        params = [
            ("kickoff_at", f"gte.{ts_from}"),
            ("kickoff_at", f"lt.{ts_to}"),
            ("order", "kickoff_at.asc"),
            ("select", "match_id"),
        ]

        log.info("  Fetching dirty matches: %s → %s", ts_from, ts_to)

        response = requests.get(
            dirty_url,
            params=params,
            headers={**self._auth_headers, "Range": "0-999"},
            timeout=20,
        )

        if not response.ok:
            log.error(
                "  Supabase dirty fetch failed: %d %s\n  URL : %s\n  Body: %s",
                response.status_code,
                response.reason,
                response.url,
                response.text[:500],
            )
            response.raise_for_status()

        dirty_rows = response.json()
        match_ids = [row["match_id"] for row in dirty_rows if row.get("match_id")]

        if not match_ids:
            log.info("  → 0 dirty match(es) found.")
            return []

        matches_url = f"{self.base_url}/rest/v1/matches"
        id_list = ",".join(match_ids)
        params = {
            "id": f"in.({id_list})",
            "order": "kickoff_at.asc",
            "select": "*",
        }

        response = requests.get(
            matches_url,
            params=params,
            headers={**self._auth_headers, "Range": "0-999"},
            timeout=20,
        )

        if not response.ok:
            log.error(
                "  Supabase match fetch for dirty IDs failed: %d %s\n  URL : %s\n  Body: %s",
                response.status_code,
                response.reason,
                response.url,
                response.text[:500],
            )
            response.raise_for_status()

        rows = response.json()
        log.info("  → %d dirty match(es) loaded.", len(rows))
        return rows

    # ── WRITE ─────────────────────────────────────────────────

    def upsert_predictions(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return

        url = f"{self.base_url}/rest/v1/predictions"
        headers = {
            **self._auth_headers,
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }

        response = requests.post(
            url,
            params={"on_conflict": "match_id"},
            headers=headers,
            data=json.dumps(rows),
            timeout=30,
        )

        if not response.ok:
            log.error(
                "  Supabase upsert failed: %d %s\n  URL : %s\n  Body: %s",
                response.status_code,
                response.reason,
                response.url,
                response.text[:500],
            )
            response.raise_for_status()


# ─────────────────────────────────────────────────────────────
# MODEL HELPERS
# ─────────────────────────────────────────────────────────────

def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def form_to_score(form: list[str]) -> float | None:
    if not form:
        return None
    max_pts = len(form) * 3
    pts = sum({"W": 3, "D": 1, "L": 0}.get(r, 0) for r in form)
    return pts / max_pts if max_pts > 0 else None


def strength_to_score(strength: int | None) -> float | None:
    if strength is None:
        return None
    return max(0.0, min(1.0, strength / 100.0))


def weighted_average(
    factors: dict[str, float | None],
    weights: dict[str, float],
) -> tuple[float, int]:
    """
    Compute weighted average of known factors only.
    Returns (score, n_known_factors).
    """
    weighted_sum = 0.0
    weight_sum = 0.0
    n_known = 0

    for key, value in factors.items():
        if value is None:
            continue
        w = weights.get(key, 0.0)
        weighted_sum += value * w
        weight_sum += w
        n_known += 1

    if weight_sum <= 0:
        return 0.50, 0

    return weighted_sum / weight_sum, n_known


def normalise_to_100(home_p: float, draw_p: float, away_p: float) -> tuple[int, int, int]:
    """
    Convert three raw probabilities to integer percentages summing to exactly 100.
    """
    total = home_p + draw_p + away_p
    if total <= 0:
        return 34, 33, 33

    vals = [home_p / total * 100.0, draw_p / total * 100.0, away_p / total * 100.0]
    ints = [int(v) for v in vals]
    remainder = 100 - sum(ints)

    # Largest remainder method
    frac_order = sorted(
        range(3),
        key=lambda i: (vals[i] - ints[i]),
        reverse=True,
    )
    for i in frac_order[:remainder]:
        ints[i] += 1

    return ints[0], ints[1], ints[2]


def _inj_count_text(text: str) -> int:
    if not text or text.strip().lower() in ("", "none", "keine", "-"):
        return 0
    return len([x for x in text.split(",") if x.strip()])


# ─────────────────────────────────────────────────────────────
# PREDICTION MODEL
# ─────────────────────────────────────────────────────────────

def compute_prediction(match: dict[str, Any]) -> dict[str, Any]:
    """
    Compute a full prediction row for one match dict.
    """

    # ── 1. Parse inputs ──────────────────────────────────────

    home_str = strength_to_score(match.get("home_strength"))
    away_str = strength_to_score(match.get("away_strength"))
    form_h = form_to_score(match.get("form_home") or [])
    form_a = form_to_score(match.get("form_away") or [])
    load_h_raw = match.get("load_home")
    load_a_raw = match.get("load_away")
    market = market_odds_to_score(match.get("market_home_win_odds"))
    inj_h = match.get("injuries_home") or ""
    inj_a = match.get("injuries_away") or ""
    mot_h = match.get("motivation_home") or ""
    mot_a = match.get("motivation_away") or ""

    # ── 2. Derive per-factor home scores [0,1] ───────────────

    if home_str is not None and away_str is not None:
        strength_score = home_str / (home_str + away_str + 1e-9)
    else:
        strength_score = None

    if form_h is not None and form_a is not None:
        form_score = (form_h + 0.5) / (form_h + form_a + 1.0)
    elif form_h is not None:
        form_score = clamp(form_h)
    else:
        form_score = None

    home_adv_score: float = 0.52

    inj_h_count = _inj_count_text(inj_h)
    inj_a_count = _inj_count_text(inj_a)

    if inj_h_count == 0 and inj_a_count == 0:
        injuries_score: float | None = None
    else:
        injury_delta = (inj_a_count - inj_h_count) * 0.06
        injuries_score = clamp(0.50 + injury_delta)

    # Motivation heuristic
    def motivation_weight(text: str) -> float:
        lowered = text.lower()
        return 0.65 if any(k in lowered for k in MOTIVATION_HIGH) else 0.50

    if mot_h or mot_a:
        mot_score: float | None = clamp(
            (motivation_weight(mot_h) + (1.0 - motivation_weight(mot_a))) / 2.0
        )
    else:
        mot_score = None

    # Load penalty -> convert to score in [0,1] from home perspective
    pen_h = LOAD_PENALTY.get(str(load_h_raw), 0.0) if load_h_raw is not None else None
    pen_a = LOAD_PENALTY.get(str(load_a_raw), 0.0) if load_a_raw is not None else None

    if pen_h is None and pen_a is None:
        load_score: float | None = None
    else:
        pen_h_val = pen_h or 0.0
        pen_a_val = pen_a or 0.0
        load_score = clamp(0.50 + (pen_a_val - pen_h_val))

    market_score = market

    # ── 3. Weighted composite home score ─────────────────────

    factors: dict[str, float | None] = {
        "strength": strength_score,
        "form": form_score,
        "home_adv": home_adv_score,
        "injuries": injuries_score,
        "motivation": mot_score,
        "load": load_score,
        "market": market_score,
    }

    home_score, n_known = weighted_average(factors, WEIGHTS)

    # Symmetry fix against home bias
    if home_str is not None and away_str is not None:
        diff = away_str - home_str
        if diff > 0:
            home_score -= diff * 0.25

    home_score = clamp(home_score, 0.02, 0.95)

    # ── 4. Derive draw and away probabilities ─────────────────

    draw_base = 0.27
    k = 3.5
    raw_draw = draw_base * pow(2.718281828, (-k * abs(home_score - 0.5)))
    raw_draw = max(0.06, raw_draw)
    raw_away = max(0.03, 1.0 - home_score - raw_draw)

    win_p, draw_p, away_p = normalise_to_100(home_score, raw_draw, raw_away)

    # ── 5. Winner / strong tip ────────────────────────────────

    if win_p >= away_p and win_p >= draw_p:
        predicted_winner = "home"
        leading_prob = win_p
    elif away_p >= win_p and away_p >= draw_p:
        predicted_winner = "away"
        leading_prob = away_p
    else:
        predicted_winner = "draw"
        leading_prob = draw_p

    is_strong_tip = leading_prob >= STRONG_TIP_THRESHOLD

    # ── 6. Risk level & tags ──────────────────────────────────

    risk_tags: list[str] = []

    if n_known <= 2:
        risk_tags.append("Wenig Datenbasis")
    if load_h_raw == "high":
        risk_tags.append("Hohe Spielplan-Belastung Heim")
    if load_a_raw == "high":
        risk_tags.append("Hohe Spielplan-Belastung Auswärts")
    if inj_h_count >= 2:
        risk_tags.append("Mehrere Ausfälle Heim")
    if inj_a_count >= 2:
        risk_tags.append("Mehrere Ausfälle Auswärts")
    if form_h is not None and form_a is not None and abs(form_h - form_a) < 0.15:
        risk_tags.append("Ausgeglichene Form")
    home_strength_raw = match.get("home_strength")
    away_strength_raw = match.get("away_strength")
    if (
        home_strength_raw is not None
        and away_strength_raw is not None
        and abs(home_strength_raw - away_strength_raw) < 10
    ):
        risk_tags.append("Ausgeglichene Stärke")
    if market is not None and market < 0.50:
        risk_tags.append("Markt favorisiert Auswärtsteam")

    n_risk = len(risk_tags)
    if n_risk >= 3 or n_known <= 1:
        risk_level = "high"
    elif n_risk >= 1:
        risk_level = "medium"
    else:
        risk_level = "low"

    # ── 7. Confidence score ───────────────────────────────────

    data_completeness = n_known / MAX_KNOWN_FACTORS
    outcome_conviction = abs(leading_prob - 50) / 50
    risk_penalty = n_risk * 0.05

    raw_confidence = (
        data_completeness * 0.50
        + outcome_conviction * 0.40
        - risk_penalty
    )
    confidence_score = int(clamp(raw_confidence, 0.0, 1.0) * 100)

    # ── 8. Text outputs ───────────────────────────────────────

    home_team = match.get("home_team", "Heim")
    away_team = match.get("away_team", "Auswärts")

    analysis_text = _build_analysis_text(
        home_team,
        away_team,
        win_p,
        draw_p,
        away_p,
        predicted_winner,
        is_strong_tip,
        n_known,
    )

    ai_reason = _build_ai_reason(
        home_team,
        away_team,
        predicted_winner,
        home_str,
        away_str,
        form_h,
        form_a,
        load_h_raw,
        load_a_raw,
        inj_h,
        inj_a,
        n_known,
    )

    pred = {
        "match_id": match["id"],
        "win_probability": win_p,
        "draw_probability": draw_p,
        "away_probability": away_p,
        "confidence_score": confidence_score,
        "predicted_winner": predicted_winner,
        "is_strong_tip": is_strong_tip,
        "risk_level": risk_level,
        "risk_tags": risk_tags,
        "analysis_text": analysis_text,
        "ai_reason": ai_reason,
        "model_version": MODEL_VERSION,
        "calibration_version": CALIBRATION_VERSION,
        "weights_version": WEIGHTS_VERSION,
        "system_version": None,
        "pipeline_run_id": None,
        # Filled later
        "market_home_prob_fair": None,
        "market_draw_prob_fair": None,
        "market_away_prob_fair": None,
        "market_overround": None,
        "market_bookmaker_count": None,
        "market_last_update_ts": None,
        "edge_home": None,
        "edge_draw": None,
        "edge_away": None,
        "ev_home": None,
        "ev_draw": None,
        "ev_away": None,
        "value_home": False,
        "value_draw": False,
        "value_away": False,
    }

    pred = enrich_with_market_metrics(match, pred)
    return pred


def enrich_with_market_metrics(match: dict[str, Any], pred: dict[str, Any]) -> dict[str, Any]:
    """
    Add fair market probabilities, overround, edge, EV and value flags.
    """
    market_metrics = derive_market_metrics(match)

    for key, value in market_metrics.items():
        pred[key] = value

    if pred["market_home_prob_fair"] is None:
        return pred

    home_odds = float(match["market_home_win_odds"])
    draw_odds = float(match["market_draw_odds"])
    away_odds = float(match["market_away_odds"])

    model_home = (pred.get("win_probability") or 0) / 100.0
    model_draw = (pred.get("draw_probability") or 0) / 100.0
    model_away = (pred.get("away_probability") or 0) / 100.0

    fair_home = float(pred["market_home_prob_fair"])
    fair_draw = float(pred["market_draw_prob_fair"])
    fair_away = float(pred["market_away_prob_fair"])

    edge_home = model_home - fair_home
    edge_draw = model_draw - fair_draw
    edge_away = model_away - fair_away

    ev_home = model_home * home_odds - 1.0
    ev_draw = model_draw * draw_odds - 1.0
    ev_away = model_away * away_odds - 1.0

    pred["edge_home"] = round(edge_home, 6)
    pred["edge_draw"] = round(edge_draw, 6)
    pred["edge_away"] = round(edge_away, 6)

    pred["ev_home"] = round(ev_home, 6)
    pred["ev_draw"] = round(ev_draw, 6)
    pred["ev_away"] = round(ev_away, 6)

    bookmaker_count = pred.get("market_bookmaker_count") or 0
    has_market_depth = bookmaker_count >= MIN_BOOKMAKER_COUNT_FOR_VALUE

    pred["value_home"] = bool(has_market_depth and edge_home >= MIN_EDGE_FOR_VALUE and ev_home >= MIN_EV_FOR_VALUE)
    pred["value_draw"] = bool(has_market_depth and edge_draw >= MIN_EDGE_FOR_VALUE and ev_draw >= MIN_EV_FOR_VALUE)
    pred["value_away"] = bool(has_market_depth and edge_away >= MIN_EDGE_FOR_VALUE and ev_away >= MIN_EV_FOR_VALUE)

    return pred


# ─────────────────────────────────────────────────────────────
# TEXT GENERATORS
# ─────────────────────────────────────────────────────────────

def _build_analysis_text(
    home: str,
    away: str,
    win_p: int,
    draw_p: int,
    away_p: int,
    predicted_winner: str,
    is_strong_tip: bool,
    n_known: int,
) -> str:
    if n_known <= 1:
        return f"{home} vs {away} – zu wenig Daten für eine verlässliche Einschätzung."

    winner_name = home if predicted_winner == "home" else (away if predicted_winner == "away" else None)

    if predicted_winner == "draw":
        return (
            f"Ausgeglichenes Duell: {home} {win_p}% – Remis {draw_p}% – {away} {away_p}%. "
            f"Kein klarer Favorit erkennbar."
        )

    strength = "klar" if (win_p if predicted_winner == "home" else away_p) >= 75 else "leicht"
    tip_hint = " Starker Tipp." if is_strong_tip else ""

    return (
        f"{winner_name} mit {win_p if predicted_winner == 'home' else away_p}% "
        f"Siegwahrscheinlichkeit {strength} favorisiert.{tip_hint}"
    )


def _build_ai_reason(
    home: str,
    away: str,
    predicted_winner: str,
    home_str: float | None,
    away_str: float | None,
    form_h: float | None,
    form_a: float | None,
    load_h: str | None,
    load_a: str | None,
    inj_h: str,
    inj_a: str,
    n_known: int,
) -> str:
    if n_known == 0:
        return "Keine Eingangsdaten verfügbar. Vorhersage basiert ausschließlich auf dem allgemeinen Heimvorteil."

    winner_name = home if predicted_winner == "home" else (away if predicted_winner == "away" else "Unentschieden")
    reasons: list[str] = []

    if home_str is not None and away_str is not None:
        diff = abs(int(home_str * 100) - int(away_str * 100))
        if diff >= 20:
            stronger = home if home_str > away_str else away
            reasons.append(f"{stronger} ist mit einem Stärkeunterschied von {diff} Punkten deutlich überlegen.")
        elif diff >= 8:
            stronger = home if home_str > away_str else away
            reasons.append(f"{stronger} liegt im Stärkevergleich knapp vorne ({diff} Punkte Vorsprung).")
        else:
            reasons.append("Beide Teams sind im Stärkevergleich nahezu gleichwertig.")

    if form_h is not None and form_a is not None:
        fh_pct = int(form_h * 100)
        fa_pct = int(form_a * 100)
        if abs(fh_pct - fa_pct) >= 20:
            better = home if form_h > form_a else away
            reasons.append(f"{better} zeigt mit {max(fh_pct, fa_pct)}% Form-Score deutlich bessere Tagesform.")
        else:
            reasons.append(f"Die aktuelle Form beider Teams ist ähnlich ({fh_pct}% vs {fa_pct}%).")

    if load_h == "high" and load_a != "high":
        reasons.append(f"{home} kommt mit hoher Spielplan-Belastung in diese Partie.")
    elif load_a == "high" and load_h != "high":
        reasons.append(f"{away} reist mit hoher Spielplan-Belastung an – Vorteil für {home}.")

    inj_a_count = _inj_count_text(inj_a)
    inj_h_count = _inj_count_text(inj_h)
    if inj_a_count:
        reasons.append(f"{away} hat {inj_a_count} gemeldete Ausfälle im Kader.")
    if inj_h_count:
        reasons.append(f"{home} hat {inj_h_count} Ausfälle zu beklagen.")

    reasons.append(f"Heimvorteil fliesst grundsätzlich zu Gunsten von {home} in die Berechnung ein.")

    if not reasons:
        return f"Das Modell sieht {winner_name} als Favoriten auf Basis verfügbarer Daten."

    return " ".join(reasons)


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute match predictions and upsert into Supabase `predictions`."
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
        help="Compute predictions but do NOT write to Supabase.",
    )
    parser.add_argument(
        "--dirty-only",
        action="store_true",
        help="Only compute matches with missing or stale predictions from v_dirty_predictions.",
    )
    return parser


def load_env() -> tuple[str, str]:
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    missing = [
        name for name, val in [
            ("SUPABASE_URL", supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", service_key),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return supabase_url, service_key


# ─────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────

def run(days_ahead: int, dry_run: bool, dirty_only: bool) -> None:
    now = datetime.now(timezone.utc)
    date_from = now.isoformat()
    date_to = (now + timedelta(days=days_ahead)).isoformat()

    log.info("═══════════════════════════════════════════════")
    log.info("EuroMatch Edge — compute_predictions.py")
    log.info("Window   : now → +%d day(s)", days_ahead)
    log.info("From     : %s", date_from)
    log.info("To       : %s", date_to)
    log.info("Dry run  : %s", dry_run)
    log.info("Dirty only: %s", dirty_only)
    log.info("Model    : v%s", MODEL_VERSION)
    log.info("═══════════════════════════════════════════════")

    _sys_version = get_system_version()
    _run_id = start_pipeline_run(
        "compute_predictions",
        system_version=_sys_version,
        metadata={"days_ahead": days_ahead, "dry_run": dry_run, "dirty_only": dirty_only},
    )
    register_system_release()
    log.info("Version    : %s", _sys_version)
    log.info("Run ID     : %s", _run_id)

    try:
        supabase_url, service_key = load_env()
        sb = SupabaseClient(supabase_url, service_key)

        try:
            if dirty_only:
                matches = sb.fetch_dirty_matches(date_from, date_to)
            else:
                matches = sb.fetch_upcoming_matches(date_from, date_to)
        except requests.HTTPError as exc:
            raise RuntimeError(f"Failed to fetch matches from Supabase: {exc}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"Network error fetching matches: {exc}") from exc

        if not matches:
            log.info("No matches found for selected mode/window. Nothing to compute.")
            log.info("Status: SUCCESS (no-op)")
            finish_pipeline_run(
                _run_id,
                status="success",
                metadata={"predictions": 0, "errors": 0, "reason": "no_upcoming_matches"},
            )
            return

        now_utc = datetime.now(timezone.utc)
        predictions: list[dict[str, Any]] = []
        compute_errors: list[str] = []
        skipped_past = 0

        for match in matches:
            match_id = match.get("id", "?")
            home_team = match.get("home_team", "?")
            away_team = match.get("away_team", "?")

            kickoff_raw = match.get("kickoff_at")
            if kickoff_raw:
                try:
                    kickoff_dt = datetime.fromisoformat(kickoff_raw.replace("Z", "+00:00"))
                    if kickoff_dt <= now_utc:
                        log.warning(
                            "  ⏭ Skipping past match: %s vs %s (kickoff %s)",
                            home_team,
                            away_team,
                            kickoff_raw,
                        )
                        skipped_past += 1
                        continue
                except ValueError:
                    log.warning(
                        "  Could not parse kickoff_at '%s' for match %s — processing anyway.",
                        kickoff_raw,
                        match_id,
                    )

            try:
                pred = compute_prediction(match)
                predictions.append(pred)
                log.info(
                    "  ✓ %s vs %s  →  home %d%%  draw %d%%  away %d%%  conf %d%%  edge_h=%s ev_h=%s %s",
                    home_team,
                    away_team,
                    pred["win_probability"],
                    pred["draw_probability"],
                    pred["away_probability"],
                    pred["confidence_score"],
                    pred.get("edge_home"),
                    pred.get("ev_home"),
                    "⚡ STRONG" if pred["is_strong_tip"] else "",
                )
            except Exception as exc:  # noqa: BLE001
                err = f"match {match_id} ({home_team} vs {away_team}): {exc}"
                log.error("  ✗ Failed to compute prediction — %s", err)
                compute_errors.append(err)

        log.info("─── Computed %d / %d prediction(s).", len(predictions), len(matches))

        computed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        for p in predictions:
            p["system_version"] = _sys_version
            p["pipeline_run_id"] = _run_id
            p["computed_at"] = computed_at
            p["created_at"] = computed_at
            p["updated_at"] = computed_at

        if dry_run:
            log.info("[DRY RUN] Would upsert %d row(s).", len(predictions))
            if predictions:
                log.info("Sample row:\n%s", json.dumps(predictions[0], indent=2, ensure_ascii=False))
        elif predictions:
            try:
                sb.upsert_predictions(predictions)
                log.info("✓ Upserted %d prediction(s).", len(predictions))
            except requests.HTTPError as exc:
                raise RuntimeError(f"Supabase upsert failed: {exc}") from exc

        strong_tips = sum(1 for p in predictions if p["is_strong_tip"])
        value_flags = sum(
            1
            for p in predictions
            if p.get("value_home") or p.get("value_draw") or p.get("value_away")
        )

        log.info("═══════════════════════════════════════════════")
        log.info("SUMMARY")
        log.info("  Matches fetched    : %d", len(matches))
        log.info("  Dirty-only mode    : %s", dirty_only)
        log.info("  Skipped (past)     : %d", skipped_past)
        log.info("  Predictions built  : %d", len(predictions))
        log.info("  Strong tips (≥%d%%): %d", STRONG_TIP_THRESHOLD, strong_tips)
        log.info("  Value flags        : %d", value_flags)
        log.info("  Compute errors     : %d", len(compute_errors))

        if compute_errors:
            log.warning("  Failed matches:")
            for err in compute_errors:
                log.warning("    - %s", err)

        if compute_errors and len(compute_errors) == len(matches):
            raise RuntimeError(f"All predictions failed: {compute_errors}")

        log.info("  Status             : SUCCESS")
        log.info("  System version     : %s", _sys_version)
        log.info("  Pipeline run ID    : %s", _run_id)
        log.info("═══════════════════════════════════════════════")

        finish_pipeline_run(
            _run_id,
            status="success",
            metadata={
                "predictions": len(predictions),
                "errors": len(compute_errors),
                "skipped_past": skipped_past,
                "dry_run": dry_run,
                "dirty_only": dirty_only,
                "value_flags": value_flags,
            },
        )

    except Exception as exc:
        finish_pipeline_run(
            _run_id,
            status="failed",
            metadata={
                "error": str(exc),
                "days_ahead": days_ahead,
                "dry_run": dry_run,
                "dirty_only": dirty_only,
            },
        )
        log.error("Pipeline failed: %s", exc)
        sys.exit(1)


def main() -> None:
    args = build_arg_parser().parse_args()
    run(days_ahead=args.days_ahead, dry_run=args.dry_run, dirty_only=args.dirty_only)


if __name__ == "__main__":
    main()