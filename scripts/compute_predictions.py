"""
compute_predictions.py
──────────────────────
EuroMatch Edge — Data Pipeline Step 2

Reads upcoming matches from Supabase `matches`, computes predictions
using the MVP model, and upserts results into `predictions`.

Environment variables required:
  SUPABASE_URL               — https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  — Supabase service_role JWT

Usage:
  python scripts/compute_predictions.py
  python scripts/compute_predictions.py --days-ahead 3
  python scripts/compute_predictions.py --dry-run
"""

import argparse
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

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
# MODEL CONFIGURATION
# ─────────────────────────────────────────────────────────────

MODEL_VERSION = "1.0"

# Minimum win_probability to be flagged as a strong tip
STRONG_TIP_THRESHOLD = 70

# Model factor weights — must sum to 1.0
WEIGHTS = {
    "strength":   0.32,
    "form":       0.22,
    "home_adv":   0.12,
    "injuries":   0.12,
    "motivation": 0.10,
    "load":       0.08,
    "market":     0.04,
}

# How many factors must be estimable for full confidence
# (fewer known factors → lower confidence_score)
MAX_KNOWN_FACTORS = len(WEIGHTS)

# Maps load_level_t enum values → numeric penalty applied to the leading team
LOAD_PENALTY: dict[str, float] = {
    "low":    0.00,
    "medium": 0.03,
    "high":   0.07,
}

# Market signal (home win odds) → home strength bonus in [0, 1]
# Lower odds = stronger market signal for home win
def market_odds_to_score(odds: float | None) -> float | None:
    """Convert decimal home-win odds to a [0,1] model score. Returns None if unavailable."""
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


# ─────────────────────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────────────────────

class SupabaseClient:
    """Minimal Supabase REST client (service_role)."""

    def __init__(self, url: str, service_role_key: str) -> None:
        self.base_url = url.rstrip("/")
        self._auth_headers = {
            "apikey":        service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type":  "application/json",
        }

    # ── READ ──────────────────────────────────────────────────

    def fetch_upcoming_matches(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        """
        Return matches with kickoff_at >= date_from and kickoff_at < date_to (exclusive upper bound).
        Both values are ISO 8601 strings with UTC timezone, microseconds stripped.

        Uses requests params= dict so that special characters ('+', ':') in timestamps
        are percent-encoded correctly — avoids HTTP 400 from malformed query strings.
        Supabase REST allows duplicate param keys for multi-filter on the same column.
        """
        url = f"{self.base_url}/rest/v1/matches"

        # Strip microseconds for cleaner URLs: "2025-06-10T06:30:00+00:00" not "...123456+00:00"
        def _clean_ts(ts: str) -> str:
            try:
                dt = datetime.fromisoformat(ts)
                return dt.replace(microsecond=0).isoformat()
            except ValueError:
                return ts

        ts_from = _clean_ts(date_from)
        ts_to   = _clean_ts(date_to)

        # requests encodes params= automatically — no manual URL building needed.
        # To pass two filters for the same key, use a list of (key, value) tuples.
        params = [
            ("kickoff_at", f"gte.{ts_from}"),
            ("kickoff_at", f"lt.{ts_to}"),
            ("order",      "kickoff_at.asc"),
            ("select",     "*"),
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

    # ── WRITE ─────────────────────────────────────────────────

    def upsert_predictions(self, rows: list[dict[str, Any]]) -> None:
        """UPSERT prediction rows. Conflict key: match_id (UNIQUE in schema)."""
        if not rows:
            return

        url = f"{self.base_url}/rest/v1/predictions"
        headers = {
            **self._auth_headers,
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(rows),
            timeout=30,
        )

        if not response.ok:
            log.error(
                "  Supabase upsert failed: %d %s\n  Body: %s",
                response.status_code,
                response.reason,
                response.text[:400],
            )
            response.raise_for_status()


# ─────────────────────────────────────────────────────────────
# MODEL HELPERS
# ─────────────────────────────────────────────────────────────

def form_to_score(form: list[str]) -> float | None:
    """
    Convert a form array like ['W','W','D','L','W'] to a [0,1] score.
    Returns None if form is empty (unknown).
    W=3pts, D=1pt, L=0pts — normalised over 5 games * 3 max pts.
    """
    if not form:
        return None
    max_pts = len(form) * 3
    pts = sum({"W": 3, "D": 1, "L": 0}.get(r, 0) for r in form)
    return pts / max_pts


def strength_to_score(strength: int | None) -> float | None:
    """Normalise a 0–100 strength rating to [0,1]. Returns None if unknown."""
    if strength is None:
        return None
    return max(0.0, min(1.0, strength / 100.0))


def weighted_average(
    factors: dict[str, float | None],
    weights: dict[str, float],
) -> tuple[float, int]:
    """
    Compute a weighted average of available (non-None) factors.

    Returns:
        (score, n_known)  where score ∈ [0,1] and n_known is the count of
        factors that contributed (used to derive confidence_score).

    Unknown factors are excluded from both numerator and denominator,
    so the remaining weights are re-normalised automatically.
    """
    total_weight = 0.0
    total_value  = 0.0
    n_known      = 0

    for name, value in factors.items():
        if value is None:
            continue
        w = weights.get(name, 0.0)
        total_value  += value * w
        total_weight += w
        n_known      += 1

    if total_weight == 0.0:
        # No data at all — fall back to coin-flip
        return 0.5, 0

    return total_value / total_weight, n_known


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def normalise_to_100(home: float, draw: float, away: float) -> tuple[int, int, int]:
    """
    Scale three raw probability floats so they sum to exactly 100.
    Uses largest-remainder (Hamilton) rounding to avoid off-by-one errors.
    """
    total = home + draw + away
    if total == 0:
        return 34, 33, 33

    raw = [home / total * 100, draw / total * 100, away / total * 100]
    floored = [math.floor(v) for v in raw]
    remainder = 100 - sum(floored)

    # Distribute remaining points to the values with largest fractional parts
    fractions = sorted(
        range(3),
        key=lambda i: raw[i] - floored[i],
        reverse=True,
    )
    for i in range(remainder):
        floored[fractions[i]] += 1

    return floored[0], floored[1], floored[2]


# ─────────────────────────────────────────────────────────────
# PREDICTION MODEL
# ─────────────────────────────────────────────────────────────

def compute_prediction(match: dict[str, Any]) -> dict[str, Any]:
    """
    Compute a full prediction row for a single match dict.

    Design principles:
    - Every input field is treated as optional (None = unknown).
    - The model degrades gracefully: fewer known inputs → lower confidence.
    - The three output probabilities always sum to exactly 100.
    - No external calls — pure function of the match dict.
    """

    # ── 1. Parse inputs ──────────────────────────────────────

    home_str   = strength_to_score(match.get("home_strength"))
    away_str   = strength_to_score(match.get("away_strength"))
    form_h     = form_to_score(match.get("form_home") or [])
    form_a     = form_to_score(match.get("form_away") or [])
    load_h_raw = match.get("load_home")
    load_a_raw = match.get("load_away")
    market     = market_odds_to_score(match.get("market_home_win_odds"))
    inj_h      = match.get("injuries_home") or ""
    inj_a      = match.get("injuries_away") or ""
    mot_h      = match.get("motivation_home") or ""
    mot_a      = match.get("motivation_away") or ""

    # ── 2. Derive per-factor home scores [0,1] ───────────────

    # Strength: home advantage expressed as fraction of combined strength
    if home_str is not None and away_str is not None:
        strength_score = home_str / (home_str + away_str + 1e-9)
    else:
        strength_score = None

    # Form: relative home form
    if form_h is not None and form_a is not None:
        form_score = (form_h + 0.5) / (form_h + form_a + 1.0)   # Laplace smoothing
    elif form_h is not None:
        form_score = clamp(form_h)
    else:
        form_score = None

    # Fixed home advantage (always known)
    home_adv_score: float = 0.58    # ~58% baseline win rate for home team

    # Injuries: symmetric — away injuries boost home score, home injuries lower it.
    # Each side contributes independently; neutral (0.50) when both unknown.
    def _inj_count(text: str) -> int | None:
        """Return count of named absentees, or None if field is empty/unknown."""
        if not text or text.strip().lower() in ("", "none", "keine", "-"):
            return None
        return len([x for x in text.split(",") if x.strip()])

    inj_h_count = _inj_count(inj_h)
    inj_a_count = _inj_count(inj_a)

    if inj_h_count is None and inj_a_count is None:
        injuries_score: float | None = None          # no data → factor excluded
    else:
        # Start from neutral 0.50; each away absence nudges home up, each home absence nudges down.
        # Cap per side at ±0.20 (5 absences × 0.04) so a single team can't dominate the factor.
        away_boost = min((inj_a_count or 0) * 0.04, 0.20)
        home_drag  = min((inj_h_count or 0) * 0.04, 0.20)
        injuries_score = clamp(0.50 + away_boost - home_drag)

    # Load: high away load = home advantage; high home load = home disadvantage
    load_h_penalty = LOAD_PENALTY.get(load_h_raw or "", 0.0)
    load_a_penalty = LOAD_PENALTY.get(load_a_raw or "", 0.0)
    if load_h_raw is not None or load_a_raw is not None:
        # Express as [0,1]: 0.5 = neutral; >0.5 = home benefit
        load_score: float | None = clamp(0.5 - load_h_penalty + load_a_penalty)
    else:
        load_score = None

    # Motivation: simple keyword heuristic
    MOTIVATION_HIGH = {
        "titel", "meister", "scudetto", "treble", "ungeschlagen",
        "abstieg", "relegation", "chapionship", "cup final",
    }
    def motivation_weight(text: str) -> float:
        lowered = text.lower()
        return 0.65 if any(k in lowered for k in MOTIVATION_HIGH) else 0.50

    if mot_h or mot_a:
        mot_score: float | None = clamp(
            (motivation_weight(mot_h) + (1.0 - motivation_weight(mot_a))) / 2.0
        )
    else:
        mot_score = None

    # Market signal (already [0,1] via market_odds_to_score)
    market_score = market

    # ── 3. Weighted composite home score ─────────────────────

    factors: dict[str, float | None] = {
        "strength":   strength_score,
        "form":       form_score,
        "home_adv":   home_adv_score,   # always present
        "injuries":   injuries_score,
        "motivation": mot_score,
        "load":       load_score,
        "market":     market_score,
    }

    home_score, n_known = weighted_average(factors, WEIGHTS)
    home_score = clamp(home_score, 0.05, 0.93)

    # ── 4. Derive draw and away probabilities ─────────────────
    # home_score IS the win probability directly.
    # Draw probability decays exponentially as the match becomes more one-sided.
    # This mirrors real-world 1X2 market distributions.
    #   draw_base = 0.27  (average draw rate in top European leagues)
    #   k = 3.5           (decay rate — tuned so draw ~8% at home_score=0.90)
    import math as _math
    _draw_base = 0.27
    _k         = 3.5
    raw_draw = _draw_base * _math.exp(-_k * abs(home_score - 0.5))
    raw_draw = max(0.06, raw_draw)                   # floor: draws never truly zero
    raw_away = max(0.03, 1.0 - home_score - raw_draw)

    win_p, draw_p, away_p = normalise_to_100(home_score, raw_draw, raw_away)

    # ── 5. Derived output fields ──────────────────────────────

    if win_p >= away_p and win_p >= draw_p:
        predicted_winner = "home"
        leading_prob     = win_p
    elif away_p >= win_p and away_p >= draw_p:
        predicted_winner = "away"
        leading_prob     = away_p
    else:
        predicted_winner = "draw"
        leading_prob     = draw_p

    is_strong_tip = leading_prob >= STRONG_TIP_THRESHOLD

    # ── 6. Risk level & tags ──────────────────────────────────

    risk_tags: list[str] = []

    if n_known <= 2:
        risk_tags.append("Wenig Datenbasis")
    if load_h_raw == "high":
        risk_tags.append("Hohe Spielplan-Belastung Heim")
    if load_a_raw == "high":
        risk_tags.append("Hohe Spielplan-Belastung Auswärts")
    if inj_h and len([x for x in inj_h.split(",") if x.strip()]) >= 2:
        risk_tags.append("Mehrere Ausfälle Heim")
    if form_h is not None and form_a is not None and abs(form_h - form_a) < 0.15:
        risk_tags.append("Ausgeglichene Form")
    if home_str is not None and away_str is not None and abs(home_str - away_str) < 10:
        risk_tags.append("Ausgeglichene Stärke")
    if market is not None and market < 0.50:
        risk_tags.append("Markt favorisiert Auswärtsteam")

    n_risk = len(risk_tags)
    if n_risk >= 3 or (n_known <= 1):
        risk_level = "high"
    elif n_risk >= 1:
        risk_level = "medium"
    else:
        risk_level = "low"

    # ── 7. Confidence score ───────────────────────────────────
    # Blend of: data completeness + outcome conviction + risk
    data_completeness  = n_known / MAX_KNOWN_FACTORS        # [0,1]
    outcome_conviction = abs(leading_prob - 50) / 50        # [0,1]
    risk_penalty       = n_risk * 0.05                      # each tag costs 5%

    raw_confidence = (
        data_completeness  * 0.50 +
        outcome_conviction * 0.40 -
        risk_penalty
    )
    confidence_score = int(clamp(raw_confidence, 0.0, 1.0) * 100)

    # ── 8. Text outputs ───────────────────────────────────────

    home_team = match.get("home_team", "Heim")
    away_team = match.get("away_team", "Auswärts")

    analysis_text = _build_analysis_text(
        home_team, away_team, win_p, draw_p, away_p,
        predicted_winner, is_strong_tip, n_known,
    )

    ai_reason = _build_ai_reason(
        home_team, away_team, predicted_winner,
        home_str, away_str, form_h, form_a,
        load_h_raw, load_a_raw, inj_h, inj_a,
        n_known,
    )

    return {
        "match_id":         match["id"],
        "win_probability":  win_p,
        "draw_probability": draw_p,
        "away_probability": away_p,
        "confidence_score": confidence_score,
        "predicted_winner": predicted_winner,
        "is_strong_tip":    is_strong_tip,
        "risk_level":       risk_level,
        "risk_tags":        risk_tags,
        "analysis_text":    analysis_text,
        "ai_reason":        ai_reason,
        "model_version":    MODEL_VERSION,
    }


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
    """One-sentence factual summary for the match card."""
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
    """Structured reasoning text for the detail drawer."""
    if n_known == 0:
        return "Keine Eingangsdaten verfügbar. Vorhersage basiert ausschließlich auf dem allgemeinen Heimvorteil."

    winner_name = home if predicted_winner == "home" else (away if predicted_winner == "away" else "Unentschieden")
    reasons: list[str] = []

    # Strength
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

    # Form
    if form_h is not None and form_a is not None:
        fh_pct = int(form_h * 100)
        fa_pct = int(form_a * 100)
        if abs(fh_pct - fa_pct) >= 20:
            better = home if form_h > form_a else away
            reasons.append(f"{better} zeigt mit {max(fh_pct, fa_pct)}% Form-Score deutlich bessere Tagesform.")
        else:
            reasons.append(f"Die aktuelle Form beider Teams ist ähnlich ({fh_pct}% vs {fa_pct}%).")

    # Load
    if load_h == "high" and load_a != "high":
        reasons.append(f"{home} kommt mit hoher Spielplan-Belastung in diese Partie.")
    elif load_a == "high" and load_h != "high":
        reasons.append(f"{away} reist mit hoher Spielplan-Belastung an – Vorteil für {home}.")

    # Injuries — use the same count logic as the model
    def _inj_count_text(text: str) -> int | None:
        if not text or text.strip().lower() in ("", "none", "keine", "-"):
            return None
        return len([x for x in text.split(",") if x.strip()])

    inj_a_count = _inj_count_text(inj_a)
    inj_h_count = _inj_count_text(inj_h)
    if inj_a_count:
        reasons.append(f"{away} hat {inj_a_count} gemeldete Ausfälle im Kader.")
    if inj_h_count:
        reasons.append(f"{home} hat {inj_h_count} Ausfälle zu beklagen.")

    # Home advantage (always mention)
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
    return parser


def load_env() -> tuple[str, str]:
    supabase_url = os.environ.get("SUPABASE_URL",              "").strip()
    service_key  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    missing = [
        name for name, val in [
            ("SUPABASE_URL",              supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", service_key),
        ]
        if not val
    ]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    return supabase_url, service_key


# ─────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────

def run(days_ahead: int, dry_run: bool) -> None:
    now      = datetime.now(timezone.utc)
    date_from = now.isoformat()
    date_to   = (now + timedelta(days=days_ahead)).isoformat()

    log.info("═══════════════════════════════════════════════")
    log.info("EuroMatch Edge — compute_predictions.py")
    log.info("Window   : now → +%d day(s)", days_ahead)
    log.info("From     : %s", date_from)
    log.info("To       : %s", date_to)
    log.info("Dry run  : %s", dry_run)
    log.info("Model    : v%s", MODEL_VERSION)
    log.info("═══════════════════════════════════════════════")

    supabase_url, service_key = load_env()
    sb = SupabaseClient(supabase_url, service_key)

    # ── Fetch matches
    try:
        matches = sb.fetch_upcoming_matches(date_from, date_to)
    except requests.HTTPError as exc:
        log.error("Failed to fetch matches from Supabase: %s", exc)
        sys.exit(1)
    except requests.RequestException as exc:
        log.error("Network error fetching matches: %s", exc)
        sys.exit(1)

    if not matches:
        log.info("No upcoming matches found in window. Nothing to compute.")
        log.info("Status: SUCCESS (no-op)")
        return

    # ── Compute predictions
    now_utc = datetime.now(timezone.utc)
    predictions:    list[dict[str, Any]] = []
    compute_errors: list[str]            = []
    skipped_past:   int                  = 0

    for match in matches:
        match_id  = match.get("id", "?")
        home_team = match.get("home_team", "?")
        away_team = match.get("away_team", "?")

        # Guard: skip any match that has already kicked off.
        # The Supabase query filters by kickoff_at >= now, but rows whose kickoff_at
        # was in the future when fetched could slip through if the pipeline runs slowly,
        # or if stale rows land in the table from a manual insert.
        kickoff_raw = match.get("kickoff_at")
        if kickoff_raw:
            try:
                kickoff_dt = datetime.fromisoformat(kickoff_raw.replace("Z", "+00:00"))
                if kickoff_dt <= now_utc:
                    log.warning(
                        "  ⏭ Skipping past match: %s vs %s (kickoff %s)",
                        home_team, away_team, kickoff_raw,
                    )
                    skipped_past += 1
                    continue
            except ValueError:
                log.warning("  Could not parse kickoff_at '%s' for match %s — processing anyway.", kickoff_raw, match_id)

        try:
            pred = compute_prediction(match)
            predictions.append(pred)
            log.info(
                "  ✓ %s vs %s  →  home %d%%  draw %d%%  away %d%%  conf %d%%  %s",
                home_team, away_team,
                pred["win_probability"], pred["draw_probability"], pred["away_probability"],
                pred["confidence_score"],
                "⚡ STRONG" if pred["is_strong_tip"] else "",
            )
        except Exception as exc:          # noqa: BLE001 — catch-all so one bad match doesn't abort the run
            err = f"match {match_id} ({home_team} vs {away_team}): {exc}"
            log.error("  ✗ Failed to compute prediction — %s", err)
            compute_errors.append(err)

    log.info("─── Computed %d / %d prediction(s).", len(predictions), len(matches))

    # ── Upsert
    if dry_run:
        log.info("[DRY RUN] Would upsert %d row(s).", len(predictions))
        if predictions:
            log.info("Sample row:\n%s", json.dumps(predictions[0], indent=2, ensure_ascii=False))
    elif predictions:
        try:
            sb.upsert_predictions(predictions)
            log.info("✓ Upserted %d prediction(s).", len(predictions))
        except requests.HTTPError as exc:
            log.error("Supabase upsert failed: %s", exc)
            sys.exit(1)

    # ── Summary
    strong_tips = sum(1 for p in predictions if p["is_strong_tip"])
    log.info("═══════════════════════════════════════════════")
    log.info("SUMMARY")
    log.info("  Matches fetched    : %d", len(matches))
    log.info("  Skipped (past)     : %d", skipped_past)
    log.info("  Predictions built  : %d", len(predictions))
    log.info("  Strong tips (≥%d%%): %d", STRONG_TIP_THRESHOLD, strong_tips)
    log.info("  Compute errors     : %d", len(compute_errors))

    if compute_errors:
        log.warning("  Failed matches:")
        for err in compute_errors:
            log.warning("    - %s", err)

    if compute_errors and len(compute_errors) == len(matches):
        log.error("  All predictions failed.")
        log.info("═══════════════════════════════════════════════")
        sys.exit(1)

    log.info("  Status             : SUCCESS")
    log.info("═══════════════════════════════════════════════")


def main() -> None:
    args = build_arg_parser().parse_args()
    run(days_ahead=args.days_ahead, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
