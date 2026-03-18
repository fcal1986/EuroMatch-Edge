import os
import sys
import json
import math
import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras


DATABASE_URL = os.environ["DATABASE_URL"]

# =========================
# CONFIG
# =========================

MODEL_VERSION = os.environ.get("MODEL_VERSION", "1.1")
CALIBRATION_VERSION = os.environ.get("CALIBRATION_VERSION", "1.1")
WEIGHTS_VERSION = os.environ.get("WEIGHTS_VERSION", "1.1")

# Modell + Markt-Mix
MODEL_WEIGHT = 0.75
MARKET_WEIGHT = 0.25

# Probability caps
MIN_OUTCOME_PROB = 0.08
MAX_OUTCOME_PROB = 0.70

# Value-Regeln
MIN_EDGE = 0.03
MIN_EV = 0.05
MIN_CONFIDENCE = 35
MIN_BOOKMAKER_COUNT = 5

# Guardrails gegen Ausreißer
MAX_EDGE = 0.25
MAX_EV = 0.50

# Strong tip
STRONG_TIP_THRESHOLD = 70

# Faktor-Gewichte
WEIGHTS = {
    "strength": 0.36,
    "form": 0.22,
    "home_adv": 0.08,
    "injuries": 0.12,
    "motivation": 0.10,
    "load": 0.08,
    "market": 0.04,
}

LOAD_PENALTY = {
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

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


# =========================
# HELPERS
# =========================

def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def renormalize_probs(home_p: float, draw_p: float, away_p: float) -> tuple[float, float, float]:
    total = home_p + draw_p + away_p
    if total <= 0:
        return 1 / 3, 1 / 3, 1 / 3
    return home_p / total, draw_p / total, away_p / total


def apply_probability_caps(
    home_p: float,
    draw_p: float,
    away_p: float,
    min_prob: float = MIN_OUTCOME_PROB,
    max_prob: float = MAX_OUTCOME_PROB,
) -> tuple[float, float, float]:
    home_p, draw_p, away_p = renormalize_probs(home_p, draw_p, away_p)

    home_p = clamp(home_p, min_prob, max_prob)
    draw_p = clamp(draw_p, min_prob, max_prob)
    away_p = clamp(away_p, min_prob, max_prob)

    home_p, draw_p, away_p = renormalize_probs(home_p, draw_p, away_p)

    home_p = clamp(home_p, min_prob, max_prob)
    draw_p = clamp(draw_p, min_prob, max_prob)
    away_p = clamp(away_p, min_prob, max_prob)

    return renormalize_probs(home_p, draw_p, away_p)


def normalise_to_100(home_p: float, draw_p: float, away_p: float) -> tuple[int, int, int]:
    total = home_p + draw_p + away_p
    if total <= 0:
        return 34, 33, 33

    vals = [home_p / total * 100.0, draw_p / total * 100.0, away_p / total * 100.0]
    ints = [int(v) for v in vals]
    remainder = 100 - sum(ints)

    frac_order = sorted(
        range(3),
        key=lambda i: (vals[i] - ints[i]),
        reverse=True,
    )
    for i in frac_order[:remainder]:
        ints[i] += 1

    return ints[0], ints[1], ints[2]


def form_to_score(form: list[str] | None) -> float | None:
    if not form:
        return None
    max_pts = len(form) * 3
    if max_pts <= 0:
        return None
    pts = sum({"W": 3, "D": 1, "L": 0}.get(str(x).upper(), 0) for x in form)
    return pts / max_pts


def strength_to_score(strength: Any) -> float | None:
    x = to_float(strength)
    if x is None:
        return None
    return clamp(x / 100.0, 0.0, 1.0)


def weighted_average(factors: dict[str, float | None], weights: dict[str, float]) -> tuple[float, int]:
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


def _inj_count_text(text: str | None) -> int:
    if not text:
        return 0
    text = str(text).strip().lower()
    if text in ("", "none", "keine", "-"):
        return 0
    return len([x for x in text.split(",") if x.strip()])


def market_odds_to_score(odds: float | None) -> float | None:
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
    home_odds = to_float(match.get("market_home_win_odds"))
    draw_odds = to_float(match.get("market_draw_odds"))
    away_odds = to_float(match.get("market_away_odds"))

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

    if home_odds is None or draw_odds is None or away_odds is None:
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


def blend_with_market(
    model_home: float,
    model_draw: float,
    model_away: float,
    market_home: float | None,
    market_draw: float | None,
    market_away: float | None,
) -> tuple[float, float, float]:
    if market_home is None or market_draw is None or market_away is None:
        return apply_probability_caps(model_home, model_draw, model_away)

    alpha = MARKET_WEIGHT

    home_p = MODEL_WEIGHT * model_home + alpha * market_home
    draw_p = MODEL_WEIGHT * model_draw + alpha * market_draw
    away_p = MODEL_WEIGHT * model_away + alpha * market_away

    return apply_probability_caps(home_p, draw_p, away_p)


def compute_ev(prob: Any, odds: Any) -> float | None:
    if odds is None:
        return None
    try:
        return float(prob) * float(odds) - 1.0
    except (TypeError, ValueError):
        return None


def build_analysis_text(home: str, away: str, pred_winner: str, win_p: int, draw_p: int, away_p: int) -> str:
    if pred_winner == "draw":
        return (
            f"Ausgeglichenes Duell: {home} {win_p}% – Remis {draw_p}% – {away} {away_p}%. "
            f"Kein klarer Favorit erkennbar."
        )

    winner = home if pred_winner == "home" else away
    winner_prob = win_p if pred_winner == "home" else away_p
    strength = "klar" if winner_prob >= 75 else "leicht"
    return f"{winner} mit {winner_prob}% Siegwahrscheinlichkeit {strength} favorisiert."


def build_ai_reason(
    home: str,
    away: str,
    home_str: float | None,
    away_str: float | None,
    form_h: float | None,
    form_a: float | None,
    inj_h: str,
    inj_a: str,
    load_h: Any,
    load_a: Any,
) -> str:
    reasons: list[str] = []

    if home_str is not None and away_str is not None:
        diff = abs(int(home_str * 100) - int(away_str * 100))
        if diff >= 20:
            stronger = home if home_str > away_str else away
            reasons.append(f"{stronger} ist im Stärkevergleich deutlich überlegen.")
        elif diff >= 8:
            stronger = home if home_str > away_str else away
            reasons.append(f"{stronger} liegt im Stärkevergleich leicht vorne.")

    if form_h is not None and form_a is not None:
        if abs(form_h - form_a) >= 0.20:
            better = home if form_h > form_a else away
            reasons.append(f"{better} zeigt die bessere aktuelle Form.")

    inj_h_count = _inj_count_text(inj_h)
    inj_a_count = _inj_count_text(inj_a)
    if inj_h_count:
        reasons.append(f"{home} hat {inj_h_count} gemeldete Ausfälle.")
    if inj_a_count:
        reasons.append(f"{away} hat {inj_a_count} gemeldete Ausfälle.")

    if str(load_h) == "high" and str(load_a) != "high":
        reasons.append(f"{home} kommt mit hoher Belastung in die Partie.")
    elif str(load_a) == "high" and str(load_h) != "high":
        reasons.append(f"{away} reist mit hoher Belastung an.")

    reasons.append(f"Heimvorteil fließt grundsätzlich zugunsten von {home} ein.")
    return " ".join(reasons) if reasons else "Das Modell bewertet die Partie anhand der verfügbaren Daten."


# =========================
# DB
# =========================

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def fetch_matches(conn, days_ahead: int = 2, dirty_only: bool = False) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    future = now + timedelta(days=days_ahead)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if dirty_only:
            cur.execute(
                """
                select m.*
                from public.matches m
                join public.v_dirty_predictions d
                  on d.match_id = m.id
                where m.kickoff_at >= %s
                  and m.kickoff_at < %s
                order by m.kickoff_at asc
                """,
                (now, future),
            )
        else:
            cur.execute(
                """
                select *
                from public.matches
                where kickoff_at >= %s
                  and kickoff_at < %s
                order by kickoff_at asc
                """,
                (now, future),
            )
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def upsert_predictions(conn, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return

    sql = """
    insert into public.predictions (
      match_id,
      win_probability,
      draw_probability,
      away_probability,
      confidence_score,
      predicted_winner,
      is_strong_tip,
      risk_level,
      risk_tags,
      analysis_text,
      ai_reason,
      model_version,
      calibration_version,
      weights_version,
      market_home_prob_fair,
      market_draw_prob_fair,
      market_away_prob_fair,
      market_overround,
      market_bookmaker_count,
      market_last_update_ts,
      edge_home,
      edge_draw,
      edge_away,
      ev_home,
      ev_draw,
      ev_away,
      value_home,
      value_draw,
      value_away,
      computed_at,
      created_at,
      updated_at
    ) values %s
    on conflict (match_id)
    do update set
      win_probability = excluded.win_probability,
      draw_probability = excluded.draw_probability,
      away_probability = excluded.away_probability,
      confidence_score = excluded.confidence_score,
      predicted_winner = excluded.predicted_winner,
      is_strong_tip = excluded.is_strong_tip,
      risk_level = excluded.risk_level,
      risk_tags = excluded.risk_tags,
      analysis_text = excluded.analysis_text,
      ai_reason = excluded.ai_reason,
      model_version = excluded.model_version,
      calibration_version = excluded.calibration_version,
      weights_version = excluded.weights_version,
      market_home_prob_fair = excluded.market_home_prob_fair,
      market_draw_prob_fair = excluded.market_draw_prob_fair,
      market_away_prob_fair = excluded.market_away_prob_fair,
      market_overround = excluded.market_overround,
      market_bookmaker_count = excluded.market_bookmaker_count,
      market_last_update_ts = excluded.market_last_update_ts,
      edge_home = excluded.edge_home,
      edge_draw = excluded.edge_draw,
      edge_away = excluded.edge_away,
      ev_home = excluded.ev_home,
      ev_draw = excluded.ev_draw,
      ev_away = excluded.ev_away,
      value_home = excluded.value_home,
      value_draw = excluded.value_draw,
      value_away = excluded.value_away,
      computed_at = excluded.computed_at,
      updated_at = excluded.updated_at
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
    conn.commit()


# =========================
# CORE LOGIC
# =========================

def compute_prediction(match: dict[str, Any]) -> dict[str, Any]:
    home_team = match.get("home_team", "Heim")
    away_team = match.get("away_team", "Auswärts")

    home_str = strength_to_score(match.get("home_strength"))
    away_str = strength_to_score(match.get("away_strength"))
    form_h = form_to_score(match.get("form_home") or [])
    form_a = form_to_score(match.get("form_away") or [])
    load_h_raw = match.get("load_home")
    load_a_raw = match.get("load_away")
    inj_h = match.get("injuries_home") or ""
    inj_a = match.get("injuries_away") or ""
    mot_h = str(match.get("motivation_home") or "")
    mot_a = str(match.get("motivation_away") or "")

    market_odds_home = to_float(match.get("market_home_win_odds"))
    market_score = market_odds_to_score(market_odds_home)
    market_metrics = derive_market_metrics(match)

    # Faktoren
    if home_str is not None and away_str is not None:
        strength_score = home_str / (home_str + away_str + 1e-9)
    else:
        strength_score = None

    if form_h is not None and form_a is not None:
        form_score = (form_h + 0.5) / (form_h + form_a + 1.0)
    elif form_h is not None:
        form_score = clamp(form_h, 0.0, 1.0)
    else:
        form_score = None

    home_adv_score = 0.52

    inj_h_count = _inj_count_text(inj_h)
    inj_a_count = _inj_count_text(inj_a)
    if inj_h_count == 0 and inj_a_count == 0:
        injuries_score = None
    else:
        injury_delta = (inj_a_count - inj_h_count) * 0.06
        injuries_score = clamp(0.50 + injury_delta, 0.0, 1.0)

    def motivation_weight(text: str) -> float:
        lowered = text.lower()
        return 0.65 if any(k in lowered for k in MOTIVATION_HIGH) else 0.50

    if mot_h or mot_a:
        motivation_score = clamp(
            (motivation_weight(mot_h) + (1.0 - motivation_weight(mot_a))) / 2.0,
            0.0,
            1.0,
        )
    else:
        motivation_score = None

    pen_h = LOAD_PENALTY.get(str(load_h_raw), 0.0) if load_h_raw is not None else None
    pen_a = LOAD_PENALTY.get(str(load_a_raw), 0.0) if load_a_raw is not None else None

    if pen_h is None and pen_a is None:
        load_score = None
    else:
        pen_h_val = pen_h or 0.0
        pen_a_val = pen_a or 0.0
        load_score = clamp(0.50 + (pen_a_val - pen_h_val), 0.0, 1.0)

    factors = {
        "strength": strength_score,
        "form": form_score,
        "home_adv": home_adv_score,
        "injuries": injuries_score,
        "motivation": motivation_score,
        "load": load_score,
        "market": market_score,
    }

    home_score, n_known = weighted_average(factors, WEIGHTS)

    # leichte Korrektur gegen zu starken Home-Bias
    if home_str is not None and away_str is not None:
        diff = away_str - home_str
        if diff > 0:
            home_score -= diff * 0.25

    home_score = clamp(home_score, 0.05, 0.90)

    draw_base = 0.27
    k = 3.5
    raw_draw = draw_base * math.exp(-k * abs(home_score - 0.5))
    raw_draw = max(0.08, raw_draw)
    raw_away = max(0.05, 1.0 - home_score - raw_draw)

    model_home, model_draw, model_away = renormalize_probs(home_score, raw_draw, raw_away)

    blended_home, blended_draw, blended_away = blend_with_market(
        model_home=model_home,
        model_draw=model_draw,
        model_away=model_away,
        market_home=market_metrics["market_home_prob_fair"],
        market_draw=market_metrics["market_draw_prob_fair"],
        market_away=market_metrics["market_away_prob_fair"],
    )

    win_p, draw_p, away_p = normalise_to_100(blended_home, blended_draw, blended_away)

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

    risk_tags: list[str] = []
    if n_known <= 2:
        risk_tags.append("Wenig Datenbasis")
    if str(load_h_raw) == "high":
        risk_tags.append("Hohe Spielplan-Belastung Heim")
    if str(load_a_raw) == "high":
        risk_tags.append("Hohe Spielplan-Belastung Auswärts")
    if inj_h_count >= 2:
        risk_tags.append("Mehrere Ausfälle Heim")
    if inj_a_count >= 2:
        risk_tags.append("Mehrere Ausfälle Auswärts")
    if form_h is not None and form_a is not None and abs(form_h - form_a) < 0.15:
        risk_tags.append("Ausgeglichene Form")
    if home_str is not None and away_str is not None and abs(home_str - away_str) < 0.10:
        risk_tags.append("Ausgeglichene Stärke")

    market_home_fair = to_float(market_metrics["market_home_prob_fair"])
    market_away_fair = to_float(market_metrics["market_away_prob_fair"])
    if market_home_fair is not None and abs((win_p / 100.0) - market_home_fair) > 0.18:
        risk_tags.append("Starke Abweichung zum Markt Heim")
    if market_away_fair is not None and abs((away_p / 100.0) - market_away_fair) > 0.18:
        risk_tags.append("Starke Abweichung zum Markt Auswärts")

    n_risk = len(risk_tags)
    if n_risk >= 3 or n_known <= 1:
        risk_level = "high"
    elif n_risk >= 1:
        risk_level = "medium"
    else:
        risk_level = "low"

    data_completeness = n_known / max(len(WEIGHTS), 1)
    outcome_conviction = abs(leading_prob - 50) / 50
    risk_penalty = n_risk * 0.05

    raw_confidence = (data_completeness * 0.50) + (outcome_conviction * 0.40) - risk_penalty
    confidence_score = int(clamp(raw_confidence, 0.0, 1.0) * 100)

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
        "analysis_text": build_analysis_text(home_team, away_team, predicted_winner, win_p, draw_p, away_p),
        "ai_reason": build_ai_reason(home_team, away_team, home_str, away_str, form_h, form_a, inj_h, inj_a, load_h_raw, load_a_raw),

        "market_home_prob_fair": market_metrics["market_home_prob_fair"],
        "market_draw_prob_fair": market_metrics["market_draw_prob_fair"],
        "market_away_prob_fair": market_metrics["market_away_prob_fair"],
        "market_overround": market_metrics["market_overround"],
        "market_bookmaker_count": market_metrics["market_bookmaker_count"],
        "market_last_update_ts": market_metrics["market_last_update_ts"],

        "edge_home": None,
        "edge_draw": None,
        "edge_away": None,
        "ev_home": None,
        "ev_draw": None,
        "ev_away": None,
        "value_home": False,
        "value_draw": False,
        "value_away": False,

        "model_version": MODEL_VERSION,
        "calibration_version": CALIBRATION_VERSION,
        "weights_version": WEIGHTS_VERSION,
    }

    if pred["market_home_prob_fair"] is not None:
        fair_home = float(pred["market_home_prob_fair"])
        fair_draw = float(pred["market_draw_prob_fair"])
        fair_away = float(pred["market_away_prob_fair"])

        model_home = win_p / 100.0
        model_draw = draw_p / 100.0
        model_away = away_p / 100.0

        edge_home = model_home - fair_home
        edge_draw = model_draw - fair_draw
        edge_away = model_away - fair_away

        ev_home = compute_ev(model_home, match.get("market_home_win_odds"))
        ev_draw = compute_ev(model_draw, match.get("market_draw_odds"))
        ev_away = compute_ev(model_away, match.get("market_away_odds"))

        pred["edge_home"] = round(edge_home, 6)
        pred["edge_draw"] = round(edge_draw, 6)
        pred["edge_away"] = round(edge_away, 6)

        pred["ev_home"] = round(ev_home, 6) if ev_home is not None else None
        pred["ev_draw"] = round(ev_draw, 6) if ev_draw is not None else None
        pred["ev_away"] = round(ev_away, 6) if ev_away is not None else None

        bookmaker_count = int(pred["market_bookmaker_count"] or 0)
        has_market_depth = bookmaker_count >= MIN_BOOKMAKER_COUNT
        has_confidence = confidence_score >= MIN_CONFIDENCE

        def is_value(edge: float | None, ev: float | None) -> bool:
            if edge is None or ev is None:
                return False
            return bool(
                has_market_depth
                and has_confidence
                and edge >= MIN_EDGE
                and ev >= MIN_EV
                and edge <= MAX_EDGE
                and ev <= MAX_EV
            )

        pred["value_home"] = is_value(pred["edge_home"], pred["ev_home"])
        pred["value_draw"] = is_value(pred["edge_draw"], pred["ev_draw"])
        pred["value_away"] = is_value(pred["edge_away"], pred["ev_away"])

    return pred


# =========================
# CLI / MAIN
# =========================

def parse_args():
    parser = argparse.ArgumentParser(description="Compute match predictions and upsert into Postgres.")
    parser.add_argument("--days-ahead", type=int, default=2, help="How many days ahead to include.")
    parser.add_argument("--dry-run", action="store_true", help="Compute predictions but do not write.")
    parser.add_argument("--dirty-only", action="store_true", help="Only compute missing/stale matches.")
    return parser.parse_args()


def main():
    args = parse_args()

    log.info("═══════════════════════════════════════════════")
    log.info("EuroMatch Edge — compute_predictions.py")
    log.info("days_ahead=%s | dry_run=%s | dirty_only=%s", args.days_ahead, args.dry_run, args.dirty_only)
    log.info("═══════════════════════════════════════════════")

    conn = get_conn()

    try:
        matches = fetch_matches(conn, days_ahead=args.days_ahead, dirty_only=args.dirty_only)
        log.info("Gefundene Matches: %d", len(matches))

        if not matches:
            log.info("Keine Matches im Zeitfenster.")
            return

        now_utc = datetime.now(timezone.utc)
        rows_for_upsert: list[tuple[Any, ...]] = []

        for m in matches:
            kickoff = m.get("kickoff_at")
            if kickoff is not None and kickoff <= now_utc:
                log.info("Skip past match: %s vs %s", m.get("home_team"), m.get("away_team"))
                continue

            pred = compute_prediction(m)

            computed_at = datetime.now(timezone.utc).replace(microsecond=0)

            rows_for_upsert.append((
                pred["match_id"],
                pred["win_probability"],
                pred["draw_probability"],
                pred["away_probability"],
                pred["confidence_score"],
                pred["predicted_winner"],
                pred["is_strong_tip"],
                pred["risk_level"],
                pred["risk_tags"],
                pred["analysis_text"],
                pred["ai_reason"],
                pred["model_version"],
                pred["calibration_version"],
                pred["weights_version"],
                pred["market_home_prob_fair"],
                pred["market_draw_prob_fair"],
                pred["market_away_prob_fair"],
                pred["market_overround"],
                pred["market_bookmaker_count"],
                pred["market_last_update_ts"],
                pred["edge_home"],
                pred["edge_draw"],
                pred["edge_away"],
                pred["ev_home"],
                pred["ev_draw"],
                pred["ev_away"],
                pred["value_home"],
                pred["value_draw"],
                pred["value_away"],
                computed_at,
                computed_at,
                computed_at,
            ))

            log.info(
                "✓ %s vs %s → H:%s D:%s A:%s | K:%s | value(H/D/A)=%s/%s/%s",
                m.get("home_team"),
                m.get("away_team"),
                pred["win_probability"],
                pred["draw_probability"],
                pred["away_probability"],
                pred["confidence_score"],
                pred["value_home"],
                pred["value_draw"],
                pred["value_away"],
            )

        if args.dry_run:
            log.info("[DRY RUN] Würde %d Rows schreiben.", len(rows_for_upsert))
            if rows_for_upsert:
                sample = rows_for_upsert[0]
                log.info("[DRY RUN] Sample tuple: %s", json.dumps([str(x) for x in sample], ensure_ascii=False))
        else:
            upsert_predictions(conn, rows_for_upsert)
            log.info("Upsert erfolgreich: %d Predictions", len(rows_for_upsert))

    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("compute_predictions.py fehlgeschlagen: %s", exc)
        sys.exit(1)