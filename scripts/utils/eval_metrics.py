"""
utils/eval_metrics.py
─────────────────────
EuroMatch Edge — Evaluation Metrics Layer

Zentrale Hilfsfunktionen für die Berechnung von Evaluationsmetriken
in model_results. Alle Funktionen:
  - sind rein (keine Seiteneffekte)
  - behandeln None/fehlende Daten defensiv
  - werfen keine Exceptions
  - sind unit-testbar ohne Datenbank

Verwendung:
    from utils.eval_metrics import (
        goals_to_1x2,
        score_str_to_goals,
        compute_eval_metrics,
    )

    metrics = compute_eval_metrics(
        predicted_score="2:1",
        actual_home=1, actual_away=1,
        prob_home=0.55, prob_draw=0.25, prob_away=0.20,
    )
    # metrics["is_tendency_hit"]  → True
    # metrics["actual_1x2"]       → "D"
    # metrics["brier_score_1x2"]  → 0.455
"""

from __future__ import annotations

import logging
import math
from typing import TypedDict

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# TYPES
# ─────────────────────────────────────────────────────────────

class EvalMetrics(TypedDict):
    """
    Alle Evaluationsmetriken pro (match, model) Zeile.
    Optional-Felder sind None wenn die nötigen Daten fehlen.
    """
    # 1×2 Tendenz
    predicted_1x2:   str | None   # "H" | "D" | "A"
    actual_1x2:      str | None   # "H" | "D" | "A"
    # Binäre Treffer
    is_exact_hit:    bool | None  # predicted_score == actual_score
    is_tendency_hit: bool | None  # predicted_1x2   == actual_1x2
    # Score-Fehler (ganze Zahlen, immer ≥ 0)
    score_error_abs: int  | None  # |Δhome| + |Δaway|
    goal_diff_error: int  | None  # |predicted_diff - actual_diff|
    # Probabilistische Metriken (None wenn probs nicht verfügbar)
    brier_score_1x2: float | None # MSE über 3-Klassen one-hot
    log_loss_1x2:    float | None # negative log-likelihood


# ─────────────────────────────────────────────────────────────
# PRIMITIVE HELPERS
# ─────────────────────────────────────────────────────────────

def goals_to_1x2(home: int | None, away: int | None) -> str | None:
    """
    Wandelt Toranzahl in 1×2-Tendenz um.
    Rückgabe: "H" (Heimsieg) | "D" (Remis) | "A" (Auswärtssieg) | None
    """
    if home is None or away is None:
        return None
    if home > away:
        return "H"
    if away > home:
        return "A"
    return "D"


def score_str_to_goals(score: str | None) -> tuple[int, int] | None:
    """
    Parst "2:1" → (2, 1). Gibt None bei ungültigem Format zurück.
    Akzeptiert sowohl ":" als auch "-" als Trennzeichen.
    """
    if not score:
        return None
    for sep in (":", "-"):
        if sep in score:
            parts = score.split(sep, 1)
            try:
                return int(parts[0].strip()), int(parts[1].strip())
            except (ValueError, IndexError):
                return None
    return None


def _brier_score(
    actual_class: str,        # "H" | "D" | "A"
    p_home:  float,
    p_draw:  float,
    p_away:  float,
) -> float:
    """
    Brier Score für 3-Klassen (1×2).
    BS = (1/3) * Σ (p_i - o_i)²  über {H, D, A}
    Bereich: [0, 2]. Perfekt = 0, Worst case = 2.
    """
    # One-hot
    o_h = 1.0 if actual_class == "H" else 0.0
    o_d = 1.0 if actual_class == "D" else 0.0
    o_a = 1.0 if actual_class == "A" else 0.0
    bs  = ((p_home - o_h) ** 2 + (p_draw - o_d) ** 2 + (p_away - o_a) ** 2) / 3.0
    return round(bs, 6)


def _log_loss(
    actual_class: str,
    p_home:  float,
    p_draw:  float,
    p_away:  float,
    eps:     float = 1e-9,
) -> float:
    """
    Log Loss (negative log-likelihood) für 3-Klassen.
    LL = -log(p_correct_class).
    Clip bei eps um log(0) zu vermeiden.
    Bereich: [0, ∞). Perfekt → 0.
    """
    p_map = {"H": p_home, "D": p_draw, "A": p_away}
    p_c   = max(p_map.get(actual_class, eps), eps)
    return round(-math.log(p_c), 6)


def _validate_probs(
    p_home: float | None,
    p_draw: float | None,
    p_away: float | None,
) -> tuple[float, float, float] | None:
    """
    Prüft ob Wahrscheinlichkeiten verwendbar sind.
    Normalisiert auf Summe 1.0 falls nötig.
    Gibt None zurück wenn Daten ungültig.
    """
    if p_home is None or p_draw is None or p_away is None:
        return None
    total = p_home + p_draw + p_away
    if total <= 0 or not all(0 <= p <= 1.0 for p in (p_home, p_draw, p_away)):
        log.debug("eval_metrics: invalid probabilities %.3f/%.3f/%.3f — skipping",
                  p_home, p_draw, p_away)
        return None
    # Normalisieren (Modelle können leicht von 1.0 abweichen)
    return (p_home / total, p_draw / total, p_away / total)


# ─────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────

def compute_eval_metrics(
    predicted_score: str   | None,
    actual_home:     int   | None,
    actual_away:     int   | None,
    *,
    prob_home:  float | None = None,
    prob_draw:  float | None = None,
    prob_away:  float | None = None,
) -> EvalMetrics:
    """
    Berechnet alle Evaluationsmetriken für eine (match, model) Zeile.

    Args:
        predicted_score: Vorhergesagter Score als String, z.B. "2:1".
        actual_home:     Tatsächliche Heimtore.
        actual_away:     Tatsächliche Auswärtstore.
        prob_home:       Modell-Wahrscheinlichkeit Heimsieg [0, 1] (optional).
        prob_draw:       Modell-Wahrscheinlichkeit Remis [0, 1] (optional).
        prob_away:       Modell-Wahrscheinlichkeit Auswärtssieg [0, 1] (optional).

    Returns:
        EvalMetrics dict — alle Felder belegt, None wenn Daten fehlen.
        Wirft keine Exceptions.
    """
    # ── Parse predicted score ─────────────────────────────────
    parsed = score_str_to_goals(predicted_score)
    pred_h: int | None = parsed[0] if parsed else None
    pred_a: int | None = parsed[1] if parsed else None

    # ── 1×2 Tendenzen ─────────────────────────────────────────
    predicted_1x2 = goals_to_1x2(pred_h, pred_a)
    actual_1x2    = goals_to_1x2(actual_home, actual_away)

    # ── Binäre Treffer ─────────────────────────────────────────
    if (
        pred_h is not None and pred_a is not None
        and actual_home is not None and actual_away is not None
    ):
        is_exact_hit    = (pred_h == actual_home and pred_a == actual_away)
        is_tendency_hit = (
            predicted_1x2 is not None
            and actual_1x2 is not None
            and predicted_1x2 == actual_1x2
        )

        # ── Score-Fehler ───────────────────────────────────────
        score_error_abs = abs(pred_h - actual_home) + abs(pred_a - actual_away)
        pred_diff       = pred_h - pred_a
        act_diff        = actual_home - actual_away
        goal_diff_error = abs(pred_diff - act_diff)
    else:
        is_exact_hit    = None
        is_tendency_hit = None
        score_error_abs = None
        goal_diff_error = None

    # ── Probabilistische Metriken (optional) ──────────────────
    brier_score_1x2: float | None = None
    log_loss_1x2:    float | None = None

    valid_probs = _validate_probs(prob_home, prob_draw, prob_away)
    if valid_probs is not None and actual_1x2 is not None:
        ph, pd, pa = valid_probs
        brier_score_1x2 = _brier_score(actual_1x2, ph, pd, pa)
        log_loss_1x2    = _log_loss(actual_1x2, ph, pd, pa)
    elif (prob_home is not None or prob_draw is not None or prob_away is not None):
        # Daten wurden übergeben, sind aber ungültig — warnen
        log.debug(
            "eval_metrics: probabilities present but not valid for brier/logloss "
            "(p_home=%s, p_draw=%s, p_away=%s, actual_1x2=%s)",
            prob_home, prob_draw, prob_away, actual_1x2,
        )

    return EvalMetrics(
        predicted_1x2   = predicted_1x2,
        actual_1x2      = actual_1x2,
        is_exact_hit    = is_exact_hit,
        is_tendency_hit = is_tendency_hit,
        score_error_abs = score_error_abs,
        goal_diff_error = goal_diff_error,
        brier_score_1x2 = brier_score_1x2,
        log_loss_1x2    = log_loss_1x2,
    )


# ─────────────────────────────────────────────────────────────
# PROBABILITY EXTRACTION HELPERS
# ─────────────────────────────────────────────────────────────

def probs_from_score_grid(grid: dict[str, float]) -> tuple[float, float, float]:
    """
    Aggregiert ein Score-Grid {"H:A": prob} zu (p_home, p_draw, p_away).
    Identisch zur Logik in winner_from_grid(), aber gibt Wahrscheinlichkeiten zurück.

    Verwendung: für Poisson- und DC-Modelle, die ein score_grid berechnen.
    """
    p_home = p_away = p_draw = 0.0
    for score, prob in grid.items():
        try:
            h, a = map(int, score.split(":"))
        except (ValueError, AttributeError):
            continue
        if h > a:
            p_home += prob
        elif a > h:
            p_away += prob
        else:
            p_draw += prob
    return (p_home, p_draw, p_away)


def probs_from_home_probability(p_home_win: float) -> tuple[float, float, float]:
    """
    Schätzt (p_home, p_draw, p_away) aus einer skalaren Home-Win-Wahrscheinlichkeit.

    Verwendet ein einfaches Modell basierend auf historischen Bundesliga/EPL-Basisraten:
      draw_base  = 0.26
      away_base  = 0.28
      home_base  = 0.46

    Das Verhältnis draw:away wird konstant gehalten, home_p variiert.

    NUR als Annäherung verwenden — nicht für präzise Calibration.
    Für Modelle (Elo, Ensemble, Market) die nur home_p ausgeben.
    """
    p_home = max(0.0, min(1.0, p_home_win))
    rest   = 1.0 - p_home
    # Basisverhältnis draw:away = 0.26:0.28 ≈ 48:52
    p_draw  = rest * 0.481
    p_away  = rest * 0.519
    return (p_home, p_draw, p_away)
