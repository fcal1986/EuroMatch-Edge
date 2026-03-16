"""
utils/calibration.py
─────────────────────
EuroMatch Edge — Kalibrierungsschicht für DC-Wahrscheinlichkeiten (P4)

Ziel: Die rohen DC-Wahrscheinlichkeiten (prob_home, prob_draw, prob_away)
      stimmen nicht perfekt mit den beobachteten Häufigkeiten überein.
      Diese Schicht korrigiert das systematisch.

Methode: Temperature Scaling (multinomial)
  - Einfach, stabil, braucht nur 2401+ Matches für Training
  - Interpretierbar: T > 1 → Modell war overconfident, T < 1 → underconfident
  - Separates T pro Klasse (Home/Draw/Away) möglich

Implementierung:
  - Keine externen Deps (kein sklearn) — reines Python + math
  - Kalibrierung wird aus Backtest-Daten gelesen (model_results in Supabase)
  - Fallback: Identitäts-Kalibrierung (T=1.0) wenn keine Daten

Verwendung:
    from utils.calibration import calibrate_dc_probs, load_calibration_from_results

    rows = [...]  # model_results rows mit prob_home, actual_1x2
    cal = load_calibration_from_results(rows)
    ph, pd, pa = calibrate_dc_probs(0.55, 0.25, 0.20, cal)
"""

from __future__ import annotations

import logging
import math
from typing import TypedDict

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

MIN_SAMPLES_FOR_CALIBRATION = 200    # Minimum Rows für valide Kalibrierung
TEMPERATURE_CLIP            = (0.5, 2.5)  # Sicherheits-Clamp für Temperature
DEFAULT_TEMPERATURE         = 1.0    # Identität = keine Kalibrierung
BUCKET_COUNT                = 10     # Anzahl Buckets für Reliability Curve


# ─────────────────────────────────────────────────────────────
# TYPES
# ─────────────────────────────────────────────────────────────

class CalibrationParams(TypedDict):
    t_home:      float      # Temperature für Home-Wahrscheinlichkeit
    t_draw:      float      # Temperature für Draw-Wahrscheinlichkeit
    t_away:      float      # Temperature für Away-Wahrscheinlichkeit
    n_samples:   int        # Wie viele Matches wurden verwendet
    version:     str        # Identifiziert diese Kalibrierung
    is_identity: bool       # True = keine echte Kalibrierung (Fallback)


def identity_calibration() -> CalibrationParams:
    """Identitäts-Kalibrierung: keine Anpassung. Immer valide als Fallback."""
    return CalibrationParams(
        t_home=DEFAULT_TEMPERATURE,
        t_draw=DEFAULT_TEMPERATURE,
        t_away=DEFAULT_TEMPERATURE,
        n_samples=0,
        version="identity",
        is_identity=True,
    )


# ─────────────────────────────────────────────────────────────
# TEMPERATURE SCALING
# ─────────────────────────────────────────────────────────────

def _apply_temperature(p: float, temperature: float) -> float:
    """
    Skaliert eine Log-Wahrscheinlichkeit via Temperature.
    Bei T > 1: Wahrscheinlichkeiten werden weicher (Richtung 0.33)
    Bei T < 1: Wahrscheinlichkeiten werden schärfer (extremer)
    """
    # Clip p für numerische Stabilität
    p = max(1e-7, min(1 - 1e-7, p))
    log_p = math.log(p) / temperature
    return math.exp(log_p)


def calibrate_dc_probs(
    p_home: float | None,
    p_draw: float | None,
    p_away: float | None,
    cal:    CalibrationParams,
) -> tuple[float | None, float | None, float | None]:
    """
    Wendet Temperature Scaling auf DC-Wahrscheinlichkeiten an.
    Normalisiert das Ergebnis auf Summe 1.0.

    Gibt (None, None, None) wenn Inputs None.
    """
    if p_home is None or p_draw is None or p_away is None:
        return None, None, None

    if cal["is_identity"]:
        return p_home, p_draw, p_away

    raw_h = _apply_temperature(p_home, cal["t_home"])
    raw_d = _apply_temperature(p_draw, cal["t_draw"])
    raw_a = _apply_temperature(p_away, cal["t_away"])

    total = raw_h + raw_d + raw_a
    if total <= 0:
        return p_home, p_draw, p_away

    # Normalize after temperature scaling, then round
    ph = raw_h / total
    pd = raw_d / total
    pa = raw_a / total
    # Ensure exact sum=1.0 by adjusting largest component
    total_r = round(ph, 4) + round(pd, 4) + round(pa, 4)
    adj     = round(1.0 - total_r, 4)
    if abs(adj) > 0:
        if ph >= pd and ph >= pa:  ph += adj
        elif pd >= ph and pd >= pa: pd += adj
        else:                       pa += adj
    return round(ph, 4), round(pd, 4), round(pa, 4)


# ─────────────────────────────────────────────────────────────
# TEMPERATURE ESTIMATION (aus Backtest-Daten)
# ─────────────────────────────────────────────────────────────

def _estimate_temperature_for_class(
    predicted_probs: list[float],
    actual_hits:     list[bool],
) -> float:
    """
    Schätzt die Temperature T für eine Klasse (Home/Draw/Away) via
    Binary Cross-Entropy Minimierung (1D-Grid-Search, analytisch nicht trivial).

    Methode: Grid-Search über T in [0.5, 2.5] mit 50 Schritten.
    Minimiert: - mean( y * log(p_calibrated) + (1-y) * log(1-p_calibrated) )

    predicted_probs: Vorhergesagte Wahrscheinlichkeiten für diese Klasse
    actual_hits:     True wenn diese Klasse tatsächlich eintrat
    """
    if len(predicted_probs) < MIN_SAMPLES_FOR_CALIBRATION:
        return DEFAULT_TEMPERATURE

    best_t    = DEFAULT_TEMPERATURE
    best_loss = float("inf")

    t_min, t_max = TEMPERATURE_CLIP
    steps = 50

    for i in range(steps + 1):
        t = t_min + (t_max - t_min) * i / steps
        loss = 0.0

        for p, y in zip(predicted_probs, actual_hits):
            p_cal = max(1e-7, min(1 - 1e-7, _apply_temperature(p, t))
                        if t != 1.0 else p)
            loss += -(math.log(p_cal) if y else math.log(1 - p_cal))

        loss /= len(predicted_probs)
        if loss < best_loss:
            best_loss = loss
            best_t    = t

    return round(best_t, 3)


def load_calibration_from_results(
    rows:    list[dict],
    version: str = "auto",
) -> CalibrationParams:
    """
    Berechnet Kalibrierungsparameter aus model_results Rows (DC-Modell).

    rows: Liste von Dicts mit:
      prob_home, prob_draw, prob_away  (float, 0–1)
      actual_1x2                       ('H', 'D', 'A')

    Gibt identity_calibration() wenn:
      - Zu wenige Samples
      - Fehlende Wahrscheinlichkeits-Felder
    """
    # Filter: nur Rows mit vollständigen Daten
    valid = [
        r for r in rows
        if r.get("prob_home") is not None
        and r.get("prob_draw") is not None
        and r.get("prob_away") is not None
        and r.get("actual_1x2") in ("H", "D", "A")
    ]

    if len(valid) < MIN_SAMPLES_FOR_CALIBRATION:
        log.info(
            "[calibration] Zu wenige Samples (%d < %d) — Identitäts-Kalibrierung",
            len(valid), MIN_SAMPLES_FOR_CALIBRATION,
        )
        return identity_calibration()

    ph_list = [r["prob_home"] for r in valid]
    pd_list = [r["prob_draw"] for r in valid]
    pa_list = [r["prob_away"] for r in valid]

    hit_h = [r["actual_1x2"] == "H" for r in valid]
    hit_d = [r["actual_1x2"] == "D" for r in valid]
    hit_a = [r["actual_1x2"] == "A" for r in valid]

    t_home = _estimate_temperature_for_class(ph_list, hit_h)
    t_draw = _estimate_temperature_for_class(pd_list, hit_d)
    t_away = _estimate_temperature_for_class(pa_list, hit_a)

    log.info(
        "[calibration] Temperatur geschätzt aus %d Matches: "
        "T_home=%.3f  T_draw=%.3f  T_away=%.3f",
        len(valid), t_home, t_draw, t_away,
    )

    return CalibrationParams(
        t_home=t_home,
        t_draw=t_draw,
        t_away=t_away,
        n_samples=len(valid),
        version=version,
        is_identity=(t_home == 1.0 and t_draw == 1.0 and t_away == 1.0),
    )


# ─────────────────────────────────────────────────────────────
# RELIABILITY CURVE (Diagnostics)
# ─────────────────────────────────────────────────────────────

def compute_reliability_curve(
    predicted_probs: list[float],
    actual_hits:     list[bool],
    n_buckets:       int = BUCKET_COUNT,
) -> list[dict]:
    """
    Berechnet Reliability Curve für eine Klasse.
    Zeigt: für Matches mit vorhergesagter P in [0.0-0.1], [0.1-0.2], ...
    wie oft hat diese Klasse tatsächlich getroffen?

    Gibt Liste von Dicts: {bucket_mid, predicted_mean, actual_rate, n}
    Buckets mit n=0 werden übersprungen.
    """
    buckets: list[list[tuple]] = [[] for _ in range(n_buckets)]

    for p, y in zip(predicted_probs, actual_hits):
        bucket_idx = min(int(p * n_buckets), n_buckets - 1)
        buckets[bucket_idx].append((p, y))

    result = []
    for i, bucket in enumerate(buckets):
        if not bucket:
            continue
        mid          = (i + 0.5) / n_buckets
        pred_mean    = sum(p for p, _ in bucket) / len(bucket)
        actual_rate  = sum(1 for _, y in bucket if y) / len(bucket)
        result.append({
            "bucket_mid":    round(mid, 2),
            "predicted_mean": round(pred_mean, 3),
            "actual_rate":   round(actual_rate, 3),
            "n":             len(bucket),
            "calibration_gap": round(actual_rate - pred_mean, 3),
        })

    return result
