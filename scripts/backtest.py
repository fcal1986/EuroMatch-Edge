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
from datetime import datetime, timezone
from urllib.parse import urlencode

# ── Logging FIRST — must be before any optional imports that may log ─
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backtest")

import requests
from dotenv import load_dotenv
load_dotenv()

# Runtime Metadata Layer
try:
    from utils.runtime_metadata import (
        get_system_version, start_pipeline_run, finish_pipeline_run,
        register_system_release,
    )
except ImportError:
    log.warning("utils.runtime_metadata not found — versioning disabled.")
    def get_system_version(): return "dev"
    def start_pipeline_run(j, **kw): return "no-run-id"
    def finish_pipeline_run(r, **kw): pass
    def register_system_release(**kw): pass

# Cross-Season Historical Feature Engine
try:
    from utils.historical_features import (
        enrich_with_historical_features,
        prev_season_year,
    )
    _HISTORICAL_FEATURES_AVAILABLE = True
except ImportError:
    log.warning("utils.historical_features not found — using Supabase enrichment only.")
    def enrich_with_historical_features(m): return m
    def prev_season_year(s): return None
    _HISTORICAL_FEATURES_AVAILABLE = False

# Kalibrierungsschicht (P4)
try:
    from utils.calibration import (
        calibrate_dc_probs,
        load_calibration_from_results,
        identity_calibration,
        CalibrationParams,
    )
    _CALIBRATION_AVAILABLE = True
except ImportError:
    log.warning("utils.calibration not found — keine DC-Kalibrierung.")
    def calibrate_dc_probs(ph, pd, pa, cal): return ph, pd, pa  # noqa
    def load_calibration_from_results(rows, v="identity"): return None  # noqa
    def identity_calibration(): return None  # noqa
    _CALIBRATION_AVAILABLE = False

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

# ── Config from environment ───────────────────────────────────
# Use .get() with empty-string defaults so the module imports cleanly
# even when env vars are not set (e.g. unit tests, --dry-run without Supabase).
# Functions that actually need these values will fail at call time, not import.
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
SERVICE_ROLE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
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

# ── Modell-Aktivierung ───────────────────────────────────────────
# DC ist das beste Einzelmodell (tend% 48.1, avg_pts 1.16, brier 0.2065).
# Das heuristische Ensemble (tend% 36.4, avg_pts 0.96) verschlechtert DC.
# Bis ein kalibriertes Meta-Ensemble vorliegt:
#   ENSEMBLE_ENABLED = False  → ensemble läuft nicht im Backtest
#   PRIMARY_MODEL     = "dc"   → v_history + Reports nutzen DC
ENSEMBLE_ENABLED: bool = False
PRIMARY_MODEL:    str  = "dc"

# Kalibrierungsparameter für DC (P4)
# Wird in run_backtest() nach dem ersten Durchlauf befüllt.
# Erster Lauf: identity_calibration() (T=1.0 = keine Änderung).
# Folgeläufe: Temperature aus Backtest-Daten geschätzt.
DC_CALIBRATION: "CalibrationParams | None" = None

def register_model(fn):
    """Decorator: registriert ein Modell in MODELS.
    Respektiert ENSEMBLE_ENABLED — wenn False, wird das Ensemble
    nicht registriert und läuft nicht im Backtest.
    """
    if fn.__name__ == "ensemble" and not ENSEMBLE_ENABLED:
        log.info("[config] ensemble deaktiviert (ENSEMBLE_ENABLED=False) "
                 "— DC ist aktives Primary-Modell")
        return fn   # Funktion bleibt verfügbar, aber nicht in MODELS
    MODELS[fn.__name__] = fn
    return fn


@register_model
def ensemble(m: dict) -> dict:
    """
    P5 — Gated Ensemble (regelbasiert, nicht heuristisch).

    Strategie: DC ist Primary. Market und Poisson werden als Signal
    eingeblendet wenn Datenqualität das rechtfertigt.

    Gewichtungsregeln:
      Gate A — Markt komplett (3 Quoten) + frisch:
        DC 60%, Market 30%, Poisson 10%
      Gate B — Nur Home-Quote vorhanden:
        DC 70%, Market 20%, Poisson 10%
      Gate C — Kein Markt:
        DC 80%, Poisson 20%
      Gate D — Keine/wenig Historie (Aufsteiger etc.):
        DC 100% (Fallback auf Stärke-Grundlage)

    Jedes Modell wird direkt aufgerufen (keine doppelte API-Anfrage).
    ENSEMBLE_ENABLED=False → diese Funktion wird nicht in MODELS registriert.
    """
    # ── Gate-Bedingungen prüfen ───────────────────────────────────
    has_home_odds  = bool(m.get("market_home_win_odds") and
                          m.get("market_home_win_odds", 0) > 1.0)
    has_full_odds  = (has_home_odds and
                      bool(m.get("market_draw_odds") and
                           m.get("market_draw_odds", 0) > 1.0) and
                      bool(m.get("market_away_odds") and
                           m.get("market_away_odds", 0) > 1.0))
    has_history    = (m.get("home_strength") is not None and
                      m.get("away_strength") is not None)

    # ── Basis-Modelle aufrufen ────────────────────────────────────
    dc_result  = dc(m)
    poi_result = poisson(m)

    dc_ph  = dc_result.get("prob_home")  or 0.0
    dc_pd  = dc_result.get("prob_draw")  or 0.0
    dc_pa  = dc_result.get("prob_away")  or 0.0
    poi_ph = poi_result.get("prob_home") or 0.0
    poi_pd = poi_result.get("prob_draw") or 0.0
    poi_pa = poi_result.get("prob_away") or 0.0

    # ── Gate D: keine Historie → DC als alleiniger Fallback ──────
    if not has_history:
        return dc_result

    # ── Markt-Wahrscheinlichkeiten wenn vorhanden ─────────────────
    mkt_ph = mkt_pd = mkt_pa = None
    if has_home_odds:
        mkt_result = market(m)
        mkt_ph = mkt_result.get("prob_home") or 0.0
        mkt_pd = mkt_result.get("prob_draw") or 0.0
        mkt_pa = mkt_result.get("prob_away") or 0.0

    # ── Gewichte nach Gate ────────────────────────────────────────
    if has_full_odds:
        # Gate A: vollständige 3-Wege-Quoten
        w_dc, w_mkt, w_poi = 0.60, 0.30, 0.10
    elif has_home_odds:
        # Gate B: nur Home-Quote
        w_dc, w_mkt, w_poi = 0.70, 0.20, 0.10
    else:
        # Gate C: kein Markt
        w_dc, w_mkt, w_poi = 0.80, 0.00, 0.20

    # ── Gewichteter Wahrscheinlichkeitsmix ────────────────────────
    if mkt_ph is not None:
        p_home = w_dc * dc_ph + w_mkt * mkt_ph + w_poi * poi_ph
        p_draw = w_dc * dc_pd + w_mkt * mkt_pd + w_poi * poi_pd
        p_away = w_dc * dc_pa + w_mkt * mkt_pa + w_poi * poi_pa
    else:
        # Re-normalisieren auf DC+Poisson
        total_w = w_dc + w_poi
        p_home  = (w_dc * dc_ph + w_poi * poi_ph) / total_w
        p_draw  = (w_dc * dc_pd + w_poi * poi_pd) / total_w
        p_away  = (w_dc * dc_pa + w_poi * poi_pa) / total_w

    # Normalisieren auf Summe 1.0
    total = p_home + p_draw + p_away
    if total > 0:
        p_home /= total
        p_draw /= total
        p_away /= total

    # ── Winner + Confidence ───────────────────────────────────────
    if p_home >= p_draw and p_home >= p_away:
        winner, lead_p = "home", p_home
    elif p_away >= p_home and p_away >= p_draw:
        winner, lead_p = "away", p_away
    else:
        winner, lead_p = "draw", p_draw

    # Konfidenz: proportional zur Stärke des Signals
    # Gate A (vollständiger Markt) = höchste Vertrauensbasis
    conf_boost = 5 if has_full_odds else (2 if has_home_odds else 0)
    conf       = min(95, int(50 + abs(lead_p - (1/3)) * 90) + conf_boost)

    # Score-Tipp: aus DC übernehmen (bester Score-Tipp laut Backtest)
    main_tip = dc_result.get("predicted_score", "1:1")

    return {"predicted_winner": winner,
            "predicted_score":  main_tip,
            "confidence_score": conf,
            "prob_home": round(p_home, 4),
            "prob_draw": round(p_draw, 4),
            "prob_away": round(p_away, 4)}


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
    _ph, _pd, _pa = probs_from_score_grid(grid)
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": conf,
            "prob_home": _ph, "prob_draw": _pd, "prob_away": _pa}


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
    _ph, _pd, _pa = probs_from_score_grid(grid)
    # ── Kalibrierung anwenden (P4) ────────────────────────────────
    if DC_CALIBRATION is not None and _CALIBRATION_AVAILABLE:
        _ph, _pd, _pa = calibrate_dc_probs(_ph, _pd, _pa, DC_CALIBRATION)
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": conf,
            "prob_home": _ph, "prob_draw": _pd, "prob_away": _pa}


@register_model
def elo(m: dict) -> dict:
    diff   = strength_diff(m)
    home_p = 1 / (1 + 10 ** (-(diff * 4 + 0.1)))
    winner = "home" if home_p > 0.58 else ("away" if home_p < 0.38 else "draw")
    conf   = int(45 + abs(home_p - 0.5) * 90)
    hg     = 1.0 + diff * 1.8
    ag     = 0.9 - diff * 0.9
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    _ph, _pd, _pa = probs_from_home_probability(home_p)
    return {"predicted_winner": winner, "predicted_score": main_tip,
            "confidence_score": min(99, conf),
            "prob_home": _ph, "prob_draw": _pd, "prob_away": _pa}


@register_model
def market(m: dict) -> dict:
    """
    Market-Modell: nutzt Wettquoten als Wahrscheinlichkeitssignal.

    ARCHITEKTUR-ENTSCHEIDUNG:
      Nur echte Quoten werden genutzt — kein Fallback auf fake Defaults.
      Wenn keine Quote vorhanden: Modell liefert Fallback auf DC-ähnliche
      Stärke+Form-Basis, aber mit explizitem market_available=False Flag.
      Begründung: quote=2.0 als Default erzeugte ~97% fake "Remis"-Signale.

    VERFÜGBARE DATEN (football-data.org Free Tier):
      - market_home_win_odds: oft vorhanden (aber nicht immer)
      - market_draw_odds:     NICHT im Schema — muss noch ergänzt werden
      - market_away_odds:     NICHT im Schema — muss noch ergänzt werden

    ÜBERRUND-ENTFERNUNG (wenn nur home_odds verfügbar):
      Wir schätzen draw_p und away_p via historische Liga-Basisraten:
        draw_base  = 0.26  (Europäischer Top-Liga-Durchschnitt)
        away_base  = 0.27
        home_base  = 0.47 → normiert auf Summe 1.0
      home_implied skaliert draw/away proportional.
    """
    raw_home_odds = m.get("market_home_win_odds") or m.get("market_signal_home")

    diff = strength_diff(m)
    hg   = 0.9 + diff * 1.2
    ag   = 0.8 - diff * 0.6
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"

    if not raw_home_odds or raw_home_odds <= 1.0:
        # Keine validen Quoten → Modell fällt auf Stärke-Diff zurück
        # (identisch mit DC-Fallback, aber transparent als "kein Marktsignal" markiert)
        home_p = min(0.88, max(0.12, 0.47 + diff * 0.4))
        _ph, _pd, _pa = probs_from_home_probability(home_p)
        return {"predicted_winner": "home" if home_p > 0.55 else ("away" if home_p < 0.40 else "draw"),
                "predicted_score":  main_tip,
                "confidence_score": 20,   # niedrig: kein Marktsignal
                "prob_home": _ph, "prob_draw": _pd, "prob_away": _pa,
                "market_available": False}

    # ── Echte Quote vorhanden ────────────────────────────────────────────
    # Schritt 1: Rohe implizite Wahrscheinlichkeit (inkl. Overround ~5-8%)
    raw_p_home = 1.0 / raw_home_odds

    # Schritt 2: Overround entfernen
    # Ohne Draw/Away-Quoten schätzen wir den Overround als ~7% (typisch)
    # und skalieren die Home-P entsprechend
    draw_odds  = m.get("market_draw_odds")      # None wenn nicht im Schema
    away_odds  = m.get("market_away_odds")       # None wenn nicht im Schema

    if draw_odds and away_odds and draw_odds > 1.0 and away_odds > 1.0:
        # ── Vollständige 3-Wege-Quoten: saubere Overround-Entfernung ────
        raw_p_draw = 1.0 / draw_odds
        raw_p_away = 1.0 / away_odds
        overround  = raw_p_home + raw_p_draw + raw_p_away   # > 1.0
        p_home = raw_p_home / overround
        p_draw = raw_p_draw / overround
        p_away = raw_p_away / overround
    else:
        # ── Nur Home-Quote: Schätzung via historische Basisraten ─────────
        # Entferne approximierten Overround von 7%
        p_home = min(0.90, raw_p_home / 1.07)
        rest   = 1.0 - p_home
        # Historisches Verhältnis Draw:Away ≈ 49:51 in Top-Ligen
        p_draw = rest * 0.490
        p_away = rest * 0.510

    # Normalisieren auf Summe 1.0 (Rundungsfehler absichern)
    total  = p_home + p_draw + p_away
    p_home /= total
    p_draw /= total
    p_away /= total

    # Wahrscheinlichkeiten auf [0.04, 0.92] clampen
    p_home = min(0.92, max(0.04, p_home))
    p_draw = min(0.92, max(0.04, p_draw))
    p_away = min(0.92, max(0.04, p_away))
    # Nochmals normalisieren nach Clamping
    total  = p_home + p_draw + p_away
    p_home /= total
    p_draw /= total
    p_away /= total

    # Sieger + Konfidenz
    if p_home >= p_draw and p_home >= p_away:
        winner, lead_p = "home", p_home
    elif p_away >= p_home and p_away >= p_draw:
        winner, lead_p = "away", p_away
    else:
        winner, lead_p = "draw", p_draw

    # Konfidenz: stark wenn Markt klare Meinung hat + vollständige Quoten
    has_full_odds = bool(draw_odds and away_odds)
    base_conf     = int(min(90, 40 + lead_p * 60))
    conf          = base_conf if has_full_odds else max(35, base_conf - 15)

    return {"predicted_winner": winner,
            "predicted_score":  main_tip,
            "confidence_score": conf,
            "prob_home": round(p_home, 4),
            "prob_draw": round(p_draw, 4),
            "prob_away": round(p_away, 4),
            "market_available": True}


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
    Load DISPLAY metadata from Supabase `matches` (home_team, away_team,
    league_abbr, flag, market_home_win_odds).

    Architectural note:
      - Feature computation (strength, form, win_probability) is handled
        by historical_features.py from football-data.org data only.
      - This function provides ONLY display/context enrichment.
      - If Supabase is unavailable, returns an empty dict and logs a warning.
        The backtest will proceed with team names from football-data.org.

    Returns dict: external_id → row  (e.g. "football_data_12345" → {...})
    """
    if not SUPABASE_URL or not SERVICE_ROLE_KEY:
        log.warning(
            "Supabase config missing (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY) — "
            "display enrichment disabled; team names from football-data.org only."
        )
        return {}

    params = [
        # Display + market columns only.
        # Strength/form are computed by historical_features.py, not from Supabase.
        ("select", "external_id,competition_code,league_abbr,flag,"
                   "home_team,away_team,"
                   "market_home_win_odds,"
                   "market_draw_odds,market_away_odds"),
        ("limit",  "5000"),
    ]
    if competition_code:
        params.append(("competition_code", f"eq.{competition_code}"))

    try:
        rows = _supabase_get("matches", params)
    except RuntimeError as exc:
        log.warning("Supabase enrichment failed — proceeding without display data.\n  %s", exc)
        return {}

    log.info("Supabase display enrichment: %d rows loaded", len(rows))
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

            # Parse team IDs and kickoff for historical feature engine
            _home_id = raw_home.get("id")
            _away_id = raw_away.get("id")
            _kickoff_dt: datetime | None = None
            _utc_date = raw.get("utcDate")
            if _utc_date:
                try:
                    _kickoff_dt = datetime.fromisoformat(
                        _utc_date.replace("Z", "+00:00")
                    )
                except ValueError:
                    _kickoff_dt = None

            all_matches.append({
                # ── Identity ──────────────────────────────────────
                # Use external_id as match_id so it matches the
                # external_id stored in the Supabase matches table.
                "id":                external_id,
                "external_id":       external_id,

                # ── Internal keys for historical feature engine ───
                # Prefixed with _ so they are never written to Supabase.
                "_home_team_id":     _home_id,
                "_away_team_id":     _away_id,
                "_kickoff_dt":       _kickoff_dt,

                # ── Actual result (from football-data.org) ────────
                "actual_home_goals": int(home_goals),
                "actual_away_goals": int(away_goals),

                # ── Display metadata (from Supabase, falls back to API) ─
                "home_team":         home_name,
                "away_team":         away_name,
                "flag":              enrich.get("flag", ""),
                "league_abbr":       enrich.get("league_abbr", comp_code),

                # ── Model inputs ─────────────────────────────────────
                # strength/form/win_probability are computed by
                # enrich_with_historical_features() from football-data history.
                # Initialised to None/[] here as placeholders.
                "home_strength":        None,
                "away_strength":        None,
                "form_home":            [],
                "form_away":            [],
                "market_home_win_odds": enrich.get("market_home_win_odds"),
                "market_draw_odds":      enrich.get("market_draw_odds"),
                "market_away_odds":      enrich.get("market_away_odds"),
                "win_probability":      50,
                "feature_src":          "pending",

                # ── Context ───────────────────────────────────────
                "competition_code":  enrich.get("competition_code", comp_code),
                "season":            season_year or "",
                "kickoff_at":        _utc_date,
            })

        time.sleep(REQUEST_DELAY_S)

    return all_matches


def fetch_finished_matches(
    season: str | None = None,
    competition: str | None = None,
) -> list:
    """
    Public entry point: load finished matches with actual results.

    Cross-Season Strategy (historical_self_contained_v2):
      1. Fetches current season (season) from football-data.org
      2. Fetches previous season (season-1) for historical context
      3. Merges both into one list sorted by kickoff_at
      4. Calls enrich_with_historical_features() to compute
         strength/form/win_probability using strict temporal ordering
         (no data leakage: only matches BEFORE each match are used)
      5. Returns only current-season matches (for evaluation)
         but uses both seasons as feature history
    """
    enrichment   = fetch_supabase_enrichment(competition)

    # Parse season year for prev-season lookup
    season_year: str | None = None
    if season:
        year_part = season.split("/")[0].strip()
        if len(year_part) == 2:
            year_part = f"20{year_part}"
        season_year = year_part if year_part.isdigit() else None

    # Fetch current season first (needed to infer year when --season not passed)
    current_matches = fetch_finished_from_api(
        competition_code=competition,
        season=season,
        enrichment=enrichment,
    )
    log.info("Current season matches: %d", len(current_matches))

    # If season_year was not explicitly given, infer it from the actual match dates.
    # football-data.org returns the current season when no ?season= param is given.
    if season_year is None and current_matches:
        inferred_dates = [
            m.get("_kickoff_dt") for m in current_matches
            if m.get("_kickoff_dt") is not None
        ]
        if inferred_dates:
            # Most matches are mid-season — take the most common year
            years = [d.year for d in inferred_dates]
            season_year = str(max(set(years), key=years.count))
            log.info("Season year inferred from match dates: %s", season_year)

    prev_year = prev_season_year(season_year)

    # Fetch previous season for cross-season history
    prev_matches: list[dict] = []
    if prev_year and _HISTORICAL_FEATURES_AVAILABLE:
        log.info("Fetching previous season %s for cross-season history...", prev_year)
        try:
            prev_matches = fetch_finished_from_api(
                competition_code=competition,
                season=prev_year,
                enrichment=enrichment,
            )
            log.info("Previous season matches: %d", len(prev_matches))
        except Exception as exc:
            log.warning("Could not fetch previous season %s: %s — proceeding without", prev_year, exc)
            prev_matches = []
    else:
        if not _HISTORICAL_FEATURES_AVAILABLE:
            log.info("historical_features not available — skipping cross-season fetch")
        elif not prev_year:
            log.info("No season specified — skipping cross-season fetch")

    if not _HISTORICAL_FEATURES_AVAILABLE:
        return current_matches

    # Merge and sort all matches by kickoff_at for history index
    all_for_history = current_matches + prev_matches
    all_for_history.sort(
        key=lambda m: m.get("_kickoff_dt") or datetime.min.replace(tzinfo=timezone.utc)
    )

    # Enrich with cross-season historical features
    # This mutates each match dict in-place
    enrich_with_historical_features(all_for_history)

    # Return only current-season matches for evaluation
    # (prev_matches were only used as history context)
    current_ids = {m["id"] for m in current_matches}
    result      = [m for m in all_for_history if m["id"] in current_ids]

    log.info("feature_src : historical_self_contained_v2")
    log.info("Matches for evaluation: %d (current season)", len(result))

    # Coverage is logged by enrich_with_historical_features() above.
    return result



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
    log.info("  competition      : %s", competition or "all active")
    log.info("  season           : %s", season or "latest")
    log.info("  dry_run          : %s", dry_run)
    log.info("  primary_model    : %s", PRIMARY_MODEL)
    log.info("  ensemble_enabled : %s", ENSEMBLE_ENABLED)
    log.info("  active_models    : %s", list(MODELS.keys()))

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
    log.info("Fetched %d finished matches for evaluation", len(matches))

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

            # ── Extended evaluation metrics ───────────────────
            metrics = compute_eval_metrics(
                predicted_score = result["predicted_score"],
                actual_home     = actual_h,
                actual_away     = actual_a,
                prob_home       = result.get("prob_home"),
                prob_draw       = result.get("prob_draw"),
                prob_away       = result.get("prob_away"),
            )

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
                # Bewertung (bestehend)
                "points":             pts,
                "outcome_label":      outcome,
                # Evaluation Metrics (neu)
                "predicted_1x2":      metrics.get("predicted_1x2"),
                "actual_1x2":         metrics.get("actual_1x2"),
                "is_exact_hit":       metrics.get("is_exact_hit"),
                "is_tendency_hit":    metrics.get("is_tendency_hit"),
                "score_error_abs":    metrics.get("score_error_abs"),
                "goal_diff_error":    metrics.get("goal_diff_error"),
                "brier_score_1x2":    metrics.get("brier_score_1x2"),
                "log_loss_1x2":       metrics.get("log_loss_1x2"),
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
        # Strip internal keys before JSON print
        sample_clean = [
            {k: v for k, v in r.items() if not k.startswith("_")}
            for r in rows[:3]
        ]
        print(json.dumps(sample_clean, indent=2, default=str))

        # Feature coverage stats
        n_tot   = len(matches)
        s_ok    = sum(1 for m in matches if m.get("home_strength") is not None)
        f_ok    = sum(1 for m in matches if m.get("form_home"))
        cs_used = sum(1 for m in matches if m.get("_n_home_matches", 0) > 0
                      or m.get("_n_away_matches", 0) > 0)
        if n_tot > 0:
            print(f"\n── Feature Coverage (dry run) ────────────────")
            print(f"  feature_src     : {matches[0].get('feature_src', '?')  if matches else '?'}")    
            print(f"  strength ok     : {s_ok}/{n_tot} ({s_ok/n_tot*100:.0f}%)")
            print(f"  form ok         : {f_ok}/{n_tot} ({f_ok/n_tot*100:.0f}%)")
            print(f"  historical data : {cs_used}/{n_tot} matches had history")
            # Sample with history
            for m in matches[:5]:
                hn   = m.get('_n_home_matches', 0)
                an   = m.get('_n_away_matches', 0)
                hs   = f"{m.get('home_strength'):.1f}" if m.get('home_strength') is not None else 'null'
                as_  = f"{m.get('away_strength'):.1f}" if m.get('away_strength') is not None else 'null'
                print(f"  sample  : {m.get('home_team','?')[:15]} vs {m.get('away_team','?')[:15]}")
                print(f"    home_strength       : {hs}")
                print(f"    away_strength       : {as_}")
                print(f"    history_home_matches: {hn}")
                print(f"    history_away_matches: {an}")
                print(f"    form_home           : {m.get('form_home',[])}")
                print(f"    form_away           : {m.get('form_away',[])}")
                print(f"    win_probability     : {m.get('win_probability', 50)}")
                print()


        # ── Per-model summary (dry run) ───────────────────
        from collections import defaultdict
        by_model: dict[str, list] = defaultdict(list)
        for r in rows:
            by_model[r["model_key"]].append(r)

        print("\n── Dry-Run Model Summary ─────────────────────")
        hdr = f"  {'model':<12s}  {'n':>4}  {'avg_pts':>7}  {'tend%':>6}  {'exact%':>6}  {'avg_err':>7}  {'avg_brier':>9}"
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for mk, rlist in sorted(by_model.items()):
            n          = len(rlist)
            avg_pts    = sum(r["points"] for r in rlist) / n if n else 0
            tend_n     = [r for r in rlist if r.get("is_tendency_hit") is not None]
            tend_pct   = (sum(1 for r in tend_n if r["is_tendency_hit"]) /
                          len(tend_n) * 100) if tend_n else float("nan")
            exact_n    = [r for r in rlist if r.get("is_exact_hit") is not None]
            exact_pct  = (sum(1 for r in exact_n if r["is_exact_hit"]) /
                          len(exact_n) * 100) if exact_n else float("nan")
            err_vals   = [r["score_error_abs"] for r in rlist
                          if r.get("score_error_abs") is not None]
            avg_err    = sum(err_vals) / len(err_vals) if err_vals else float("nan")
            brier_vals = [r["brier_score_1x2"] for r in rlist
                          if r.get("brier_score_1x2") is not None]
            avg_brier  = sum(brier_vals) / len(brier_vals) if brier_vals else None
            brier_str  = f"{avg_brier:9.4f}" if avg_brier is not None else "      n/a"
            print(
                f"  {mk:<12s}  {n:>4d}  {avg_pts:7.2f}  "
                f"{tend_pct:6.1f}  {exact_pct:6.1f}  {avg_err:7.2f}  {brier_str}"
            )
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
