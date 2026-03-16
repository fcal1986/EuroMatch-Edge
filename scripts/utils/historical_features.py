"""
utils/historical_features.py
─────────────────────────────
EuroMatch Edge — Cross-Season Historical Feature Engine
feature_src: historical_self_contained_v2

Berechnet Stärke, Form und Win-Wahrscheinlichkeit aus historischen Matches
über Saisongrenzen hinweg (current + previous season), ohne Data Leakage.

Regeln:
  - Nur Matches BEFORE current_match.kickoff_at werden berücksichtigt
  - Strength Window: letzte 10 Matches (gesamt), 6 venue-spezifische
  - Form Window:     letzte  5 Matches
  - Fallback (< MIN_HISTORY_MATCHES):  strength=None, form=[], win_prob=50
  - Min History:     3 Matches

P3-Erweiterung (Home/Away getrennt):
  - home_strength: gemischt aus Gesamt-Stärke (60%) + Heim-spezifisch (40%)
  - away_strength: gemischt aus Gesamt-Stärke (60%) + Auswärts-spezifisch (40%)
  - Heimvorteil ist real: Top-Ligen ~58% Heimsiege, ~27% Remis
  - Venue-Split braucht MIN_VENUE_HISTORY = 2 spez. Matches

Verwendung:
    from utils.historical_features import enrich_with_historical_features
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TypedDict

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

STRENGTH_WINDOW     = 10    # letzte N Matches für Gesamt-Stärke
VENUE_WINDOW        = 6     # letzte N Heim/Auswärts-Matches für venue-spezifische Stärke
FORM_WINDOW         = 5     # letzte N Matches für Form
MIN_HISTORY         = 3     # Minimum Matches für valide Gesamt-Stärke
MIN_VENUE_HISTORY   = 2     # Minimum venue-spez. Matches für venue Blend
STRENGTH_MIN        = 0.0
STRENGTH_MAX        = 100.0

# Gewichtung Stärke-Komponenten
STRENGTH_W_PPG      = 0.7   # points per game
STRENGTH_W_GDIFF    = 0.3   # goal diff per game

# Blending: Gesamt-Stärke vs. Venue-spezifische Stärke
# 60% Gesamt (stabiler, mehr Matches) + 40% Venue (spezifischer aber kleiner Sample)
VENUE_BLEND_GENERAL = 0.60
VENUE_BLEND_VENUE   = 0.40

# Heimvorteil-Anpassung wenn venue-Stärke nicht verfügbar
# Basierend auf europäischem Top-Liga-Durchschnitt: ~+5 Punkte Heimvorteil
HOME_ADVANTAGE_BONUS = 3.0   # addiert auf home_strength wenn keine Venue-Daten
AWAY_DISADVANTAGE    = -3.0  # subtrahiert von away_strength wenn keine Venue-Daten

# Normalisierung
PPG_MAX             = 3.0
GDIFF_CLIP          = 3.0


# ─────────────────────────────────────────────────────────────
# TYPE
# ─────────────────────────────────────────────────────────────

class TeamFeatures(TypedDict):
    strength:       float | None   # 0–100 Gesamt
    home_strength:  float | None   # 0–100 Heim-adjustiert
    away_strength:  float | None   # 0–100 Auswärts-adjustiert
    form:           list[str]      # ["W","D","L",...] neueste zuerst
    n_matches:      int
    n_home_matches: int
    n_away_matches: int


# ─────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────

def _parse_kickoff(dt_str: str | None) -> datetime | None:
    """Parst utcDate-String von football-data.org zu UTC-datetime."""
    if not dt_str:
        return None
    try:
        s = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


def _match_result_for_team(match: dict, team_id: int) -> str | None:
    """Gibt 'W', 'D', oder 'L' zurück aus Sicht von team_id."""
    home_id = match.get("_home_team_id")
    away_id = match.get("_away_team_id")
    home_g  = match.get("actual_home_goals")
    away_g  = match.get("actual_away_goals")

    if home_g is None or away_g is None:
        return None

    is_home = (team_id == home_id)
    is_away = (team_id == away_id)

    if not is_home and not is_away:
        return None

    if home_g > away_g:
        return "W" if is_home else "L"
    if away_g > home_g:
        return "W" if is_away else "L"
    return "D"


def _goals_for_team(match: dict, team_id: int) -> tuple[int, int] | None:
    """Gibt (scored, conceded) aus Sicht von team_id zurück."""
    home_id = match.get("_home_team_id")
    away_id = match.get("_away_team_id")
    home_g  = match.get("actual_home_goals")
    away_g  = match.get("actual_away_goals")

    if home_g is None or away_g is None:
        return None

    if team_id == home_id:
        return (int(home_g), int(away_g))
    if team_id == away_id:
        return (int(away_g), int(home_g))
    return None


def _is_home_match(match: dict, team_id: int) -> bool:
    """True wenn team_id in diesem Match Heimteam war."""
    return match.get("_home_team_id") == team_id


# ─────────────────────────────────────────────────────────────
# STRENGTH COMPUTATION
# ─────────────────────────────────────────────────────────────

def _strength_from_matches(
    matches: list[dict],
    team_id: int,
    window: int,
) -> float | None:
    """
    Berechnet rohe Stärke [0–100] aus den letzten `window` Matches.
    matches: sortiert nach kickoff DESC.
    Gibt None zurück wenn weniger als MIN_HISTORY Matches vorhanden.
    """
    w = matches[:window]
    if len(w) < MIN_HISTORY:
        return None

    total_pts   = 0.0
    total_gdiff = 0.0

    for m in w:
        result = _match_result_for_team(m, team_id)
        goals  = _goals_for_team(m, team_id)

        total_pts += {"W": 3, "D": 1, "L": 0}.get(result or "", 0)
        if goals:
            total_gdiff += goals[0] - goals[1]

    n          = len(w)
    ppg        = total_pts / n
    gdiff_pg   = max(-GDIFF_CLIP, min(GDIFF_CLIP, total_gdiff / n))

    ppg_norm   = ppg / PPG_MAX
    gdiff_norm = (gdiff_pg + GDIFF_CLIP) / (2 * GDIFF_CLIP)

    raw = STRENGTH_W_PPG * ppg_norm + STRENGTH_W_GDIFF * gdiff_norm
    return max(STRENGTH_MIN, min(STRENGTH_MAX, round(raw * 100.0, 1)))


def _compute_general_strength(
    team_matches: list[dict],
    team_id: int,
) -> float | None:
    """Gesamt-Stärke aus letzten STRENGTH_WINDOW Matches (alle Venues)."""
    return _strength_from_matches(team_matches, team_id, STRENGTH_WINDOW)


def _compute_venue_strength(
    team_matches: list[dict],
    team_id: int,
    as_home: bool,
) -> float | None:
    """
    Venue-spezifische Stärke aus letzten VENUE_WINDOW Heim- oder Auswärts-Matches.

    as_home=True  → nur Spiele in denen team_id Heimteam war
    as_home=False → nur Spiele in denen team_id Auswärtsteam war

    Gibt None wenn weniger als MIN_VENUE_HISTORY venue-spezifische Matches.
    """
    venue_matches = [
        m for m in team_matches
        if _is_home_match(m, team_id) == as_home
    ]

    if len(venue_matches) < MIN_VENUE_HISTORY:
        return None

    return _strength_from_matches(venue_matches, team_id, VENUE_WINDOW)


def _blend_strengths(
    general: float | None,
    venue:   float | None,
    is_home: bool,
) -> float | None:
    """
    Mischt Gesamt-Stärke und Venue-spezifische Stärke.

    Wenn venue verfügbar:
      result = 60% * general + 40% * venue

    Wenn venue NICHT verfügbar:
      result = general + HOME_ADVANTAGE_BONUS (Heim) oder + AWAY_DISADVANTAGE (Auswärts)
      Begründung: Heimvorteil ist real (~+5% Siegwahrscheinlichkeit) auch ohne
      ausreichend venue-spezifische Daten, daher harte Korrektur statt None.

    Gibt None wenn general auch None.
    """
    if general is None:
        return None

    if venue is not None:
        blended = VENUE_BLEND_GENERAL * general + VENUE_BLEND_VENUE * venue
    else:
        adjustment = HOME_ADVANTAGE_BONUS if is_home else AWAY_DISADVANTAGE
        blended = general + adjustment

    return max(STRENGTH_MIN, min(STRENGTH_MAX, round(blended, 1)))


# ─────────────────────────────────────────────────────────────
# FORM COMPUTATION
# ─────────────────────────────────────────────────────────────

def _compute_form(
    team_matches: list[dict],
    team_id: int,
) -> list[str]:
    """
    Gibt Form-Liste der letzten FORM_WINDOW Matches zurück.
    team_matches: sortiert nach kickoff DESC (neueste zuerst).
    Ergebnis: ["W","D","L",...] mit max 5 Einträgen.
    """
    form = []
    for m in team_matches[:FORM_WINDOW]:
        result = _match_result_for_team(m, team_id)
        if result:
            form.append(result)
    return form


# ─────────────────────────────────────────────────────────────
# BUILD TEAM HISTORY INDEX
# ─────────────────────────────────────────────────────────────

def build_team_history_index(
    all_matches: list[dict],
) -> dict[int, list[dict]]:
    """
    Baut einen Index: team_id → [matches sorted by kickoff DESC].

    Jedes Match muss folgende Felder haben:
      _home_team_id, _away_team_id, _kickoff_dt (datetime),
      actual_home_goals, actual_away_goals

    Nur Matches mit validen Ergebnissen werden indexiert.
    """
    idx: dict[int, list[dict]] = {}

    for m in all_matches:
        home_id = m.get("_home_team_id")
        away_id = m.get("_away_team_id")
        if not home_id or not away_id:
            continue
        if m.get("actual_home_goals") is None or m.get("actual_away_goals") is None:
            continue

        for tid in (home_id, away_id):
            if tid not in idx:
                idx[tid] = []
            idx[tid].append(m)

    # Sort each team's matches by kickoff DESC
    for tid in idx:
        idx[tid].sort(
            key=lambda x: x.get("_kickoff_dt") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

    return idx


# ─────────────────────────────────────────────────────────────
# COMPUTE FEATURES FOR ONE MATCH
# ─────────────────────────────────────────────────────────────

def compute_features_for_match(
    match:       dict,
    history_idx: dict[int, list[dict]],
) -> dict:
    """
    Berechnet home_strength, away_strength, form_home, form_away, win_probability
    für ein einzelnes Match. Nutzt nur Matches STRICTLY BEFORE kickoff_at.

    P3-Erweiterung: home_strength und away_strength sind jetzt venue-adjustiert:
      - home_strength = 60% Gesamt + 40% Heim-spezifisch  (+3 Bonus wenn keine Venue-Daten)
      - away_strength = 60% Gesamt + 40% Auswärts-spezifisch (-3 Penalty wenn keine Daten)

    Gibt ein Dict mit den Feature-Feldern zurück.
    """
    home_id    = match.get("_home_team_id")
    away_id    = match.get("_away_team_id")
    kickoff_dt = match.get("_kickoff_dt")

    # ── Filter: nur Matches VOR diesem Match ─────────────────
    def prior_matches(team_id: int) -> list[dict]:
        if team_id is None:
            return []
        all_tm = history_idx.get(team_id, [])
        if kickoff_dt is None:
            return all_tm
        return [m for m in all_tm
                if m.get("_kickoff_dt") is not None
                and m["_kickoff_dt"] < kickoff_dt]

    home_matches = prior_matches(home_id)
    away_matches = prior_matches(away_id)

    # ── Gesamt-Stärke ────────────────────────────────────────
    general_home = _compute_general_strength(home_matches, home_id) if home_id else None
    general_away = _compute_general_strength(away_matches, away_id) if away_id else None

    # ── Venue-spezifische Stärke (P3) ────────────────────────
    # Heimteam: wie stark ist es wenn es ZU HAUSE spielt?
    venue_home = _compute_venue_strength(home_matches, home_id, as_home=True) if home_id else None
    # Auswärtsteam: wie stark ist es wenn es AUSWÄRTS spielt?
    venue_away = _compute_venue_strength(away_matches, away_id, as_home=False) if away_id else None

    # ── Venue-adjustierte Stärken (Blend) ────────────────────
    home_strength = _blend_strengths(general_home, venue_home, is_home=True)
    away_strength = _blend_strengths(general_away, venue_away, is_home=False)

    # ── Form ─────────────────────────────────────────────────
    form_home = _compute_form(home_matches, home_id) if home_id else []
    form_away = _compute_form(away_matches, away_id) if away_id else []

    # ── Win probability ───────────────────────────────────────
    if home_strength is not None and away_strength is not None:
        diff            = home_strength - away_strength
        win_probability = max(5, min(95, round(50 + diff * 0.6)))
    else:
        win_probability = 50

    # ── Venue-spezifische Heim-Matches zählen ─────────────────
    n_home_venue = sum(1 for m in home_matches if _is_home_match(m, home_id or 0))
    n_away_venue = sum(1 for m in away_matches if not _is_home_match(m, away_id or 0))

    return {
        "home_strength":     home_strength,
        "away_strength":     away_strength,
        "form_home":         form_home,
        "form_away":         form_away,
        "win_probability":   win_probability,
        # Diagnostics (nur für Logging, nicht in Supabase)
        "_n_home_matches":   len(home_matches),
        "_n_away_matches":   len(away_matches),
        "_n_home_venue":     n_home_venue,    # Anzahl Heim-spez. Matches
        "_n_away_venue":     n_away_venue,    # Anzahl Auswärts-spez. Matches
        "_venue_home_used":  venue_home is not None,
        "_venue_away_used":  venue_away is not None,
    }


# ─────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────

def enrich_with_historical_features(
    matches: list[dict],
) -> list[dict]:
    """
    Reichert eine Match-Liste mit cross-season historischen Features an.
    P3-erweitert: home/away_strength sind venue-adjustiert.

    Erwartet pro Match:
      _home_team_id, _away_team_id, _kickoff_dt, actual_home/away_goals

    Schreibt:
      home_strength, away_strength, form_home, form_away, win_probability
    """
    history_idx = build_team_history_index(matches)

    n_total         = len(matches)
    n_strength_ok   = 0
    n_form_ok       = 0
    n_fallback      = 0
    n_cross_season  = 0
    n_venue_used    = 0   # Matches wo venue-spez. Stärke genutzt wurde

    for m in matches:
        feats = compute_features_for_match(m, history_idx)

        # Counters
        if feats["home_strength"] is not None and feats["away_strength"] is not None:
            n_strength_ok += 1
        else:
            n_fallback += 1

        if feats["form_home"] or feats["form_away"]:
            n_form_ok += 1

        if feats.get("_venue_home_used") or feats.get("_venue_away_used"):
            n_venue_used += 1

        # Cross-season detection (calendar year approximation — diagnostic only)
        kickoff_dt     = m.get("_kickoff_dt")
        kickoff_season = (kickoff_dt or datetime.min.replace(tzinfo=timezone.utc)).year
        home_id        = m.get("_home_team_id")
        away_id        = m.get("_away_team_id")

        def _prior(tid):
            if tid is None:
                return []
            all_tm = history_idx.get(tid, [])
            if kickoff_dt is None:
                return all_tm
            return [x for x in all_tm
                    if x.get("_kickoff_dt") is not None
                    and x["_kickoff_dt"] < kickoff_dt]

        prior_home = _prior(home_id)
        prior_away = _prior(away_id)

        # NOTE: calendar year comparison used as approximation for cross-season history.
        # Football seasons span two calendar years (e.g. 2023/24 runs Aug 2023–May 2024).
        # This is intentionally simple — it is only used for the diagnostic counter below.
        cross = any(
            x.get("_kickoff_dt") and x["_kickoff_dt"].year < kickoff_season
            for x in prior_home[:STRENGTH_WINDOW]
        ) or any(
            x.get("_kickoff_dt") and x["_kickoff_dt"].year < kickoff_season
            for x in prior_away[:STRENGTH_WINDOW]
        )
        if cross:
            n_cross_season += 1

        # Inject features into match dict
        m["home_strength"]    = feats["home_strength"]
        m["away_strength"]    = feats["away_strength"]
        m["form_home"]        = feats["form_home"]
        m["form_away"]        = feats["form_away"]
        m["win_probability"]  = feats["win_probability"]
        m["feature_src"]      = "historical_self_contained_v2"
        # Diagnostics
        m["_n_home_matches"]  = feats["_n_home_matches"]
        m["_n_away_matches"]  = feats["_n_away_matches"]
        m["_n_home_venue"]    = feats["_n_home_venue"]
        m["_n_away_venue"]    = feats["_n_away_venue"]
        m["_venue_home_used"] = feats["_venue_home_used"]
        m["_venue_away_used"] = feats["_venue_away_used"]

    # ── Coverage logging (single authoritative source) ────────
    if n_total > 0:
        log.info(
            "Historical feature coverage — "
            "strength: %d/%d (%.1f%%)  form: %d/%d (%.1f%%)  fallbacks: %d",
            n_strength_ok, n_total, n_strength_ok / n_total * 100,
            n_form_ok,     n_total, n_form_ok     / n_total * 100,
            n_fallback,
        )
        log.info(
            "Venue-specific strength used: %d/%d (%.1f%%)  "
            "cross-season matches: %d/%d",  # calendar year approximation
            n_venue_used,   n_total, n_venue_used  / n_total * 100,
            n_cross_season, n_total,
        )

    return matches


# ─────────────────────────────────────────────────────────────
# PREVIOUS SEASON YEAR HELPER
# ─────────────────────────────────────────────────────────────

def prev_season_year(season_year: str | None) -> str | None:
    """
    Gibt das Vorjahres-Saison-Jahr zurück.
    "2024" → "2023", None → None
    """
    if not season_year or not season_year.isdigit():
        return None
    return str(int(season_year) - 1)
