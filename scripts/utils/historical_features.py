"""
utils/historical_features.py
─────────────────────────────
EuroMatch Edge — Cross-Season Historical Feature Engine
feature_src: historical_self_contained_v2

Berechnet Stärke, Form und Win-Wahrscheinlichkeit aus historischen Matches
über Saisongrenzen hinweg (current + previous season), ohne Data Leakage.

Regeln:
  - Nur Matches BEFORE current_match.kickoff_at werden berücksichtigt
  - Strength Window: letzte 10 Matches
  - Form Window:     letzte  5 Matches
  - Fallback (< MIN_HISTORY_MATCHES):  strength=None, form=[], win_prob=50
  - Min History:     3 Matches

Verwendung:
    from utils.historical_features import enrich_with_historical_features

    matches = fetch_all_matches_two_seasons(...)    # bereits sortiert nach utcDate
    enriched = enrich_with_historical_features(matches)
    # enriched[i] hat home_strength, away_strength, form_home, form_away, win_probability
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TypedDict

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

STRENGTH_WINDOW    = 10    # letzte N Matches für Stärkeberechnung
FORM_WINDOW        = 5     # letzte N Matches für Form
MIN_HISTORY        = 3     # Minimum Matches für valide Features
STRENGTH_MIN       = 0.0   # Skalierungsbereich
STRENGTH_MAX       = 100.0

# Gewichtung Stärke-Komponenten
STRENGTH_W_PPG     = 0.7   # points per game
STRENGTH_W_GDIFF   = 0.3   # goal diff per game

# Für Normalisierung: max ppg = 3.0, max |goal_diff| ≈ 3.0 pro Spiel
PPG_MAX            = 3.0
GDIFF_CLIP         = 3.0   # Clamp goal_diff_per_game auf [-3, +3]


# ─────────────────────────────────────────────────────────────
# TYPE
# ─────────────────────────────────────────────────────────────

class TeamFeatures(TypedDict):
    strength:    float | None   # 0–100
    form:        list[str]      # ["W","D","L",...] neueste zuerst
    n_matches:   int            # wie viele Matches gefunden


# ─────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────

def _parse_kickoff(dt_str: str | None) -> datetime | None:
    """Parst utcDate-String von football-data.org zu UTC-datetime."""
    if not dt_str:
        return None
    try:
        # Format: "2024-08-17T11:30:00Z"
        s = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


def _match_result_for_team(match: dict, team_id: int) -> str | None:
    """
    Gibt 'W', 'D', oder 'L' zurück aus Sicht von team_id.
    Gibt None zurück wenn Ergebnis nicht bestimmbar.
    """
    home_id   = match.get("_home_team_id")
    away_id   = match.get("_away_team_id")
    home_g    = match.get("actual_home_goals")
    away_g    = match.get("actual_away_goals")

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
    """
    Gibt (scored, conceded) aus Sicht von team_id zurück.
    """
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


def _compute_strength(team_matches: list[dict], team_id: int) -> float | None:
    """
    Berechnet Stärke [0–100] aus den letzten STRENGTH_WINDOW Matches.
    team_matches: Matches sortiert nach kickoff DESC (neueste zuerst).
    """
    window = team_matches[:STRENGTH_WINDOW]
    if len(window) < MIN_HISTORY:
        return None

    total_points  = 0.0
    total_gdiff   = 0.0

    for m in window:
        result = _match_result_for_team(m, team_id)
        goals  = _goals_for_team(m, team_id)

        pts = {"W": 3, "D": 1, "L": 0}.get(result or "", 0)
        total_points += pts

        if goals:
            total_gdiff += goals[0] - goals[1]

    n             = len(window)
    ppg           = total_points / n
    gdiff_pg      = max(-GDIFF_CLIP, min(GDIFF_CLIP, total_gdiff / n))

    # Normalisieren: ppg 0–3 → 0–1, gdiff -3..+3 → 0–1
    ppg_norm      = ppg / PPG_MAX
    gdiff_norm    = (gdiff_pg + GDIFF_CLIP) / (2 * GDIFF_CLIP)

    strength_raw  = STRENGTH_W_PPG * ppg_norm + STRENGTH_W_GDIFF * gdiff_norm
    # Skalieren auf 0–100
    strength      = round(strength_raw * 100.0, 1)
    return max(STRENGTH_MIN, min(STRENGTH_MAX, strength))


def _compute_form(team_matches: list[dict], team_id: int) -> list[str]:
    """
    Gibt Form-Liste der letzten FORM_WINDOW Matches zurück.
    team_matches: Matches sortiert nach kickoff DESC (neueste zuerst).
    Ergebnis: ["W","D","L",...] mit max 5 Einträgen.
    """
    window = team_matches[:FORM_WINDOW]
    form   = []
    for m in window:
        result = _match_result_for_team(m, team_id)
        if result:
            form.append(result)
    return form


def _extract_team_id(raw_or_dict: dict, side: str) -> int | None:
    """
    Extrahiert team_id aus einem Match-Dict.
    side: "home" oder "away"
    """
    key = f"_{side}_team_id"
    v   = raw_or_dict.get(key)
    if v is not None:
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    return None


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
        idx[tid].sort(key=lambda x: x.get("_kickoff_dt") or datetime.min.replace(tzinfo=timezone.utc),
                      reverse=True)

    return idx


# ─────────────────────────────────────────────────────────────
# COMPUTE FEATURES FOR ONE MATCH
# ─────────────────────────────────────────────────────────────

def compute_features_for_match(
    match:      dict,
    history_idx: dict[int, list[dict]],
) -> dict:
    """
    Berechnet home_strength, away_strength, form_home, form_away, win_probability
    für ein einzelnes Match. Nutzt nur Matches STRICTLY BEFORE kickoff_at.

    Gibt ein Dict mit den Feature-Feldern zurück (kann None-Werte enthalten).
    """
    home_id    = match.get("_home_team_id")
    away_id    = match.get("_away_team_id")
    kickoff_dt = match.get("_kickoff_dt")

    # Filter: nur Matches VOR diesem Match (kein Data Leakage)
    def prior_matches(team_id: int) -> list[dict]:
        if team_id is None:
            return []
        all_tm = history_idx.get(team_id, [])
        if kickoff_dt is None:
            return all_tm
        # already sorted DESC — filter STRICTLY BEFORE kickoff
        return [m for m in all_tm
                if m.get("_kickoff_dt") is not None
                and m["_kickoff_dt"] < kickoff_dt]

    home_matches = prior_matches(home_id)
    away_matches = prior_matches(away_id)

    home_strength = _compute_strength(home_matches, home_id) if home_id else None
    away_strength = _compute_strength(away_matches, away_id) if away_id else None
    form_home     = _compute_form(home_matches, home_id)     if home_id else []
    form_away     = _compute_form(away_matches, away_id)     if away_id else []

    # Win probability
    if home_strength is not None and away_strength is not None:
        diff            = home_strength - away_strength
        win_probability = max(5, min(95, round(50 + diff * 0.6)))
    else:
        win_probability = 50

    return {
        "home_strength":    home_strength,
        "away_strength":    away_strength,
        "form_home":        form_home,
        "form_away":        form_away,
        "win_probability":  win_probability,
        # Diagnostics (nicht in Supabase geschrieben, nur für Logging)
        "_n_home_matches":  len(home_matches),
        "_n_away_matches":  len(away_matches),
    }


# ─────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────

def enrich_with_historical_features(
    matches: list[dict],
) -> list[dict]:
    """
    Reichert eine Match-Liste mit cross-season historischen Features an.

    Erwartet, dass jedes Match-Dict enthält:
      _home_team_id   (int)      — football-data.org team ID Heimteam
      _away_team_id   (int)      — football-data.org team ID Auswärtsteam
      _kickoff_dt     (datetime) — UTC-Kickoff als datetime-Objekt
      actual_home_goals (int)
      actual_away_goals (int)

    Schreibt in jedes Match-Dict:
      home_strength, away_strength  → float|None
      form_home, form_away          → list[str]
      win_probability               → int

    Gibt die angereicherte Liste zurück (in-place Mutation + return).
    """
    # Baue Index über ALLE Matches (Geschichte über beide Saisonen)
    history_idx = build_team_history_index(matches)

    n_total          = len(matches)
    n_cross_season   = 0     # Matches bei denen Vorjahres-Daten nötig waren
    n_strength_ok    = 0
    n_form_ok        = 0
    n_fallback       = 0

    for m in matches:
        feats = compute_features_for_match(m, history_idx)

        # Cross-season detection: previous season data was used
        # We detect this by checking if n_home or n_away matches > same-season count
        # Simple proxy: if a team had 0 matches in current season, cross-season data helped
        n_home = feats["_n_home_matches"]
        n_away = feats["_n_away_matches"]

        # Cross-season detection: at least one match in the strength window
        # belongs to a prior calendar year.
        #
        # NOTE: calendar year comparison used as approximation for cross-season
        # history. Football seasons span two calendar years (e.g. 2023/24 runs
        # Aug 2023 – May 2024). A match from Dec 2023 and one from Feb 2024
        # are in the SAME season but have different calendar years.
        # This approximation is intentionally kept simple and is only used
        # for the diagnostic log counter — it does not affect feature values.
        kickoff_dt = m.get("_kickoff_dt")
        kickoff_season = (kickoff_dt or datetime.min.replace(tzinfo=timezone.utc)).year
        home_id = m.get("_home_team_id")
        away_id = m.get("_away_team_id")

        def _prior(team_id):
            """All matches for team_id strictly before current kickoff, DESC."""
            if team_id is None:
                return []
            all_tm = history_idx.get(team_id, [])
            if kickoff_dt is None:
                return all_tm
            return [x for x in all_tm
                    if x.get("_kickoff_dt") is not None
                    and x["_kickoff_dt"] < kickoff_dt]

        prior_home = _prior(home_id)
        prior_away = _prior(away_id)

        cross = any(
            x.get("_kickoff_dt") and x["_kickoff_dt"].year < kickoff_season
            for x in prior_home[:STRENGTH_WINDOW]
        ) or any(
            x.get("_kickoff_dt") and x["_kickoff_dt"].year < kickoff_season
            for x in prior_away[:STRENGTH_WINDOW]
        )
        if cross:
            n_cross_season += 1

        if feats["home_strength"] is not None and feats["away_strength"] is not None:
            n_strength_ok += 1
        else:
            n_fallback += 1

        if feats["form_home"] or feats["form_away"]:
            n_form_ok += 1

        # Inject features into match dict (overwrite Supabase enrichment values)
        m["home_strength"]   = feats["home_strength"]
        m["away_strength"]   = feats["away_strength"]
        m["form_home"]       = feats["form_home"]
        m["form_away"]       = feats["form_away"]
        m["win_probability"] = feats["win_probability"]
        m["feature_src"]     = "historical_self_contained_v2"
        # Diagnostics keys (used for logging, stripped before DB write)
        m["_n_home_matches"] = n_home
        m["_n_away_matches"] = n_away

    # ── Coverage logging (single authoritative source for these metrics) ──
    # Note: n_total includes BOTH current and previous season matches
    # (the full history used for feature computation).
    if n_total > 0:
        log.info(
            "Historical feature coverage — "
            "strength: %d/%d (%.1f%%)  form: %d/%d (%.1f%%)  fallbacks: %d",
            n_strength_ok, n_total, n_strength_ok / n_total * 100,
            n_form_ok,     n_total, n_form_ok     / n_total * 100,
            n_fallback,
        )
        # Cross-season: calendar year approximation — diagnostic only
        log.info(
            "Cross-season history usage: %d/%d matches "  # calendar year approximation
            "(see historical_features.py for exact definition)",
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
