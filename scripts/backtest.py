#!/usr/bin/env python3
"""
EuroMatch Edge — Serverseitiger Backtest-Runner
================================================
Läuft als GitHub Actions Step (oder lokal) nach Spieltagen.
Liest abgeschlossene Spiele aus Supabase, berechnet Modell-Prognosen
und schreibt Ergebnisse mit dem SERVICE_ROLE_KEY zurück nach model_results.

Voraussetzungen:
  pip install requests python-dotenv

Umgebungsvariablen (GitHub Actions Secrets):
  SUPABASE_URL              z.B. https://xxx.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  langer JWT — NICHT der anon key
  SUPABASE_ANON_KEY          für lesende Supabase-Aufrufe (optional hier)

Aufruf:
  python scripts/backtest.py
  python scripts/backtest.py --season 2024/25 --competition BL1
"""

import os
import sys
import json
import argparse
import math
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL      = os.environ["SUPABASE_URL"]
SERVICE_ROLE_KEY  = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
TABLE             = "model_results"

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


# ── Kicktipp scoring ──────────────────────────────────────────

def calc_points(pred: str, actual_home: int, actual_away: int) -> int:
    """4 = exakter Score, 2 = richtige Tendenz, 0 = falsch."""
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


# ── Poisson model (Python equivalent of models/poisson.js) ───

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
    grid = {}
    for h in range(max_g + 1):
        for a in range(max_g + 1):
            grid[f"{h}:{a}"] = pois(h, lambda_h) * pois(a, lambda_a)
    return grid


def apply_dixon_coles(grid: dict, lh: float, la: float, rho: float = 0.13) -> dict:
    def tau(h, a):
        if h == 0 and a == 0: return 1 - lh * la * rho
        if h == 1 and a == 0: return 1 + la * rho
        if h == 0 and a == 1: return 1 + lh * rho
        if h == 1 and a == 1: return 1 - rho
        return 1.0

    corrected = dict(grid)
    for h, a in [(0,0),(1,0),(0,1),(1,1)]:
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


# ── Model implementations ─────────────────────────────────────

MODELS = {}

def register_model(fn):
    MODELS[fn.__name__] = fn
    return fn


@register_model
def ensemble(m: dict) -> dict:
    diff    = strength_diff(m)
    form_h  = form_score(m.get("form_home") or [])
    form_a  = form_score(m.get("form_away") or [])
    form_adv = form_h - form_a
    raw      = (m.get("win_probability", 50) / 100) * 0.6 + (0.5 + diff * 0.25 + form_adv * 0.15)
    home_p   = min(0.95, max(0.05, raw / 1.6))
    winner   = "home" if home_p > 0.55 else ("away" if home_p < 0.40 else "draw")
    conf     = int(50 + abs(home_p - 0.5) * 80)
    hg       = 1.2 + diff * 1.5 + form_adv * 0.5
    ag       = 0.7 - diff * 0.8 + (1 - form_adv) * 0.3
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    return {"predicted_winner": winner, "predicted_score": main_tip, "confidence_score": min(99, conf)}


@register_model
def poisson(m: dict) -> dict:
    diff    = strength_diff(m)
    form_h  = form_score(m.get("form_home") or [])
    form_a  = form_score(m.get("form_away") or [])
    lh      = max(0.3, 1.4 + diff * 1.2 + (form_h - 0.5) * 0.6)
    la      = max(0.3, 1.1 - diff * 0.8 + (form_a - 0.5) * 0.4)
    grid    = score_grid(lh, la)
    winner  = winner_from_grid(grid)
    top     = top_scores(grid, 1)
    main_tip = top[0][0] if top else f"{round(lh)}:{round(la)}"
    conf     = int(min(85, top[0][1] * 100 * 2.5)) if top else 50
    return {"predicted_winner": winner, "predicted_score": main_tip, "confidence_score": conf}


@register_model
def dc(m: dict) -> dict:
    diff    = strength_diff(m)
    form_h  = form_score(m.get("form_home") or [])
    form_a  = form_score(m.get("form_away") or [])
    lh      = max(0.3, 1.3 + diff * 1.0 + (form_h - 0.5) * 0.5)
    la      = max(0.3, 1.0 - diff * 0.7 + (form_a - 0.5) * 0.4)
    raw     = score_grid(lh, la)
    grid    = apply_dixon_coles(raw, lh, la)
    winner  = winner_from_grid(grid)
    top     = top_scores(grid, 1)
    main_tip = top[0][0] if top else f"{round(lh)}:{round(la)}"
    conf     = int(min(85, top[0][1] * 100 * 2.5)) if top else 50
    return {"predicted_winner": winner, "predicted_score": main_tip, "confidence_score": conf}


@register_model
def elo(m: dict) -> dict:
    diff    = strength_diff(m)
    home_p  = 1 / (1 + 10 ** (-(diff * 4 + 0.1)))
    winner  = "home" if home_p > 0.58 else ("away" if home_p < 0.38 else "draw")
    conf    = int(45 + abs(home_p - 0.5) * 90)
    hg      = 1.0 + diff * 1.8
    ag      = 0.9 - diff * 0.9
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    return {"predicted_winner": winner, "predicted_score": main_tip, "confidence_score": min(99, conf)}


@register_model
def market(m: dict) -> dict:
    quote   = m.get("market_signal_home") or 2.0
    implied = min(0.92, max(0.08, (1 / quote) * 0.9))
    winner  = "home" if implied > 0.55 else ("away" if implied < 0.40 else "draw")
    conf    = int(40 + implied * 45)
    diff    = strength_diff(m)
    hg      = 0.9 + diff * 1.2
    ag      = 0.8 - diff * 0.6
    main_tip = f"{max(0, round(hg))}:{max(0, round(ag))}"
    return {"predicted_winner": winner, "predicted_score": main_tip, "confidence_score": min(90, conf)}


# ── Supabase helpers ──────────────────────────────────────────

def fetch_finished_matches(season: str = None, competition: str = None) -> list:
    """Lädt abgeschlossene Spiele (actual_home_goals nicht NULL)."""
    params = [
        ("select", "*"),
        ("actual_home_goals", "not.is.null"),
        ("actual_away_goals", "not.is.null"),
        ("order",  "kickoff_at.asc"),
        ("limit",  "1000"),
    ]
    if season:      params.append(("season",           f"eq.{season}"))
    if competition: params.append(("competition_code", f"eq.{competition}"))

    url = f"{SUPABASE_URL}/rest/v1/matches"
    from urllib.parse import urlencode
    r = requests.get(f"{url}?{urlencode(params)}", headers=HEADERS_READ, timeout=15)
    r.raise_for_status()
    return r.json()


def upsert_results(rows: list) -> None:
    """Schreibt Backtest-Ergebnisse mit SERVICE_ROLE_KEY nach model_results."""
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=match_id,model_key"
    r = requests.post(url, headers=HEADERS_WRITE, json=rows, timeout=30)
    if not r.ok:
        print(f"[backtest.py] Supabase Fehler {r.status_code}: {r.text[:300]}", file=sys.stderr)
        r.raise_for_status()
    print(f"[backtest.py] {len(rows)} Einträge nach {TABLE} geschrieben")


# ── Main ──────────────────────────────────────────────────────

def run_backtest(season: str = None, competition: str = None, dry_run: bool = False):
    matches = fetch_finished_matches(season=season, competition=competition)
    print(f"[backtest.py] {len(matches)} abgeschlossene Spiele geladen")

    rows = []
    for m in matches:
        actual_h = m.get("actual_home_goals")
        actual_a = m.get("actual_away_goals")
        if actual_h is None or actual_a is None:
            continue
        actual_score  = f"{actual_h}:{actual_a}"
        actual_winner = "home" if actual_h > actual_a else ("away" if actual_a > actual_h else "draw")

        for model_key, model_fn in MODELS.items():
            try:
                result = model_fn(m)
            except Exception as e:
                print(f"  [WARN] {model_key} auf {m.get('id')} fehlgeschlagen: {e}", file=sys.stderr)
                continue

            pts = calc_points(result["predicted_score"], actual_h, actual_a)
            rows.append({
                "match_id":         m["id"],          # uuid
                "model_key":        model_key,
                "model_version":    "1.0",
                "predicted_score":  result["predicted_score"],
                "predicted_winner": result["predicted_winner"],
                "confidence_score": result["confidence_score"],
                "actual_score":     actual_score,
                "actual_winner":    actual_winner,
                "points":           pts,
                "competition_code": m.get("competition_code", ""),
                "season":           m.get("season", season or ""),
                "kickoff_at":       m.get("kickoff_at"),
            })

    print(f"[backtest.py] {len(rows)} Prognosen berechnet")

    if dry_run:
        print("[backtest.py] DRY RUN — kein Supabase-Write")
        print(json.dumps(rows[:3], indent=2, default=str))
    else:
        upsert_results(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EuroMatch Edge Backtest Pipeline")
    parser.add_argument("--season",      default=None,  help="z.B. 2024/25")
    parser.add_argument("--competition", default=None,  help="z.B. BL1")
    parser.add_argument("--dry-run",     action="store_true")
    args = parser.parse_args()
    run_backtest(season=args.season, competition=args.competition, dry_run=args.dry_run)
