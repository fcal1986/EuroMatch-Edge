"""
utils/test_eval_metrics.py
──────────────────────────
EuroMatch Edge — Unit Tests für utils/eval_metrics.py

Ausführen:
    cd scripts
    python -m utils.test_eval_metrics          # direkt
    python -m pytest utils/test_eval_metrics.py -v  # mit pytest (falls installiert)

Keine externen Dependencies außer der Standardbibliothek.
"""

import math
import sys
import traceback
from utils.eval_metrics import (
    goals_to_1x2,
    score_str_to_goals,
    compute_eval_metrics,
    probs_from_score_grid,
    probs_from_home_probability,
)

# ─────────────────────────────────────────────────────────────
# Test runner (kein pytest nötig)
# ─────────────────────────────────────────────────────────────

_passed = 0
_failed = 0

def _check(label: str, condition: bool) -> None:
    global _passed, _failed
    if condition:
        _passed += 1
        print(f"  ✓  {label}")
    else:
        _failed += 1
        print(f"  ✗  FAIL: {label}")

def _section(title: str) -> None:
    print(f"\n── {title} {'─' * (50 - len(title))}")


# ─────────────────────────────────────────────────────────────
# goals_to_1x2
# ─────────────────────────────────────────────────────────────
_section("goals_to_1x2")
_check("Heimsieg 2:1 → H",         goals_to_1x2(2, 1) == "H")
_check("Auswärtssieg 0:2 → A",     goals_to_1x2(0, 2) == "A")
_check("Remis 1:1 → D",            goals_to_1x2(1, 1) == "D")
_check("Remis 0:0 → D",            goals_to_1x2(0, 0) == "D")
_check("Hoher Heimsieg 5:0 → H",   goals_to_1x2(5, 0) == "H")
_check("None home → None",         goals_to_1x2(None, 1) is None)
_check("None away → None",         goals_to_1x2(2, None) is None)
_check("None both → None",         goals_to_1x2(None, None) is None)


# ─────────────────────────────────────────────────────────────
# score_str_to_goals
# ─────────────────────────────────────────────────────────────
_section("score_str_to_goals")
_check("'2:1' → (2,1)",            score_str_to_goals("2:1")  == (2, 1))
_check("'0:0' → (0,0)",            score_str_to_goals("0:0")  == (0, 0))
_check("'3:2' → (3,2)",            score_str_to_goals("3:2")  == (3, 2))
_check("'1-0' (dash) → (1,0)",     score_str_to_goals("1-0")  == (1, 0))
_check("None → None",              score_str_to_goals(None)   is None)
_check("'' → None",                score_str_to_goals("")     is None)
_check("'abc' → None",             score_str_to_goals("abc")  is None)
_check("'1:x' → None",             score_str_to_goals("1:x")  is None)
_check("' 2 : 1 ' → (2,1)",        score_str_to_goals(" 2 : 1 ") == (2, 1))


# ─────────────────────────────────────────────────────────────
# compute_eval_metrics — Basisfälle
# ─────────────────────────────────────────────────────────────
_section("compute_eval_metrics — exakter Treffer")
m = compute_eval_metrics("2:1", 2, 1)
_check("predicted_1x2 = H",        m["predicted_1x2"]   == "H")
_check("actual_1x2 = H",           m["actual_1x2"]       == "H")
_check("is_exact_hit = True",       m["is_exact_hit"]     is True)
_check("is_tendency_hit = True",    m["is_tendency_hit"]  is True)
_check("score_error_abs = 0",       m["score_error_abs"]  == 0)
_check("goal_diff_error = 0",       m["goal_diff_error"]  == 0)
_check("brier = None (no probs)",   m["brier_score_1x2"]  is None)
_check("logloss = None (no probs)", m["log_loss_1x2"]     is None)

_section("compute_eval_metrics — richtige Tendenz, falscher Score")
m = compute_eval_metrics("2:1", 1, 0)   # pred H 2:1, actual H 1:0
_check("predicted_1x2 = H",        m["predicted_1x2"]   == "H")
_check("actual_1x2 = H",           m["actual_1x2"]       == "H")
_check("is_exact_hit = False",      m["is_exact_hit"]     is False)
_check("is_tendency_hit = True",    m["is_tendency_hit"]  is True)
_check("score_error_abs = 2",       m["score_error_abs"]  == 2)   # |2-1|+|1-0|=2
_check("goal_diff_error = 0",       m["goal_diff_error"]  == 0)   # both diff=+1

_section("compute_eval_metrics — falsche Tendenz")
m = compute_eval_metrics("2:1", 0, 2)   # pred H, actual A
_check("predicted_1x2 = H",        m["predicted_1x2"]   == "H")
_check("actual_1x2 = A",           m["actual_1x2"]       == "A")
_check("is_exact_hit = False",      m["is_exact_hit"]     is False)
_check("is_tendency_hit = False",   m["is_tendency_hit"]  is False)
_check("score_error_abs = 3",       m["score_error_abs"]  == 3)   # |2-0|+|1-2| = 2+1 = 3
_check("goal_diff_error = 3",       m["goal_diff_error"]  == 3)   # |(2-1)-(0-2)| = |1+2| = 3

_section("compute_eval_metrics — Remis")
m = compute_eval_metrics("1:1", 1, 1)
_check("predicted_1x2 = D",        m["predicted_1x2"]   == "D")
_check("actual_1x2 = D",           m["actual_1x2"]       == "D")
_check("is_exact_hit = True",       m["is_exact_hit"]     is True)
_check("score_error_abs = 0",       m["score_error_abs"]  == 0)

_section("compute_eval_metrics — fehlende Daten")
m = compute_eval_metrics(None, 2, 1)
_check("None pred → is_exact_hit None",    m["is_exact_hit"]     is None)
_check("None pred → is_tendency_hit None", m["is_tendency_hit"]  is None)
_check("None pred → score_error None",     m["score_error_abs"]  is None)
_check("None pred → predicted_1x2 None",  m["predicted_1x2"]    is None)

m = compute_eval_metrics("2:1", None, None)
_check("None actual → is_exact_hit None",  m["is_exact_hit"]     is None)
_check("None actual → actual_1x2 None",   m["actual_1x2"]        is None)

m = compute_eval_metrics("2:1", 2, None)
_check("partial actual → is_exact_hit None",  m["is_exact_hit"]     is None)
_check("partial actual → is_tendency_hit None", m["is_tendency_hit"] is None)
_check("partial actual → score_error None",   m["score_error_abs"]  is None)
_check("partial actual → goal_diff_error None", m["goal_diff_error"] is None)

m = compute_eval_metrics("invalid", 2, 1)
_check("invalid pred → is_exact_hit None", m["is_exact_hit"]     is None)


# ─────────────────────────────────────────────────────────────
# compute_eval_metrics — Brier Score
# ─────────────────────────────────────────────────────────────
_section("compute_eval_metrics — Brier Score")
# Perfekte Vorhersage: H gewinnt, p_home=1.0
m = compute_eval_metrics("2:1", 2, 1, prob_home=1.0, prob_draw=0.0, prob_away=0.0)
_check("Brier = 0.0 bei perfekter Vorhersage",
       m["brier_score_1x2"] == 0.0)
_check("Log loss ≈ 0.0 bei perfekter Vorhersage",
       m["log_loss_1x2"] is not None and m["log_loss_1x2"] < 0.001)

# Gleichverteilung: 1/3 jede Klasse
m = compute_eval_metrics("1:1", 1, 1, prob_home=1/3, prob_draw=1/3, prob_away=1/3)
_check("Brier ≈ 0.222 bei Gleichverteilung + D trifft",
       m["brier_score_1x2"] is not None and abs(m["brier_score_1x2"] - 2/9) < 0.001)
_check("Log loss ≈ 1.099 bei Gleichverteilung",
       m["log_loss_1x2"] is not None and abs(m["log_loss_1x2"] - math.log(3)) < 0.001)

# Falsche Vorhersage mit Sicherheit: H erwartet, A eingetreten
m = compute_eval_metrics("2:1", 0, 2, prob_home=1.0, prob_draw=0.0, prob_away=0.0)
_check("Brier ≈ 0.667 bei falscher Sicherheit",
       m["brier_score_1x2"] is not None and abs(m["brier_score_1x2"] - 2/3) < 0.001)

# Probs nicht verfügbar → None
m = compute_eval_metrics("2:1", 2, 1)
_check("Brier = None ohne probs",  m["brier_score_1x2"] is None)
_check("LogLoss = None ohne probs",m["log_loss_1x2"] is None)

# Ungültige probs → None
m = compute_eval_metrics("2:1", 2, 1, prob_home=-0.1, prob_draw=0.5, prob_away=0.6)
_check("Brier = None bei ungültigen probs", m["brier_score_1x2"] is None)


# ─────────────────────────────────────────────────────────────
# probs_from_score_grid
# ─────────────────────────────────────────────────────────────
_section("probs_from_score_grid")
# Einfaches Grid: nur ein Score
grid = {"2:1": 0.6, "0:0": 0.4}
ph, pd, pa = probs_from_score_grid(grid)
_check("p_home = 0.6 (Heimsieg-Grid)",  abs(ph - 0.6) < 1e-9)
_check("p_draw = 0.4 (Remis-Grid)",     abs(pd - 0.4) < 1e-9)
_check("p_away = 0.0 (kein Auswärtssieg)", abs(pa - 0.0) < 1e-9)
_check("Summe = 1.0",                   abs(ph + pd + pa - 1.0) < 1e-9)

# Leeres Grid
ph, pd, pa = probs_from_score_grid({})
_check("Leeres Grid → 0.0, 0.0, 0.0",  ph == 0.0 and pd == 0.0 and pa == 0.0)


# ─────────────────────────────────────────────────────────────
# probs_from_home_probability
# ─────────────────────────────────────────────────────────────
_section("probs_from_home_probability")
ph, pd, pa = probs_from_home_probability(0.6)
_check("p_home = 0.6",          abs(ph - 0.6) < 1e-9)
_check("Summe = 1.0",           abs(ph + pd + pa - 1.0) < 1e-9)
_check("p_draw > 0",            pd > 0)
_check("p_away > 0",            pa > 0)

ph, pd, pa = probs_from_home_probability(0.0)
_check("p_home=0 → p_home=0.0", ph == 0.0)
_check("p_home=0 → rest sums 1", abs(pd + pa - 1.0) < 1e-9)

ph, pd, pa = probs_from_home_probability(1.0)
_check("p_home=1 → p_home=1.0", ph == 1.0)
_check("p_home=1 → rest=0",     abs(pd + pa) < 1e-9)

# Clipping
ph, pd, pa = probs_from_home_probability(1.5)  # über 1
_check("p_home > 1 geclippt",   ph == 1.0)
ph, pd, pa = probs_from_home_probability(-0.1)  # unter 0
_check("p_home < 0 geclippt",   ph == 0.0)


# ─────────────────────────────────────────────────────────────
# score_error_abs Randfälle
# ─────────────────────────────────────────────────────────────
_section("score_error_abs Randfälle")
m = compute_eval_metrics("0:0", 5, 5)
_check("0:0 pred, 5:5 actual → error=10", m["score_error_abs"] == 10)

m = compute_eval_metrics("3:0", 0, 3)
_check("3:0 pred, 0:3 actual → error=6", m["score_error_abs"] == 6)
_check("goal_diff_error = 6",             m["goal_diff_error"]  == 6)  # |3-(-3)|=6


# ─────────────────────────────────────────────────────────────
# Ergebnis
# ─────────────────────────────────────────────────────────────
print(f"\n{'═'*55}")
print(f"  Ergebnis: {_passed} bestanden, {_failed} fehlgeschlagen")
if _failed:
    print(f"  ✗ FEHLER — bitte Implementierung prüfen")
    sys.exit(1)
else:
    print(f"  ✓ ALLE TESTS BESTANDEN")
print(f"{'═'*55}")


if __name__ == "__main__":
    pass   # Tests laufen beim Import durch
