import os
import logging
from datetime import datetime, timedelta, timezone
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ["DATABASE_URL"]

# =========================
# CONFIG
# =========================
MODEL_WEIGHT = 0.7
MARKET_WEIGHT = 0.3

MIN_PROB = 0.10
MAX_PROB = 0.70

MIN_EDGE = 0.02
MIN_CONFIDENCE = 35
MAX_EDGE = 0.25  # verhindert extreme Ausreißer

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# =========================
# HELPERS
# =========================
def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def mix_prob(model_p, market_p):
    if market_p is None:
        return model_p
    return MODEL_WEIGHT * model_p + MARKET_WEIGHT * market_p


def compute_edge(model_p, market_p):
    if market_p is None:
        return None
    return model_p - market_p


def compute_ev(prob, odds):
    if odds is None:
        return None
    return prob * odds - 1


# =========================
# FETCH MATCHES
# =========================
def fetch_matches(conn, days_ahead=2):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    now = datetime.now(timezone.utc)
    future = now + timedelta(days=days_ahead)

    cur.execute("""
        SELECT *
        FROM matches
        WHERE kickoff_at >= %s
          AND kickoff_at < %s
        ORDER BY kickoff_at ASC
    """, (now, future))

    return cur.fetchall()


# =========================
# CORE LOGIC
# =========================
def compute_prediction(match):
    # Modell-Wahrscheinlichkeiten (bereits vorhanden)
    p_home = match["home_strength"] / (match["home_strength"] + match["away_strength"] + 1e-6)
    p_away = match["away_strength"] / (match["home_strength"] + match["away_strength"] + 1e-6)
    p_draw = 1 - p_home - p_away

    # Markt (fair probs)
    m_home = match.get("market_home_prob_fair")
    m_draw = match.get("market_draw_prob_fair")
    m_away = match.get("market_away_prob_fair")

    # === MIX ===
    p_home = mix_prob(p_home, m_home)
    p_draw = mix_prob(p_draw, m_draw)
    p_away = mix_prob(p_away, m_away)

    # === CLAMP ===
    p_home = clamp(p_home, MIN_PROB, MAX_PROB)
    p_draw = clamp(p_draw, MIN_PROB, MAX_PROB)
    p_away = clamp(p_away, MIN_PROB, MAX_PROB)

    # === NORMALIZE ===
    total = p_home + p_draw + p_away
    p_home /= total
    p_draw /= total
    p_away /= total

    # === EDGE ===
    edge_home = compute_edge(p_home, m_home)
    edge_draw = compute_edge(p_draw, m_draw)
    edge_away = compute_edge(p_away, m_away)

    # === EV ===
    ev_home = compute_ev(p_home, match.get("market_home_win_odds"))
    ev_draw = compute_ev(p_draw, match.get("market_draw_odds"))
    ev_away = compute_ev(p_away, match.get("market_away_odds"))

    # === CONFIDENCE ===
    confidence = int(max(p_home, p_draw, p_away) * 100)

    # === VALUE LOGIC ===
    def is_value(edge, ev):
        return (
            edge is not None
            and ev is not None
            and edge > MIN_EDGE
            and edge < MAX_EDGE
            and ev > 0
            and confidence >= MIN_CONFIDENCE
        )

    value_home = is_value(edge_home, ev_home)
    value_draw = is_value(edge_draw, ev_draw)
    value_away = is_value(edge_away, ev_away)

    # === WINNER ===
    probs = {"home": p_home, "draw": p_draw, "away": p_away}
    predicted_winner = max(probs, key=probs.get)

    return {
        "win_probability": int(p_home * 100),
        "draw_probability": int(p_draw * 100),
        "away_probability": int(p_away * 100),
        "confidence_score": confidence,
        "predicted_winner": predicted_winner,
        "value_home": value_home,
        "value_draw": value_draw,
        "value_away": value_away,
        "edge_home": edge_home,
        "edge_draw": edge_draw,
        "edge_away": edge_away,
        "ev_home": ev_home,
        "ev_draw": ev_draw,
        "ev_away": ev_away,
        "computed_at": datetime.now(timezone.utc),
    }


# =========================
# UPSERT
# =========================
def upsert_predictions(conn, rows):
    cur = conn.cursor()

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO predictions (
            match_id,
            win_probability,
            draw_probability,
            away_probability,
            confidence_score,
            predicted_winner,
            value_home,
            value_draw,
            value_away,
            edge_home,
            edge_draw,
            edge_away,
            ev_home,
            ev_draw,
            ev_away,
            computed_at,
            created_at,
            updated_at
        ) VALUES %s
        ON CONFLICT (match_id)
        DO UPDATE SET
            win_probability = EXCLUDED.win_probability,
            draw_probability = EXCLUDED.draw_probability,
            away_probability = EXCLUDED.away_probability,
            confidence_score = EXCLUDED.confidence_score,
            predicted_winner = EXCLUDED.predicted_winner,
            value_home = EXCLUDED.value_home,
            value_draw = EXCLUDED.value_draw,
            value_away = EXCLUDED.value_away,
            edge_home = EXCLUDED.edge_home,
            edge_draw = EXCLUDED.edge_draw,
            edge_away = EXCLUDED.edge_away,
            ev_home = EXCLUDED.ev_home,
            ev_draw = EXCLUDED.ev_draw,
            ev_away = EXCLUDED.ev_away,
            computed_at = EXCLUDED.computed_at,
            updated_at = EXCLUDED.updated_at
        """,
        rows,
    )

    conn.commit()


# =========================
# MAIN
# =========================
def main():
    conn = psycopg2.connect(DATABASE_URL)

    matches = fetch_matches(conn)

    rows = []
    for m in matches:
        pred = compute_prediction(m)

        rows.append((
            m["id"],
            pred["win_probability"],
            pred["draw_probability"],
            pred["away_probability"],
            pred["confidence_score"],
            pred["predicted_winner"],
            pred["value_home"],
            pred["value_draw"],
            pred["value_away"],
            pred["edge_home"],
            pred["edge_draw"],
            pred["edge_away"],
            pred["ev_home"],
            pred["ev_draw"],
            pred["ev_away"],
            pred["computed_at"],
            pred["computed_at"],
            pred["computed_at"],
        ))

    if rows:
        upsert_predictions(conn, rows)
        logging.info(f"Inserted/Updated {len(rows)} predictions")

    conn.close()


if __name__ == "__main__":
    main()