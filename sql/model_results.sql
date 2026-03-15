-- ══════════════════════════════════════════════════════════════
-- EuroMatch Edge — model_results
-- Speichert Modell-Prognosen gegen tatsächliche Spielergebnisse.
-- Wird ausschließlich vom Backend (GitHub Actions / Python) befüllt.
-- Das Frontend liest nur (anon SELECT via RLS).
--
-- match_id ist TEXT (nicht uuid), weil backtest.py external_ids
-- wie "football_data_12345" schreibt — keine uuid-Konvention.
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS model_results (

  -- ── Primärschlüssel ──────────────────────────────────────────
  match_id        text        NOT NULL,
  model_key       text        NOT NULL,
  CONSTRAINT pk_model_results PRIMARY KEY (match_id, model_key),

  -- ── Modell-Metadaten ─────────────────────────────────────────
  model_version   text        NOT NULL DEFAULT '1.0',

  -- ── Prognose ─────────────────────────────────────────────────
  predicted_score  text,       -- z.B. '2:1'
  predicted_winner text,       -- 'home' | 'away' | 'draw'
  confidence_score smallint,   -- 0–100

  -- ── Tatsächliches Ergebnis ───────────────────────────────────
  actual_score    text        NOT NULL,   -- z.B. '2:1'
  actual_winner   text,                   -- 'home' | 'away' | 'draw'
  actual_home_goals smallint,             -- absoluter Heimtreffer
  actual_away_goals smallint,             -- absoluter Auswärtstref.

  -- ── Bewertung ─────────────────────────────────────────────────
  points          smallint    NOT NULL DEFAULT 0
    CONSTRAINT chk_points CHECK (points IN (0, 2, 4)),
    -- Kicktipp: 4 = exakter Score, 2 = richtige Tendenz, 0 = falsch

  -- ── Feinere Bewertungskategorien ─────────────────────────────
  -- Für die Historien-Seite: menschenlesbare Kategorie
  outcome_label   text
    CONSTRAINT chk_outcome CHECK (
      outcome_label IN ('exact', 'goal_diff', 'tendency', 'wrong') OR outcome_label IS NULL
    ),
  -- exact       = Exakter Score getroffen         (4 Pkt)
  -- goal_diff   = Richtige Tordifferenz, falscher Score (2 Pkt)
  -- tendency    = Richtige Tendenz (H/U/A), falsche Tordiff   (2 Pkt)
  -- wrong       = Komplett daneben               (0 Pkt)

  -- ── Evaluation Metrics (berechnet via utils/eval_metrics.py) ──────
  predicted_1x2    text
    CONSTRAINT chk_predicted_1x2 CHECK (
      predicted_1x2 IN ('H', 'D', 'A') OR predicted_1x2 IS NULL
    ),                           -- "H" | "D" | "A"
  actual_1x2       text
    CONSTRAINT chk_actual_1x2 CHECK (
      actual_1x2 IN ('H', 'D', 'A') OR actual_1x2 IS NULL
    ),                           -- "H" | "D" | "A"
  is_exact_hit     boolean,     -- predicted_score == actual_score
  is_tendency_hit  boolean,     -- predicted_1x2   == actual_1x2
  score_error_abs  smallint,    -- |Δhome| + |Δaway|
  goal_diff_error  smallint,    -- |predicted_diff - actual_diff|
  brier_score_1x2  real,        -- [0,2/3], null wenn probs fehlen
  log_loss_1x2     real,        -- [0,∞), null wenn probs fehlen

  -- ── Versioning ──────────────────────────────────────────────────────
  system_version      text,
  pipeline_run_id     text,
  calibration_version text,
  weights_version     text,

  -- ── Spielkontext (denormalisiert für schnellen Frontend-Zugriff) ─
  home_team       text,
  away_team       text,
  flag            text,        -- Emoji, z.B. '🏴󠁧󠁢󠁥󠁮󠁧󠁿'
  league_abbr     text,        -- z.B. 'BL1', 'PL'
  competition_code text,
  season          text,        -- z.B. '2024'
  kickoff_at      timestamptz,

  -- ── Audit ────────────────────────────────────────────────────
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- ── updated_at automatisch setzen ───────────────────────────
CREATE OR REPLACE FUNCTION set_model_results_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_model_results_updated_at ON model_results;
CREATE TRIGGER trg_model_results_updated_at
  BEFORE UPDATE ON model_results
  FOR EACH ROW EXECUTE FUNCTION set_model_results_updated_at();

-- ── Indizes ──────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_mr_model_key        ON model_results (model_key);
CREATE INDEX IF NOT EXISTS idx_mr_competition_code ON model_results (competition_code);
CREATE INDEX IF NOT EXISTS idx_mr_kickoff_at       ON model_results (kickoff_at DESC);
CREATE INDEX IF NOT EXISTS idx_mr_points           ON model_results (points);
CREATE INDEX IF NOT EXISTS idx_mr_outcome_label    ON model_results (outcome_label);
-- Compound index for the history page's most common query
CREATE INDEX IF NOT EXISTS idx_mr_history
  ON model_results (model_key, kickoff_at DESC)
  WHERE kickoff_at IS NOT NULL;

-- ══════════════════════════════════════════════════════════════
-- Row Level Security
-- Frontend (anon): nur SELECT
-- Backend (service_role): INSERT / UPDATE / DELETE
-- ══════════════════════════════════════════════════════════════
ALTER TABLE model_results ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "anon_select"  ON model_results;
DROP POLICY IF EXISTS "auth_select"  ON model_results;

CREATE POLICY "anon_select"
  ON model_results FOR SELECT TO anon        USING (true);
CREATE POLICY "auth_select"
  ON model_results FOR SELECT TO authenticated USING (true);


-- ══════════════════════════════════════════════════════════════
-- View: v_model_rankings
-- Aggregierte Rankings — Frontend lädt per GET /v_model_rankings
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW v_model_rankings AS
SELECT
  model_key,
  model_version,
  COUNT(*)                                        AS matches_evaluated,
  SUM(points)                                     AS total_points,
  ROUND(AVG(points)::numeric, 2)                  AS avg_points,
  COUNT(*) FILTER (WHERE points = 4)              AS exact_hits,
  ROUND(
    COUNT(*) FILTER (WHERE points = 4)::numeric
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                               AS exact_rate,
  COUNT(*) FILTER (WHERE points = 2)              AS tendency_hits,
  ROUND(
    COUNT(*) FILTER (WHERE points = 2)::numeric
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                               AS tendency_rate,
  COUNT(*) FILTER (WHERE points = 0)              AS wrong_predictions,
  ROUND(
    (COUNT(*) FILTER (WHERE points IN (2, 4)))::numeric
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                               AS score_accuracy
FROM model_results
GROUP BY model_key, model_version
ORDER BY avg_points DESC;

GRANT SELECT ON v_model_rankings TO anon;
GRANT SELECT ON v_model_rankings TO authenticated;


-- ══════════════════════════════════════════════════════════════
-- View: v_model_eval_summary
-- Aggregierte Evaluationsmetriken pro Modell.
-- Ergänzt v_model_rankings um die neuen Metriken.
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW v_model_eval_summary AS
SELECT
  model_key,
  model_version,
  COUNT(*)                                              AS matches_evaluated,

  ROUND(AVG(points)::numeric, 3)                        AS avg_kicktipp_points,
  COUNT(*) FILTER (WHERE points = 4)                    AS exact_score_hits,
  COUNT(*) FILTER (WHERE points = 2)                    AS tendency_hits_kicktipp,

  COUNT(*) FILTER (WHERE is_exact_hit = true)           AS exact_hits,
  COUNT(*) FILTER (WHERE is_tendency_hit = true)        AS tendency_hits,
  ROUND(
    COUNT(*) FILTER (WHERE is_tendency_hit = true)::numeric
    / NULLIF(COUNT(*) FILTER (WHERE is_tendency_hit IS NOT NULL), 0) * 100, 1
  )                                                     AS tendency_hit_rate_pct,
  ROUND(
    COUNT(*) FILTER (WHERE is_exact_hit = true)::numeric
    / NULLIF(COUNT(*) FILTER (WHERE is_exact_hit IS NOT NULL), 0) * 100, 1
  )                                                     AS exact_hit_rate_pct,

  ROUND(AVG(score_error_abs)::numeric, 3)               AS avg_score_error,
  ROUND(AVG(goal_diff_error)::numeric, 3)               AS avg_goal_diff_error,
  ROUND(AVG(brier_score_1x2)::numeric, 4)               AS avg_brier_score,
  ROUND(AVG(log_loss_1x2)::numeric, 4)                  AS avg_log_loss,

  COUNT(*) FILTER (WHERE actual_1x2 = 'H')              AS actual_home_wins,
  COUNT(*) FILTER (WHERE actual_1x2 = 'D')              AS actual_draws,
  COUNT(*) FILTER (WHERE actual_1x2 = 'A')              AS actual_away_wins
FROM model_results
WHERE actual_1x2 IS NOT NULL
GROUP BY model_key, model_version
ORDER BY avg_kicktipp_points DESC;

GRANT SELECT ON v_model_eval_summary TO authenticated;


-- ══════════════════════════════════════════════════════════════
-- View: v_history
-- Historische Prognosen für das Frontend.
-- Gibt den Ensemble-Tipp pro Spiel zurück (canonical model).
-- Gefiltert auf model_key = 'ensemble' — eine Zeile pro Spiel.
-- Sortiert nach kickoff_at DESC für paginierte Abfragen.
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW v_history AS
SELECT
  match_id,
  home_team,
  away_team,
  flag,
  league_abbr,
  competition_code,
  season,
  kickoff_at,
  predicted_score,
  predicted_winner,
  confidence_score,
  actual_score,
  actual_winner,
  actual_home_goals,
  actual_away_goals,
  points,
  outcome_label,
  model_version,
  created_at
FROM model_results
WHERE model_key = 'ensemble'
  AND actual_score IS NOT NULL
  AND kickoff_at   IS NOT NULL
ORDER BY kickoff_at DESC;

GRANT SELECT ON v_history TO anon;
GRANT SELECT ON v_history TO authenticated;


-- ══════════════════════════════════════════════════════════════
-- Beispiel-Upsert (Python / GitHub Actions):
--
--   INSERT INTO model_results (
--     match_id, model_key, model_version,
--     predicted_score, predicted_winner, confidence_score,
--     actual_score, actual_winner, actual_home_goals, actual_away_goals,
--     points, outcome_label,
--     home_team, away_team, flag, league_abbr,
--     competition_code, season, kickoff_at
--   ) VALUES (
--     'football_data_12345', 'ensemble', '1.0',
--     '2:1', 'home', 72,
--     '2:0', 'home', 2, 0,
--     2, 'tendency',
--     'Bayern München', 'Borussia Dortmund', '🇩🇪', 'BL1',
--     'BUNDESLIGA', '2024', '2025-03-15T15:30:00Z'
--   )
--   ON CONFLICT (match_id, model_key)
--   DO UPDATE SET
--     actual_score      = EXCLUDED.actual_score,
--     points            = EXCLUDED.points,
--     outcome_label     = EXCLUDED.outcome_label,
--     updated_at        = now();
-- ══════════════════════════════════════════════════════════════
