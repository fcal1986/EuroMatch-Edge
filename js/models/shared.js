'use strict';

/* ── ModelShared ─────────────────────────────────────────────────
   Pure math helpers shared across all model files.
   No dependencies on State, CFG or DOM.
────────────────────────────────────────────────────────────────── */
const ModelShared = {

  /* ── Form ─────────────────────────────────────────────────── */

  /** W=3, D=1, L=0 → normalised 0–1 */
  formScore(form) {
    if (!form || !form.length) return 0.5;
    const pts = form.reduce((s, r) => s + (r === 'W' ? 3 : r === 'D' ? 1 : 0), 0);
    return pts / (form.length * 3);
  },

  /* ── Strength ─────────────────────────────────────────────── */

  /** Returns (homeStrength - awayStrength) / 100.  Range: –1…+1 */
  strengthDiff(m) {
    if (m.homeStrength == null || m.awayStrength == null) return 0;
    return (m.homeStrength - m.awayStrength) / 100;
  },

  /* ── Poisson math ─────────────────────────────────────────── */

  /** P(X = k) for Poisson distribution */
  pois(k, lambda) {
    let e = Math.exp(-lambda), prod = 1;
    for (let i = 1; i <= k; i++) prod *= lambda / i;
    return e * prod;
  },

  /** P(teamA goals > teamB goals) via 6×6 grid */
  poissonWinProb(lambdaA, lambdaB) {
    let p = 0;
    for (let a = 0; a <= 5; a++) {
      for (let b = 0; b < a; b++) {
        p += this.pois(a, lambdaA) * this.pois(b, lambdaB);
      }
    }
    return Math.min(0.90, p);
  },

  /* ── Score Probability Distribution ──────────────────────── */

  /**
   * Build a full score probability grid (0–maxG × 0–maxG).
   * Returns a plain object: { "0:0": 0.11, "1:0": 0.21, … }
   * Probabilities are raw Poisson — do NOT apply DC correction here.
   * @param {number} lambdaH  expected goals home
   * @param {number} lambdaA  expected goals away
   * @param {number} maxG     max goals to model per team (default 6)
   * @returns {Object.<string, number>}
   */
  scoreGrid(lambdaH, lambdaA, maxG = 6) {
    const grid = {};
    for (let h = 0; h <= maxG; h++) {
      for (let a = 0; a <= maxG; a++) {
        grid[`${h}:${a}`] = this.pois(h, lambdaH) * this.pois(a, lambdaA);
      }
    }
    return grid;
  },

  /**
   * Apply Dixon–Coles low-score correction to an existing score grid.
   * Adjusts 0:0, 1:0, 0:1, 1:1 cells using parameter rho.
   * @param {Object.<string, number>} grid
   * @param {number} lambdaH
   * @param {number} lambdaA
   * @param {number} rho  correction strength (e.g. 0.13)
   * @returns {Object.<string, number>} corrected grid (re-normalised)
   */
  applyDixonColes(grid, lambdaH, lambdaA, rho = 0.13) {
    const corrected = { ...grid };

    // τ (tau) function — Dixon & Coles 1997
    const tau = (h, a, lH, lA, r) => {
      if      (h === 0 && a === 0) return 1 - lH * lA * r;
      else if (h === 1 && a === 0) return 1 + lA * r;
      else if (h === 0 && a === 1) return 1 + lH * r;
      else if (h === 1 && a === 1) return 1 - r;
      return 1;
    };

    [[0,0],[1,0],[0,1],[1,1]].forEach(([h, a]) => {
      const key = `${h}:${a}`;
      if (corrected[key] != null) {
        corrected[key] *= tau(h, a, lambdaH, lambdaA, rho);
      }
    });

    // Re-normalise so probabilities sum to 1
    const total = Object.values(corrected).reduce((s, v) => s + v, 0);
    if (total > 0) {
      for (const key in corrected) corrected[key] /= total;
    }
    return corrected;
  },

  /**
   * Pick the top-N most probable scores from a grid.
   * @param {Object.<string, number>} grid
   * @param {number} n
   * @returns {Array<{score: string, prob: number}>}
   */
  topScores(grid, n = 5) {
    return Object.entries(grid)
      .map(([score, prob]) => ({ score, prob }))
      .sort((a, b) => b.prob - a.prob)
      .slice(0, n);
  },

  /**
   * Derive predicted_winner from a score grid.
   * @param {Object.<string, number>} grid
   * @returns {'home'|'away'|'draw'}
   */
  winnerFromGrid(grid) {
    let pHome = 0, pAway = 0, pDraw = 0;
    for (const [score, prob] of Object.entries(grid)) {
      const [h, a] = score.split(':').map(Number);
      if (h > a) pHome += prob;
      else if (a > h) pAway += prob;
      else pDraw += prob;
    }
    return pHome >= pAway && pHome >= pDraw ? 'home'
         : pAway >= pHome && pAway >= pDraw ? 'away' : 'draw';
  },

  /* ── Output helpers ───────────────────────────────────────── */

  /** Round float goals to a "N:M" score string */
  scoreTip(homeGoals, awayGoals) {
    return `${Math.max(0, Math.round(homeGoals))}:${Math.max(0, Math.round(awayGoals))}`;
  },

  /** Map confidence score to risk label */
  riskLevel(conf) {
    return conf >= 65 ? 'low' : conf >= 45 ? 'medium' : 'high';
  },

  /** Last word of a team name — used in short reason strings */
  shortName(teamName) {
    return teamName.split(' ').slice(-1)[0];
  },

  /**
   * Builds a standard model output object.
   * score_distribution is optional — supplied by Poisson-family models.
   */
  buildOutput({
    model_key, model_name, model_version = '1.0', icon_class,
    predicted_winner, main_score_tip, alt_score_tip,
    confidence_score, summary_reason, factors_used,
    score_distribution = null,
    is_ai_agent = false,
  }) {
    return {
      model_key,
      model_name,
      model_version,
      icon_class,
      predicted_winner,
      main_score_tip,
      alt_score_tip,
      confidence_score: Math.min(99, Math.max(1, Math.round(confidence_score))),
      risk_level: this.riskLevel(confidence_score),
      summary_reason,
      factors_used,
      score_distribution,   // null for non-Poisson models
      is_ai_agent,
    };
  },
};
