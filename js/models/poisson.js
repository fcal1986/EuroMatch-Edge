'use strict';

/* ── Model: Poisson ──────────────────────────────────────────────
   Models expected goals (lambda) per team via a Poisson process,
   then builds a full score probability distribution (0–6 × 0–6).
   The most probable score becomes main_score_tip.
   score_distribution (top-5) is passed to the Drawer for display.
   Deps: ModelShared (global)
────────────────────────────────────────────────────────────────── */
const ModelPoisson = {

  KEY:     'poisson',
  NAME:    'Poisson',
  VERSION: '2.0',

  run(m) {
    const diff  = ModelShared.strengthDiff(m);
    const formH = ModelShared.formScore(m.formHome);
    const formA = ModelShared.formScore(m.formAway);

    // Expected goals (λ): league avg home≈1.4, away≈1.1
    const lambdaH = Math.max(0.3, 1.4 + diff * 1.2 + (formH - 0.5) * 0.6);
    const lambdaA = Math.max(0.3, 1.1 - diff * 0.8 + (formA - 0.5) * 0.4);

    // Full score probability grid
    const grid = ModelShared.scoreGrid(lambdaH, lambdaA);

    // Winner and confidence from grid
    const winner = ModelShared.winnerFromGrid(grid);
    const top    = ModelShared.topScores(grid, 5);
    const conf   = top[0] ? top[0].prob * 100 * 2.5 : 50; // scale: single score rarely >40%

    // Top-2 scores → main and alt tip
    const mainTip = top[0] ? top[0].score : ModelShared.scoreTip(lambdaH, lambdaA);
    const altTip  = top[1] ? top[1].score : ModelShared.scoreTip(Math.max(0, lambdaH - 0.5), lambdaA + 0.3);

    return ModelShared.buildOutput({
      model_key:          this.KEY,
      model_name:         this.NAME,
      model_version:      this.VERSION,
      icon_class:         'poisson',
      predicted_winner:   winner,
      main_score_tip:     mainTip,
      alt_score_tip:      altTip,
      confidence_score:   Math.min(85, conf),
      summary_reason:     this._reason(m, winner, lambdaH, lambdaA, top),
      factors_used:       ['Torerwartung', 'Form', 'Stärke'],
      score_distribution: top,
    });
  },

  _reason(m, winner, lH, lA, top) {
    const topScore = top[0] ? top[0].score : '?';
    const topPct   = top[0] ? Math.round(top[0].prob * 100) : 0;
    if (winner === 'draw')
      return `Torerwartung fast gleich (λ ${lH.toFixed(1)}:${lA.toFixed(1)}) – ${topScore} wahrscheinlichstes Ergebnis (${topPct}%).`;
    const side = winner === 'home' ? ModelShared.shortName(m.homeTeam) : ModelShared.shortName(m.awayTeam);
    return `${side} erzielt mehr Tore (λ ${lH.toFixed(1)} vs ${lA.toFixed(1)}). Wahrscheinlichstes Ergebnis: ${topScore} (${topPct}%).`;
  },
};
