'use strict';

/* ── Model: Dixon–Coles ──────────────────────────────────────────
   Extends Poisson with the Dixon–Coles (1997) low-score correction.
   Adjusts 0:0 / 1:0 / 0:1 / 1:1 probabilities using parameter ρ
   so that draws and tight scorelines are less under-predicted.
   Uses ModelShared.applyDixonColes() on the full score grid.
   Deps: ModelShared (global)
────────────────────────────────────────────────────────────────── */
const ModelDixonColes = {

  KEY:     'dc',
  NAME:    'Dixon–Coles',
  VERSION: '2.0',

  /** ρ (rho): correction strength — 0 = pure Poisson, 0.13 = standard DC */
  RHO: 0.13,

  run(m) {
    const diff  = ModelShared.strengthDiff(m);
    const formH = ModelShared.formScore(m.formHome);
    const formA = ModelShared.formScore(m.formAway);

    const lambdaH = Math.max(0.3, 1.3 + diff * 1.0 + (formH - 0.5) * 0.5);
    const lambdaA = Math.max(0.3, 1.0 - diff * 0.7 + (formA - 0.5) * 0.4);

    // Build Poisson grid then apply DC correction
    const rawGrid  = ModelShared.scoreGrid(lambdaH, lambdaA);
    const grid     = ModelShared.applyDixonColes(rawGrid, lambdaH, lambdaA, this.RHO);

    const winner   = ModelShared.winnerFromGrid(grid);
    const top      = ModelShared.topScores(grid, 5);
    const conf     = top[0] ? top[0].prob * 100 * 2.5 : 50;

    const mainTip  = top[0] ? top[0].score : ModelShared.scoreTip(lambdaH, lambdaA);
    const altTip   = top[1] ? top[1].score : '1:1';

    // Check if DC actually shifted the recommendation vs pure Poisson
    const drawBoostActive = Math.abs(diff) < 0.15 && lambdaH < 1.5 && lambdaA < 1.5;

    return ModelShared.buildOutput({
      model_key:          this.KEY,
      model_name:         this.NAME,
      model_version:      this.VERSION,
      icon_class:         'dc',
      predicted_winner:   winner,
      main_score_tip:     mainTip,
      alt_score_tip:      altTip,
      confidence_score:   Math.min(85, conf),
      summary_reason:     this._reason(m, winner, drawBoostActive, top),
      factors_used:       ['Torerwartung', 'DC-Korrektur', 'Stärke'],
      score_distribution: top,
    });
  },

  _reason(m, winner, drawBoostActive, top) {
    const topScore = top[0] ? top[0].score : '?';
    const topPct   = top[0] ? Math.round(top[0].prob * 100) : 0;
    if (drawBoostActive)
      return `DC-Korrektur aktiv: Remis und knappe Ergebnisse wahrscheinlicher. Wahrscheinlichstes Ergebnis: ${topScore} (${topPct}%).`;
    if (winner === 'draw')
      return `Remis leicht wahrscheinlicher als Basismodelle zeigen. Wahrscheinlichstes Ergebnis: ${topScore} (${topPct}%).`;
    const side = winner === 'home' ? ModelShared.shortName(m.homeTeam) : ModelShared.shortName(m.awayTeam);
    return `${side} laut DC-Modell vorne. Wahrscheinlichstes Ergebnis: ${topScore} (${topPct}%).`;
  },
};
