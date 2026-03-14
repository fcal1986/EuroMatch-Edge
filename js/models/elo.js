'use strict';

/* ── Model: Elo ──────────────────────────────────────────────────
   Classic Elo-style win probability based on team-strength rating.
   Home advantage: +0.1 in the logistic exponent.
   Deps: ModelShared (global)
────────────────────────────────────────────────────────────────── */
const ModelElo = {

  KEY:     'elo',
  NAME:    'Elo',
  VERSION: '1.0',

  run(m) {
    const diff     = ModelShared.strengthDiff(m);
    // Standard Elo formula: P = 1 / (1 + 10^(−d))
    // d includes home advantage (+0.1)
    const homeWinP = 1 / (1 + Math.pow(10, -(diff * 4 + 0.1)));
    const winner   = homeWinP > 0.58 ? 'home' : homeWinP < 0.38 ? 'away' : 'draw';
    const conf     = 45 + Math.abs(homeWinP - 0.5) * 90;

    const hg = 1.0 + diff * 1.8;
    const ag = 0.9 - diff * 0.9;

    return ModelShared.buildOutput({
      model_key:        this.KEY,
      model_name:       this.NAME,
      model_version:    this.VERSION,
      icon_class:       'elo',
      predicted_winner: winner,
      main_score_tip:   ModelShared.scoreTip(hg, ag),
      alt_score_tip:    ModelShared.scoreTip(Math.max(1, hg), Math.max(0, ag - 1)),
      confidence_score: conf,
      summary_reason:   this._reason(m, winner, diff),
      factors_used:     ['Teamstärke', 'Heimvorteil'],
    });
  },

  _reason(m, winner, diff) {
    if (m.homeStrength == null) return 'Keine Stärkedaten – Elo kann nicht berechnet werden.';
    if (winner === 'draw')      return 'Stärke-Unterschied zu gering für eine klare Empfehlung.';
    const side = winner === 'home' ? ModelShared.shortName(m.homeTeam) : ModelShared.shortName(m.awayTeam);
    if (Math.abs(diff) > 0.4)  return `${side} liegt im Elo-Rating deutlich vorne. Sieg sehr wahrscheinlich.`;
    return `${side} hat einen leichten Elo-Vorsprung, aber der Gegner ist konkurrenzfähig.`;
  },
};
