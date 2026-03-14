'use strict';

/* ── Model: Ensemble ─────────────────────────────────────────────
   Weighted combination of the Supabase pipeline output,
   team-strength differential and recent form.
   Deps: ModelShared (global)
────────────────────────────────────────────────────────────────── */
const ModelEnsemble = {

  KEY:     'ensemble',
  NAME:    'Ensemble',
  VERSION: '1.0',

  run(m) {
    const diff    = ModelShared.strengthDiff(m);
    const formH   = ModelShared.formScore(m.formHome);
    const formA   = ModelShared.formScore(m.formAway);
    const formAdv = formH - formA;

    // Blend pipeline win-prob (60 %) with strength+form signal (40 %)
    const raw      = (m.winProbability / 100) * 0.6 + (0.5 + diff * 0.25 + formAdv * 0.15);
    const homeWinP = Math.min(0.95, Math.max(0.05, raw / 1.6));
    const winner   = homeWinP > 0.55 ? 'home' : homeWinP < 0.40 ? 'away' : 'draw';
    const conf     = 50 + Math.abs(homeWinP - 0.5) * 80;

    const hg = 1.2 + diff * 1.5 + formAdv * 0.5;
    const ag = 0.7 - diff * 0.8 + (1 - formAdv) * 0.3;

    return ModelShared.buildOutput({
      model_key:        this.KEY,
      model_name:       this.NAME,
      model_version:    this.VERSION,
      icon_class:       'ensemble',
      predicted_winner: winner,
      main_score_tip:   ModelShared.scoreTip(hg, ag),
      alt_score_tip:    ModelShared.scoreTip(Math.max(0, hg - 1), ag),
      confidence_score: conf,
      summary_reason:   this._reason(m, winner, diff, formAdv),
      factors_used:     ['Stärke', 'Form', 'Pipeline', 'Heimvorteil'],
    });
  },

  _reason(m, winner, diff, formAdv) {
    if (winner === 'draw') return 'Beide Teams liegen nah beieinander. Remis ist eine reale Option.';
    const side = winner === 'home' ? ModelShared.shortName(m.homeTeam) : ModelShared.shortName(m.awayTeam);
    if (Math.abs(diff) > 0.4)    return `${side} ist klar stärker und sollte dieses Spiel gewinnen.`;
    if (Math.abs(formAdv) > 0.3) return `${side} bringt die bessere Tagesform mit – knappes Ergebnis erwartet.`;
    return `Leichter Vorteil für ${side}, aber das Spiel bleibt eng.`;
  },
};
