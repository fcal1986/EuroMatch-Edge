'use strict';

/* ── Model: Market ───────────────────────────────────────────────
   Converts bookmaker odds (marketSignalHome) into implied
   win probability, then derives a score tip.
   Bookmakers have ~10 % margin built in, so we apply a 0.9 factor
   to the raw implied probability (overround correction).
   Deps: ModelShared (global)
────────────────────────────────────────────────────────────────── */
const ModelMarket = {

  KEY:     'market',
  NAME:    'Markt',
  VERSION: '1.0',

  /** Bookmaker margin correction factor */
  MARGIN_FACTOR: 0.9,

  run(m) {
    const quote        = m.marketSignalHome ?? 2.0;
    const impliedHomeP = Math.min(0.92, Math.max(0.08, (1 / quote) * this.MARGIN_FACTOR));
    const winner       = impliedHomeP > 0.55 ? 'home' : impliedHomeP < 0.40 ? 'away' : 'draw';
    const conf         = 40 + impliedHomeP * 45;

    const diff = ModelShared.strengthDiff(m);
    const hg   = 0.9 + diff * 1.2;
    const ag   = 0.8 - diff * 0.6;

    return ModelShared.buildOutput({
      model_key:        this.KEY,
      model_name:       this.NAME,
      model_version:    this.VERSION,
      icon_class:       'market',
      predicted_winner: winner,
      main_score_tip:   ModelShared.scoreTip(hg, ag),
      alt_score_tip:    ModelShared.scoreTip(hg, ag + 1),
      confidence_score: conf,
      summary_reason:   this._reason(m, winner, quote),
      factors_used:     ['Marktquoten', 'Implied Probability'],
    });
  },

  _reason(m, winner, quote) {
    if (!m.marketSignalHome) return 'Keine Marktdaten verfügbar.';
    if (winner === 'draw')   return `Markt sieht ein offenes Spiel – Quote Heimsieg: ${quote.toFixed(2)}.`;
    const side = winner === 'home' ? ModelShared.shortName(m.homeTeam) : ModelShared.shortName(m.awayTeam);
    if (quote <= 1.30) return `Sehr niedrige Quote (${quote.toFixed(2)}) – Markt ist sich sehr sicher bei ${side}.`;
    if (quote <= 1.60) return `Klare Markt-Favorisierung von ${side} (Quote ${quote.toFixed(2)}).`;
    return `Markt sieht ${side} leicht vorne, aber das Rennen ist offen (Quote ${quote.toFixed(2)}).`;
  },
};
