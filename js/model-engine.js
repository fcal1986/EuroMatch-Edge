'use strict';

/* ── ModelEngine ─────────────────────────────────────────────────
   Orchestrator — runs models, computes weighted consensus.

   Load order:
     models/shared.js → models/ensemble.js → models/elo.js →
     models/poisson.js → models/dixon-coles.js → models/market.js →
     agents/ai-agent.js → model-engine.js

   Model output schema:
   {
     model_key, model_name, model_version, icon_class,
     predicted_winner: 'home'|'away'|'draw',
     main_score_tip, alt_score_tip,
     confidence_score, risk_level,
     summary_reason, factors_used,
     score_distribution: [{score, prob}] | null,
     is_ai_agent: boolean
   }
────────────────────────────────────────────────────────────────── */
const ModelEngine = {

  /* ── Registry ───────────────────────────────────────────────── */

  MODELS: [
    ModelEnsemble,
    ModelElo,
    ModelPoisson,
    ModelDixonColes,
    ModelMarket,
  ],

  /* ── Consensus weights ──────────────────────────────────────── */
  /**
   * Weight per model_key for weighted consensus.
   * Higher weight = more influence on the recommended score.
   * Adjust as empirical evidence accumulates.
   * Falls back to weight=1 for unregistered keys.
   */
  WEIGHTS: {
    ensemble: 3,
    poisson:  2,
    elo:      2,
    dc:       2,
    market:   1,
  },

  /* ── Run ─────────────────────────────────────────────────────── */

  run(m) {
    const results = this.MODELS.map(model => {
      try {
        return model.run(m);
      } catch (err) {
        console.warn(`[ModelEngine] ${model.NAME} failed:`, err);
        return null;
      }
    }).filter(Boolean);

    // Cache in State (runtime-safe — State defined later in load order)
    if (typeof State !== 'undefined' && State.modelsByMatch) {
      State.modelsByMatch[m.id] = results;
    }

    // AI agent runs after deterministic models
    const agent = AIAgent.run(m, results);
    results.push(agent);

    return results;
  },

  /* ── Weighted Consensus ─────────────────────────────────────── */

  /**
   * Derive Kicktipp consensus using model weights.
   * AI agent is excluded from the vote.
   *
   * Algorithm:
   *   For each non-AI model → look up its weight (default 1).
   *   Accumulate weight_sum per main_score_tip.
   *   Winner = score with highest weight_sum.
   *   Confidence band based on weight_sum / total_weight ratio.
   *
   * Falls back to unweighted count if all weights are 1 (equal).
   *
   * @param  {object[]} models  array returned by run()
   * @returns {{
   *   consensus_score:    string,
   *   weight_sum:         number,
   *   total_weight:       number,
   *   models_supporting:  string[],
   *   backing_models:     string[],   (alias)
   *   summary_reason:     string,
   *   confidence:         'strong'|'moderate'|'weak',
   *   score:              string,     (alias for drawer.js)
   *   reason:             string,     (alias for drawer.js)
   * } | null}
   */
  consensus(models) {
    const voters = models.filter(mod => !mod.is_ai_agent);
    if (!voters.length) return null;

    // Accumulate weights per score
    const scores = {};
    let totalWeight = 0;

    voters.forEach(mod => {
      const w     = this.WEIGHTS[mod.model_key] ?? 1;
      const score = mod.main_score_tip;
      if (!scores[score]) scores[score] = { weight_sum: 0, models: [] };
      scores[score].weight_sum += w;
      scores[score].models.push(mod.model_name);
      totalWeight += w;
    });

    // Pick winning score
    const sorted = Object.entries(scores)
      .sort((a, b) => b[1].weight_sum - a[1].weight_sum);
    const [topScore, topData] = sorted[0];

    const weightRatio = totalWeight > 0 ? topData.weight_sum / totalWeight : 0;
    const confidence  = weightRatio >= 0.55 ? 'strong'
                      : weightRatio >= 0.35 ? 'moderate' : 'weak';

    const summary_reason = this._consensusReason(
      topScore, topData.weight_sum, totalWeight, topData.models, confidence
    );

    return {
      consensus_score:   topScore,
      weight_sum:        topData.weight_sum,
      total_weight:      totalWeight,
      models_supporting: topData.models,
      backing_models:    topData.models,   // alias
      summary_reason,
      confidence,
      // Legacy aliases expected by drawer.js
      score:  topScore,
      count:  topData.models.length,
      reason: summary_reason,
    };
  },

  _consensusReason(score, weightSum, totalWeight, modelNames, confidence) {
    const ratio = Math.round((weightSum / totalWeight) * 100);
    if (confidence === 'strong')
      return `${modelNames.join(', ')} erwarten ${score} – Gewichts-Konsens ${ratio}%.`;
    if (confidence === 'moderate')
      return `${modelNames.join(', ')} tendieren zu ${score} (${ratio}% Gewicht). Solider Tipp.`;
    return `Kein starker Konsens – ${score} hat ${ratio}% Modellgewicht. Vorsicht.`;
  },

  /* ── Helpers ─────────────────────────────────────────────────── */

  mainModels(models)  { return models.filter(mod => !mod.is_ai_agent); },
  agentResult(models) { return models.find(mod => mod.is_ai_agent) || null; },
};
