'use strict';

/* ── AIAgent ─────────────────────────────────────────────────────
   AI Agent module for EuroMatch Edge.

   Architecture:
   - run(m, modelResults) → agent output object
   - inject(matchId, output) → store external API result in State
   - clear(matchId) → remove cached result

   To connect a real AI API later:
   1. Call your API in fetchFromAPI(m, modelResults)
   2. Store the result via AIAgent.inject(m.id, apiResult)
   3. The next time the detail drawer opens, it picks it up automatically

   Output schema (identical to model output schema):
   {
     model_key:       'ai_agent'
     model_name:      'AI Agent'
     model_version:   'preview' | '1.0' | string
     icon_class:      'ai'
     predicted_winner: 'home' | 'away' | 'draw'
     main_score_tip:  string   e.g. '2:1'
     alt_score_tip:   string
     confidence_score: number 0–100
     risk_level:      'low' | 'medium' | 'high'
     summary_reason:  string  (1–2 sentences, plain language)
     factors_used:    string[]
     is_ai_agent:     true
   }

   Deps: ModelShared, State (both global, runtime-safe)
────────────────────────────────────────────────────────────────── */
const AIAgent = {

  MODEL_KEY:  'ai_agent',
  MODEL_NAME: 'AI Agent',

  /* ── Public API ─────────────────────────────────────────────── */

  /**
   * Returns an agent output for match m.
   * Uses State.aiAgentResults[m.id] if an injected result exists,
   * otherwise falls back to a heuristic based on the model ensemble.
   *
   * @param {object} m           - match ViewModel
   * @param {Array}  modelResults - outputs from all regular models
   * @returns {object} agent output
   */
  run(m, modelResults) {
    // 1. Use externally injected result (real API response)
    const cached = State.aiAgentResults[m.id];
    if (cached) {
      return this._fromCached(cached);
    }

    // 2. Derive heuristic opinion from model results
    return this._heuristic(m, modelResults);
  },

  /**
   * Store an external AI API result for a match.
   * Shape of output must match the agent output schema above.
   * Call this after your API responds, before the user opens the drawer.
   *
   * @param {string} matchId
   * @param {object} output  - agent output object
   */
  inject(matchId, output) {
    if (!State || !State.aiAgentResults) {
      console.warn('[AIAgent] State not ready — cannot inject result for', matchId);
      return;
    }
    State.aiAgentResults[matchId] = output;
  },

  /** Remove a cached result (e.g. after data refresh) */
  clear(matchId) {
    if (State && State.aiAgentResults) delete State.aiAgentResults[matchId];
  },

  /* ── Internal ───────────────────────────────────────────────── */

  _fromCached(cached) {
    return {
      model_key:        this.MODEL_KEY,
      model_name:       this.MODEL_NAME,
      model_version:    cached.model_version || '1.0',
      icon_class:       'ai',
      predicted_winner: cached.predicted_winner,
      main_score_tip:   cached.main_score_tip,
      alt_score_tip:    cached.alt_score_tip   || '–',
      confidence_score: cached.confidence_score || 50,
      risk_level:       ModelShared.riskLevel(cached.confidence_score || 50),
      summary_reason:   cached.summary_reason,
      factors_used:     cached.factors_used    || ['API'],
      is_ai_agent:      true,
    };
  },

  /**
   * Heuristic agent opinion derived from existing model outputs.
   * This is used when no real API result is available.
   */
  _heuristic(m, modelResults) {
    // Find ensemble output as reference
    const ensemble = modelResults.find(r => r.model_key === 'ensemble') || modelResults[0];
    if (!ensemble) return this._noData();

    const diff     = ModelShared.strengthDiff(m);
    const formAdv  = ModelShared.formScore(m.formHome) - ModelShared.formScore(m.formAway);
    const cautious = (m.riskTags && m.riskTags.length > 0) || m.riskLevel === 'high';
    const agreement = Math.abs(diff) > 0.25 || Math.abs(formAdv) > 0.25;

    // Check model consensus: do most models agree on winner?
    const winnerVotes = {};
    modelResults.forEach(r => {
      if (r.is_ai_agent) return;
      winnerVotes[r.predicted_winner] = (winnerVotes[r.predicted_winner] || 0) + 1;
    });
    const topWinner = Object.entries(winnerVotes).sort((a,b) => b[1]-a[1])[0];
    const strongConsensus = topWinner && topWinner[1] >= 3;

    const reason = this._buildReason(m, ensemble, cautious, agreement, strongConsensus);

    // Agent is slightly more conservative than ensemble
    const adjConf = Math.max(20, ensemble.confidence_score - (cautious ? 15 : 8));

    return ModelShared.buildOutput({
      model_key:        this.MODEL_KEY,
      model_name:       this.MODEL_NAME,
      model_version:    'preview',
      icon_class:       'ai',
      predicted_winner: ensemble.predicted_winner,
      main_score_tip:   ensemble.main_score_tip,
      alt_score_tip:    ensemble.alt_score_tip,
      confidence_score: adjConf,
      summary_reason:   reason,
      factors_used:     ['Ensemble', 'Risiko', 'Modell-Konsens'],
      is_ai_agent:      true,
    });
  },

  _buildReason(m, ensemble, cautious, agreement, strongConsensus) {
    if (cautious && !agreement) {
      const tag = m.riskTags && m.riskTags[0] ? m.riskTags[0] : 'Risikofaktor';
      return `Agent rät zur Vorsicht: ${tag} aktiv. Prognose unsicher.`;
    }
    if (cautious && strongConsensus) {
      const tag = m.riskTags && m.riskTags[0] ? m.riskTags[0] : 'Risikofaktor';
      return `Agent stimmt dem Ensemble zu, weist aber auf ${tag} hin.`;
    }
    if (!agreement) {
      return 'Agent: Prognose wenig eindeutig – eher konservativ tippen.';
    }
    if (strongConsensus) {
      return 'Agent bestätigt das Ensemble. Starker Modell-Konsens – klarer Tipp.';
    }
    return 'Agent bestätigt das Ensemble-Modell. Kein Widerspruch zu den Hauptfaktoren.';
  },

  _noData() {
    return ModelShared.buildOutput({
      model_key:        this.MODEL_KEY,
      model_name:       this.MODEL_NAME,
      model_version:    'preview',
      icon_class:       'ai',
      predicted_winner: 'draw',
      main_score_tip:   '1:1',
      alt_score_tip:    '0:0',
      confidence_score: 20,
      summary_reason:   'Keine Modelldaten verfügbar – Agent kann keine Einschätzung liefern.',
      factors_used:     [],
      is_ai_agent:      true,
    });
  },

  /* ── Future API hook ────────────────────────────────────────── */
  /**
   * Placeholder for real API call.
   * Replace this method body to call your AI backend.
   * Must return a promise that resolves to an agent output object.
   *
   * Example (Claude API via Anthropic):
   *   const res = await fetch('/api/agent', { method:'POST', body: JSON.stringify({m, modelResults}) });
   *   const data = await res.json();
   *   AIAgent.inject(m.id, data);
   */
  async fetchFromAPI(m, modelResults) {
    // TODO: implement real API call here
    console.log('[AIAgent] fetchFromAPI not implemented — using heuristic');
    return this._heuristic(m, modelResults);
  },
};
