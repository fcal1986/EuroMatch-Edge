'use strict';

/* ── Drawer ──────────────────────────────────────────────────────
   Detail drawer / bottom sheet.
   Sections:
     1. Siegwahrscheinlichkeit
     2. Teamstärke + Form
     3. Kicktipp-Empfehlung (weighted consensus)
     4. Modellvergleich (non-AI models, each with score distribution)
     5. AI Agent Meinung
     6. Risiko & Konfidenz
   Deps: ModelEngine, State, fmtTime, formStrip, badgeInfo (all global)
────────────────────────────────────────────────────────────────── */
const Drawer = {

  open(id) {
    const m = State.allMatches.find(x => x.id === id);
    if (!m) return;

    const homeWin = m.predictedWinner === 'home';
    const awayWin = m.predictedWinner === 'away';
    const [bCls, bLabel] = badgeInfo(m);

    // Wochentag aus kickoffAt berechnen (de-DE, z.B. "Samstag")
    const weekday = m.kickoffAt
      ? m.kickoffAt.toLocaleDateString('de-DE', { weekday: 'long' })
      : '';

    document.getElementById('drawer-league').innerHTML =
      `${weekday ? `<span class="dh-weekday">${weekday}</span><span class="dh-sep">·</span>` : ''}` +
      `${m.flag} <span>${m.competitionName}</span>` +
      `<span class="dh-sep">·</span><span class="dh-time">${fmtTime(m.kickoffAt)} Uhr</span>`;
    document.getElementById('dh-teams').textContent =
      `${m.homeTeam}  ×  ${m.awayTeam}`;
    document.getElementById('dh-tags').innerHTML =
      `<span class="badge ${bCls}">${bLabel}</span>` +
      m.riskTags.map(t => `<span class="rtag">${t}</span>`).join('');

    // Run models (cached if available from earlier open)
    const models     = State.modelsByMatch[m.id] || ModelEngine.run(m);
    const ktipp      = ModelEngine.consensus(models);
    const mainModels = ModelEngine.mainModels(models);
    const aiAgent    = ModelEngine.agentResult(models);

    const riskHtml = m.riskTags.length
      ? m.riskTags.map(t => `<span class="rtag-d">${t}</span>`).join('')
      : '<span class="rtag-ok">✓ Keine Risiken erkannt</span>';

    document.getElementById('drawer-body').innerHTML =
      this._sectionProb(m, homeWin, awayWin) +
      this._sectionStrengthForm(m) +
      (ktipp ? this._sectionKtipp(ktipp) : '') +
      this._sectionModelGrid(mainModels) +
      (aiAgent ? this._sectionAIAgent(aiAgent) : '') +
      this._sectionRisk(riskHtml, m);

    document.getElementById('drawer-overlay').classList.add('open');
    document.body.style.overflow = 'hidden';

    let ty0 = 0;
    const dr = document.getElementById('drawer');
    dr.addEventListener('touchstart', e => { ty0 = e.touches[0].clientY; }, { passive: true, once: true });
    dr.addEventListener('touchmove',  e => { if (e.touches[0].clientY - ty0 > 70) Drawer.close(); }, { passive: true, once: true });
  },

  close() {
    document.getElementById('drawer-overlay').classList.remove('open');
    document.body.style.overflow = '';
  },

  /* ── Section renderers ──────────────────────────────────────── */

  _sectionProb(m, homeWin, awayWin) {
    return `
      <div class="sub-label">Siegwahrscheinlichkeit</div>
      <div class="prob3">
        <div class="p3-team ${homeWin ? 'fav' : ''}">
          <div class="p3-name">${m.homeTeam}</div>
          <div class="p3-val">${m.winProbability}%</div>
        </div>
        <div class="p3-draw">
          <div class="p3-draw-label">Remis</div>
          <div class="p3-draw-val">${m.drawProbability}%</div>
        </div>
        <div class="p3-team ${awayWin ? 'fav' : ''}">
          <div class="p3-name">${m.awayTeam}</div>
          <div class="p3-val">${m.awayProbability}%</div>
        </div>
      </div>
      <div class="prob-bar-full">
        <div class="pbf-h" style="width:${m.winProbability}%"></div>
        <div class="pbf-d" style="width:${m.drawProbability}%"></div>
        <div class="pbf-a" style="flex:1"></div>
      </div>`;
  },

  _sectionStrengthForm(m) {
    const hasStrength = m.homeStrength !== null && m.awayStrength !== null;
    return `
      ${hasStrength ? `
        <div class="sub-label">Mannschaftsstärke</div>
        <div class="strength-row">
          <div class="str-team">${m.homeTeam.split(' ')[0]}</div>
          <div class="str-bar">
            <div class="str-home" style="width:${m.homeStrength}%"></div>
            <div class="str-away" style="width:${m.awayStrength}%"></div>
          </div>
          <div class="str-team" style="text-align:right">${m.awayTeam.split(' ').slice(-1)[0]}</div>
        </div>` : ''}
      <div class="sub-label">Letzte 5 Spiele</div>
      <div class="form-grid">
        <div>
          <div class="form-col-name">${m.homeTeam}</div>
          <div class="form-strip">${formStrip(m.formHome)}</div>
        </div>
        <div>
          <div class="form-col-name" style="text-align:right">${m.awayTeam}</div>
          <div class="form-strip" style="justify-content:flex-end">${formStrip(m.formAway)}</div>
        </div>
      </div>`;
  },

  _sectionKtipp(ktipp) {
    const confBadge = {
      strong:   `<span class="ktipp-conf-badge strong">Starker Konsens</span>`,
      moderate: `<span class="ktipp-conf-badge moderate">Moderater Konsens</span>`,
      weak:     `<span class="ktipp-conf-badge weak">Kein Konsens</span>`,
    }[ktipp.confidence] || '';

    const weightInfo = ktipp.weight_sum != null
      ? `Gewicht ${ktipp.weight_sum}/${ktipp.total_weight}`
      : `${ktipp.models_supporting.length} Modelle`;

    return `
      <div class="sub-label">Kicktipp-Empfehlung</div>
      <div class="ktipp-box">
        <div class="ktipp-score">${ktipp.consensus_score || ktipp.score}</div>
        <div class="ktipp-info">
          <div class="ktipp-label">⚽ ${ktipp.backing_models.join(', ')} · ${weightInfo} ${confBadge}</div>
          <div class="ktipp-sub">${ktipp.summary_reason || ktipp.reason}</div>
        </div>
      </div>`;
  },

  _sectionModelGrid(mainModels) {
    return `
      <div class="sub-label">Modellvergleich</div>
      <div class="model-grid">
        ${mainModels.map(mod => this._modelCardHtml(mod)).join('')}
      </div>`;
  },

  _sectionAIAgent(aiAgent) {
    const statusLabel = aiAgent.model_version === 'preview' ? 'Vorschau' : 'LIVE';
    const tipLine = aiAgent.main_score_tip
      ? `<div class="ai-agent-tip">Tipp: <strong>${aiAgent.main_score_tip}</strong> · K: ${aiAgent.confidence_score}%</div>`
      : '';
    return `
      <div class="ai-agent-box">
        <div class="ai-agent-head">
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
            <circle cx="6" cy="6" r="4.5" stroke="currentColor" stroke-width="1.2"/>
            <circle cx="6" cy="6" r="1.5" fill="currentColor"/>
            <path d="M6 1.5V3M6 9v1.5M1.5 6H3M9 6h1.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>
          </svg>
          AI Agent
          <span class="ai-agent-status">${statusLabel}</span>
        </div>
        <div class="ai-agent-text">${aiAgent.summary_reason}</div>
        ${tipLine}
      </div>`;
  },

  _sectionRisk(riskHtml, m) {
    return `
      <div class="sub-label">Risiko-Analyse</div>
      <div class="rtag-wrap">${riskHtml}</div>
      <div class="conf-row">
        <div class="conf-label">Konfidenz</div>
        <div class="conf-track"><div class="conf-fill" style="width:${m.confidenceScore}%"></div></div>
        <div class="conf-val">${m.confidenceScore}%</div>
      </div>`;
  },

  /* ── Model card ─────────────────────────────────────────────── */

  _modelCardHtml(mod) {
    const confCls     = mod.confidence_score >= 65 ? 'high' : mod.confidence_score >= 45 ? 'mid' : 'low';
    const isEnsemble  = mod.model_key === 'ensemble';
    const factorsHtml = mod.factors_used.map(f => `<span class="mrf">${f}</span>`).join('');
    const distHtml    = this._scoreDistHtml(mod.score_distribution);

    return `
      <div class="model-card${isEnsemble ? ' is-ensemble' : ''}">
        <div class="model-head">
          <div class="model-icon ${mod.icon_class}">${this._modelEmoji(mod.model_key)}</div>
          <div class="model-name">${mod.model_name}</div>
          <div class="model-version">v${mod.model_version}</div>
        </div>
        <div class="model-tips">
          <div class="model-tip-cell">
            <div class="mtc-label">Haupttipp</div>
            <div class="mtc-val tip-main">${mod.main_score_tip}</div>
          </div>
          <div class="model-tip-cell">
            <div class="mtc-label">Alternative</div>
            <div class="mtc-val tip-alt">${mod.alt_score_tip}</div>
          </div>
          <div class="model-tip-cell">
            <div class="mtc-label">Konfidenz</div>
            <div class="mtc-val tip-conf ${confCls}">${mod.confidence_score}%</div>
          </div>
        </div>
        ${distHtml}
        <div class="model-reason">
          ${mod.summary_reason}
          <div class="model-reason-factors">${factorsHtml}</div>
        </div>
      </div>`;
  },

  /**
   * Render top-5 score distribution as a compact bar chart.
   * Only shown for models that provide score_distribution (Poisson, DC).
   * @param {Array<{score:string, prob:number}>|null} dist
   */
  _scoreDistHtml(dist) {
    if (!dist || !dist.length) return '';

    // Max prob is the 100% reference for bar widths
    const maxProb = dist[0].prob;

    const bars = dist.map((d, i) => {
      const pct      = Math.round(d.prob * 100);
      const barWidth = maxProb > 0 ? Math.round((d.prob / maxProb) * 100) : 0;
      const isBest   = i === 0;
      return `
        <div class="sdist-row">
          <span class="sdist-score${isBest ? ' sdist-score-top' : ''}">${d.score}</span>
          <div class="sdist-bar-wrap">
            <div class="sdist-bar${isBest ? ' sdist-bar-top' : ''}" style="width:${barWidth}%"></div>
          </div>
          <span class="sdist-pct">${pct}%</span>
        </div>`;
    }).join('');

    return `
      <div class="sdist-wrap">
        <div class="sdist-label">Ergebnisverteilung (Top 5)</div>
        ${bars}
      </div>`;
  },

  _modelEmoji(key) {
    const map = { ensemble: '⚡', elo: '📊', poisson: '📈', dc: '🔬', market: '💹', ai_agent: '🤖' };
    return map[key] ?? '◆';
  },
};
