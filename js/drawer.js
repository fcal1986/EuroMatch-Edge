'use strict';

/* ── Drawer ──────────────────────────────────────────────────────
   Detail-Drawer / Bottom Sheet.
   Kompatibel mit bestehender App:
   - Drawer.open(id)
   - Drawer.close()
   - Drawer._isOpen
────────────────────────────────────────────────────────────────── */
const Drawer = {

  /* ── Interner State ──────────────────────────────────────── */
  _isOpen:     false,
  _escHandler: null,
  _scrollY:    0,

  /* ══════════════════════════════════════════════════════════
     OPEN
  ══════════════════════════════════════════════════════════ */
  open(id) {
    const m = State.allMatches.find(x => x.id === id);
    if (!m) return;

    const homeWin = m.predictedWinner === 'home';
    const awayWin = m.predictedWinner === 'away';
    const [bCls, bLabel] = badgeInfo(m);

    const weekday = m.kickoffAt
      ? m.kickoffAt.toLocaleDateString('de-DE', { weekday: 'long' })
      : '';

    document.getElementById('drawer-league').innerHTML =
      (weekday
        ? `<span class="dh-weekday">${weekday}</span><span class="dh-sep">·</span>`
        : '') +
      `${m.flag} <span>${m.competitionName}</span>` +
      `<span class="dh-sep">·</span>` +
      `<span class="dh-time">${fmtTime(m.kickoffAt)} Uhr</span>`;

    document.getElementById('dh-teams').textContent =
      `${m.homeTeam}  ×  ${m.awayTeam}`;

    document.getElementById('dh-tags').innerHTML =
      `<span class="badge ${bCls}">${bLabel}</span>` +
      (m.riskTags || []).map(t => `<span class="rtag">${t}</span>`).join('');

    const models     = State.modelsByMatch[m.id] || ModelEngine.run(m);
    const ktipp      = ModelEngine.consensus(models);
    const mainModels = ModelEngine.mainModels(models);
    const aiAgent    = ModelEngine.agentResult(models);

    const riskHtml = (m.riskTags && m.riskTags.length)
      ? m.riskTags.map(t => `<span class="rtag-d">${t}</span>`).join('')
      : '<span class="rtag-ok">✓ Keine Risiken erkannt</span>';

    document.getElementById('drawer-body').innerHTML =
      this._sectionProb(m, homeWin, awayWin) +
      this._sectionStrengthForm(m) +
      this._sectionMarket(m) +
      (ktipp ? this._sectionKtipp(ktipp) : '') +
      this._sectionModelGrid(mainModels) +
      (aiAgent ? this._sectionAIAgent(aiAgent) : '') +
      this._sectionRisk(riskHtml, m);

    this._scrollY = window.scrollY;
    document.body.style.overflow = 'hidden';
    document.body.style.position = 'fixed';
    document.body.style.top      = `-${this._scrollY}px`;
    document.body.style.width    = '100%';

    document.getElementById('drawer-overlay').classList.add('open');
    this._isOpen = true;

    this._escHandler = (e) => { if (e.key === 'Escape') this.close(); };
    document.addEventListener('keydown', this._escHandler);

    const dr = document.getElementById('drawer');
    let swipeStartY = 0;

    const onTouchStart = (e) => { swipeStartY = e.touches[0].clientY; };
    const onTouchMove  = (e) => {
      if (e.touches[0].clientY - swipeStartY > 70) this.close();
    };

    [dr.querySelector('.drawer-handle'), dr.querySelector('.drawer-head')]
      .forEach(el => {
        if (!el) return;
        el.addEventListener('touchstart', onTouchStart, { passive: true, once: true });
        el.addEventListener('touchmove',  onTouchMove,  { passive: true, once: true });
      });
  },

  /* ══════════════════════════════════════════════════════════
     CLOSE
  ══════════════════════════════════════════════════════════ */
  close() {
    if (!this._isOpen) return;

    document.getElementById('drawer-overlay').classList.remove('open');
    this._isOpen = false;

    if (this._escHandler) {
      document.removeEventListener('keydown', this._escHandler);
      this._escHandler = null;
    }

    document.body.style.overflow = '';
    document.body.style.position = '';
    document.body.style.top      = '';
    document.body.style.width    = '';
    window.scrollTo(0, this._scrollY);
  },

  /* ══════════════════════════════════════════════════════════
     HELPERS
  ══════════════════════════════════════════════════════════ */

  _fmtPct01(v) {
    if (v === null || v === undefined) return '–';
    return `${Math.round(Number(v) * 100)}%`;
  },

  _fmtPctSigned01(v) {
    if (v === null || v === undefined) return '–';
    const n = Number(v) * 100;
    return `${n >= 0 ? '+' : ''}${n.toFixed(1)}%`;
  },

  _fmtSigned(v) {
    if (v === null || v === undefined) return '–';
    const n = Number(v);
    return `${n >= 0 ? '+' : ''}${n.toFixed(2)}`;
  },

  _pickTopValue(m) {
    const market = m.market;
    if (!market) return null;

    const rows = [
      {
        key: 'home',
        label: m.homeTeam,
        edge: market.edge?.home,
        ev: market.ev?.home,
        value: !!market.value?.home,
      },
      {
        key: 'draw',
        label: 'Remis',
        edge: market.edge?.draw,
        ev: market.ev?.draw,
        value: !!market.value?.draw,
      },
      {
        key: 'away',
        label: m.awayTeam,
        edge: market.edge?.away,
        ev: market.ev?.away,
        value: !!market.value?.away,
      },
    ].filter(x => x.value);

    if (!rows.length) return null;
    rows.sort((a, b) => Number(b.ev ?? -999) - Number(a.ev ?? -999));
    return rows[0];
  },

  /* ══════════════════════════════════════════════════════════
     SECTION RENDERERS
  ══════════════════════════════════════════════════════════ */

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
          <div class="form-col-name">${m.awayTeam}</div>
          <div class="form-strip">${formStrip(m.formAway)}</div>
        </div>
      </div>`;
  },

  _sectionMarket(m) {
    const market = m.market;

    if (!market || !market.hasOdds) {
      return `
        <div class="sub-label">Markt & Value</div>
        <div class="market-box market-box-empty">
          <div class="market-empty-title">📈 Markt</div>
          <div class="market-empty-text">Keine Marktdaten verfügbar.</div>
        </div>`;
    }

    const topValue = this._pickTopValue(m);

    return `
      <div class="sub-label">Markt & Value</div>
      <div class="market-box">
        <div class="market-head">
          <div class="market-head-title">📈 Marktquoten</div>
          <div class="market-head-meta">
            ${market.bookmakerCount ?? '–'} Buchmacher
            ${market.overround !== null && market.overround !== undefined
              ? ` · Overround ${Number(market.overround).toFixed(3)}`
              : ''}
          </div>
        </div>

        <div class="market-grid">
          <div class="market-col ${market.value?.home ? 'is-value' : ''}">
            <div class="market-col-title">${m.homeTeam}</div>
            <div class="market-odds">${market.odds.home ?? '–'}</div>
            <div class="market-line">Fair: ${this._fmtPct01(market.probs.home)}</div>
            <div class="market-line">Edge: ${this._fmtPctSigned01(market.edge.home)}</div>
            <div class="market-line">EV: ${this._fmtSigned(market.ev.home)}</div>
            ${market.value?.home ? '<div class="market-badge">Value</div>' : ''}
          </div>

          <div class="market-col ${market.value?.draw ? 'is-value' : ''}">
            <div class="market-col-title">Remis</div>
            <div class="market-odds">${market.odds.draw ?? '–'}</div>
            <div class="market-line">Fair: ${this._fmtPct01(market.probs.draw)}</div>
            <div class="market-line">Edge: ${this._fmtPctSigned01(market.edge.draw)}</div>
            <div class="market-line">EV: ${this._fmtSigned(market.ev.draw)}</div>
            ${market.value?.draw ? '<div class="market-badge">Value</div>' : ''}
          </div>

          <div class="market-col ${market.value?.away ? 'is-value' : ''}">
            <div class="market-col-title">${m.awayTeam}</div>
            <div class="market-odds">${market.odds.away ?? '–'}</div>
            <div class="market-line">Fair: ${this._fmtPct01(market.probs.away)}</div>
            <div class="market-line">Edge: ${this._fmtPctSigned01(market.edge.away)}</div>
            <div class="market-line">EV: ${this._fmtSigned(market.ev.away)}</div>
            ${market.value?.away ? '<div class="market-badge">Value</div>' : ''}
          </div>
        </div>

        <div class="market-footer">
          ${
            topValue
              ? `⭐ Top-Value: <strong>${topValue.label}</strong> · Edge ${this._fmtPctSigned01(topValue.edge)} · EV ${this._fmtSigned(topValue.ev)}`
              : 'Kein klares Value-Signal.'
          }
        </div>
      </div>`;
  },

  _sectionKtipp(ktipp) {
    return `
      <div class="sub-label">Kicktipp-Empfehlung</div>
      <div class="kt-card">
        <div class="kt-top">
          <div class="kt-title">⚽ ${ktipp.label}</div>
          <div class="kt-weight">Gewicht ${ktipp.weight}/10</div>
        </div>
        <div class="kt-score">${ktipp.primary}</div>
        <div class="kt-note">${ktipp.note}</div>
      </div>`;
  },

  _sectionModelGrid(models) {
    if (!models || !models.length) return '';
    return `
      <div class="sub-label">Modellvergleich</div>
      <div class="model-grid">
        ${models.map(m => this._modelCard(m)).join('')}
      </div>`;
  },

  _modelCard(model) {
    const tags = (model.tags || []).map(t => `<span class="mf">${t}</span>`).join('');
    const dist = Array.isArray(model.distribution) && model.distribution.length
      ? `
        <div class="mc-dist">
          ${model.distribution.map(d => `
            <div class="mc-dist-row">
              <span>${d.score}</span>
              <div class="mc-dist-bar"><i style="width:${d.prob}%"></i></div>
              <span>${d.prob}%</span>
            </div>`).join('')}
        </div>`
      : '';

    return `
      <div class="model-card">
        <div class="mc-head">
          <div class="mc-name">${model.name}</div>
          <div class="mc-ver">${model.version || ''}</div>
        </div>
        <div class="mc-kpis">
          <div><span>Haupttipp</span><strong>${model.primary || '–'}</strong></div>
          <div><span>Alternative</span><strong>${model.alternative || '–'}</strong></div>
          <div><span>Konfidenz</span><strong>${model.confidence ?? '–'}%</strong></div>
        </div>
        ${model.text ? `<div class="mc-text">${model.text}</div>` : ''}
        ${dist}
        ${tags ? `<div class="mc-tags">${tags}</div>` : ''}
      </div>`;
  },

  _sectionAIAgent(ai) {
    return `
      <div class="sub-label">AI Agent</div>
      <div class="ai-box">
        <div class="ai-title">Vorschau</div>
        <div class="ai-text">${ai.text || '–'}</div>
        <div class="ai-tip">Tipp: <strong>${ai.tip || '–'}</strong> · K: ${ai.confidence ?? '–'}%</div>
      </div>`;
  },

  _sectionRisk(riskHtml, m) {
    return `
      <div class="sub-label">Risiko-Analyse</div>
      <div class="risk-box">${riskHtml}</div>
      <div class="conf-box">
        <div class="conf-label">Konfidenz</div>
        <div class="conf-bar">
          <i style="width:${m.confidenceScore}%"></i>
        </div>
        <div class="conf-val">${m.confidenceScore}%</div>
      </div>`;
  },
};