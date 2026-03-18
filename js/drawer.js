'use strict';

/* ── Drawer ──────────────────────────────────────────────────────
   Detail-Drawer / Bottom Sheet.
   Kompatibel mit bestehender App:
   - Drawer.open(id)
   - Drawer.close()
   - Drawer._isOpen
────────────────────────────────────────────────────────────────── */
const Drawer = {
  _isOpen: false,
  _escHandler: null,
  _scrollY: 0,

  open(id) {
    try {
      if (window.Debug?.info) {
        Debug.info('Drawer.open() aufgerufen', { id });
      }

      const m = State.allMatches.find(x => x.id === id);

      if (!m) {
        if (window.Debug?.warn) {
          Debug.warn('Drawer: Match nicht gefunden', {
            id,
            stateCount: State?.allMatches?.length ?? 0
          });
        }
        return;
      }

      if (window.Debug?.inspectMatch) {
        Debug.inspectMatch(m);
      }

      if (window.Debug?.debug) {
        Debug.debug('Drawer Match gefunden', {
          id: m.id,
          homeTeam: m.homeTeam,
          awayTeam: m.awayTeam,
          competitionName: m.competitionName,
          confidenceScore: m.confidenceScore,
          predictedWinner: m.predictedWinner,
          market: m.market || null
        });
      }

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

      const models = State.modelsByMatch[m.id] || (ModelEngine.run ? ModelEngine.run(m) : []) || [];
      const ktipp = ModelEngine.consensus ? ModelEngine.consensus(models) : null;
      const mainModels = ModelEngine.mainModels ? ModelEngine.mainModels(models) : [];
      const aiAgent = ModelEngine.agentResult ? ModelEngine.agentResult(models) : null;

      if (window.Debug?.debug) {
        Debug.debug('Drawer ModelEngine Output', {
          modelsType: Array.isArray(models) ? 'array' : typeof models,
          modelsLength: Array.isArray(models) ? models.length : null,
          consensus: ktipp || null,
          mainModelsLength: Array.isArray(mainModels) ? mainModels.length : null,
          firstModel: Array.isArray(mainModels) && mainModels.length ? mainModels[0] : null,
          aiAgent: aiAgent || null
        });
      }

      const riskHtml = (m.riskTags && m.riskTags.length)
        ? m.riskTags.map(t => `<span class="rtag-d">${t}</span>`).join('')
        : '<span class="rtag-ok">✓ Keine Risiken erkannt</span>';

      const html =
        this._sectionProb(m, homeWin, awayWin) +
        this._sectionStrengthForm(m) +
        this._sectionMarket(m) +
        this._sectionKtipp(ktipp) +
        this._sectionModelGrid(mainModels) +
        this._sectionAIAgent(aiAgent) +
        this._sectionRisk(riskHtml, m);

      document.getElementById('drawer-body').innerHTML = html;

      if (window.Debug?.success) {
        Debug.success('Drawer HTML gerendert', {
          htmlLength: html.length,
          hasMarket: !!m.market,
          hasOdds: !!(m.market && m.market.odds),
          hasModels: Array.isArray(mainModels) && mainModels.length > 0
        });
      }

      this._scrollY = window.scrollY;
      document.body.style.overflow = 'hidden';
      document.body.style.position = 'fixed';
      document.body.style.top = `-${this._scrollY}px`;
      document.body.style.width = '100%';

      document.getElementById('drawer-overlay').classList.add('open');
      this._isOpen = true;

      this._escHandler = (e) => {
        if (e.key === 'Escape') this.close();
      };
      document.addEventListener('keydown', this._escHandler);

      const dr = document.getElementById('drawer');
      let swipeStartY = 0;

      const onTouchStart = (e) => {
        swipeStartY = e.touches[0].clientY;
      };

      const onTouchMove = (e) => {
        if (e.touches[0].clientY - swipeStartY > 70) this.close();
      };

      [dr.querySelector('.drawer-handle'), dr.querySelector('.drawer-head')]
        .forEach(el => {
          if (!el) return;
          el.addEventListener('touchstart', onTouchStart, { passive: true, once: true });
          el.addEventListener('touchmove', onTouchMove, { passive: true, once: true });
        });

      if (window.Debug?.success) {
        Debug.success('Drawer geöffnet', {
          id: m.id,
          overlayOpen: document.getElementById('drawer-overlay')?.classList.contains('open'),
          drawerOpen: this._isOpen
        });
      }
    } catch (err) {
      if (window.Debug?.error) {
        Debug.error('Drawer.open() Fehler', {
          id,
          message: err?.message || String(err),
          stack: err?.stack || null
        });
      }
      throw err;
    }
  },

  close() {
    if (window.Debug?.info) {
      Debug.info('Drawer.close() aufgerufen');
    }

    if (!this._isOpen) {
      if (window.Debug?.warn) {
        Debug.warn('Drawer.close(): Drawer war bereits geschlossen');
      }
      return;
    }

    document.getElementById('drawer-overlay')?.classList.remove('open');
    this._isOpen = false;

    if (this._escHandler) {
      document.removeEventListener('keydown', this._escHandler);
      this._escHandler = null;
    }

    document.body.style.overflow = '';
    document.body.style.position = '';
    document.body.style.top = '';
    document.body.style.width = '';
    window.scrollTo(0, this._scrollY);

    if (window.Debug?.success) {
      Debug.success('Drawer geschlossen', {
        scrollRestore: this._scrollY
      });
    }
  },

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
    const hasStrength =
      m.homeStrength !== null && m.homeStrength !== undefined &&
      m.awayStrength !== null && m.awayStrength !== undefined;

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

    if (window.Debug?.debug) {
      Debug.debug('Drawer._sectionMarket input', market || null);
    }

    const hasOdds =
      market &&
      market.odds &&
      market.odds.home != null &&
      market.odds.draw != null &&
      market.odds.away != null;

    if (!hasOdds) {
      if (window.Debug?.warn) {
        Debug.warn('Drawer Marktblock: keine Odds vorhanden', {
          market: market || null
        });
      }

      return `
        <div class="sub-label">Markt & Value</div>
        <div class="market-box market-box-empty">
          <div class="market-empty-title">📈 Markt</div>
          <div class="market-empty-text">Keine Marktdaten verfügbar.</div>
        </div>`;
    }

    const topValue = this._pickTopValue(m);

    if (window.Debug?.success) {
      Debug.success('Drawer Marktblock: Odds vorhanden', {
        odds: market.odds,
        probs: market.probs,
        edge: market.edge,
        ev: market.ev,
        value: market.value,
        bookmakerCount: market.bookmakerCount,
        overround: market.overround
      });
    }

    return `
      <div class="sub-label">Markt & Value</div>
      <div class="market-box">
        <div class="market-head">
          <div class="market-head-title">📈 Markt</div>
          <div class="market-head-meta">
            ${market.bookmakerCount ?? '–'} Buchmacher
            ${market.overround != null ? ` · Overround ${Number(market.overround).toFixed(3)}` : ''}
          </div>
        </div>

        <div class="market-grid">
          <div class="market-col ${market.value?.home ? 'is-value' : ''}">
            <div class="market-col-title">${m.homeTeam}</div>
            <div class="market-odds">${market.odds.home}</div>
            <div class="market-line">Fair: ${this._fmtPct01(market.probs?.home)}</div>
            <div class="market-line">Edge: ${this._fmtPctSigned01(market.edge?.home)}</div>
            <div class="market-line">EV: ${this._fmtSigned(market.ev?.home)}</div>
            ${market.value?.home ? '<div class="market-badge">Value</div>' : ''}
          </div>

          <div class="market-col ${market.value?.draw ? 'is-value' : ''}">
            <div class="market-col-title">Remis</div>
            <div class="market-odds">${market.odds.draw}</div>
            <div class="market-line">Fair: ${this._fmtPct01(market.probs?.draw)}</div>
            <div class="market-line">Edge: ${this._fmtPctSigned01(market.edge?.draw)}</div>
            <div class="market-line">EV: ${this._fmtSigned(market.ev?.draw)}</div>
            ${market.value?.draw ? '<div class="market-badge">Value</div>' : ''}
          </div>

          <div class="market-col ${market.value?.away ? 'is-value' : ''}">
            <div class="market-col-title">${m.awayTeam}</div>
            <div class="market-odds">${market.odds.away}</div>
            <div class="market-line">Fair: ${this._fmtPct01(market.probs?.away)}</div>
            <div class="market-line">Edge: ${this._fmtPctSigned01(market.edge?.away)}</div>
            <div class="market-line">EV: ${this._fmtSigned(market.ev?.away)}</div>
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
    if (window.Debug?.debug) {
      Debug.debug('Drawer._sectionKtipp input', ktipp || null);
    }

    if (!ktipp) return '';

    const title =
      ktipp.label ||
      ktipp.name ||
      'Konsens';

    const weight =
      ktipp.weight ??
      ktipp.scoreWeight ??
      5;

    const primary =
      ktipp.primary ||
      ktipp.tip ||
      ktipp.mainTip ||
      '–';

    const note =
      ktipp.note ||
      ktipp.text ||
      ktipp.reason ||
      'Keine Zusatzinfo verfügbar.';

    return `
      <div class="sub-label">Kicktipp-Empfehlung</div>
      <div class="kt-card">
        <div class="kt-top">
          <div class="kt-title">⚽ ${title}</div>
          <div class="kt-weight">Gewicht ${weight}/10</div>
        </div>
        <div class="kt-score">${primary}</div>
        <div class="kt-note">${note}</div>
      </div>`;
  },

  _sectionModelGrid(models) {
    if (window.Debug?.debug) {
      Debug.debug('Drawer._sectionModelGrid input', {
        isArray: Array.isArray(models),
        length: Array.isArray(models) ? models.length : null,
        sample: Array.isArray(models) && models.length ? models[0] : null
      });
    }

    if (!Array.isArray(models) || !models.length) {
      if (window.Debug?.warn) {
        Debug.warn('Drawer Modellvergleich: keine Modelldaten');
      }

      return `
        <div class="sub-label">Modellvergleich</div>
        <div class="model-card">
          <div class="mc-text">Keine Modelldaten verfügbar.</div>
        </div>`;
    }

    return `
      <div class="sub-label">Modellvergleich</div>
      <div class="model-grid">
        ${models.map(m => this._modelCard(m)).join('')}
      </div>`;
  },

  _modelCard(model) {
    if (!model) return '';

    if (window.Debug?.debug) {
      Debug.debug('Drawer._modelCard input', model);
    }

    const name =
      model.name ||
      model.label ||
      model.model ||
      'Modell';

    const version =
      model.version ||
      model.ver ||
      '';

    const primary =
      model.primary ||
      model.tip ||
      model.mainTip ||
      '–';

    const alternative =
      model.alternative ||
      model.alt ||
      model.secondary ||
      '–';

    const confidence =
      model.confidence ??
      model.conf ??
      null;

    const text =
      model.text ||
      model.note ||
      model.reason ||
      '';

    const tags = Array.isArray(model.tags)
      ? model.tags.map(t => `<span class="mf">${t}</span>`).join('')
      : '';

    const dist = Array.isArray(model.distribution) && model.distribution.length
      ? `
        <div class="mc-dist">
          ${model.distribution.map(d => `
            <div class="mc-dist-row">
              <span>${d.score ?? '–'}</span>
              <div class="mc-dist-bar"><i style="width:${d.prob ?? 0}%"></i></div>
              <span>${d.prob ?? 0}%</span>
            </div>`).join('')}
        </div>`
      : '';

    return `
      <div class="model-card">
        <div class="mc-head">
          <div class="mc-name">${name}</div>
          <div class="mc-ver">${version}</div>
        </div>
        <div class="mc-kpis">
          <div><span>Haupttipp</span><strong>${primary}</strong></div>
          <div><span>Alternative</span><strong>${alternative}</strong></div>
          <div><span>Konfidenz</span><strong>${confidence !== null ? `${confidence}%` : '–'}</strong></div>
        </div>
        ${text ? `<div class="mc-text">${text}</div>` : ''}
        ${dist}
        ${tags ? `<div class="mc-tags">${tags}</div>` : ''}
      </div>`;
  },

  _sectionAIAgent(ai) {
    if (window.Debug?.debug) {
      Debug.debug('Drawer._sectionAIAgent input', ai || null);
    }

    if (!ai) return '';

    const text =
      ai.text ||
      ai.note ||
      ai.reason ||
      '–';

    const tip =
      ai.tip ||
      ai.primary ||
      '–';

    const confidence =
      ai.confidence ??
      ai.conf ??
      null;

    return `
      <div class="sub-label">AI Agent</div>
      <div class="ai-box">
        <div class="ai-title">Vorschau</div>
        <div class="ai-text">${text}</div>
        <div class="ai-tip">Tipp: <strong>${tip}</strong> · K: ${confidence !== null ? `${confidence}%` : '–'}</div>
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