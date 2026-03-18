'use strict';

const Drawer = (() => {
  let _currentId = null;

  function _fmtPct01(v) {
    if (v === null || v === undefined) return '–';
    return `${Math.round(Number(v) * 100)}%`;
  }

  function _fmtPctSigned01(v) {
    if (v === null || v === undefined) return '–';
    const n = Number(v) * 100;
    return `${n >= 0 ? '+' : ''}${n.toFixed(1)}%`;
  }

  function _fmtSigned(v) {
    if (v === null || v === undefined) return '–';
    const n = Number(v);
    return `${n >= 0 ? '+' : ''}${n.toFixed(2)}`;
  }

  function _pickTopValue(m) {
    const market = m.market;
    if (!market) return null;

    const rows = [
      { key: 'home', label: m.homeTeam, edge: market.edge?.home, ev: market.ev?.home, value: !!market.value?.home },
      { key: 'draw', label: 'Remis', edge: market.edge?.draw, ev: market.ev?.draw, value: !!market.value?.draw },
      { key: 'away', label: m.awayTeam, edge: market.edge?.away, ev: market.ev?.away, value: !!market.value?.away }
    ].filter(x => x.value);

    if (!rows.length) return null;
    rows.sort((a, b) => Number(b.ev ?? -999) - Number(a.ev ?? -999));
    return rows[0];
  }

  function _sectionMarket(m) {
    const market = m.market;

    if (!market || !market.hasOdds) {
      return `
        <div class="sub-label">Markt & Value</div>
        <div class="market-box market-box-empty">
          <div class="market-empty-title">📈 Markt</div>
          <div class="market-empty-text">Keine Marktdaten verfügbar.</div>
        </div>`;
    }

    const topValue = _pickTopValue(m);

    return `
      <div class="sub-label">Markt & Value</div>
      <div class="market-box">
        <div class="market-head">
          <div class="market-head-title">📈 Marktquoten</div>
          <div class="market-head-meta">
            ${market.bookmakerCount ?? '–'} Buchmacher
          </div>
        </div>

        <div class="market-grid">
          ${_marketCol(m.homeTeam, market.odds.home, market.probs.home, market.edge.home, market.ev.home, market.value.home)}
          ${_marketCol('Remis', market.odds.draw, market.probs.draw, market.edge.draw, market.ev.draw, market.value.draw)}
          ${_marketCol(m.awayTeam, market.odds.away, market.probs.away, market.edge.away, market.ev.away, market.value.away)}
        </div>

        <div class="market-footer">
          ${
            topValue
              ? `⭐ Top-Value: <strong>${topValue.label}</strong> · Edge ${_fmtPctSigned01(topValue.edge)} · EV ${_fmtSigned(topValue.ev)}`
              : 'Kein klares Value-Signal.'
          }
        </div>
      </div>`;
  }

  function _marketCol(label, odds, prob, edge, ev, isValue) {
    return `
      <div class="market-col ${isValue ? 'is-value' : ''}">
        <div class="market-col-title">${label}</div>
        <div class="market-odds">${odds ?? '–'}</div>
        <div class="market-line">Fair: ${_fmtPct01(prob)}</div>
        <div class="market-line">Edge: ${_fmtPctSigned01(edge)}</div>
        <div class="market-line">EV: ${_fmtSigned(ev)}</div>
        ${isValue ? '<div class="market-badge">Value</div>' : ''}
      </div>`;
  }

  return {
    open(match) {
      const el = document.getElementById('drawer-body');

      el.innerHTML =
        _sectionMarket(match);
    }
  };
})();