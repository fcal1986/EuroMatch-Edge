'use strict';

/* ── Debug HUD / Dialog ─────────────────────────────────────── */

const Debug = (() => {
  let enabled = true;
  let selectedMatchId = null;
  let refreshTimer = null;

  function el(id) {
    return document.getElementById(id);
  }

  function safeJson(value) {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

  function short(v) {
    if (v === null) return 'null';
    if (v === undefined) return 'undefined';
    if (Array.isArray(v)) return `Array(${v.length})`;
    if (typeof v === 'object') return 'object';
    return String(v);
  }

  function boolCls(v) {
    return v ? 'dbg-ok' : 'dbg-bad';
  }

  function row(label, value, ok = null) {
    const cls = ok === null ? 'dbg-neutral' : (ok ? 'dbg-ok' : 'dbg-bad');
    return `
      <div class="dbg-row">
        <div class="dbg-k">${label}</div>
        <div class="dbg-v ${cls}">${value}</div>
      </div>
    `;
  }

  function section(title, inner) {
    return `
      <div class="dbg-section">
        <div class="dbg-title">${title}</div>
        ${inner}
      </div>
    `;
  }

  function findCurrentMatch() {
    if (selectedMatchId && window.State?.allMatches) {
      return State.allMatches.find(m => m.id === selectedMatchId) || null;
    }
    return null;
  }

  function getDrawerState() {
    const overlay = el('drawer-overlay');
    const drawer = el('drawer');
    const body = el('drawer-body');

    return {
      jsOpen: !!window.Drawer?._isOpen,
      clsOpen: !!overlay?.classList.contains('open'),
      overlayVisible: overlay ? getComputedStyle(overlay).visibility : 'n/a',
      overlayPE: overlay ? getComputedStyle(overlay).pointerEvents : 'n/a',
      drawerDisplay: drawer ? getComputedStyle(drawer).display : 'n/a',
      drawerHeight: drawer ? `${Math.round(drawer.getBoundingClientRect().height)}px` : 'n/a',
      bodyScrollH: body ? body.scrollHeight : 'n/a',
      bodyClientH: body ? body.clientHeight : 'n/a',
      bodyOverflowY: body ? getComputedStyle(body).overflowY : 'n/a',
    };
  }

  function inspectMatch(match) {
    if (!match) {
      return section('MATCH', row('selectedMatchId', short(selectedMatchId), false) +
        row('match', 'nicht gefunden', false));
    }

    const market = match.market || null;
    const hasMarketObj = !!market;
    const hasOddsStruct = !!market?.odds;
    const hasOddsValues = (
      market?.odds?.home != null &&
      market?.odds?.draw != null &&
      market?.odds?.away != null
    );

    const models = window.ModelEngine?.run ? ModelEngine.run(match) : null;
    const consensus = window.ModelEngine?.consensus ? ModelEngine.consensus(models) : null;
    const mainModels = window.ModelEngine?.mainModels ? ModelEngine.mainModels(models) : null;
    const aiAgent = window.ModelEngine?.agentResult ? ModelEngine.agentResult(models) : null;

    const marketSection = section('MARKET', [
      row('market obj', short(market), hasMarketObj),
      row('market.odds', short(market?.odds), hasOddsStruct),
      row('odds.home', short(market?.odds?.home), market?.odds?.home != null),
      row('odds.draw', short(market?.odds?.draw), market?.odds?.draw != null),
      row('odds.away', short(market?.odds?.away), market?.odds?.away != null),
      row('probs.home', short(market?.probs?.home), market?.probs?.home != null),
      row('edge.home', short(market?.edge?.home), market?.edge?.home != null),
      row('ev.home', short(market?.ev?.home), market?.ev?.home != null),
      row('value.home', short(market?.value?.home), market?.value?.home !== undefined),
      row('has odds values', String(hasOddsValues), hasOddsValues),
    ].join(''));

    const modelSection = section('MODELS', [
      row('ModelEngine.run', short(models), Array.isArray(models)),
      row('models.length', short(models?.length), Array.isArray(models)),
      row('consensus', short(consensus), !!consensus),
      row('consensus.label', short(consensus?.label ?? consensus?.name ?? consensus?.tip), !!(consensus?.label ?? consensus?.name ?? consensus?.tip)),
      row('mainModels', short(mainModels), Array.isArray(mainModels)),
      row('mainModels.length', short(mainModels?.length), Array.isArray(mainModels)),
      row('first model', short(mainModels?.[0]?.name ?? mainModels?.[0]?.label ?? mainModels?.[0]?.model), !!mainModels?.[0]),
      row('aiAgent', short(aiAgent), !!aiAgent),
      row('aiAgent.text', short(aiAgent?.text ?? aiAgent?.note ?? aiAgent?.reason), !!(aiAgent?.text ?? aiAgent?.note ?? aiAgent?.reason)),
    ].join(''));

    const coreSection = section('MATCH', [
      row('id', match.id, true),
      row('homeTeam', short(match.homeTeam), !!match.homeTeam),
      row('awayTeam', short(match.awayTeam), !!match.awayTeam),
      row('competition', short(match.competitionName), !!match.competitionName),
      row('kickoffAt', short(match.kickoffAt), !!match.kickoffAt),
      row('winProbability', short(match.winProbability), match.winProbability != null),
      row('drawProbability', short(match.drawProbability), match.drawProbability != null),
      row('awayProbability', short(match.awayProbability), match.awayProbability != null),
      row('confidenceScore', short(match.confidenceScore), match.confidenceScore != null),
      row('predictedWinner', short(match.predictedWinner), !!match.predictedWinner),
    ].join(''));

    const rawDump = section('RAW MATCH OBJECT', `
      <pre class="dbg-pre">${escapeHtml(safeJson(match))}</pre>
    `);

    return coreSection + marketSection + modelSection + rawDump;
  }

  function escapeHtml(str) {
    return String(str)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;');
  }

  function render() {
    if (!enabled) return;

    const root = el('debug-dialog-body');
    if (!root) return;

    const drawer = getDrawerState();
    const match = findCurrentMatch();

    root.innerHTML =
      section('DRAWER', [
        row('open (JS)', String(drawer.jsOpen), drawer.jsOpen),
        row('open (class)', String(drawer.clsOpen), drawer.clsOpen),
        row('overlay visibility', drawer.overlayVisible, drawer.overlayVisible === 'visible'),
        row('overlay pointerEvents', drawer.overlayPE, drawer.overlayPE !== 'none'),
        row('drawer display', drawer.drawerDisplay, drawer.drawerDisplay !== 'none'),
        row('drawer height', drawer.drawerHeight, true),
        row('body scrollHeight', short(drawer.bodyScrollH), true),
        row('body clientHeight', short(drawer.bodyClientH), true),
        row('body overflowY', drawer.bodyOverflowY, true),
      ].join('')) +
      inspectMatch(match);
  }

  function ensureUI() {
    if (el('debug-dialog')) return;

    const html = `
      <div id="debug-dialog" class="debug-dialog hidden">
        <div class="debug-dialog-card">
          <div class="debug-dialog-head">
            <div class="debug-dialog-title">EuroMatch Live Debug</div>
            <div class="debug-dialog-actions">
              <button id="debug-refresh-btn" class="debug-btn">Refresh</button>
              <button id="debug-close-btn" class="debug-btn debug-btn-close">×</button>
            </div>
          </div>
          <div id="debug-dialog-body" class="debug-dialog-body"></div>
        </div>
      </div>
      <button id="debug-fab" class="debug-fab">🐞</button>
    `;

    document.body.insertAdjacentHTML('beforeend', html);

    el('debug-fab').addEventListener('click', open);
    el('debug-close-btn').addEventListener('click', close);
    el('debug-refresh-btn').addEventListener('click', render);
    el('debug-dialog').addEventListener('click', (e) => {
      if (e.target.id === 'debug-dialog') close();
    });
  }

  function injectStyles() {
    if (el('debug-dialog-styles')) return;
    const css = `
      <style id="debug-dialog-styles">
        .debug-dialog.hidden { display:none; }
        .debug-dialog {
          position: fixed;
          inset: 0;
          z-index: 99999;
          background: rgba(0,0,0,.65);
          display: flex;
          align-items: flex-end;
          justify-content: center;
          padding: 12px;
        }
        .debug-dialog-card {
          width: min(920px, 100%);
          max-height: 88vh;
          background: #081308;
          border: 1px solid rgba(198,241,53,.35);
          border-radius: 18px;
          overflow: hidden;
          box-shadow: 0 20px 80px rgba(0,0,0,.45);
        }
        .debug-dialog-head {
          display:flex;
          align-items:center;
          justify-content:space-between;
          gap:10px;
          padding:14px 16px;
          border-bottom:1px solid rgba(255,255,255,.08);
        }
        .debug-dialog-title {
          color:#fff;
          font-weight:800;
          font-size:18px;
        }
        .debug-dialog-actions {
          display:flex;
          gap:8px;
        }
        .debug-btn {
          border:1px solid rgba(198,241,53,.35);
          background:#162516;
          color:#d8ff66;
          border-radius:10px;
          padding:8px 12px;
          font-weight:700;
        }
        .debug-btn-close {
          color:#fff;
          min-width:42px;
        }
        .debug-dialog-body {
          overflow:auto;
          max-height: calc(88vh - 58px);
          padding: 14px;
        }
        .dbg-section {
          margin-bottom: 14px;
          border: 1px solid rgba(255,255,255,.07);
          border-radius: 14px;
          padding: 12px;
          background: rgba(255,255,255,.02);
        }
        .dbg-title {
          color:#d8ff66;
          font-weight:800;
          margin-bottom:10px;
          letter-spacing:.04em;
          text-transform:uppercase;
          font-size:12px;
        }
        .dbg-row {
          display:grid;
          grid-template-columns: 160px 1fr;
          gap:10px;
          padding:6px 0;
          border-bottom:1px solid rgba(255,255,255,.04);
        }
        .dbg-row:last-child { border-bottom:none; }
        .dbg-k { color:#9fb39f; font-size:13px; }
        .dbg-v { font-size:13px; word-break:break-word; }
        .dbg-ok { color:#bfff72; }
        .dbg-bad { color:#ff7d7d; }
        .dbg-neutral { color:#fff; }
        .dbg-pre {
          margin:0;
          white-space:pre-wrap;
          word-break:break-word;
          color:#d9e7d9;
          font-size:12px;
          line-height:1.45;
        }
        .debug-fab {
          position: fixed;
          right: 14px;
          bottom: 14px;
          z-index: 99998;
          width: 52px;
          height: 52px;
          border-radius: 50%;
          border: 1px solid rgba(198,241,53,.35);
          background: #1b2c12;
          color: #d8ff66;
          font-size: 24px;
          box-shadow: 0 10px 30px rgba(0,0,0,.35);
        }
        @media (max-width: 640px) {
          .dbg-row {
            grid-template-columns: 1fr;
            gap: 4px;
          }
        }
      </style>
    `;
    document.head.insertAdjacentHTML('beforeend', css);
  }

  function open() {
    ensureUI();
    injectStyles();
    el('debug-dialog').classList.remove('hidden');
    render();

    if (!refreshTimer) {
      refreshTimer = setInterval(render, 1200);
    }
  }

  function close() {
    el('debug-dialog')?.classList.add('hidden');
    if (refreshTimer) {
      clearInterval(refreshTimer);
      refreshTimer = null;
    }
  }

  function inspectMatchById(id) {
    selectedMatchId = id;
    if (!el('debug-dialog')) {
      ensureUI();
      injectStyles();
    }
    render();
  }

  function init() {
    ensureUI();
    injectStyles();
  }

  return {
    init,
    open,
    close,
    render,
    inspectMatchById,
  };
})();