'use strict';

window.Debug = (() => {
  const state = {
    enabled: true,
    panelOpen: false,
    logs: [],
    maxLogs: 300,
    platform: detectPlatform(),
    uiReady: false,
  };

  function detectPlatform() {
    const ua = navigator.userAgent || '';
    if (/Android/i.test(ua)) return 'Android';
    if (/iPhone|iPad|iPod/i.test(ua)) return 'iOS';
    if (/Windows/i.test(ua)) return 'Windows';
    if (/Macintosh|Mac OS X/i.test(ua)) return 'macOS';
    return 'Web';
  }

  function nowTime() {
    const d = new Date();
    const pad = (n, l = 2) => String(n).padStart(l, '0');
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
  }

  function esc(str) {
    return String(str)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;');
  }

  function iconFor(level) {
    switch (level) {
      case 'success': return '✅';
      case 'warn': return '⚠️';
      case 'error': return '❌';
      case 'event': return '🔔';
      case 'debug': return '🐞';
      default: return 'ℹ️';
    }
  }

  function clsFor(level) {
    switch (level) {
      case 'success': return 'dbg-log-success';
      case 'warn': return 'dbg-log-warn';
      case 'error': return 'dbg-log-error';
      case 'event': return 'dbg-log-event';
      case 'debug': return 'dbg-log-debug';
      default: return 'dbg-log-info';
    }
  }

  function fmtData(data) {
    if (data === undefined) return '';
    if (typeof data === 'string') return data;
    try {
      return JSON.stringify(data);
    } catch {
      return String(data);
    }
  }

  function push(level, message, data) {
    const entry = {
      ts: nowTime(),
      level,
      message,
      data: fmtData(data),
    };

    state.logs.push(entry);
    if (state.logs.length > state.maxLogs) {
      state.logs.shift();
    }

    renderLogs();
  }

  function log(message, data) {
    push('info', message, data);
  }

  function success(message, data) {
    push('success', message, data);
  }

  function warn(message, data) {
    push('warn', message, data);
  }

  function error(message, data) {
    push('error', message, data);
  }

  function event(message, data) {
    push('event', message, data);
  }

  function debug(message, data) {
    push('debug', message, data);
  }

  function ensureStyles() {
    if (document.getElementById('debug-styles')) return;

    const style = document.createElement('style');
    style.id = 'debug-styles';
    style.textContent = `
      .debug-fab {
        position: fixed;
        right: 18px;
        bottom: 18px;
        width: 64px;
        height: 64px;
        border-radius: 999px;
        border: 3px solid #4f7cff;
        background:
          radial-gradient(circle at 30% 30%, rgba(79,124,255,.22), rgba(79,124,255,.08) 45%, rgba(0,0,0,.0) 70%),
          #111a33;
        color: #d8e6ff;
        font-size: 28px;
        line-height: 1;
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow:
          0 0 0 2px rgba(79,124,255,.12),
          0 0 28px rgba(79,124,255,.35),
          0 12px 28px rgba(0,0,0,.35);
        z-index: 99998;
        cursor: pointer;
        user-select: none;
      }

      .debug-fab:active {
        transform: scale(.97);
      }

      .debug-panel {
        position: fixed;
        left: 0;
        right: 0;
        bottom: 0;
        z-index: 99999;
        background: linear-gradient(180deg, rgba(11,16,33,.98), rgba(6,10,20,.98));
        border-top: 2px solid #4f7cff;
        box-shadow: 0 -12px 32px rgba(0,0,0,.4);
        transform: translateY(100%);
        transition: transform .22s ease;
        max-height: 52vh;
        display: flex;
        flex-direction: column;
      }

      .debug-panel.open {
        transform: translateY(0);
      }

      .debug-panel-head {
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 12px 14px;
        border-bottom: 1px solid rgba(255,255,255,.08);
        background: rgba(255,255,255,.02);
      }

      .debug-panel-title {
        display: flex;
        align-items: center;
        gap: 10px;
        font-weight: 800;
        letter-spacing: .08em;
        text-transform: uppercase;
        color: #5f86ff;
        white-space: nowrap;
      }

      .debug-panel-meta {
        color: rgba(255,255,255,.6);
        white-space: nowrap;
      }

      .debug-panel-spacer {
        flex: 1;
      }

      .debug-btn {
        height: 34px;
        min-width: 34px;
        padding: 0 12px;
        border-radius: 10px;
        border: 1px solid rgba(255,255,255,.12);
        background: rgba(255,255,255,.04);
        color: rgba(255,255,255,.85);
        font-weight: 700;
        cursor: pointer;
      }

      .debug-btn:active {
        transform: scale(.98);
      }

      .debug-panel-body {
        overflow: auto;
        padding: 6px 0 12px 0;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        font-size: 13px;
        line-height: 1.5;
      }

      .dbg-log {
        display: grid;
        grid-template-columns: 84px 22px 1fr;
        gap: 8px;
        padding: 6px 14px;
        border-bottom: 1px solid rgba(255,255,255,.04);
      }

      .dbg-log-time {
        color: rgba(255,255,255,.45);
      }

      .dbg-log-icon {
        text-align: center;
      }

      .dbg-log-text {
        white-space: pre-wrap;
        word-break: break-word;
      }

      .dbg-log-info .dbg-log-text { color: #cfd7ea; }
      .dbg-log-success .dbg-log-text { color: #68e08a; }
      .dbg-log-warn .dbg-log-text { color: #f4c25b; }
      .dbg-log-error .dbg-log-text { color: #ff7d7d; }
      .dbg-log-event .dbg-log-text { color: #c48bff; }
      .dbg-log-debug .dbg-log-text { color: #63b3ff; }

      .dbg-empty {
        padding: 18px 14px;
        color: rgba(255,255,255,.45);
        text-align: center;
      }
    `;
    document.head.appendChild(style);
  }

  function ensureUI() {
    if (state.uiReady) return;

    ensureStyles();

    const fab = document.createElement('button');
    fab.id = 'debug-fab';
    fab.className = 'debug-fab';
    fab.type = 'button';
    fab.title = 'Debug öffnen';
    fab.innerHTML = '🐛';

    const panel = document.createElement('div');
    panel.id = 'debug-panel';
    panel.className = 'debug-panel';
    panel.innerHTML = `
      <div class="debug-panel-head">
        <div class="debug-panel-title">🐛 AUTH DEBUG</div>
        <div class="debug-panel-meta" id="debug-panel-meta"></div>
        <div class="debug-panel-spacer"></div>
        <button id="debug-copy" class="debug-btn" type="button">COPY</button>
        <button id="debug-clear" class="debug-btn" type="button">CLEAR</button>
        <button id="debug-close" class="debug-btn" type="button">✕</button>
      </div>
      <div id="debug-panel-body" class="debug-panel-body"></div>
    `;

    document.body.appendChild(fab);
    document.body.appendChild(panel);

    fab.addEventListener('click', toggle);
    panel.querySelector('#debug-close').addEventListener('click', close);
    panel.querySelector('#debug-clear').addEventListener('click', clear);
    panel.querySelector('#debug-copy').addEventListener('click', copyLogs);

    updateMeta();
    renderLogs();

    state.uiReady = true;
  }

  function updateMeta() {
    const meta = document.getElementById('debug-panel-meta');
    if (!meta) return;
    meta.textContent = `${window.innerWidth}×${window.innerHeight} | ${state.platform}`;
  }

  function renderLogs() {
    const body = document.getElementById('debug-panel-body');
    if (!body) return;

    updateMeta();

    if (!state.logs.length) {
      body.innerHTML = `<div class="dbg-empty">Noch keine Logs vorhanden.</div>`;
      return;
    }

    body.innerHTML = state.logs.map(item => `
      <div class="dbg-log ${clsFor(item.level)}">
        <div class="dbg-log-time">${esc(item.ts)}</div>
        <div class="dbg-log-icon">${iconFor(item.level)}</div>
        <div class="dbg-log-text">${esc(item.message)}${item.data ? `\n${esc(item.data)}` : ''}</div>
      </div>
    `).join('');

    body.scrollTop = body.scrollHeight;
  }

  function open() {
    ensureUI();
    document.getElementById('debug-panel')?.classList.add('open');
    state.panelOpen = true;
    updateMeta();
  }

  function close() {
    document.getElementById('debug-panel')?.classList.remove('open');
    state.panelOpen = false;
  }

  function toggle() {
    if (state.panelOpen) close();
    else open();
  }

  function clear() {
    state.logs = [];
    renderLogs();
    info('Debug-Log geleert');
  }

  async function copyLogs() {
    const text = state.logs.map(item =>
      `[${item.ts}] [${item.level.toUpperCase()}] ${item.message}${item.data ? ` | ${item.data}` : ''}`
    ).join('\n');

    try {
      await navigator.clipboard.writeText(text);
      success('Logs in Zwischenablage kopiert');
    } catch (e) {
      warn('Copy fehlgeschlagen', String(e));
    }
  }

  function info(message, data) {
    log(message, data);
  }

  function inspectMatch(match) {
    if (!match) {
      warn('inspectMatch: kein Match übergeben');
      return;
    }

    debug('Match geöffnet', {
      id: match.id,
      homeTeam: match.homeTeam,
      awayTeam: match.awayTeam,
      competitionName: match.competitionName,
      winProbability: match.winProbability,
      drawProbability: match.drawProbability,
      awayProbability: match.awayProbability,
      confidenceScore: match.confidenceScore,
      predictedWinner: match.predictedWinner,
      market: match.market || null,
    });
  }

  function init() {
    ensureUI();

    info('Debug init gestartet');
    info('Viewport erkannt', `${window.innerWidth}x${window.innerHeight}`);
    info('UserAgent', navigator.userAgent);

    window.addEventListener('resize', () => {
      updateMeta();
      debug('Resize', `${window.innerWidth}x${window.innerHeight}`);
    });

    success('Debug bereit');
  }

  return {
    init,
    open,
    close,
    toggle,
    clear,
    copyLogs,
    log,
    info,
    success,
    warn,
    error,
    event,
    debug,
    inspectMatch,
  };
})();