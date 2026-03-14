'use strict';

/* ── Debug HUD ───────────────────────────────────────────────────
   Aktivierung:    URL-Hash #debug  ODER  10× Logo tippen
   Deaktivierung:  URL-Hash #debug  ODER  ✕-Button im HUD

   Zeigt live (alle 500ms aktualisiert):
   - Viewport-Größe + 100dvh in px
   - Drawer: open-State, Höhe, display, overflow, Schließen-Zustand
   - drawer-body: scrollHeight / clientHeight / overflow-y
   - document.body: scrollHeight / overflow / position (für iOS-Lock-Check)
   - CSS-Version aus dem Link-Tag (bestätigt Cache-Busting)
   - CSS-Custom-Property --bg-1 (bestätigt neue CSS geladen)
────────────────────────────────────────────────────────────────── */
const Debug = {

  _hud:      null,
  _interval: null,
  _dvhEl:    null,
  _tapCount: 0,
  _tapTimer: null,

  init() {
    if (location.hash === '#debug') this.show();

    // 10× Logo tippen aktiviert HUD
    const logo = document.querySelector('.logo');
    if (logo) {
      logo.addEventListener('click', () => {
        this._tapCount++;
        clearTimeout(this._tapTimer);
        this._tapTimer = setTimeout(() => { this._tapCount = 0; }, 2000);
        if (this._tapCount >= 10) {
          this._tapCount = 0;
          this.toggle();
        }
      });
    }
  },

  toggle() { this._hud ? this.hide() : this.show(); },

  show() {
    if (this._hud) return;

    const hud = document.createElement('div');
    hud.id = 'debug-hud';
    hud.style.cssText = [
      'position:fixed', 'bottom:80px', 'right:10px',
      'z-index:99999',
      'background:rgba(0,0,0,0.92)',
      'color:#c6f135',
      'font-family:monospace', 'font-size:10px', 'line-height:1.7',
      'padding:10px 12px', 'border-radius:8px',
      'border:1px solid #c6f135',
      'max-width:220px', 'word-break:break-all',
      'pointer-events:all',
    ].join(';');

    const content = document.createElement('div');
    const closeBtn = document.createElement('button');
    closeBtn.textContent = '✕ schließen';
    closeBtn.style.cssText = 'display:block;margin-top:8px;background:#c6f135;color:#000;border:none;border-radius:4px;padding:2px 8px;font-size:10px;cursor:pointer;width:100%';
    closeBtn.onclick = () => this.hide();

    hud.appendChild(content);
    hud.appendChild(closeBtn);
    document.body.appendChild(hud);
    this._hud = hud;
    this._content = content;

    // Dummy-Element um 100dvh in px zu messen
    this._dvhEl = document.createElement('div');
    this._dvhEl.style.cssText = 'position:fixed;top:0;left:0;height:100dvh;width:0;pointer-events:none;z-index:-1;visibility:hidden';
    document.body.appendChild(this._dvhEl);

    this._update();
    this._interval = setInterval(() => this._update(), 500);
  },

  hide() {
    if (this._hud)   { this._hud.remove();   this._hud = null; }
    if (this._dvhEl) { this._dvhEl.remove();  this._dvhEl = null; }
    clearInterval(this._interval);
    this._interval = null;
  },

  _update() {
    if (!this._content) return;

    const overlay    = document.getElementById('drawer-overlay');
    const drawer     = document.getElementById('drawer');
    const drawerBody = document.getElementById('drawer-body');
    const cssLink    = document.querySelector('link[href*="main.css"]');

    // Viewport
    const dvhPx = this._dvhEl ? this._dvhEl.offsetHeight : '?';

    // Drawer state (from Drawer._isOpen JS flag, not just CSS class)
    const isOpenCls  = overlay?.classList.contains('open') ?? false;
    const isOpenJS   = typeof Drawer !== 'undefined' ? Drawer._isOpen : '?';
    const drawerRect = drawer?.getBoundingClientRect();
    const drawerH    = drawerRect ? drawerRect.height.toFixed(0) : '–';
    const drawerComp = drawer ? getComputedStyle(drawer).height : '–';
    const drawerDisp = drawer ? getComputedStyle(drawer).display : '–';
    const drawerOvf  = drawer ? getComputedStyle(drawer).overflow : '–';
    const overlayPE  = overlay ? getComputedStyle(overlay).pointerEvents : '–';
    const overlayVis = overlay ? getComputedStyle(overlay).visibility : '–';

    // drawer-body
    const dbScrollH  = drawerBody ? drawerBody.scrollHeight : '–';
    const dbClientH  = drawerBody ? drawerBody.clientHeight : '–';
    const dbOvfY     = drawerBody ? getComputedStyle(drawerBody).overflowY : '–';

    // document.body (iOS scroll-lock check)
    const docBodyOvf = document.body.style.overflow || '(css)';
    const docBodyPos = document.body.style.position || '(css)';
    const docScrollH = document.documentElement.scrollHeight;
    const docClientH = document.documentElement.clientHeight;

    // CSS
    const cssHref = cssLink ? cssLink.href.split('/').pop() : '–';
    const bg1     = getComputedStyle(document.documentElement)
                      .getPropertyValue('--bg-1').trim() || '–';

    // Search input state
    const searchInput = document.getElementById('search-input');
    const searchPE    = searchInput ? getComputedStyle(searchInput).pointerEvents : '–';
    const searchVis   = searchInput ? getComputedStyle(searchInput).visibility : '–';
    const searchDisp  = searchInput ? getComputedStyle(searchInput).display : '–';

    const row = (label, val, warn = false) =>
      `<span style="color:${warn ? '#ff6b6b' : '#888'}">${label}:</span> <span style="color:${warn ? '#ff6b6b' : '#c6f135'}">${val}</span>`;

    this._content.innerHTML = `
      <b style="color:#fff;font-size:11px">EuroMatch Debug</b><br>
      <span style="color:#555">──────────────</span><br>
      ${row('viewport', `${window.innerWidth}×${window.innerHeight}`)}<br>
      ${row('100dvh', `${dvhPx}px`)}<br>
      <span style="color:#555">── DRAWER ─────</span><br>
      ${row('open (JS)', isOpenJS, isOpenJS !== isOpenCls)}<br>
      ${row('open (cls)', isOpenCls)}<br>
      ${row('height (rect)', `${drawerH}px`, drawerH < 100 && isOpenJS)}<br>
      ${row('height (comp)', drawerComp)}<br>
      ${row('display', drawerDisp)}<br>
      ${row('overflow', drawerOvf)}<br>
      ${row('overlay PE', overlayPE, overlayPE !== 'none' && !isOpenCls)}<br>
      ${row('overlay vis', overlayVis, overlayVis === 'visible' && !isOpenCls)}<br>
      <span style="color:#555">── DRAWER BODY ─</span><br>
      ${row('scrollH', dbScrollH)}<br>
      ${row('clientH', dbClientH)}<br>
      ${row('ovf-y', dbOvfY)}<br>
      <span style="color:#555">── DOC BODY ────</span><br>
      ${row('body.ovf', docBodyOvf, docBodyOvf === 'hidden')}<br>
      ${row('body.pos', docBodyPos, docBodyPos === 'fixed')}<br>
      ${row('doc scrollH', docScrollH, docScrollH < 200)}<br>
      ${row('doc clientH', docClientH)}<br>
      <span style="color:#555">── SEARCH ──────</span><br>
      ${row('pointerEvents', searchPE, searchPE === 'none')}<br>
      ${row('visibility', searchVis, searchVis === 'hidden')}<br>
      ${row('display', searchDisp, searchDisp === 'none')}<br>
      <span style="color:#555">── CSS ─────────</span><br>
      ${row('file', cssHref)}<br>
      ${row('--bg-1', bg1)}
    `.trim();
  },
};

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => Debug.init());
} else {
  Debug.init();
}
