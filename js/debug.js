'use strict';

/* ── Debug HUD ───────────────────────────────────────────────────
   Aktivierung: URL-Hash #debug   → sofort sichtbar
   Oder:        10× schnell auf Logo tippen
   Deaktivierung: nochmal #debug in URL oder X-Button

   Zeigt live:
   - window.innerHeight / innerWidth
   - 100dvh in px (über CSS custom property gemessen)
   - .drawer tatsächliche Höhe + computed height
   - .drawer-body scrollHeight / clientHeight
   - CSS-Wert der --bg-1 Variable (zeigt ob neues CSS geladen)
   - main.css href (zeigt cache-busting Version)
────────────────────────────────────────────────────────────────── */
const Debug = {

  _hud: null,
  _interval: null,
  _tapCount: 0,
  _tapTimer: null,

  init() {
    // Aktiviere über URL-Hash
    if (location.hash === '#debug') this.show();

    // Aktiviere über 10× Logo-Tipp
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

  toggle() {
    this._hud ? this.hide() : this.show();
  },

  show() {
    if (this._hud) return;

    // Erstelle HUD-Element
    const hud = document.createElement('div');
    hud.id = 'debug-hud';
    hud.style.cssText = [
      'position: fixed',
      'bottom: 80px',
      'right: 10px',
      'z-index: 99999',
      'background: rgba(0,0,0,0.88)',
      'color: #c6f135',
      'font-family: monospace',
      'font-size: 10px',
      'line-height: 1.6',
      'padding: 10px 12px',
      'border-radius: 8px',
      'border: 1px solid #c6f135',
      'max-width: 200px',
      'word-break: break-all',
      'pointer-events: all',
    ].join(';');

    const closeBtn = document.createElement('button');
    closeBtn.textContent = '✕ schließen';
    closeBtn.style.cssText = 'display:block;margin-top:8px;background:#c6f135;color:#000;border:none;border-radius:4px;padding:2px 8px;font-size:10px;cursor:pointer;width:100%';
    closeBtn.onclick = () => this.hide();

    hud.appendChild(document.createElement('div')); // content div
    hud.appendChild(closeBtn);
    document.body.appendChild(hud);
    this._hud = hud;

    // Messe dvh über dummy-Element
    this._dvhEl = document.createElement('div');
    this._dvhEl.style.cssText = 'position:fixed;top:0;left:0;height:100dvh;width:0;pointer-events:none;z-index:-1;visibility:hidden';
    document.body.appendChild(this._dvhEl);

    this._update();
    this._interval = setInterval(() => this._update(), 500);
  },

  hide() {
    if (this._hud) { this._hud.remove(); this._hud = null; }
    if (this._dvhEl) { this._dvhEl.remove(); this._dvhEl = null; }
    clearInterval(this._interval);
  },

  _update() {
    if (!this._hud) return;

    const drawer     = document.getElementById('drawer');
    const drawerBody = document.getElementById('drawer-body');
    const cssLink    = document.querySelector('link[href*="main.css"]');
    const dvhPx      = this._dvhEl ? this._dvhEl.offsetHeight : '?';

    // Computed drawer height
    const drawerH    = drawer ? drawer.getBoundingClientRect().height.toFixed(0) : '–';
    const drawerComp = drawer ? getComputedStyle(drawer).height : '–';
    const drawerDisp = drawer ? getComputedStyle(drawer).display : '–';
    const drawerOvf  = drawer ? getComputedStyle(drawer).overflow : '–';
    const bodyScroll = drawerBody ? drawerBody.scrollHeight : '–';
    const bodyClient = drawerBody ? drawerBody.clientHeight : '–';
    const bodyOvfY   = drawerBody ? getComputedStyle(drawerBody).overflowY : '–';
    const isOpen     = document.getElementById('drawer-overlay')?.classList.contains('open');
    const cssHref    = cssLink ? cssLink.href.split('/').pop() : '–';
    const bg1        = getComputedStyle(document.documentElement)
                         .getPropertyValue('--bg-1').trim() || '–';

    const content = this._hud.querySelector('div');
    content.innerHTML = [
      `<b style="color:#fff">EuroMatch Debug</b>`,
      `──────────────`,
      `viewport: ${window.innerWidth}×${window.innerHeight}`,
      `100dvh: ${dvhPx}px`,
      `──────────────`,
      `drawer open: ${isOpen ? '✓' : '✗'}`,
      `drawer.h (rect): ${drawerH}px`,
      `drawer.h (comp): ${drawerComp}`,
      `drawer display: ${drawerDisp}`,
      `drawer overflow: ${drawerOvf}`,
      `──────────────`,
      `body scrollH: ${bodyScroll}`,
      `body clientH: ${bodyClient}`,
      `body ovf-y: ${bodyOvfY}`,
      `──────────────`,
      `css file: ${cssHref}`,
      `--bg-1: ${bg1}`,
    ].join('<br>');
  },
};

// Auto-init nach DOM ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => Debug.init());
} else {
  Debug.init();
}
