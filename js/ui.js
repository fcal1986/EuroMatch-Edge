'use strict';

/* ── UI ──────────────────────────────────────────────────────── */
const UI = {
  showBanner(type, msg) {
    const el = document.getElementById('status-banner');
    el.className = `status-banner banner-${type} visible`;
    el.innerHTML = `<span>${msg}</span>`;
  },
  hideBanner() { document.getElementById('status-banner').className = 'status-banner'; },
  setSourceBadge(source) {
    const b = document.getElementById('data-badge');
    if (source === 'supabase') { b.className = 'src-badge src-live';  b.textContent = 'LIVE'; }
    else if (source === 'cache' || source === 'stale') { b.className = 'src-badge src-cache'; b.textContent = source==='stale'?'CACHED (alt)':'CACHED'; }
    else { b.className = 'src-badge src-mock'; b.textContent = 'DEMO'; }
  },
};

