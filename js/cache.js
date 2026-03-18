'use strict';

/* ── Cache ───────────────────────────────────────────────────── */
const Cache = (() => {
  const LS_KEY       = 'eme_cache_v2';
  const SCHEMA_VER   = 1;
  let _mem           = null;

  function _load() {
    if (_mem) return _mem;
    try {
      const raw = localStorage.getItem(LS_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (parsed?.schemaVersion !== SCHEMA_VER) return null;
      _mem = parsed;
      return _mem;
    } catch (e) {
      console.warn('[Cache] localStorage parse error, clearing.', e);
      try { localStorage.removeItem(LS_KEY); } catch (_) {}
      return null;
    }
  }

  function _save(entry) {
    _mem = entry;
    try { localStorage.setItem(LS_KEY, JSON.stringify(entry)); }
    catch (e) { console.warn('[Cache] localStorage write failed:', e); }
  }

  return {
    write(rows, source = 'unknown') {
      _save({ rows, source, ts: Date.now(), schemaVersion: SCHEMA_VER });
    },

    read() {
      const entry = _load();
      if (!entry) return null;
      if (Date.now() - entry.ts > CFG.CACHE_TTL_MS) return null;
      return entry.rows;
    },

    readStale() {
      const entry = _load();
      return entry ? entry.rows : null;
    },

    ttlRemaining() {
      const entry = _load();
      if (!entry) return 0;
      return Math.max(0, CFG.CACHE_TTL_MS - (Date.now() - entry.ts));
    },

    lastSource() {
      const entry = _load();
      return entry ? entry.source : null;
    },

    clear() {
      _mem = null;
      try { localStorage.removeItem(LS_KEY); } catch (_) {}
    },
  };
})();