'use strict';

/* ── DataLayer ───────────────────────────────────────────────── */
const DataLayer = {
  _lastSource: 'mock',
  async load(forceRefresh = false) {
    if (!forceRefresh) {
      const cached = Cache.read();
      if (cached) { this._lastSource = 'cache'; return { rows: cached, source: 'cache' }; }
    }
    if (CFG.DATA_MODE === 'mock') {
      const rows = Mapper.fromRows(MOCK_ROWS); Cache.write(rows, 'mock'); this._lastSource = 'mock';
      return { rows, source: 'mock' };
    }
    if (CFG.DATA_MODE === 'supabase' || CFG.DATA_MODE === 'auto') {
      try {
        const rows = await this._fetchSupabase(); Cache.write(rows, 'supabase'); this._lastSource = 'supabase';
        return { rows, source: 'supabase' };
      } catch (err) {
        console.warn('[DataLayer] Supabase fetch failed:', err.message);
        if (CFG.DATA_MODE === 'supabase') {
          const stale = Cache.readStale();
          if (stale) return { rows: stale, source: 'stale' };
          throw err;
        }
        const stale = Cache.readStale();
        if (stale) return { rows: stale, source: 'stale' };
        const rows = Mapper.fromRows(MOCK_ROWS); Cache.write(rows, 'mock'); this._lastSource = 'mock';
        return { rows, source: 'mock' };
      }
    }
    const rows = Mapper.fromRows(MOCK_ROWS); Cache.write(rows, 'mock');
    return { rows, source: 'mock' };
  },
  async _fetchSupabase() {
    // Sauberes Tagesfenster: 00:00 heute bis 00:00 übermorgen (lokal)
    const now      = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
    const dayAfterTomorrow = new Date(todayStart.getTime() + 2 * 86_400_000);

    // ISO ohne Millisekunden, mit +00:00 suffix für Supabase
    const toISO = d => d.toISOString().replace(/\.\d+Z$/, '+00:00');
    const from  = toISO(todayStart);
    const to    = toISO(dayAfterTomorrow);

    const params = new URLSearchParams([
      ['kickoff_at', `gte.${from}`], ['kickoff_at', `lt.${to}`],
      ['order', 'kickoff_at.asc'],   ['select', '*'],
    ]);
    const url = `${CFG.SUPABASE_URL}/rest/v1/${CFG.SUPABASE_VIEW}?${params}`;
    const response = await fetch(url, {
      headers: { 'apikey': CFG.SUPABASE_ANON_KEY, 'Authorization': `Bearer ${CFG.SUPABASE_ANON_KEY}`, 'Accept': 'application/json', 'Range': '0-499' },
      signal: AbortSignal.timeout(8_000),
    });
    if (!response.ok) { const body = await response.text().catch(()=>''); throw new Error(`Supabase ${response.status}: ${body.slice(0,200)}`); }
    
    const raw = await response.json();

    
    if (window.Debug?.success) {
      Debug.success ('Supabase raw geladen', {count: raw.length, sample: raw[0]});
    }
    
    if (!Array.isArray(raw)) throw new Error('Supabase returned unexpected format');
    
    const mapped = Wrapper.fromRows(raw);
    if (window.Debug?.success) {
      Debug.success('Mapping abgeschlossen',{count: mapped.length, sample: mapped[0]});
    }

    return mapped;
    
  },
};

