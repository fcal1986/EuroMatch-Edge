'use strict';

/* ── History ─────────────────────────────────────────────────────
   Historische Prognosen mit Tages-Navigation.

   Datenmodell:
     v_history_days  → Liste aller Tage mit Spielen (einmalig geladen)
     v_history       → Spiele eines ausgewählten Tages (on demand)

   Navigation:
     - Datum-Anzeige: "Dienstag, 15. Apr. 2025"
     - ◀ / ▶ Buttons: springen zum nächsten/vorherigen Spieltag
     - Liga-Chips: filtern den ausgewählten Tag

   KPI-Strip:
     Basiert immer auf dem ausgewählten Tag (+ akt. Liga-Filter).

   Canonical model_key: 'ensemble'
     Ensemble kombiniert Pipeline-Output, Teamstärke und Form —
     vollständigster Einzeltipp. v_history filtert bereits darauf.

   Deps: CFG, State, fmtTime, dayKey, fmtWeekday (alle global)
────────────────────────────────────────────────────────────────── */
const History = {

  /* ── Konstanten ─────────────────────────────────────────────── */
  VIEW_HISTORY: 'v_history',
  VIEW_DAYS:    'v_history_days',

  OUTCOMES: {
    exact:     { cls: 'exact',    icon: '⚡', label: 'Exakter Treffer' },
    goal_diff: { cls: 'goal-diff',icon: '✓',  label: 'Richtige Tordiff.' },
    tendency:  { cls: 'tendency', icon: '↗',  label: 'Richtige Tendenz' },
    wrong:     { cls: 'wrong',    icon: '✕',  label: 'Daneben' },
  },

  _loading: false,

  /* ══════════════════════════════════════════════════════════════
     INIT
  ══════════════════════════════════════════════════════════════ */

  async init() {
    // Einmalig: alle verfügbaren Spieltage laden (klein, schnell)
    if (!State.historyDays || State.historyDays.length === 0) {
      await this._loadDays();
    } else {
      // Tage schon vorhanden — direkt zum zuletzt gewählten Tag
      this._renderNav();
      await this._loadDay(State.historySelectedDay);
    }
  },

  /* ══════════════════════════════════════════════════════════════
     LADEN: Spieltag-Index (v_history_days)
  ══════════════════════════════════════════════════════════════ */

  async _loadDays() {
    this._setLoadingState(true);
    try {
      const params = new URLSearchParams([
        ['select', 'match_date,match_count,exact_count,goal_diff_count,tendency_count,wrong_count,hit_rate'],
        ['order',  'match_date.desc'],
        ['limit',  '365'],   // maximal ein Jahr rückwärts
      ]);
      const url = `${CFG.SUPABASE_URL}/rest/v1/${this.VIEW_DAYS}?${params}`;
      const res  = await fetch(url, {
        headers: {
          'apikey':        CFG.SUPABASE_ANON_KEY,
          'Authorization': `Bearer ${CFG.SUPABASE_ANON_KEY}`,
          'Accept':        'application/json',
        },
        signal: AbortSignal.timeout(8_000),
      });
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`Supabase ${res.status}: ${body.slice(0, 200)}`);
      }
      const data = await res.json();
      if (!Array.isArray(data)) throw new Error('Unerwartetes Format von v_history_days');

      State.historyDays = data;  // [{match_date, match_count, …}, …] desc

      if (data.length === 0) {
        this._setLoadingState(false);
        this._showEmpty();
        return;
      }

      // Neuesten Tag vorauswählen (falls noch keiner gesetzt)
      if (!State.historySelectedDay) {
        State.historySelectedDay = data[0].match_date;
      }

      this._renderNav();
      await this._loadDay(State.historySelectedDay);

    } catch (err) {
      console.error('[History] _loadDays failed:', err.message);
      this._setLoadingState(false);
      this._showError(err.message);
    }
  },

  /* ══════════════════════════════════════════════════════════════
     LADEN: Spiele eines einzelnen Tages (v_history)
  ══════════════════════════════════════════════════════════════ */

  async _loadDay(matchDate) {
    if (!matchDate) return;
    if (this._loading) return;

    this._loading = true;
    this._setLoadingState(true);
    State.historySelectedDay = matchDate;

    try {
      const params = new URLSearchParams([
        ['select',     '*'],
        ['match_date', `eq.${matchDate}`],
        ['order',      'kickoff_at.asc'],
        ['limit',      '50'],
      ]);
      if (State.historyFilter !== 'ALL') {
        params.append('competition_code', `eq.${State.historyFilter}`);
      }

      const url = `${CFG.SUPABASE_URL}/rest/v1/${this.VIEW_HISTORY}?${params}`;
      const res  = await fetch(url, {
        headers: {
          'apikey':        CFG.SUPABASE_ANON_KEY,
          'Authorization': `Bearer ${CFG.SUPABASE_ANON_KEY}`,
          'Accept':        'application/json',
        },
        signal: AbortSignal.timeout(8_000),
      });

      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`Supabase ${res.status}: ${body.slice(0, 200)}`);
      }

      const data = await res.json();
      if (!Array.isArray(data)) throw new Error('Unerwartetes Format von v_history');

      // Date-Strings → Date-Objekte
      State.historyMatches = data.map(r => ({
        ...r,
        kickoff_at: r.kickoff_at ? new Date(r.kickoff_at) : null,
      }));

    } catch (err) {
      console.error('[History] _loadDay failed:', err.message);
      this._showError(err.message);
    } finally {
      this._loading = false;
      this._setLoadingState(false);
    }

    this._render();
  },

  /* ══════════════════════════════════════════════════════════════
     RENDER (vollständig für aktuellen Tag)
  ══════════════════════════════════════════════════════════════ */

  _render() {
    const items = this._filteredItems();
    this._renderKPIs(items);
    this._renderFilters();
    this._renderList(items);
  },

  /* ── KPI-Strip: immer für den aktuellen Tag ─────────────────── */
  _renderKPIs(items) {
    const el = document.getElementById('hist-kpi-strip');
    if (!el) return;

    const n        = items.length;
    const exact    = items.filter(h => h.outcome_label === 'exact').length;
    const goalDiff = items.filter(h => h.outcome_label === 'goal_diff').length;
    const tendency = items.filter(h => h.outcome_label === 'tendency').length;
    const wrong    = items.filter(h => h.outcome_label === 'wrong').length;
    const hitRate  = n > 0 ? Math.round((exact + goalDiff + tendency) / n * 100) : 0;
    const exactRate= n > 0 ? Math.round(exact / n * 100) : 0;

    el.innerHTML = `
      <div class="kpi k-lime">
        <div class="kpi-label">Prognosen</div>
        <div class="kpi-val">${n}</div>
        <div class="kpi-sub">an diesem Tag</div>
      </div>
      <div class="kpi k-lime">
        <div class="kpi-label">⚡ Exakt</div>
        <div class="kpi-val">${exact}</div>
        <div class="kpi-sub">${exactRate}%</div>
      </div>
      <div class="kpi k-sky">
        <div class="kpi-label">✓ Tordiff.</div>
        <div class="kpi-val">${goalDiff}</div>
        <div class="kpi-sub">${n > 0 ? Math.round(goalDiff/n*100) : 0}%</div>
      </div>
      <div class="kpi k-amber">
        <div class="kpi-label">↗ Tendenz</div>
        <div class="kpi-val">${tendency}</div>
        <div class="kpi-sub">${n > 0 ? Math.round(tendency/n*100) : 0}%</div>
      </div>
      <div class="kpi k-red">
        <div class="kpi-label">✕ Daneben</div>
        <div class="kpi-val">${wrong}</div>
        <div class="kpi-sub">${n > 0 ? Math.round(wrong/n*100) : 0}%</div>
      </div>
      <div class="kpi k-sky">
        <div class="kpi-label">Trefferquote</div>
        <div class="kpi-val">${hitRate}%</div>
        <div class="kpi-sub">Tendenz oder besser</div>
      </div>`;
  },

  /* ── Tages-Navigation ────────────────────────────────────────── */
  _renderNav() {
    const el = document.getElementById('hist-day-nav');
    if (!el) return;

    const days   = State.historyDays || [];
    const selDay = State.historySelectedDay;
    const idx    = days.findIndex(d => d.match_date === selDay);

    // Vor/Zurück: im Array geht desc, also idx+1 = älter, idx-1 = neuer
    const hasPrev = idx < days.length - 1;  // älterer Tag
    const hasNext = idx > 0;                 // neuerer Tag

    // Datum für Anzeige formatieren
    const dateLabel = selDay
      ? this._formatDayLabel(selDay)
      : '–';

    // Gesamttreffer für diesen Tag (aus dem Index)
    const dayData  = days[idx];
    const hitInfo  = dayData
      ? `${dayData.match_count} Spiele · ${dayData.hit_rate ?? 0}% Treffer`
      : '';

    el.innerHTML = `
      <div class="hist-nav-bar">
        <button class="hist-nav-btn" id="hist-nav-prev"
          onclick="History.prevDay()"
          ${!hasPrev ? 'disabled' : ''}
          title="Älterer Spieltag">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M9 2L4 7l5 5" stroke="currentColor" stroke-width="2"
              stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </button>

        <div class="hist-nav-center">
          <div class="hist-nav-date">${dateLabel}</div>
          <div class="hist-nav-sub">${hitInfo}</div>
        </div>

        <button class="hist-nav-btn" id="hist-nav-next"
          onclick="History.nextDay()"
          ${!hasNext ? 'disabled' : ''}
          title="Neuerer Spieltag">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M5 2l5 5-5 5" stroke="currentColor" stroke-width="2"
              stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </button>
      </div>`;
  },

  _formatDayLabel(isoDate) {
    // isoDate = '2025-04-15'
    const d = new Date(isoDate + 'T12:00:00Z');
    return d.toLocaleDateString('de-DE', {
      weekday: 'long',
      day:     'numeric',
      month:   'short',
      year:    'numeric',
    });
  },

  /* ── Liga-Filter ────────────────────────────────────────────── */
  _renderFilters() {
    const el = document.getElementById('hist-filter-bar');
    if (!el) return;

    // Nur Ligen anzeigen, die am aktuellen Tag gespielt haben
    const allDay = State.historyMatches;
    const codes  = [...new Set(allDay.map(h => h.competition_code).filter(Boolean))].sort();
    const total  = allDay.length;

    el.innerHTML = [
      `<button class="lchip ${State.historyFilter==='ALL'?'active':''}"
         onclick="History.setFilter('ALL')">Alle<span class="lcount">${total}</span></button>`,
      ...codes.map(code => {
        const sample = allDay.find(h => h.competition_code === code);
        const lbl    = sample ? `${sample.flag || ''} ${sample.league_abbr || code}`.trim() : code;
        const cnt    = allDay.filter(h => h.competition_code === code).length;
        return `<button class="lchip ${State.historyFilter===code?'active':''}"
                  onclick="History.setFilter('${code}')">${lbl}<span class="lcount">${cnt}</span></button>`;
      }),
    ].join('');
  },

  /* ── Spiele-Liste ───────────────────────────────────────────── */
  _renderList(items) {
    const el = document.getElementById('hist-list');
    if (!el) return;

    if (!items.length) {
      const msg = State.historyMatches.length > 0 && State.historyFilter !== 'ALL'
        ? 'Keine Spiele der gewählten Liga an diesem Tag.'
        : 'Keine historischen Prognosen für diesen Tag.';
      el.innerHTML = `
        <div class="empty">
          <div class="empty-icon">📋</div>
          <h3>${msg}</h3>
          <p>Ändere den Liga-Filter oder navigiere zu einem anderen Tag.</p>
        </div>`;
      return;
    }

    el.innerHTML = items.map((h, i) => this._histRowHtml(h, Math.min(i * 0.03, 0.3))).join('');
  },

  /* ── History Row ────────────────────────────────────────────── */
  _histRowHtml(h, animDelay = 0) {
    const outcome   = this.OUTCOMES[h.outcome_label] || this.OUTCOMES.wrong;
    const homeWin   = h.predicted_winner === 'home';
    const awayWin   = h.predicted_winner === 'away';
    const time      = h.kickoff_at ? fmtTime(h.kickoff_at) : '?';
    const actualHtml= `<span class="hist-actual outcome-${outcome.cls}">${h.actual_score || '–'}</span>`;
    const predHtml  = `<span class="hist-pred">${h.predicted_score || '–'}</span>`;

    return `
      <div class="match-row hist-row anim outcome-row-${outcome.cls}"
           style="animation-delay:${animDelay}s"
           data-id="${h.match_id}">

        <div class="mr-left">
          <div class="mr-time">${time}</div>
          <div class="mr-league-flag">${h.flag || ''}</div>
          <div class="mr-league-abbr">${h.league_abbr || ''}</div>
        </div>

        <div class="mr-center">
          <div class="mr-teams">
            <span class="mr-team ${homeWin?'fav':''}">${h.home_team || '?'}</span>
            <span class="mr-vs">VS</span>
            <span class="mr-team ${awayWin?'fav':''}">${h.away_team || '?'}</span>
          </div>
          <div class="hist-scores">
            <div class="hist-score-cell">
              <div class="hist-score-label">Prognose</div>
              ${predHtml}
            </div>
            <div class="hist-score-sep">→</div>
            <div class="hist-score-cell">
              <div class="hist-score-label">Ergebnis</div>
              ${actualHtml}
            </div>
          </div>
        </div>

        <div class="mr-right hist-outcome-col">
          <div class="outcome-badge outcome-${outcome.cls}">
            <span class="outcome-icon">${outcome.icon}</span>
            <span class="outcome-lbl">${outcome.label}</span>
          </div>
          <div class="hist-conf">K: ${h.confidence_score ?? '?'}%</div>
        </div>

      </div>`;
  },

  /* ══════════════════════════════════════════════════════════════
     ÖFFENTLICHE AKTIONEN
  ══════════════════════════════════════════════════════════════ */

  async prevDay() {
    const days = State.historyDays || [];
    const idx  = days.findIndex(d => d.match_date === State.historySelectedDay);
    if (idx < days.length - 1) {
      await this._loadDay(days[idx + 1].match_date);
      this._renderNav();
    }
  },

  async nextDay() {
    const days = State.historyDays || [];
    const idx  = days.findIndex(d => d.match_date === State.historySelectedDay);
    if (idx > 0) {
      await this._loadDay(days[idx - 1].match_date);
      this._renderNav();
    }
  },

  async setFilter(code) {
    State.historyFilter = code;
    // Neu laden damit Supabase-seitig filtert (nicht nur clientseitig)
    await this._loadDay(State.historySelectedDay);
    this._renderNav();
  },

  /* ══════════════════════════════════════════════════════════════
     INTERNE HELFER
  ══════════════════════════════════════════════════════════════ */

  _filteredItems() {
    // Liga-Filter ist bereits in _loadDay() als Supabase-param gesetzt.
    // Diese Methode gibt State.historyMatches unverändert zurück —
    // clientseitiges Filtern ist nicht nötig und würde KPIs verfälschen.
    return State.historyMatches;
  },

  _setLoadingState(on) {
    const el  = document.getElementById('hist-loading');
    const nav = document.getElementById('hist-day-nav');
    if (el) el.style.display = on ? '' : 'none';
    if (nav) {
      nav.querySelectorAll('.hist-nav-btn').forEach(b => { b.disabled = on; });
    }
  },

  _showEmpty() {
    const el = document.getElementById('hist-list');
    if (el) el.innerHTML = `
      <div class="empty">
        <div class="empty-icon">📋</div>
        <h3>Noch keine historischen Prognosen</h3>
        <p>Der Backtest muss mindestens einmal gelaufen sein.</p>
      </div>`;
    const nav = document.getElementById('hist-day-nav');
    if (nav) nav.innerHTML = '';
  },

  _showError(msg) {
    const el = document.getElementById('hist-list');
    if (el) el.innerHTML = `
      <div class="empty">
        <div class="empty-icon">⚠️</div>
        <h3>Fehler beim Laden</h3>
        <p>${msg}</p>
      </div>`;
  },
};
