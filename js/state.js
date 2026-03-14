'use strict';

/* ── State ───────────────────────────────────────────────────────
   Central application state.
   Rule: only primitive mutations here — no DOM, no fetch calls.
────────────────────────────────────────────────────────────────── */
const State = {

  /* ── Match data ─────────────────────────────────────────────── */
  allMatches:   [],
  lastUpdated:  null,

  /* ── UI filters ─────────────────────────────────────────────── */
  activeFilter: 'ALL',
  searchQuery:  '',

  /* ── Favorites (persisted) ──────────────────────────────────── */
  favIds: new Set(
    JSON.parse(localStorage.getItem('eme_favs') || '[]')
  ),

  /* ── Collapsible day-groups (session only) ──────────────────── */
  collapsedDays: new Set(),

  /* ── Model results cache ─────────────────────────────────────
     modelsByMatch: { [match_id]: ModelOutput[] }
     Populated by ModelEngine.run() — avoids re-running models
     when the same drawer is opened multiple times.
  ────────────────────────────────────────────────────────────── */
  modelsByMatch: {},

  /* ── AI Agent results ────────────────────────────────────────
     aiAgentResults: { [match_id]: AgentOutput }
     Injected externally via AIAgent.inject(matchId, output)
     after a real API call resolves.
  ────────────────────────────────────────────────────────────── */
  aiAgentResults: {},

  /* ── Backtest ────────────────────────────────────────────────
     backtestResults: BacktestRecord[]
     Populated by BacktestEngine.run(historicMatches)
  ────────────────────────────────────────────────────────────── */
  backtestResults: [],

  /* ── Model rankings ──────────────────────────────────────────
     modelRankings: ModelRanking[]
     Computed by BacktestEngine.rankings()
     Shape per entry:
     {
       model_key, model_name,
       matches_evaluated, total_points, avg_points,
       exact_hits, exact_rate,
       tendency_hits, tendency_rate,
       score_accuracy,
     }
  ────────────────────────────────────────────────────────────── */
  modelRankings: [],

  /* ── Methods ─────────────────────────────────────────────────── */

  filtered() {
    let items = this.allMatches;
    if (this.activeFilter !== 'ALL') {
      items = items.filter(m => m.competitionCode === this.activeFilter);
    }
    if (this.searchQuery) {
      const q = this.searchQuery.toLowerCase();
      items = items.filter(m =>
        m.homeTeam.toLowerCase().includes(q) ||
        m.awayTeam.toLowerCase().includes(q) ||
        m.competitionName.toLowerCase().includes(q)
      );
    }
    return items;
  },

  toggleFav(id) {
    if (this.favIds.has(id)) this.favIds.delete(id);
    else this.favIds.add(id);
    try {
      localStorage.setItem('eme_favs', JSON.stringify([...this.favIds]));
    } catch (_) {}
  },

  toggleDay(dayKey) {
    if (this.collapsedDays.has(dayKey)) this.collapsedDays.delete(dayKey);
    else this.collapsedDays.add(dayKey);
  },

  /** Clear model caches — call after data refresh */
  clearModelCache() {
    this.modelsByMatch  = {};
    this.aiAgentResults = {};
  },
};
