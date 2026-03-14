'use strict';

/* ── App ─────────────────────────────────────────────────────── */
const App = {
  async init() {
    document.getElementById('header-date').textContent =
      new Date().toLocaleDateString('de-DE', { weekday:'short', day:'numeric', month:'long' });

    // ── Restore persisted UI state ────────────────────────────
    const savedTheme  = localStorage.getItem('eme_theme')  || 'dark';
    const savedFilter = localStorage.getItem('eme_filter') || 'ALL';
    const savedSearch = localStorage.getItem('eme_search') || '';

    // Theme
    let dark = savedTheme === 'dark';
    document.documentElement.setAttribute('data-theme', savedTheme);
    document.getElementById('ico-sun').classList.toggle('hidden', !dark);
    document.getElementById('ico-moon').classList.toggle('hidden', dark);

    // Filter + Search into State
    State.activeFilter = savedFilter;
    State.searchQuery  = savedSearch;

    // Prefill search input if there's a saved query
    const searchInput = document.getElementById('search-input');
    if (savedSearch) {
      searchInput.value = savedSearch;
      // Also expand the search bar on mobile so the filled value is visible
      document.getElementById('header-search').classList.add('expanded');
    }

    // ── Search input ──────────────────────────────────────────
    searchInput.addEventListener('input', e => {
      State.searchQuery = e.target.value.trim();
      localStorage.setItem('eme_search', State.searchQuery);
      this._applyFilters();
    });

    // Expandable search on mobile
    const searchWrap = document.getElementById('header-search');
    const searchIcon  = document.getElementById('search-icon');
    searchIcon.addEventListener('click', () => {
      if (!searchWrap.classList.contains('expanded')) {
        searchWrap.classList.add('expanded');
        setTimeout(() => searchInput.focus(), 50);
      }
    });
    searchInput.addEventListener('blur', () => {
      if (!searchInput.value) {
        searchWrap.classList.remove('expanded');
      }
    });
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') {
        Drawer.close();
        searchInput.value = '';
        searchWrap.classList.remove('expanded');
        State.searchQuery = '';
        localStorage.setItem('eme_search', '');
        this._applyFilters();
      }
    });

    // ── Theme toggle ──────────────────────────────────────────
    document.getElementById('btn-theme').onclick = () => {
      dark = !dark;
      const theme = dark ? 'dark' : 'light';
      document.documentElement.setAttribute('data-theme', theme);
      document.getElementById('ico-sun').classList.toggle('hidden', !dark);
      document.getElementById('ico-moon').classList.toggle('hidden', dark);
      localStorage.setItem('eme_theme', theme);
    };

    window.addEventListener('scroll', () => {
      document.getElementById('scroll-top').classList.toggle('show', window.scrollY > 260);
    });

    await this.refresh();
  },

  async refresh() {
    Render.loading(); UI.hideBanner();
    const btn = document.getElementById('btn-refresh');
    btn.classList.add('loading');
    try {
      const { rows, source } = await DataLayer.load(true);
      State.allMatches  = rows;
      State.lastUpdated = new Date();
      UI.setSourceBadge(source);
      document.getElementById('footer-updated').textContent = `Stand: ${State.lastUpdated.toLocaleTimeString('de-DE')}`;
      if (source === 'stale') UI.showBanner('warn', '⚠ Supabase nicht erreichbar — zeige zwischengespeicherte Daten.');
      else if (source === 'mock') UI.showBanner('info', 'ℹ Demo-Modus: Trage deine Supabase-Daten in CFG ein, um Live-Daten zu sehen.');
      Render.filters(rows);
      this._applyFilters();
    } catch (err) {
      console.error('[App] Fatal load error:', err);
      UI.showBanner('error', '⛔ Fehler beim Laden: ' + err.message);
      document.getElementById('list-all').innerHTML = `<div class="empty"><div class="empty-icon">🔌</div><h3>Keine Daten</h3><p>${err.message}</p></div>`;
    } finally {
      btn.classList.remove('loading');
    }
  },

  setFilter(code) {
    State.activeFilter = code;
    localStorage.setItem('eme_filter', code);
    Render.filters(State.allMatches);
    this._applyFilters();
  },

  toggleFav(id, event) {
    // Prevent the click from bubbling to the match-row (which opens the drawer)
    event.stopPropagation();
    State.toggleFav(id);
    // Re-render only the affected star buttons without rebuilding the full list
    document.querySelectorAll(`[data-fav="${id}"]`).forEach(btn => {
      const isFav = State.favIds.has(id);
      btn.classList.toggle('starred', isFav);
      btn.title = isFav ? 'Favorit entfernen' : 'Als Favorit markieren';
      btn.innerHTML = isFav
        ? `<svg width="16" height="16" viewBox="0 0 16 16" fill="var(--amber)"><path d="M8 1l1.8 3.6L14 5.4l-3 2.9.7 4.1L8 10.4l-3.7 2 .7-4.1-3-2.9 4.2-.8z"/></svg>`
        : `<svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M8 1l1.8 3.6L14 5.4l-3 2.9.7 4.1L8 10.4l-3.7 2 .7-4.1-3-2.9 4.2-.8z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/></svg>`;
    });
    // Update favorites section
    Render.favorites();
  },

  toggleDay(dayKey) {
    State.toggleDay(dayKey);
    const group = document.querySelector(`.day-group[data-day="${dayKey}"]`);
    if (group) group.classList.toggle('collapsed', State.collapsedDays.has(dayKey));
  },

  _applyFilters() { Render.matches(State.filtered()); },
};

/* ── Boot ────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => App.init());
