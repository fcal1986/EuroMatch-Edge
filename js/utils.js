'use strict';

/* ── Date / Weekday Utils ────────────────────────────────────── */
const WEEKDAYS_DE = ['Sonntag','Montag','Dienstag','Mittwoch','Donnerstag','Freitag','Samstag'];

/** Returns the german weekday label for a Date object */
function fmtWeekday(date) {
  if (!date) return '?';
  return WEEKDAYS_DE[date.getDay()];
}

/** Returns a sortable day key string 'YYYY-MM-DD' in local time */
function dayKey(date) {
  if (!date) return 'unknown';
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/** Returns true if a date is local-today */
function isToday(date) {
  if (!date) return false;
  const now = new Date();
  return date.getFullYear() === now.getFullYear() &&
         date.getMonth()    === now.getMonth()    &&
         date.getDate()     === now.getDate();
}

/**
 * Groups an array of matches by local day.
 * Returns [ { key, label, isToday, matches }... ] sorted by key.
 */
function groupByDay(matches) {
  const groups = {};
  for (const m of matches) {
    const k = dayKey(m.kickoffAt);
    if (!groups[k]) groups[k] = { key: k, label: fmtWeekday(m.kickoffAt), isToday: isToday(m.kickoffAt), matches: [] };
    groups[k].matches.push(m);
  }
  return Object.values(groups).sort((a, b) => a.key.localeCompare(b.key));
}

/* ── Render helpers ──────────────────────────────────────────── */
function fmtTime(date) {
  if (!date) return '?';
  return date.toLocaleTimeString('de-DE', { hour:'2-digit', minute:'2-digit' });
}
function formStrip(form) {
  if (!form || !form.length) return '<span style="color:var(--t-muted);font-size:11px">–</span>';
  return form.map(r => `<div class="fr ${r}">${r}</div>`).join('');
}
function probClass(p) { return p >= 75 ? 'high' : p >= 60 ? 'mid' : 'low'; }
function badgeInfo(m) {
  const p = m.predictedWinner==='home' ? m.winProbability : m.predictedWinner==='away' ? m.awayProbability : m.drawProbability;
  if (p >= 80) return ['strong','⚡ Starker Tipp'];
  if (p >= 70) return ['solid', '✓ Solider Tipp'];
  if (p >= 60) return ['watch', '👁 Beobachten'];
  return ['risky','✕ Unsicher'];
}

/* ── Render ──────────────────────────────────────────────────── */

/** Builds a single match-row HTML string. Used by both matches and favorites. */
function matchRowHtml(m, animDelay = 0) {
  const homeWin  = m.predictedWinner === 'home', awayWin = m.predictedWinner === 'away';
  const isStrong = m.isStrongTip, isRisky = m.riskLevel === 'high' && !isStrong;
  const [bCls, bLabel] = badgeInfo(m);
  const prob     = homeWin ? m.winProbability : awayWin ? m.awayProbability : m.drawProbability;
  const isFav    = State.favIds.has(m.id);

  // Star SVG: filled when favourite
  const starSvg = isFav
    ? `<svg width="16" height="16" viewBox="0 0 16 16" fill="var(--amber)"><path d="M8 1l1.8 3.6L14 5.4l-3 2.9.7 4.1L8 10.4l-3.7 2 .7-4.1-3-2.9 4.2-.8z"/></svg>`
    : `<svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M8 1l1.8 3.6L14 5.4l-3 2.9.7 4.1L8 10.4l-3.7 2 .7-4.1-3-2.9 4.2-.8z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/></svg>`;

  return `<div class="match-row anim ${isStrong?'is-strong':''} ${isRisky?'is-risky':''}"
               data-id="${m.id}"
               style="animation-delay:${animDelay}s">
    <div class="mr-left" onclick="Drawer.open('${m.id}')">
      <div class="mr-time">${fmtTime(m.kickoffAt)}</div>
      <div class="mr-league-flag">${m.flag}</div>
      <div class="mr-league-abbr">${m.leagueAbbr}</div>
    </div>
    <div class="mr-center" onclick="Drawer.open('${m.id}')">
      <div class="mr-teams">
        <span class="mr-team ${homeWin?'fav':''}">${m.homeTeam}</span>
        <span class="mr-vs">VS</span>
        <span class="mr-team ${awayWin?'fav':''}">${m.awayTeam}</span>
      </div>
      <div class="mr-probbar">
        <div class="mr-probbar-h" style="width:${m.winProbability}%"></div>
        <div class="mr-probbar-d" style="width:${m.drawProbability}%"></div>
        <div class="mr-probbar-a"></div>
      </div>
      <div class="mr-factors">
        <span class="mf ${m.formHome.filter(r=>r==='W').length>2?'good':''}">📈 Form</span>
        <span class="mf ${m.homeStrength&&m.awayStrength&&m.homeStrength-m.awayStrength>20?'good':''}">🏟️ Heim</span>
        <span class="mf ${m.riskTags.length?'bad':''}">⚡ Risiko</span>
        <span class="mf">K: ${m.confidenceScore}%</span>
      </div>
    </div>
    <div class="mr-right" onclick="Drawer.open('${m.id}')">
      <div class="mr-pct ${probClass(prob)}">${prob}%</div>
      <span class="badge ${bCls}">${bLabel}</span>
    </div>
    <button class="btn-star ${isFav?'starred':''}" data-fav="${m.id}" title="${isFav?'Favorit entfernen':'Als Favorit markieren'}" onclick="App.toggleFav('${m.id}',event)">
      ${starSvg}
    </button>
  </div>`;
}

