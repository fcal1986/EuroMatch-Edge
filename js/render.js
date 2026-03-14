'use strict';

const Render = {
  loading() {
    document.getElementById('list-all').innerHTML = Array.from({length:4},()=>`<div class="skel skel-card"></div>`).join('');
    document.getElementById('list-strong').innerHTML = Array.from({length:3},()=>`<div class="skel skel-top"></div>`).join('');
    ['kpi-total','kpi-strong','kpi-conf','kpi-risk'].forEach(id => document.getElementById(id).textContent = '–');
  },

  matches(items) {
    const all_m   = State.allMatches;
    const strong  = all_m.filter(m => m.isStrongTip);
    const risky   = all_m.filter(m => m.riskLevel === 'high');
    const avgConf = all_m.length ? Math.round(all_m.reduce((s,m) => s + m.confidenceScore, 0) / all_m.length) : 0;

    document.getElementById('kpi-total').textContent  = all_m.length;
    document.getElementById('kpi-strong').textContent = strong.length;
    document.getElementById('kpi-conf').textContent   = avgConf + '%';
    document.getElementById('kpi-risk').textContent   = risky.length;
    document.getElementById('count-all').textContent  = items.length;

    // Top Chancen (strong tips, horizontal scroll cards)
    const strongItems = items.filter(m => m.isStrongTip).sort((a,b) => b.winProbability - a.winProbability);
    document.getElementById('count-strong').textContent = strongItems.length;
    const secS = document.getElementById('section-strong');
    if (strongItems.length) {
      secS.style.display = '';
      document.getElementById('list-strong').innerHTML = strongItems.map((m,i) => {
        const p = m.predictedWinner==='home' ? m.winProbability : m.awayProbability;
        const winner = m.predictedWinner==='home' ? m.homeTeam : m.awayTeam;
        return `<div class="top-card anim" style="animation-delay:${i*.05}s" onclick="Drawer.open('${m.id}')">
          <div class="tc-rank">#${i+1} · ${m.leagueAbbr}</div>
          <div class="tc-pct">${p}%</div>
          <div class="tc-winner">${winner}</div>
          <div class="tc-meta">${m.flag} ${fmtTime(m.kickoffAt)} · K: ${m.confidenceScore}%</div>
        </div>`;
      }).join('');
    } else {
      secS.style.display = 'none';
    }

    // All matches — grouped by weekday
    const listAll = document.getElementById('list-all');
    if (!items.length) {
      listAll.innerHTML = `<div class="empty"><div class="empty-icon">⚽</div><h3>Keine Spiele</h3><p>Passe deine Filter an.</p></div>`;
    } else {
      const groups = groupByDay(items);
      // By default: collapse groups that are NOT today (only on first render when State has no explicit collapses recorded)
      // We only auto-collapse if collapsedDays is still empty (initial load)
      if (State.collapsedDays.size === 0) {
        groups.forEach(g => { if (!g.isToday) State.collapsedDays.add(g.key); });
      }
      listAll.innerHTML = groups.map(g => {
        const isCollapsed = State.collapsedDays.has(g.key);
        return `<div class="day-group${isCollapsed?' collapsed':''}" data-day="${g.key}">
          <div class="day-group-header" onclick="App.toggleDay('${g.key}')">
            <span class="day-label${g.isToday?' is-today':''}">${g.label}</span>
            <span class="day-badge">${g.matches.length}</span>
            <svg class="day-chevron" width="12" height="12" viewBox="0 0 12 12" fill="none">
              <path d="M2 4l4 4 4-4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </div>
          <div class="day-group-body">
            ${g.matches.map((m,i) => matchRowHtml(m, Math.min(i*.03,.3))).join('')}
          </div>
        </div>`;
      }).join('');
    }

    // Risk section
    const riskyItems = State.allMatches.filter(m => m.riskLevel === 'high');
    document.getElementById('count-risk').textContent = riskyItems.length;
    const secR = document.getElementById('section-risk');
    if (riskyItems.length) {
      secR.style.display = '';
      document.getElementById('list-risk').innerHTML = riskyItems.map((m,i) => {
        const prob = m.predictedWinner==='home' ? m.winProbability : m.predictedWinner==='away' ? m.awayProbability : m.drawProbability;
        return `<div class="risk-row anim" style="animation-delay:${i*.05}s" onclick="Drawer.open('${m.id}')">
          <div class="risk-badge-icon">⚠️</div>
          <div class="risk-info">
            <div class="risk-teams">${m.homeTeam} × ${m.awayTeam}</div>
            <div class="risk-tags">${m.riskTags.map(t=>`<span class="rtag">${t}</span>`).join('')}</div>
          </div>
          <div class="risk-pct-wrap">
            <div class="risk-pct">${prob}%</div>
            <div class="risk-time">${fmtTime(m.kickoffAt)} · ${m.flag}</div>
          </div>
        </div>`;
      }).join('');
    } else {
      secR.style.display = 'none';
    }

    // Render favorites too (they may have changed)
    this.favorites();
  },

  favorites() {
    const favMatches = State.allMatches.filter(m => State.favIds.has(m.id));
    document.getElementById('count-favs').textContent = favMatches.length;
    const list = document.getElementById('list-favs');
    if (!favMatches.length) {
      list.innerHTML = `<div class="fav-empty"><div class="fav-empty-icon">⭐</div><p>Noch keine Favoriten — tippe auf den Stern neben einem Spiel.</p></div>`;
    } else {
      list.innerHTML = favMatches.map((m,i) => matchRowHtml(m, i*.03)).join('');
    }
  },

  filters(matches) {
    const codes = [...new Set(matches.map(m => m.competitionCode))];
    document.getElementById('filter-bar').innerHTML = [
      `<button class="lchip ${State.activeFilter==='ALL'?'active':''}" onclick="App.setFilter('ALL')">Alle<span class="lcount">${matches.length}</span></button>`,
      ...codes.map(code => {
        const m   = matches.find(x => x.competitionCode === code);
        const lbl = m ? `${m.flag} ${m.leagueAbbr}` : code;
        const cnt = matches.filter(x => x.competitionCode === code).length;
        return `<button class="lchip ${State.activeFilter===code?'active':''}" onclick="App.setFilter('${code}')">${lbl}<span class="lcount">${cnt}</span></button>`;
      }),
    ].join('');
  },
};

