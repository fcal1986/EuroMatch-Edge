'use strict';

/* ── BacktestEngine ──────────────────────────────────────────────
   Frontend-Modul für lokale Modellbewertung und Rankings.

   ARCHITEKTUR-TRENNUNG:
   ┌─────────────────────────────────────────────────────────────┐
   │ FRONTEND (dieses Modul)             READ-ONLY               │
   │   • evaluateMatch()  — lokale Berechnung, kein Write        │
   │   • run()            — In-Memory Batch, kein Write          │
   │   • rankings()       — aus State.backtestResults            │
   │   • loadFromSupabase() — SELECT, niemals INSERT/UPDATE      │
   └─────────────────────────────────────────────────────────────┘
   ┌─────────────────────────────────────────────────────────────┐
   │ BACKEND (GitHub Actions / Python Pipeline)  WRITE           │
   │   • scripts/backtest.py liest Supabase-Matches mit          │
   │     actual_home_goals / actual_away_goals                   │
   │   • berechnet Modell-Tipps serverseitig                     │
   │   • schreibt mit SERVICE_ROLE_KEY nach model_results        │
   └─────────────────────────────────────────────────────────────┘

   Supabase-Tabelle (READ):
     model_results — vollständiges Schema siehe unten

   Deps: ModelEngine, State, CFG (alle global, runtime-safe)
────────────────────────────────────────────────────────────────── */
const BacktestEngine = {

  /* ── Config ─────────────────────────────────────────────────── */
  SUPABASE_TABLE: 'model_results',

  /* ── Kicktipp scoring (pure, kein I/O) ──────────────────────── */

  /**
   * Berechnet Kicktipp-Punkte für Prognose vs. Ergebnis.
   * @param {string} predicted   z.B. '2:1'
   * @param {number} actualHome
   * @param {number} actualAway
   * @returns {0|2|4}
   */
  calcPoints(predicted, actualHome, actualAway) {
    if (!predicted || actualHome == null || actualAway == null) return 0;
    const [ph, pa] = predicted.split(':').map(Number);
    if (isNaN(ph) || isNaN(pa)) return 0;
    if (ph === actualHome && pa === actualAway) return 4;
    const predT = ph > pa ? 'H' : ph < pa ? 'A' : 'D';
    const actT  = actualHome > actualAway ? 'H' : actualHome < actualAway ? 'A' : 'D';
    return predT === actT ? 2 : 0;
  },

  /* ── Lokale Einzelspiel-Auswertung (kein Supabase-Write) ──── */

  /**
   * Wertet alle Modelle für ein abgeschlossenes Spiel aus.
   * Ergebnis bleibt im Arbeitsspeicher — kein Supabase-Write.
   * @param {object} m  — ViewModel mit actual_home_goals / actual_away_goals
   * @returns {object[]} BacktestRecord[]
   */
  evaluateMatch(m) {
    if (m.actual_home_goals == null || m.actual_away_goals == null) return [];

    const modelResults = ModelEngine.run(m);
    const actualScore  = `${m.actual_home_goals}:${m.actual_away_goals}`;
    const actualWinner = m.actual_home_goals > m.actual_away_goals ? 'home'
                       : m.actual_home_goals < m.actual_away_goals ? 'away' : 'draw';

    return modelResults.map(mod => {
      const pts = this.calcPoints(
        mod.main_score_tip, m.actual_home_goals, m.actual_away_goals
      );
      return {
        match_id:         m.id,
        model_key:        mod.model_key,
        model_name:       mod.model_name,
        model_version:    mod.model_version,
        predicted_score:  mod.main_score_tip,
        actual_score:     actualScore,
        predicted_winner: mod.predicted_winner,
        actual_winner:    actualWinner,
        points:           pts,
        is_exact:         pts === 4,
        is_tendency:      pts === 2,
        confidence_score: mod.confidence_score,
        kickoff_at:       m.kickoffAt,
        competition_code: m.competitionCode,
      };
    });
  },

  /* ── Lokaler Batch-Backtest (kein Supabase-Write) ────────────── */

  /**
   * Wertet ein Array historischer Spiele lokal aus.
   * Schreibt NUR in State — niemals nach Supabase.
   * @param {object[]} historicMatches
   * @returns {object[]} alle BacktestRecord[]
   */
  run(historicMatches) {
    const results = [];
    for (const m of historicMatches) {
      results.push(...this.evaluateMatch(m));
    }
    State.backtestResults = results;
    State.modelRankings   = this.rankings(results);
    console.log(`[BacktestEngine] lokal: ${historicMatches.length} Spiele, ${results.length} Prognosen`);
    return results;
  },

  /* ── Rankings (aus lokalem State oder Supabase-Daten) ────────── */

  /**
   * Berechnet Modell-Rankings aus BacktestRecord[].
   * Kann sowohl lokale als auch aus Supabase geladene Daten verwenden.
   * @param {object[]} [records]  — falls nicht angegeben: State.backtestResults
   * @returns {object[]} sortiert nach avg_points absteigend
   */
  rankings(records) {
    const src = records || State.backtestResults || [];
    if (!src.length) return [];

    const byModel = {};
    for (const r of src) {
      if (!byModel[r.model_key]) {
        byModel[r.model_key] = {
          model_key:  r.model_key,
          model_name: r.model_name || r.model_key,
          recs:       [],
        };
      }
      byModel[r.model_key].recs.push(r);
    }

    return Object.values(byModel).map(entry => {
      const { recs } = entry;
      const n     = recs.length;
      const total = recs.reduce((s, r) => s + r.points, 0);
      const exact = recs.filter(r => r.is_exact || r.points === 4).length;
      const tend  = recs.filter(r => r.is_tendency || r.points === 2).length;

      return {
        model_key:         entry.model_key,
        model_name:        entry.model_name,
        matches_evaluated: n,
        total_points:      total,
        avg_points:        n ? +(total / n).toFixed(2) : 0,
        exact_hits:        exact,
        exact_rate:        n ? +(exact / n * 100).toFixed(1) : 0,
        tendency_hits:     tend,
        tendency_rate:     n ? +(tend / n * 100).toFixed(1) : 0,
        wrong_predictions: recs.filter(r => r.points === 0).length,
        score_accuracy:    n ? +((exact + tend) / n * 100).toFixed(1) : 0,
      };
    }).sort((a, b) => b.avg_points - a.avg_points);
  },

  /* ── Supabase READ — Rankings und Ergebnisse laden ───────────── */

  /**
   * Lädt persistierte Backtest-Ergebnisse von Supabase (READ-ONLY).
   * Befüllt State.backtestResults und State.modelRankings.
   * Nutzt den ANON KEY — kein Service Role Key im Frontend.
   *
   * RLS-Anforderung in Supabase:
   *   ALTER TABLE model_results ENABLE ROW LEVEL SECURITY;
   *   CREATE POLICY "anon_select" ON model_results
   *     FOR SELECT TO anon USING (true);
   *
   * @param {{ modelKey?: string, competitionCode?: string, limit?: number }} [opts]
   * @returns {Promise<object[]>}
   */
  async loadFromSupabase(opts = {}) {
    if (!CFG || !CFG.SUPABASE_URL || !CFG.SUPABASE_ANON_KEY) {
      console.warn('[BacktestEngine] CFG fehlt — Supabase-Load übersprungen');
      return [];
    }

    const params = new URLSearchParams([
      ['select', '*'],
      ['order',  'created_at.desc'],
      ['limit',  String(opts.limit ?? 2000)],
    ]);
    if (opts.modelKey)        params.append('model_key',       `eq.${opts.modelKey}`);
    if (opts.competitionCode) params.append('competition_code', `eq.${opts.competitionCode}`);

    const url = `${CFG.SUPABASE_URL}/rest/v1/${this.SUPABASE_TABLE}?${params}`;

    try {
      const res = await fetch(url, {
        method: 'GET',
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

      const raw = await res.json();
      if (!Array.isArray(raw)) throw new Error('Unerwartetes Supabase-Format');

      // Deduplizieren falls bereits lokale Daten vorhanden
      const existing = new Set(
        (State.backtestResults || []).map(r => `${r.match_id}__${r.model_key}`)
      );
      const merged = [
        ...(State.backtestResults || []),
        ...raw.filter(r => !existing.has(`${r.match_id}__${r.model_key}`)),
      ];

      State.backtestResults = merged;
      State.modelRankings   = this.rankings(merged);

      console.log(`[BacktestEngine] ${raw.length} Einträge von Supabase geladen`);
      return raw;

    } catch (err) {
      console.error('[BacktestEngine] loadFromSupabase fehlgeschlagen:', err.message);
      return [];
    }
  },

  /**
   * Aggregierte Rankings direkt aus Supabase laden (via Supabase View).
   * Setzt voraus, dass die View `v_model_rankings` existiert (optional).
   * Falls nicht vorhanden, fällt es auf rankings() mit geladenen Rohdaten zurück.
   */
  async loadRankingsFromSupabase() {
    try {
      const records = await this.loadFromSupabase();
      // rankings() wird bereits in loadFromSupabase() berechnet
      return State.modelRankings;
    } catch (err) {
      console.error('[BacktestEngine] loadRankings fehlgeschlagen:', err.message);
      return [];
    }
  },

  /* ── Lokale Hilfs-Methoden ───────────────────────────────────── */

  forModel(key)  { return (State.backtestResults || []).filter(r => r.model_key       === key); },
  forMatch(id)   { return (State.backtestResults || []).filter(r => r.match_id        === id); },
  forLeague(code){ return (State.backtestResults || []).filter(r => r.competition_code === code); },

  toCSV() {
    const recs = State.backtestResults || [];
    if (!recs.length) return '';
    const hdr  = 'match_id,model_key,model_version,predicted_score,actual_score,points,confidence_score,competition_code\n';
    const rows = recs.map(r =>
      [r.match_id, r.model_key, r.model_version ?? '',
       r.predicted_score, r.actual_score, r.points,
       r.confidence_score, r.competition_code ?? ''].join(',')
    ).join('\n');
    return hdr + rows;
  },
};
