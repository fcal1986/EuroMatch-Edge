'use strict';

/* ── Mapper ──────────────────────────────────────────────────── */
const Mapper = {
  fromRow(row) {
    const winProb  = row.win_probability  ?? 0;
    const drawProb = row.draw_probability ?? 0;
    const awayProb = row.away_probability ?? 0;

    let leadProb, leadLabel;
    if (row.predicted_winner === 'home') {
      leadProb = winProb;
      leadLabel = row.home_team;
    } else if (row.predicted_winner === 'away') {
      leadProb = awayProb;
      leadLabel = row.away_team;
    } else {
      leadProb = drawProb;
      leadLabel = 'Unentschieden';
    }

    const marketHomeOdds = row.market_home_win_odds ?? null;
    const marketDrawOdds = row.market_draw_odds ?? null;
    const marketAwayOdds = row.market_away_odds ?? null;

    const marketHomeProbFair = row.market_home_prob_fair ?? null;
    const marketDrawProbFair = row.market_draw_prob_fair ?? null;
    const marketAwayProbFair = row.market_away_prob_fair ?? null;

    const edgeHome = row.edge_home ?? null;
    const edgeDraw = row.edge_draw ?? null;
    const edgeAway = row.edge_away ?? null;

    const evHome = row.ev_home ?? null;
    const evDraw = row.ev_draw ?? null;
    const evAway = row.ev_away ?? null;

    const valueHome = row.value_home ?? false;
    const valueDraw = row.value_draw ?? false;
    const valueAway = row.value_away ?? false;

    const hasMarketOdds =
      marketHomeOdds !== null &&
      marketDrawOdds !== null &&
      marketAwayOdds !== null;

    const hasMarketModel =
      marketHomeProbFair !== null &&
      marketDrawProbFair !== null &&
      marketAwayProbFair !== null;

    return {
      id:              row.match_id || row.id || Math.random().toString(36).slice(2),
      kickoffAt:       row.kickoff_at ? new Date(row.kickoff_at) : null,
      competitionCode: row.competition_code ?? '',
      competitionName: row.competition_name ?? '',
      leagueAbbr:      row.league_abbr ?? '',
      flag:            row.flag ?? '🌍',
      homeTeam:        row.home_team ?? 'Heim',
      awayTeam:        row.away_team ?? 'Auswärts',
      homeStrength:    row.home_strength ?? null,
      awayStrength:    row.away_strength ?? null,
      formHome:        row.form_home ?? [],
      formAway:        row.form_away ?? [],
      winProbability:  winProb,
      drawProbability: drawProb,
      awayProbability: awayProb,
      confidenceScore: row.confidence_score ?? 0,
      predictedWinner: row.predicted_winner ?? 'draw',
      isStrongTip:     row.is_strong_tip ?? false,
      riskLevel:       row.risk_level ?? 'low',
      riskTags:        row.risk_tags ?? [],
      analysisText:    row.analysis_text ?? '',
      aiReason:        row.ai_reason ?? '',
      leadProb,
      leadLabel,

      market: {
        hasOdds: hasMarketOdds,
        hasModel: hasMarketModel,
        provider: row.market_odds_provider ?? null,
        bookmakerCount: row.market_bookmaker_count ?? row.market_odds_bookmaker_count ?? null,
        overround: row.market_overround ?? null,
        fetchedAt: row.market_odds_fetched_at ?? null,
        updatedAt: row.market_last_update_ts ?? row.market_odds_updated_at ?? null,

        odds: {
          home: marketHomeOdds,
          draw: marketDrawOdds,
          away: marketAwayOdds,
        },

        probs: {
          home: marketHomeProbFair,
          draw: marketDrawProbFair,
          away: marketAwayProbFair,
        },

        edge: {
          home: edgeHome,
          draw: edgeDraw,
          away: edgeAway,
        },

        ev: {
          home: evHome,
          draw: evDraw,
          away: evAway,
        },

        value: {
          home: valueHome,
          draw: valueDraw,
          away: valueAway,
        },
      },
    };
  },

  fromRows(rows) {
    return rows.map(r => this.fromRow(r));
  },
};