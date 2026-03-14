'use strict';

/* ── Mapper ──────────────────────────────────────────────────── */
const Mapper = {
  fromRow(row) {
    const winProb  = row.win_probability  ?? 0;
    const drawProb = row.draw_probability ?? 0;
    const awayProb = row.away_probability ?? 0;
    let leadProb, leadLabel;
    if (row.predicted_winner === 'home')      { leadProb = winProb;  leadLabel = row.home_team; }
    else if (row.predicted_winner === 'away') { leadProb = awayProb; leadLabel = row.away_team; }
    else                                       { leadProb = drawProb; leadLabel = 'Unentschieden'; }
    return {
      id:              row.match_id || row.id || Math.random().toString(36).slice(2),
      kickoffAt:       row.kickoff_at ? new Date(row.kickoff_at) : null,
      competitionCode: row.competition_code ?? '',
      competitionName: row.competition_name ?? '',
      leagueAbbr:      row.league_abbr      ?? '',
      flag:            row.flag             ?? '🌍',
      homeTeam:        row.home_team        ?? 'Heim',
      awayTeam:        row.away_team        ?? 'Auswärts',
      homeStrength:    row.home_strength    ?? null,
      awayStrength:    row.away_strength    ?? null,
      formHome:        row.form_home        ?? [],
      formAway:        row.form_away        ?? [],
      winProbability:  winProb,
      drawProbability: drawProb,
      awayProbability: awayProb,
      confidenceScore: row.confidence_score ?? 0,
      predictedWinner: row.predicted_winner ?? 'draw',
      isStrongTip:     row.is_strong_tip    ?? false,
      riskLevel:       row.risk_level       ?? 'low',
      riskTags:        row.risk_tags        ?? [],
      analysisText:    row.analysis_text    ?? '',
      aiReason:        row.ai_reason        ?? '',
      leadProb, leadLabel,
    };
  },
  fromRows(rows) { return rows.map(r => this.fromRow(r)); },
};

