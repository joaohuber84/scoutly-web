function generateOpportunities(matches) {

  const leaguesWhitelist = [
    "Premier League",
    "La Liga",
    "Bundesliga",
    "Serie A",
    "Ligue 1",
    "Eredivisie",
    "Brasileirão Série A",
    "Brasileirão Série B",
    "MLS",
    "Liga Argentina",
    "Champions League",
    "Europa League",
    "Conference League",
    "Libertadores",
    "Sul-Americana",
    "Copa do Brasil"
  ];

  const opportunities = [];

  matches.forEach(match => {

    if (!leaguesWhitelist.includes(match.league)) return;

    const goalsProjection =
      (match.home_goals_avg + match.away_goals_avg) / 2;

    const cornersProjection =
      (match.home_corners_avg + match.away_corners_avg);

    const shotsProjection =
      (match.home_shots_avg + match.away_shots_avg);

    const markets = [];

    if (goalsProjection > 2.6) {
      markets.push({
        market: "Mais de 2.5 gols",
        strength: "muito_forte",
        score: goalsProjection
      });
    }

    if (goalsProjection < 2.3) {
      markets.push({
        market: "Menos de 3.5 gols",
        strength: "boa",
        score: goalsProjection
      });
    }

    if (cornersProjection > 9.5) {
      markets.push({
        market: "Mais de 9.5 escanteios",
        strength: "boa",
        score: cornersProjection
      });
    }

    if (cornersProjection < 8.5) {
      markets.push({
        market: "Menos de 10.5 escanteios",
        strength: "moderada",
        score: cornersProjection
      });
    }

    if (shotsProjection > 22) {
      markets.push({
        market: "Mais de 1.5 gols",
        strength: "muito_forte",
        score: shotsProjection
      });
    }

    if (match.home_form > match.away_form + 2) {
      markets.push({
        market: `Vitória do ${match.home_team}`,
        strength: "boa",
        score: match.home_form
      });
    }

    if (match.away_form > match.home_form + 2) {
      markets.push({
        market: `Vitória do ${match.away_team}`,
        strength: "boa",
        score: match.away_form
      });
    }

    if (Math.abs(match.home_form - match.away_form) < 2) {
      markets.push({
        market: `Dupla chance ${match.home_team} ou empate`,
        strength: "moderada",
        score: 5
      });
    }

    markets.sort((a, b) => b.score - a.score);

    const topMarkets = markets.slice(0, 3);

    if (topMarkets.length > 0) {

      opportunities.push({
        league: match.league,
        home_team: match.home_team,
        away_team: match.away_team,
        kickoff: match.kickoff,
        main_market: topMarkets[0],
        other_markets: topMarkets.slice(1),
        goals_projection: goalsProjection.toFixed(1),
        corners_projection: cornersProjection.toFixed(1),
        shots_projection: shotsProjection.toFixed(0)
      });

    }

  });

  opportunities.sort((a, b) => b.main_market.score - a.main_market.score);

  return {
    tip_of_day: opportunities[0],
    top5: opportunities.slice(1, 6),
    matches: opportunities
  };

}
