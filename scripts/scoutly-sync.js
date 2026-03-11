const { createClient } = require("@supabase/supabase-js");

const APISPORTS_KEY = process.env.APISPORTS_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!APISPORTS_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Faltam variáveis de ambiente.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "x-apisports-key": APISPORTS_KEY,
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Erro API-Football ${res.status}: ${text}`);
  }

  return res.json();
}

function isoDateParts(date = new Date()) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function normalizeLeagueName(league) {
  return league?.name || "Sem liga";
}

function buildLogoUrl(team) {
  return team?.logo || null;
}

function formFromLastFive(fixtures, teamId) {
  if (!fixtures || !Array.isArray(fixtures)) return null;

  const finished = fixtures
    .filter((f) => ["FT", "AET", "PEN"].includes(f?.fixture?.status?.short))
    .slice(0, 5);

  if (!finished.length) return null;

  return finished
    .map((f) => {
      const isHome = f.teams.home.id === teamId;
      const goalsFor = isHome ? f.goals.home : f.goals.away;
      const goalsAgainst = isHome ? f.goals.away : f.goals.home;

      if (goalsFor > goalsAgainst) return "W";
      if (goalsFor < goalsAgainst) return "L";
      return "D";
    })
    .join("");
}

function avgFromFixtures(fixtures, selector) {
  if (!fixtures || !fixtures.length) return null;
  const values = fixtures.map(selector).filter((v) => typeof v === "number" && !Number.isNaN(v));
  if (!values.length) return null;
  return Number((values.reduce((a, b) => a + b, 0) / values.length).toFixed(1));
}

function confidenceLabel(prob) {
  if (prob >= 70) return "Muito forte";
  if (prob >= 60) return "Forte";
  if (prob >= 50) return "Boa";
  if (prob >= 40) return "Moderada";
  return "Arriscada";
}

function pickFromRow(row) {
  const options = [
    {
      label: "Mais de 2.5 gols",
      prob: row.over25_prob ?? 0,
    },
    {
      label: "Ambas marcam",
      prob: row.btts_prob ?? 0,
    },
    {
      label: "Ambas NÃO marcam",
      prob: row.btts_prob != null ? 100 - row.btts_prob : 0,
    },
    {
      label: "Mais de 8.5 escanteios",
      prob: row.corners_over85_prob ?? 0,
    },
    {
      label: "Mandante para vencer",
      prob: row.home_result_prob ?? row.home_win_prob ?? 0,
    },
  ];

  options.sort((a, b) => b.prob - a.prob);
  return options[0];
}

async function fetchTeamLastFixtures(teamId, leagueId, season) {
  const url = `https://v3.football.api-sports.io/fixtures?team=${teamId}&league=${leagueId}&season=${season}&last=5`;
  const data = await fetchJson(url);
  return data.response || [];
}

async function main() {
  const today = isoDateParts();

  console.log("Buscando jogos reais do dia:", today);

  const fixturesUrl = `https://v3.football.api-sports.io/fixtures?date=${today}`;
  const fixturesData = await fetchJson(fixturesUrl);
  const fixtures = fixturesData.response || [];

  if (!fixtures.length) {
    console.log("Nenhum jogo encontrado para hoje.");
    return;
  }

  console.log(`Jogos encontrados: ${fixtures.length}`);

  const rows = [];

  for (const f of fixtures) {
    try {
      const fixtureId = f.fixture.id;
      const leagueId = f.league.id;
      const season = f.league.season;
      const homeTeamId = f.teams.home.id;
      const awayTeamId = f.teams.away.id;

      const [homeLast, awayLast] = await Promise.all([
        fetchTeamLastFixtures(homeTeamId, leagueId, season),
        fetchTeamLastFixtures(awayTeamId, leagueId, season),
      ]);

      const homeForm = formFromLastFive(homeLast, homeTeamId);
      const awayForm = formFromLastFive(awayLast, awayTeamId);

      const avgGoalsHome = avgFromFixtures(homeLast, (x) => {
        const isHome = x.teams.home.id === homeTeamId;
        return isHome ? x.goals.home : x.goals.away;
      });

      const avgGoalsAway = avgFromFixtures(awayLast, (x) => {
        const isHome = x.teams.home.id === awayTeamId;
        return isHome ? x.goals.home : x.goals.away;
      });

      const avgGoals = avgFromFixtures(
        [
          ...(homeLast || []),
          ...(awayLast || []),
        ],
        (x) => {
          const home = x.goals.home ?? 0;
          const away = x.goals.away ?? 0;
          return home + away;
        }
      );

      const avgShots = null;
      const avgCorners = null;

      let homeStrength = 50;
      let awayStrength = 50;

      const formScore = (form) =>
        (form || "").split("").reduce((acc, c) => {
          if (c === "W") return acc + 3;
          if (c === "D") return acc + 1;
          return acc;
        }, 0);

      const homeFormScore = formScore(homeForm);
      const awayFormScore = formScore(awayForm);

      homeStrength = Math.min(90, 40 + homeFormScore * 2);
      awayStrength = Math.min(90, 40 + awayFormScore * 2);

      const drawProb = 100 - Math.min(80, Math.max(20, homeStrength + awayStrength - 60));
      const homeWinProb = Math.max(15, Math.min(75, 50 + Math.round((homeStrength - awayStrength) / 2)));
      const awayWinProb = Math.max(10, 100 - homeWinProb - drawProb);

      const over25Prob = avgGoals != null ? Math.max(20, Math.min(80, Math.round(avgGoals * 22))) : null;

      const bttsProb =
        avgGoalsHome != null && avgGoalsAway != null
          ? Math.max(15, Math.min(80, Math.round(((avgGoalsHome + avgGoalsAway) / 2) * 30)))
          : null;

      const cornersOver85Prob = avgCorners != null ? Math.max(20, Math.min(80, Math.round(avgCorners * 7))) : null;

      const rowBase = {
        id: fixtureId,
        created_at: new Date().toISOString(),
        match_date: f.fixture.date ? f.fixture.date.slice(0, 10) : today,
        kickoff: f.fixture.date || null,
        home_team: f.teams.home.name,
        away_team: f.teams.away.name,
        league: normalizeLeagueName(f.league),
        home_logo: buildLogoUrl(f.teams.home),
        away_logo: buildLogoUrl(f.teams.away),
        avg_goals: avgGoals,
        avg_corners: avgCorners,
        avg_shots: avgShots,
        home_win_prob: homeWinProb,
        draw_prob: drawProb,
        away_win_prob: awayWinProb,
        home_result_prob: homeWinProb,
        away_result_prob: awayWinProb,
        over25_prob: over25Prob,
        btts_prob: bttsProb,
        corners_over85_prob: cornersOver85Prob,
        home_form: homeForm,
        away_form: awayForm,
        power_home: homeStrength,
        power_away: awayStrength,
        market_odds_over25: null,
        market_odds_btts: null,
        market_odds_corners85: null,
        over15_prob: null,
        under25_prob: over25Prob != null ? 100 - over25Prob : null,
        under35_prob: null,
        insight: null,
        pick: null,
      };

      const bestPick = pickFromRow(rowBase);

      rowBase.pick = bestPick.label;
      rowBase.insight = `Força da oportunidade — ${confidenceLabel(bestPick.prob)} (${bestPick.prob}%)`;

      rows.push(rowBase);
    } catch (err) {
      console.error("Erro ao processar fixture:", f?.fixture?.id, err.message);
    }
  }

  if (!rows.length) {
    console.log("Nenhuma linha pronta para gravar.");
    return;
  }

  console.log(`Gravando ${rows.length} jogos no Supabase...`);

  const { error } = await supabase.from("matches").upsert(rows, { onConflict: "id" });

  if (error) {
    throw new Error(`Erro ao gravar no Supabase: ${error.message}`);
  }

  console.log("Scoutly Sync concluído com sucesso.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
