const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY || ""
const SUPABASE_URL = process.env.SUPABASE_URL || ""
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_KEY ||
  process.env.SUPABASE_KEY ||
  ""

if (!APISPORTS_KEY) throw new Error("APISPORTS_KEY não encontrada.")
if (!SUPABASE_URL) throw new Error("SUPABASE_URL não encontrada.")
if (!SUPABASE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY não encontrada.")

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

const SYNC_VERSION = "V13.4"

const WINDOW_HOURS = Number(process.env.WINDOW_HOURS || 168)
const FORM_LIMIT_GENERAL = 10
const FORM_LIMIT_HOME_AWAY = 5
const MAX_RECENT_FIXTURES_FETCH = 20
const MIN_REQUIRED_RECENT_MATCHES = 3
const MIN_REQUIRED_STATS_MATCHES = 2

const MAX_DAILY_PICKS = 20
const MAX_RADAR_GAMES = 80

const MAX_SAME_MARKET_IN_DAILY = 2
const MAX_SAME_LEAGUE_IN_DAILY = 4
const MAX_INTERNATIONAL_IN_DAILY = 8
const MAX_BRAZIL_IN_DAILY = 6
const MAX_GOALS_MARKETS = 6
const MAX_TOTAL_CORNERS_MARKETS = 5
const MAX_TEAM_CORNERS_MARKETS = 5
const MAX_CARDS_MARKETS = 4
const MAX_DOUBLE_CHANCE_MARKETS = 4

const MARKET_PRIORITY_ORDER = {
  team_corners: 1,
  total_corners: 2,
  cards: 3,
  goals: 4,
  double_chance: 5,
  under_goals: 6,
}

const IMPORTANT_KEYWORDS = [
  "champions league",
  "libertadores",
  "sudamericana",
  "europa league",
  "conference league",
  "world cup",
  "copa do mundo",
  "euro",
  "copa america",
  "nations league",
  "premier league",
  "la liga",
  "serie a",
  "bundesliga",
  "ligue 1",
  "brasileirão",
  "brasileirao",
  "copa do brasil",
  "fa cup",
  "efl cup",
  "coppa italia",
  "copa del rey",
  "dfb pokal",
]

const BAD_COMPETITION_KEYWORDS = [
  "u17",
  "u18",
  "u19",
  "u20",
  "u21",
  "u23",
  "youth",
  "reserve",
  "reserves",
  "academy",
  "junior",
  "juniors",
  "women youth",
]

const BIG_TEAMS = [
  "real madrid",
  "barcelona",
  "atletico madrid",
  "manchester city",
  "manchester united",
  "liverpool",
  "arsenal",
  "chelsea",
  "tottenham",
  "bayern munich",
  "borussia dortmund",
  "psg",
  "paris saint germain",
  "juventus",
  "inter",
  "ac milan",
  "napoli",
  "roma",
  "benfica",
  "porto",
  "sporting",
  "ajax",
  "psv",
  "feyenoord",
  "flamengo",
  "fluminense",
  "palmeiras",
  "corinthians",
  "sao paulo",
  "são paulo",
  "santos",
  "botafogo",
  "vasco",
  "gremio",
  "grêmio",
  "internacional",
  "cruzeiro",
  "atletico mineiro",
  "atlético mineiro",
  "river plate",
  "boca juniors",
  "racing club",
  "independiente",
  "olympique marseille",
  "lyon",
  "monaco",
]

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function nowISO() {
  return new Date().toISOString()
}

function addHours(date, hours) {
  return new Date(date.getTime() + hours * 60 * 60 * 1000)
}

function toDateString(date) {
  return date.toISOString().slice(0, 10)
}

function normalizeText(value = "") {
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim()
}

function round(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return null
  const factor = 10 ** digits
  return Math.round(Number(value) * factor) / factor
}

function pct(value, total) {
  if (!total) return 0
  return round((value / total) * 100, 1)
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function safeNumber(value) {
  if (value === null || value === undefined) return 0
  if (typeof value === "number") return value
  const parsed = Number(String(value).replace("%", "").trim())
  return Number.isFinite(parsed) ? parsed : 0
}

function isBadCompetition(leagueName = "") {
  const n = normalizeText(leagueName)
  return BAD_COMPETITION_KEYWORDS.some((k) => n.includes(normalizeText(k)))
}

function isImportantCompetition(leagueName = "") {
  const n = normalizeText(leagueName)
  return IMPORTANT_KEYWORDS.some((k) => n.includes(normalizeText(k)))
}

function isBrazilCompetition(country = "", leagueName = "") {
  const c = normalizeText(country)
  const l = normalizeText(leagueName)
  return c.includes("brazil") || c.includes("brasil") || l.includes("brasileirao") || l.includes("copa do brasil")
}

function isInternationalCompetition(country = "", leagueName = "") {
  const c = normalizeText(country)
  const l = normalizeText(leagueName)
  return (
    c.includes("world") ||
    c.includes("international") ||
    l.includes("world cup") ||
    l.includes("euro") ||
    l.includes("nations league") ||
    l.includes("copa america") ||
    l.includes("friendlies") ||
    l.includes("qualifiers")
  )
}

function hasBigTeam(home = "", away = "") {
  const h = normalizeText(home)
  const a = normalizeText(away)
  return BIG_TEAMS.some((team) => h.includes(normalizeText(team)) || a.includes(normalizeText(team)))
}

async function apiGet(path, params = {}, retry = 2) {
  const url = new URL(`${API}${path}`)
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value))
    }
  })

  for (let attempt = 0; attempt <= retry; attempt++) {
    try {
      const res = await fetch(url.toString(), {
        headers: {
          "x-apisports-key": APISPORTS_KEY,
        },
      })

      if (!res.ok) {
        if (attempt < retry) {
          await sleep(900 + attempt * 700)
          continue
        }
        throw new Error(`API error ${res.status}: ${url.pathname}`)
      }

      const json = await res.json()

      if (json.errors && Object.keys(json.errors).length) {
        console.log("API errors:", json.errors)
      }

      await sleep(250)
      return json.response || []
    } catch (err) {
      if (attempt < retry) {
        await sleep(1000 + attempt * 1000)
        continue
      }
      throw err
    }
  }

  return []
}

function statValue(statsArr, teamId, labelCandidates) {
  const teamStats = statsArr.find((s) => s.team?.id === teamId)
  if (!teamStats?.statistics) return 0

  for (const candidate of labelCandidates) {
    const item = teamStats.statistics.find((x) => normalizeText(x.type) === normalizeText(candidate))
    if (item) return safeNumber(item.value)
  }

  return 0
}

function extractFixtureStat(statsArr, fixture, teamId, opponentId, side) {
  const teamGoals = side === "home" ? fixture.goals?.home : fixture.goals?.away
  const oppGoals = side === "home" ? fixture.goals?.away : fixture.goals?.home

  const corners = statValue(statsArr, teamId, ["Corner Kicks", "Corners"])
  const oppCorners = statValue(statsArr, opponentId, ["Corner Kicks", "Corners"])

  const shotsOnGoal = statValue(statsArr, teamId, ["Shots on Goal", "Shots on target"])
  const oppShotsOnGoal = statValue(statsArr, opponentId, ["Shots on Goal", "Shots on target"])

  const totalShots = statValue(statsArr, teamId, ["Total Shots", "Shots total"])
  const oppTotalShots = statValue(statsArr, opponentId, ["Total Shots", "Shots total"])

  const yellowCards = statValue(statsArr, teamId, ["Yellow Cards"])
  const redCards = statValue(statsArr, teamId, ["Red Cards"])
  const oppYellowCards = statValue(statsArr, opponentId, ["Yellow Cards"])
  const oppRedCards = statValue(statsArr, opponentId, ["Red Cards"])

  return {
    fixture_id: fixture.fixture?.id,
    date: fixture.fixture?.date,
    side,
    team_goals: safeNumber(teamGoals),
    opponent_goals: safeNumber(oppGoals),
    total_goals: safeNumber(teamGoals) + safeNumber(oppGoals),
    corners,
    opponent_corners: oppCorners,
    total_corners: corners + oppCorners,
    shots_on_goal: shotsOnGoal,
    opponent_shots_on_goal: oppShotsOnGoal,
    total_shots,
    opponent_total_shots: oppTotalShots,
    yellow_cards: yellowCards,
    red_cards: redCards,
    cards: yellowCards + redCards,
    opponent_cards: oppYellowCards + oppRedCards,
    total_cards: yellowCards + redCards + oppYellowCards + oppRedCards,
  }
}

function aggregateTeamForm(rows = []) {
  const valid = rows.filter(Boolean)

  if (!valid.length) {
    return {
      matches: 0,
      avg_goals_for: 0,
      avg_goals_against: 0,
      avg_total_goals: 0,
      avg_corners_for: 0,
      avg_corners_against: 0,
      avg_total_corners: 0,
      avg_shots_on_goal_for: 0,
      avg_total_shots_for: 0,
      avg_cards_for: 0,
      avg_cards_total: 0,
      over_15_goals_pct: 0,
      over_25_goals_pct: 0,
      under_35_goals_pct: 0,
      over_75_corners_pct: 0,
      over_85_corners_pct: 0,
      over_95_corners_pct: 0,
      team_over_25_corners_pct: 0,
      team_over_35_corners_pct: 0,
      team_over_45_corners_pct: 0,
      cards_over_15_pct: 0,
      cards_over_25_pct: 0,
      cards_over_35_pct: 0,
      cards_under_55_pct: 0,
    }
  }

  const n = valid.length
  const sum = (key) => valid.reduce((acc, row) => acc + safeNumber(row[key]), 0)
  const count = (fn) => valid.filter(fn).length

  return {
    matches: n,
    avg_goals_for: round(sum("team_goals") / n),
    avg_goals_against: round(sum("opponent_goals") / n),
    avg_total_goals: round(sum("total_goals") / n),
    avg_corners_for: round(sum("corners") / n),
    avg_corners_against: round(sum("opponent_corners") / n),
    avg_total_corners: round(sum("total_corners") / n),
    avg_shots_on_goal_for: round(sum("shots_on_goal") / n),
    avg_total_shots_for: round(sum("total_shots") / n),
    avg_cards_for: round(sum("cards") / n),
    avg_cards_total: round(sum("total_cards") / n),
    over_15_goals_pct: pct(count((x) => x.total_goals >= 2), n),
    over_25_goals_pct: pct(count((x) => x.total_goals >= 3), n),
    under_35_goals_pct: pct(count((x) => x.total_goals <= 3), n),
    over_75_corners_pct: pct(count((x) => x.total_corners >= 8), n),
    over_85_corners_pct: pct(count((x) => x.total_corners >= 9), n),
    over_95_corners_pct: pct(count((x) => x.total_corners >= 10), n),
    team_over_25_corners_pct: pct(count((x) => x.corners >= 3), n),
    team_over_35_corners_pct: pct(count((x) => x.corners >= 4), n),
    team_over_45_corners_pct: pct(count((x) => x.corners >= 5), n),
    cards_over_15_pct: pct(count((x) => x.total_cards >= 2), n),
    cards_over_25_pct: pct(count((x) => x.total_cards >= 3), n),
    cards_over_35_pct: pct(count((x) => x.total_cards >= 4), n),
    cards_under_55_pct: pct(count((x) => x.total_cards <= 5), n),
  }
}

function getFixtureBase(fixture) {
  return {
    fixture_id: fixture.fixture?.id,
    date: fixture.fixture?.date,
    timestamp: fixture.fixture?.timestamp,
    status: fixture.fixture?.status?.short,
    status_long: fixture.fixture?.status?.long,
    league_id: fixture.league?.id,
    league_name: fixture.league?.name,
    league_country: fixture.league?.country,
    league_logo: fixture.league?.logo,
    league_flag: fixture.league?.flag,
    season: fixture.league?.season,
    round: fixture.league?.round,
    home_team_id: fixture.teams?.home?.id,
    home_team_name: fixture.teams?.home?.name,
    home_team_logo: fixture.teams?.home?.logo,
    away_team_id: fixture.teams?.away?.id,
    away_team_name: fixture.teams?.away?.name,
    away_team_logo: fixture.teams?.away?.logo,
    home_goals: fixture.goals?.home,
    away_goals: fixture.goals?.away,
    venue_name: fixture.fixture?.venue?.name,
    venue_city: fixture.fixture?.venue?.city,
    updated_at: nowISO(),
  }
}

async function fetchTeamRecentForm(teamId, leagueId, season, sideFilter = null) {
  const fixtures = await apiGet("/fixtures", {
    team: teamId,
    season,
    league: leagueId,
    last: MAX_RECENT_FIXTURES_FETCH,
    timezone: TIMEZONE,
  })

  const finished = fixtures
    .filter((f) => ["FT", "AET", "PEN"].includes(f.fixture?.status?.short))
    .filter((f) => {
      if (!sideFilter) return true
      if (sideFilter === "home") return f.teams?.home?.id === teamId
      if (sideFilter === "away") return f.teams?.away?.id === teamId
      return true
    })
    .slice(0, sideFilter ? FORM_LIMIT_HOME_AWAY : FORM_LIMIT_GENERAL)

  const rows = []

  for (const fx of finished) {
    const stats = await apiGet("/fixtures/statistics", { fixture: fx.fixture?.id })

    const isHome = fx.teams?.home?.id === teamId
    const opponentId = isHome ? fx.teams?.away?.id : fx.teams?.home?.id

    rows.push(
      extractFixtureStat(
        stats,
        fx,
        teamId,
        opponentId,
        isHome ? "home" : "away"
      )
    )
  }

  return {
    raw: rows,
    agg: aggregateTeamForm(rows),
  }
}

function buildGameContext(fixture, homeGeneral, awayGeneral, homeSide, awaySide) {
  const leagueName = fixture.league_name || ""
  const country = fixture.league_country || ""
  const home = fixture.home_team_name || ""
  const away = fixture.away_team_name || ""

  const importantCompetition = isImportantCompetition(leagueName)
  const brazilCompetition = isBrazilCompetition(country, leagueName)
  const internationalCompetition = isInternationalCompetition(country, leagueName)
  const bigTeamGame = hasBigTeam(home, away)

  const avgGoalProfile =
    (safeNumber(homeGeneral.avg_total_goals) + safeNumber(awayGeneral.avg_total_goals)) / 2

  const avgCornerProfile =
    (safeNumber(homeGeneral.avg_total_corners) + safeNumber(awayGeneral.avg_total_corners)) / 2

  const avgCardProfile =
    (safeNumber(homeGeneral.avg_cards_total) + safeNumber(awayGeneral.avg_cards_total)) / 2

  const homeAttackStrength =
    safeNumber(homeSide.avg_goals_for) +
    safeNumber(homeSide.avg_shots_on_goal_for) * 0.35 +
    safeNumber(homeSide.avg_corners_for) * 0.18

  const awayAttackStrength =
    safeNumber(awaySide.avg_goals_for) +
    safeNumber(awaySide.avg_shots_on_goal_for) * 0.35 +
    safeNumber(awaySide.avg_corners_for) * 0.18

  const balanceGap = Math.abs(homeAttackStrength - awayAttackStrength)

  let gameProfile = "balanced"

  if (avgGoalProfile >= 2.75 && avgCornerProfile >= 8.5) gameProfile = "open_game"
  if (avgGoalProfile <= 2.1 && avgCardProfile >= 4) gameProfile = "tight_physical"
  if (balanceGap >= 1.8) gameProfile = "favorite_pressure"
  if (importantCompetition && avgCardProfile >= 3.4) gameProfile = "high_stakes"

  const contextBoost =
    (importantCompetition ? 8 : 0) +
    (bigTeamGame ? 8 : 0) +
    (brazilCompetition ? 4 : 0) +
    (internationalCompetition ? 3 : 0)

  return {
    importantCompetition,
    brazilCompetition,
    internationalCompetition,
    bigTeamGame,
    gameProfile,
    avgGoalProfile: round(avgGoalProfile),
    avgCornerProfile: round(avgCornerProfile),
    avgCardProfile: round(avgCardProfile),
    homeAttackStrength: round(homeAttackStrength),
    awayAttackStrength: round(awayAttackStrength),
    balanceGap: round(balanceGap),
    contextBoost,
  }
}

function pushPick(picks, pick) {
  if (!pick) return
  if (!pick.market_type || !pick.market) return
  if (!Number.isFinite(Number(pick.score))) return
  picks.push({
    ...pick,
    score: round(clamp(pick.score, 0, 100), 1),
  })
}

function confidenceLabel(score) {
  if (score >= 82) return "Muito forte"
  if (score >= 74) return "Forte"
  if (score >= 66) return "Boa"
  if (score >= 58) return "Moderada"
  return "Leve"
}

function buildGoalsPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context) {
  const picks = []

  const over15Pct =
    (safeNumber(homeGeneral.over_15_goals_pct) + safeNumber(awayGeneral.over_15_goals_pct)) / 2

  const over25Pct =
    (safeNumber(homeGeneral.over_25_goals_pct) + safeNumber(awayGeneral.over_25_goals_pct)) / 2

  const under35Pct =
    (safeNumber(homeGeneral.under_35_goals_pct) + safeNumber(awayGeneral.under_35_goals_pct)) / 2

  const avgTotalGoals =
    (safeNumber(homeGeneral.avg_total_goals) + safeNumber(awayGeneral.avg_total_goals)) / 2

  const avgShotsOnGoal =
    safeNumber(homeSide.avg_shots_on_goal_for) + safeNumber(awaySide.avg_shots_on_goal_for)

  if (over15Pct >= 62 && avgTotalGoals >= 2.1) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "goals",
      market: "Mais de 1.5 gols",
      side: "match",
      line: 1.5,
      direction: "over",
      score:
        50 +
        over15Pct * 0.34 +
        avgTotalGoals * 4 +
        avgShotsOnGoal * 1.5 +
        (context.gameProfile === "open_game" ? 6 : 0) +
        context.contextBoost * 0.25,
      reason: `Tendência de gols consistente: ${round(over15Pct)}% dos jogos recentes bateram over 1.5, com média geral de ${round(avgTotalGoals)} gols.`,
    })
  }

  if (over25Pct >= 54 && avgTotalGoals >= 2.55 && avgShotsOnGoal >= 7) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "goals",
      market: "Mais de 2.5 gols",
      side: "match",
      line: 2.5,
      direction: "over",
      score:
        46 +
        over25Pct * 0.36 +
        avgTotalGoals * 5 +
        avgShotsOnGoal * 1.3 +
        (context.gameProfile === "open_game" ? 7 : 0) +
        context.contextBoost * 0.2,
      reason: `Jogo com perfil ofensivo: média recente de ${round(avgTotalGoals)} gols e boa produção em chutes no alvo.`,
    })
  }

  if (under35Pct >= 66 && avgTotalGoals <= 2.7 && context.gameProfile !== "open_game") {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "under_goals",
      market: "Menos de 3.5 gols",
      side: "match",
      line: 3.5,
      direction: "under",
      score:
        48 +
        under35Pct * 0.35 +
        (3.2 - avgTotalGoals) * 6 +
        (context.gameProfile === "tight_physical" ? 5 : 0) +
        context.contextBoost * 0.1,
      reason: `Linha conservadora: ${round(under35Pct)}% dos jogos recentes ficaram abaixo de 3.5 gols.`,
    })
  }

  return picks
}

function buildTotalCornersPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context) {
  const picks = []

  const over75Pct =
    (safeNumber(homeGeneral.over_75_corners_pct) + safeNumber(awayGeneral.over_75_corners_pct)) / 2

  const over85Pct =
    (safeNumber(homeGeneral.over_85_corners_pct) + safeNumber(awayGeneral.over_85_corners_pct)) / 2

  const over95Pct =
    (safeNumber(homeGeneral.over_95_corners_pct) + safeNumber(awayGeneral.over_95_corners_pct)) / 2

  const avgTotalCorners =
    (safeNumber(homeGeneral.avg_total_corners) + safeNumber(awayGeneral.avg_total_corners)) / 2

  const sideCornerSum =
    safeNumber(homeSide.avg_corners_for) +
    safeNumber(homeSide.avg_corners_against) +
    safeNumber(awaySide.avg_corners_for) +
    safeNumber(awaySide.avg_corners_against)

  if (over75Pct >= 60 && avgTotalCorners >= 8) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "total_corners",
      market: "Mais de 7.5 escanteios",
      side: "match",
      line: 7.5,
      direction: "over",
      score:
        48 +
        over75Pct * 0.35 +
        avgTotalCorners * 2.5 +
        sideCornerSum * 0.7 +
        (context.gameProfile === "open_game" || context.gameProfile === "favorite_pressure" ? 5 : 0) +
        context.contextBoost * 0.22,
      reason: `Boa base para escanteios totais: média de ${round(avgTotalCorners)} cantos recentes e ${round(over75Pct)}% acima da linha 7.5.`,
    })
  }

  if (over85Pct >= 56 && avgTotalCorners >= 8.8) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "total_corners",
      market: "Mais de 8.5 escanteios",
      side: "match",
      line: 8.5,
      direction: "over",
      score:
        46 +
        over85Pct * 0.36 +
        avgTotalCorners * 2.8 +
        sideCornerSum * 0.65 +
        (context.gameProfile === "open_game" || context.gameProfile === "favorite_pressure" ? 5 : 0) +
        context.contextBoost * 0.2,
      reason: `Jogo com volume favorável para cantos: média recente de ${round(avgTotalCorners)} escanteios.`,
    })
  }

  if (over95Pct >= 52 && avgTotalCorners >= 9.6) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "total_corners",
      market: "Mais de 9.5 escanteios",
      side: "match",
      line: 9.5,
      direction: "over",
      score:
        44 +
        over95Pct * 0.36 +
        avgTotalCorners * 3 +
        (context.gameProfile === "favorite_pressure" ? 6 : 0) +
        context.contextBoost * 0.18,
      reason: `Linha mais agressiva de cantos: média geral próxima/acima de 10 e bom histórico recente.`,
    })
  }

  return picks
}

function buildTeamCornersPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context) {
  const picks = []

  const candidates = [
    {
      team: fixture.home_team_name,
      side: "home",
      form: homeSide,
      general: homeGeneral,
      opponent: awaySide,
      opponentGeneral: awayGeneral,
    },
    {
      team: fixture.away_team_name,
      side: "away",
      form: awaySide,
      general: awayGeneral,
      opponent: homeSide,
      opponentGeneral: homeGeneral,
    },
  ]

  for (const c of candidates) {
    const avgFor = safeNumber(c.form.avg_corners_for)
    const avgAgainstOpponent = safeNumber(c.opponent.avg_corners_against)
    const avgCombined = (avgFor + avgAgainstOpponent) / 2

    const over25 =
      (safeNumber(c.form.team_over_25_corners_pct) +
        safeNumber(c.general.team_over_25_corners_pct)) /
      2

    const over35 =
      (safeNumber(c.form.team_over_35_corners_pct) +
        safeNumber(c.general.team_over_35_corners_pct)) /
      2

    const over45 =
      (safeNumber(c.form.team_over_45_corners_pct) +
        safeNumber(c.general.team_over_45_corners_pct)) /
      2

    const pressureBoost =
      context.gameProfile === "favorite_pressure" &&
      ((c.side === "home" && context.homeAttackStrength > context.awayAttackStrength) ||
        (c.side === "away" && context.awayAttackStrength > context.homeAttackStrength))
        ? 6
        : 0

    if (over25 >= 58 && avgCombined >= 3) {
      pushPick(picks, {
        fixture_id: fixture.fixture_id,
        market_type: "team_corners",
        market: `${c.team} mais de 2.5 escanteios`,
        team: c.team,
        side: c.side,
        line: 2.5,
        direction: "over",
        score:
          46 +
          over25 * 0.34 +
          avgCombined * 5 +
          pressureBoost +
          context.contextBoost * 0.18,
        reason: `${c.team} tem boa produção individual de cantos: média projetada de ${round(avgCombined)} escanteios.`,
      })
    }

    if (over35 >= 54 && avgCombined >= 3.8) {
      pushPick(picks, {
        fixture_id: fixture.fixture_id,
        market_type: "team_corners",
        market: `${c.team} mais de 3.5 escanteios`,
        team: c.team,
        side: c.side,
        line: 3.5,
        direction: "over",
        score:
          44 +
          over35 * 0.35 +
          avgCombined * 5.3 +
          pressureBoost +
          context.contextBoost * 0.16,
        reason: `${c.team} mostra força para linha individual de escanteios, com média recente de ${round(avgFor)} a favor.`,
      })
    }

    if (over45 >= 50 && avgCombined >= 4.6) {
      pushPick(picks, {
        fixture_id: fixture.fixture_id,
        market_type: "team_corners",
        market: `${c.team} mais de 4.5 escanteios`,
        team: c.team,
        side: c.side,
        line: 4.5,
        direction: "over",
        score:
          42 +
          over45 * 0.35 +
          avgCombined * 5.5 +
          pressureBoost +
          context.contextBoost * 0.14,
        reason: `${c.team} tem perfil de pressão ofensiva e volume para linha alta de escanteios.`,
      })
    }
  }

  return picks
}

function buildCardsPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context) {
  const picks = []

  const over15Pct =
    (safeNumber(homeGeneral.cards_over_15_pct) + safeNumber(awayGeneral.cards_over_15_pct)) / 2

  const over25Pct =
    (safeNumber(homeGeneral.cards_over_25_pct) + safeNumber(awayGeneral.cards_over_25_pct)) / 2

  const over35Pct =
    (safeNumber(homeGeneral.cards_over_35_pct) + safeNumber(awayGeneral.cards_over_35_pct)) / 2

  const under55Pct =
    (safeNumber(homeGeneral.cards_under_55_pct) + safeNumber(awayGeneral.cards_under_55_pct)) / 2

  const avgCards =
    (safeNumber(homeGeneral.avg_cards_total) +
      safeNumber(awayGeneral.avg_cards_total) +
      safeNumber(homeSide.avg_cards_total) +
      safeNumber(awaySide.avg_cards_total)) /
    4

  const highStakesBoost =
    context.gameProfile === "high_stakes" || context.gameProfile === "tight_physical" ? 7 : 0

  if (over15Pct >= 58 && avgCards >= 2.2) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "cards",
      market: "Mais de 1.5 cartões",
      side: "match",
      line: 1.5,
      direction: "over",
      score:
        45 +
        over15Pct * 0.33 +
        avgCards * 5 +
        highStakesBoost +
        context.contextBoost * 0.2,
      reason: `Tendência de cartões presente: média recente de ${round(avgCards)} cartões e contexto favorável.`,
    })
  }

  if (over25Pct >= 54 && avgCards >= 3) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "cards",
      market: "Mais de 2.5 cartões",
      side: "match",
      line: 2.5,
      direction: "over",
      score:
        43 +
        over25Pct * 0.34 +
        avgCards * 5.2 +
        highStakesBoost +
        context.contextBoost * 0.18,
      reason: `Jogo com perfil físico/competitivo: média projetada de ${round(avgCards)} cartões.`,
    })
  }

  if (over35Pct >= 50 && avgCards >= 3.8) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "cards",
      market: "Mais de 3.5 cartões",
      side: "match",
      line: 3.5,
      direction: "over",
      score:
        41 +
        over35Pct * 0.35 +
        avgCards * 5.4 +
        highStakesBoost +
        context.contextBoost * 0.16,
      reason: `Linha forte para cartões em jogo de maior tensão e média alta de advertências.`,
    })
  }

  if (under55Pct >= 62 && avgCards <= 4.6 && context.gameProfile !== "high_stakes") {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "cards",
      market: "Menos de 5.5 cartões",
      side: "match",
      line: 5.5,
      direction: "under",
      score:
        44 +
        under55Pct * 0.34 +
        (5.5 - avgCards) * 4 +
        context.contextBoost * 0.08,
      reason: `Boa proteção no under de cartões: ${round(under55Pct)}% dos jogos recentes ficaram abaixo de 5.5 cartões.`,
    })
  }

  return picks
}

function buildDoubleChancePicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context) {
  const picks = []

  const homePower =
    safeNumber(homeSide.avg_goals_for) * 8 +
    safeNumber(homeSide.avg_shots_on_goal_for) * 3 +
    safeNumber(homeSide.avg_corners_for) * 1.2 -
    safeNumber(homeSide.avg_goals_against) * 4

  const awayPower =
    safeNumber(awaySide.avg_goals_for) * 8 +
    safeNumber(awaySide.avg_shots_on_goal_for) * 3 +
    safeNumber(awaySide.avg_corners_for) * 1.2 -
    safeNumber(awaySide.avg_goals_against) * 4

  const diff = homePower - awayPower

  if (diff >= 5) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "double_chance",
      market: `${fixture.home_team_name} ou empate`,
      team: fixture.home_team_name,
      side: "home",
      line: null,
      direction: "home_or_draw",
      score:
        56 +
        diff * 1.3 +
        safeNumber(homeSide.avg_shots_on_goal_for) * 2 +
        (context.bigTeamGame ? 4 : 0) +
        context.contextBoost * 0.18,
      reason: `${fixture.home_team_name} tem vantagem técnica/territorial pelo recorte recente como mandante.`,
    })
  }

  if (diff <= -5) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "double_chance",
      market: `${fixture.away_team_name} ou empate`,
      team: fixture.away_team_name,
      side: "away",
      line: null,
      direction: "away_or_draw",
      score:
        56 +
        Math.abs(diff) * 1.3 +
        safeNumber(awaySide.avg_shots_on_goal_for) * 2 +
        (context.bigTeamGame ? 4 : 0) +
        context.contextBoost * 0.18,
      reason: `${fixture.away_team_name} chega com força suficiente para proteção em dupla chance.`,
    })
  }

  return picks
}

function buildMatchPicks(fixture, forms, context) {
  const {
    homeGeneral,
    awayGeneral,
    homeSide,
    awaySide,
  } = forms

  const allPicks = [
    ...buildTeamCornersPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context),
    ...buildTotalCornersPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context),
    ...buildCardsPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context),
    ...buildGoalsPicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context),
    ...buildDoubleChancePicks(fixture, homeGeneral, awayGeneral, homeSide, awaySide, context),
  ]

  return allPicks
    .filter((p) => p.score >= 58)
    .sort((a, b) => {
      const pa = MARKET_PRIORITY_ORDER[a.market_type] || 99
      const pb = MARKET_PRIORITY_ORDER[b.market_type] || 99
      if (pa !== pb) return pa - pb
      return b.score - a.score
    })
}

function getBestPickForRadar(picks = []) {
  if (!picks.length) return null

  const sorted = [...picks].sort((a, b) => {
    const pa = MARKET_PRIORITY_ORDER[a.market_type] || 99
    const pb = MARKET_PRIORITY_ORDER[b.market_type] || 99

    const weightedA = safeNumber(a.score) + (pa <= 3 ? 4 : 0)
    const weightedB = safeNumber(b.score) + (pb <= 3 ? 4 : 0)

    return weightedB - weightedA
  })

  return sorted[0]
}

function calculateRadarPriority(fixture, picks, context) {
  const bestPick = getBestPickForRadar(picks)

  if (!bestPick) {
    return {
      radar_score: 0,
      radar_reason: "Sem pick forte suficiente para radar.",
      is_radar_visible: false,
    }
  }

  const startTime = fixture.timestamp ? new Date(fixture.timestamp * 1000) : new Date(fixture.date)
  const hoursUntilGame = (startTime.getTime() - Date.now()) / (1000 * 60 * 60)

  let timeBoost = 0

  if (hoursUntilGame >= -2 && hoursUntilGame <= 6) timeBoost = 12
  else if (hoursUntilGame > 6 && hoursUntilGame <= 24) timeBoost = 9
  else if (hoursUntilGame > 24 && hoursUntilGame <= 72) timeBoost = 5
  else timeBoost = 1

  const marketDiversityBoost = new Set(picks.map((p) => p.market_type)).size * 2

  const radar_score =
    safeNumber(bestPick.score) +
    context.contextBoost +
    timeBoost +
    marketDiversityBoost +
    (context.bigTeamGame ? 8 : 0) +
    (context.importantCompetition ? 8 : 0)

  const is_radar_visible =
    radar_score >= 66 ||
    context.bigTeamGame ||
    context.importantCompetition ||
    safeNumber(bestPick.score) >= 72

  return {
    radar_score: round(clamp(radar_score, 0, 100), 1),
    radar_reason: bestPick.reason,
    is_radar_visible,
  }
}

function limitMarketDistribution(picks = []) {
  const sorted = [...picks].sort((a, b) => b.score - a.score)

  const counts = {
    total: 0,
    league: {},
    market: {},
    goals: 0,
    total_corners: 0,
    team_corners: 0,
    cards: 0,
    double_chance: 0,
    under_goals: 0,
    international: 0,
    brazil: 0,
  }

  const selected = []

  for (const pick of sorted) {
    if (selected.length >= MAX_DAILY_PICKS) break

    const leagueKey = normalizeText(pick.league_name || "")
    const marketKey = normalizeText(pick.market || "")
    const marketType = pick.market_type

    const isBrazil = pick.is_brazil_competition
    const isInternational = pick.is_international_competition

    if ((counts.market[marketKey] || 0) >= MAX_SAME_MARKET_IN_DAILY) continue
    if ((counts.league[leagueKey] || 0) >= MAX_SAME_LEAGUE_IN_DAILY) continue

    if (isBrazil && counts.brazil >= MAX_BRAZIL_IN_DAILY) continue
    if (isInternational && counts.international >= MAX_INTERNATIONAL_IN_DAILY) continue

    if (marketType === "goals" && counts.goals >= MAX_GOALS_MARKETS) continue
    if (marketType === "under_goals" && counts.under_goals >= 2) continue
    if (marketType === "total_corners" && counts.total_corners >= MAX_TOTAL_CORNERS_MARKETS) continue
    if (marketType === "team_corners" && counts.team_corners >= MAX_TEAM_CORNERS_MARKETS) continue
    if (marketType === "cards" && counts.cards >= MAX_CARDS_MARKETS) continue
    if (marketType === "double_chance" && counts.double_chance >= MAX_DOUBLE_CHANCE_MARKETS) continue

    selected.push(pick)

    counts.market[marketKey] = (counts.market[marketKey] || 0) + 1
    counts.league[leagueKey] = (counts.league[leagueKey] || 0) + 1
    counts[marketType] = (counts[marketType] || 0) + 1
    if (isBrazil) counts.brazil++
    if (isInternational) counts.international++
  }

  return selected.map((pick, index) => ({
    ...pick,
    daily_rank: index + 1,
  }))
}

async function upsertMatch(fixture, forms, context, picks, radar) {
  const bestPick = getBestPickForRadar(picks)

  const matchPayload = {
    ...fixture,
    sync_version: SYNC_VERSION,

    home_form_general: forms.homeGeneral,
    away_form_general: forms.awayGeneral,
    home_form_home: forms.homeSide,
    away_form_away: forms.awaySide,

    game_context: context,
    game_profile: context.gameProfile,

    best_market_type: bestPick?.market_type || null,
    best_market: bestPick?.market || null,
    best_pick_score: bestPick?.score || null,
    best_pick_reason: bestPick?.reason || null,

    radar_score: radar.radar_score,
    radar_reason: radar.radar_reason,
    is_radar_visible: radar.is_radar_visible,

    is_match_visible: true,
    is_competition_visible: true,
    updated_at: nowISO(),
  }

  const { error: matchError } = await supabase
    .from("matches")
    .upsert(matchPayload, { onConflict: "fixture_id" })

  if (matchError) {
    console.log("Erro upsert matches:", matchError.message)
  }

  const analysisPayload = {
    fixture_id: fixture.fixture_id,
    sync_version: SYNC_VERSION,

    home_team_name: fixture.home_team_name,
    away_team_name: fixture.away_team_name,
    league_name: fixture.league_name,
    league_country: fixture.league_country,
    date: fixture.date,

    game_context: context,
    home_general: forms.homeGeneral,
    away_general: forms.awayGeneral,
    home_side: forms.homeSide,
    away_side: forms.awaySide,
    picks,

    summary: bestPick
      ? `${bestPick.market} aparece como melhor leitura do jogo. ${bestPick.reason}`
      : "Jogo analisado, mas sem mercado forte o suficiente no momento.",

    updated_at: nowISO(),
  }

  const { error: analysisError } = await supabase
    .from("match_analysis")
    .upsert(analysisPayload, { onConflict: "fixture_id" })

  if (analysisError) {
    console.log("Erro upsert match_analysis:", analysisError.message)
  }
}

async function saveDailyPicks(allPicks) {
  const today = toDateString(new Date())

  const selected = limitMarketDistribution(allPicks)

  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .eq("date_key", today)

  if (deleteError) {
    console.log("Erro limpando daily_picks:", deleteError.message)
  }

  if (!selected.length) return []

  const rows = selected.map((pick) => ({
    date_key: today,
    fixture_id: pick.fixture_id,

    daily_rank: pick.daily_rank,

    market_type: pick.market_type,
    market: pick.market,
    team: pick.team || null,
    side: pick.side || null,
    line: pick.line || null,
    direction: pick.direction || null,

    score: pick.score,
    confidence: confidenceLabel(pick.score),
    reason: pick.reason,

    league_id: pick.league_id,
    league_name: pick.league_name,
    league_country: pick.league_country,

    home_team_name: pick.home_team_name,
    away_team_name: pick.away_team_name,
    date: pick.date,

    game_profile: pick.game_profile,
    is_brazil_competition: pick.is_brazil_competition,
    is_international_competition: pick.is_international_competition,
    is_big_team_game: pick.is_big_team_game,
    is_important_competition: pick.is_important_competition,

    sync_version: SYNC_VERSION,
    created_at: nowISO(),
    updated_at: nowISO(),
  }))

  const { error } = await supabase
    .from("daily_picks")
    .insert(rows)

  if (error) {
    console.log("Erro inserindo daily_picks:", error.message)
  }

  return selected
}

async function updateMatchStatus(fixtures) {
  const rows = fixtures.map((f) => ({
    fixture_id: f.fixture?.id,
    status: f.fixture?.status?.short,
    status_long: f.fixture?.status?.long,
    elapsed: f.fixture?.status?.elapsed,
    home_goals: f.goals?.home,
    away_goals: f.goals?.away,
    updated_at: nowISO(),
  }))

  if (!rows.length) return

  const { error } = await supabase
    .from("match_status")
    .upsert(rows, { onConflict: "fixture_id" })

  if (error) {
    console.log("Erro upsert match_status:", error.message)
  }
}

function enrichPickWithFixture(pick, fixture, context) {
  return {
    ...pick,
    league_id: fixture.league_id,
    league_name: fixture.league_name,
    league_country: fixture.league_country,

    home_team_name: fixture.home_team_name,
    away_team_name: fixture.away_team_name,
    date: fixture.date,

    game_profile: context.gameProfile,
    is_brazil_competition: context.brazilCompetition,
    is_international_competition: context.internationalCompetition,
    is_big_team_game: context.bigTeamGame,
    is_important_competition: context.importantCompetition,
  }
}

async function processFixture(rawFixture) {
  const fixture = getFixtureBase(rawFixture)

  if (!fixture.fixture_id) return null
  if (!fixture.home_team_id || !fixture.away_team_id) return null
  if (isBadCompetition(fixture.league_name)) return null

  console.log(
    `Analisando ${fixture.home_team_name} x ${fixture.away_team_name} - ${fixture.league_name}`
  )

  const [
    homeGeneralForm,
    awayGeneralForm,
    homeHomeForm,
    awayAwayForm,
  ] = await Promise.all([
    fetchTeamRecentForm(
      fixture.home_team_id,
      fixture.league_id,
      fixture.season,
      null
    ),
    fetchTeamRecentForm(
      fixture.away_team_id,
      fixture.league_id,
      fixture.season,
      null
    ),
    fetchTeamRecentForm(
      fixture.home_team_id,
      fixture.league_id,
      fixture.season,
      "home"
    ),
    fetchTeamRecentForm(
      fixture.away_team_id,
      fixture.league_id,
      fixture.season,
      "away"
    ),
  ])

  if (
    homeGeneralForm.agg.matches < MIN_REQUIRED_RECENT_MATCHES ||
    awayGeneralForm.agg.matches < MIN_REQUIRED_RECENT_MATCHES
  ) {
    console.log("Poucos jogos recentes. Salvando jogo básico, sem pick forte.")

    const emptyForms = {
      homeGeneral: homeGeneralForm.agg,
      awayGeneral: awayGeneralForm.agg,
      homeSide: homeHomeForm.agg,
      awaySide: awayAwayForm.agg,
    }

    const context = buildGameContext(
      fixture,
      emptyForms.homeGeneral,
      emptyForms.awayGeneral,
      emptyForms.homeSide,
      emptyForms.awaySide
    )

    await upsertMatch(
      fixture,
      emptyForms,
      context,
      [],
      {
        radar_score: context.contextBoost,
        radar_reason: "Poucos dados recentes disponíveis.",
        is_radar_visible: context.importantCompetition || context.bigTeamGame,
      }
    )

    return {
      fixture,
      picks: [],
      radar_score: context.contextBoost,
    }
  }

  const forms = {
    homeGeneral: homeGeneralForm.agg,
    awayGeneral: awayGeneralForm.agg,
    homeSide: homeHomeForm.agg,
    awaySide: awayAwayForm.agg,
  }

  const context = buildGameContext(
    fixture,
    forms.homeGeneral,
    forms.awayGeneral,
    forms.homeSide,
    forms.awaySide
  )

  let picks = buildMatchPicks(fixture, forms, context)

  picks = picks.map((pick) => enrichPickWithFixture(pick, fixture, context))

  const radar = calculateRadarPriority(fixture, picks, context)

  await upsertMatch(fixture, forms, context, picks, radar)

  return {
    fixture,
    picks,
    radar_score: radar.radar_score,
  }
}

async function fetchUpcomingFixtures() {
  const now = new Date()
  const end = addHours(now, WINDOW_HOURS)

  const from = toDateString(now)
  const to = toDateString(end)

  const fixtures = await apiGet("/fixtures", {
    from,
    to,
    timezone: TIMEZONE,
  })

  return fixtures
    .filter((f) => {
      const status = f.fixture?.status?.short
      return ["NS", "TBD"].includes(status)
    })
    .filter((f) => !isBadCompetition(f.league?.name))
}

async function runSync() {
  console.log(`Scoutly Sync ${SYNC_VERSION} iniciado em ${nowISO()}`)

  const fixtures = await fetchUpcomingFixtures()

  console.log(`Fixtures encontrados: ${fixtures.length}`)

  await updateMatchStatus(fixtures)

  const allDailyCandidates = []

  const priorityFixtures = fixtures.sort((a, b) => {
    const aImportant =
      (isImportantCompetition(a.league?.name) ? 20 : 0) +
      (hasBigTeam(a.teams?.home?.name, a.teams?.away?.name) ? 20 : 0)

    const bImportant =
      (isImportantCompetition(b.league?.name) ? 20 : 0) +
      (hasBigTeam(b.teams?.home?.name, b.teams?.away?.name) ? 20 : 0)

    const aTime = a.fixture?.timestamp || 0
    const bTime = b.fixture?.timestamp || 0

    if (aImportant !== bImportant) return bImportant - aImportant
    return aTime - bTime
  })

  for (const rawFixture of priorityFixtures) {
    try {
      const result = await processFixture(rawFixture)

      if (result?.picks?.length) {
        for (const pick of result.picks) {
          allDailyCandidates.push(pick)
        }
      }
    } catch (err) {
      console.log(
        `Erro analisando fixture ${rawFixture.fixture?.id}:`,
        err.message
      )
    }
  }

  const savedDaily = await saveDailyPicks(allDailyCandidates)

  console.log(`Daily picks salvos: ${savedDaily.length}`)
  console.log(`Scoutly Sync ${SYNC_VERSION} finalizado em ${nowISO()}`)
}

runSync()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Erro geral no Sync:", err)
    process.exit(1)
  })
