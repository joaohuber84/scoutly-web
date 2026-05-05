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
const SYNC_VERSION = "V13.6"

const WINDOW_HOURS = Number(process.env.WINDOW_HOURS || 168)

const FORM_LIMIT_GENERAL = 10
const FORM_LIMIT_HOME_AWAY = 5
const FORM_LIMIT_LAST5 = 5
const MAX_RECENT_FIXTURES_FETCH = 20
const MIN_REQUIRED_RECENT_MATCHES = 3

const MAX_DAILY_PICKS = 20
const MAX_RADAR_GAMES = 80

const MAX_SAME_MARKET_IN_DAILY = 2
const MAX_SAME_LEAGUE_IN_DAILY = 4
const MAX_INTERNATIONAL_IN_DAILY = 8
const MAX_BRAZIL_IN_DAILY = 6

const MAX_GOALS_MARKETS = 5
const MAX_TOTAL_CORNERS_MARKETS = 5
const MAX_TEAM_CORNERS_MARKETS = 6
const MAX_CARDS_MARKETS = 5
const MAX_SHOTS_MARKETS = 4
const MAX_SOT_MARKETS = 4
const MAX_DOUBLE_CHANCE_MARKETS = 3

const MARKET_PRIORITY_ORDER = {
  team_corners: 1,
  total_corners: 2,
  shots_on_target: 3,
  shots_total: 4,
  cards: 5,
  goals: 6,
  under_goals: 7,
  double_chance: 8,
}

const IMPORTANT_KEYWORDS = [
  "champions league",
  "libertadores",
  "sudamericana",
  "europa league",
  "conference league",
  "world cup",
  "club world cup",
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
  "under 17",
  "under 18",
  "under 19",
  "under 20",
  "under 21",
  "under 23",
  "youth",
  "reserve",
  "reserves",
  "academy",
  "junior",
  "juniors",
]

const BIG_TEAMS = [
  "real madrid",
  "barcelona",
  "atletico madrid",
  "atlético madrid",
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
  if (typeof value === "number") return Number.isFinite(value) ? value : 0
  const parsed = Number(String(value).replace("%", "").trim())
  return Number.isFinite(parsed) ? parsed : 0
}

function confidenceLabel(score) {
  if (score >= 82) return "Muito forte"
  if (score >= 74) return "Forte"
  if (score >= 66) return "Boa"
  if (score >= 58) return "Moderada"
  return "Leve"
}

function probabilityFromScore(score) {
  return round(clamp(safeNumber(score) / 100, 0.45, 0.9), 3)
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

  return (
    c.includes("brazil") ||
    c.includes("brasil") ||
    l.includes("brasileirao") ||
    l.includes("brasileirão") ||
    l.includes("copa do brasil")
  )
}

function isInternationalCompetition(country = "", leagueName = "") {
  const c = normalizeText(country)
  const l = normalizeText(leagueName)

  return (
    c.includes("world") ||
    c.includes("international") ||
    l.includes("world cup") ||
    l.includes("club world cup") ||
    l.includes("euro") ||
    l.includes("nations league") ||
    l.includes("copa america") ||
    l.includes("friendlies") ||
    l.includes("qualifiers") ||
    l.includes("champions league") ||
    l.includes("europa league") ||
    l.includes("conference league") ||
    l.includes("libertadores") ||
    l.includes("sudamericana")
  )
}

function hasBigTeam(home = "", away = "") {
  const h = normalizeText(home)
  const a = normalizeText(away)

  return BIG_TEAMS.some((team) => {
    const t = normalizeText(team)
    return h.includes(t) || a.includes(t)
  })
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
    const item = teamStats.statistics.find(
      (x) => normalizeText(x.type) === normalizeText(candidate)
    )

    if (item) return safeNumber(item.value)
  }

  return 0
}

function extractFixtureStat(statsArr, fixture, teamId, opponentId, side) {
  const teamGoals = side === "home" ? fixture.goals?.home : fixture.goals?.away
  const opponentGoals = side === "home" ? fixture.goals?.away : fixture.goals?.home

  const corners = statValue(statsArr, teamId, ["Corner Kicks", "Corners"])
  const opponentCorners = statValue(statsArr, opponentId, ["Corner Kicks", "Corners"])

  const shotsOnGoal = statValue(statsArr, teamId, ["Shots on Goal", "Shots on target"])
  const opponentShotsOnGoal = statValue(statsArr, opponentId, ["Shots on Goal", "Shots on target"])

  const totalShots = statValue(statsArr, teamId, ["Total Shots", "Shots total"])
  const opponentTotalShots = statValue(statsArr, opponentId, ["Total Shots", "Shots total"])

  const yellowCards = statValue(statsArr, teamId, ["Yellow Cards"])
  const redCards = statValue(statsArr, teamId, ["Red Cards"])
  const opponentYellowCards = statValue(statsArr, opponentId, ["Yellow Cards"])
  const opponentRedCards = statValue(statsArr, opponentId, ["Red Cards"])

  const cards = yellowCards + redCards
  const opponentCards = opponentYellowCards + opponentRedCards

  return {
    fixture_id: fixture.fixture?.id,
    date: fixture.fixture?.date,
    side,

    team_goals: safeNumber(teamGoals),
    opponent_goals: safeNumber(opponentGoals),
    total_goals: safeNumber(teamGoals) + safeNumber(opponentGoals),

    corners,
    opponent_corners: opponentCorners,
    total_corners: corners + opponentCorners,

    shots_on_goal: shotsOnGoal,
    opponent_shots_on_goal: opponentShotsOnGoal,
    total_match_shots_on_goal: shotsOnGoal + opponentShotsOnGoal,

    total_shots: totalShots,
    opponent_total_shots: opponentTotalShots,
    total_match_shots: totalShots + opponentTotalShots,

    yellow_cards: yellowCards,
    red_cards: redCards,
    cards,
    opponent_cards: opponentCards,
    total_cards: cards + opponentCards,
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
      avg_shots_on_goal_against: 0,
      avg_total_shots_for: 0,
      avg_total_shots_against: 0,
      avg_cards_for: 0,
      avg_cards_against: 0,
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
      team_over_75_shots_pct: 0,
      team_over_95_shots_pct: 0,
      team_over_115_shots_pct: 0,
      team_over_25_sot_pct: 0,
      team_over_35_sot_pct: 0,
      team_over_45_sot_pct: 0,
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
    avg_shots_on_goal_against: round(sum("opponent_shots_on_goal") / n),
    avg_total_shots_for: round(sum("total_shots") / n),
    avg_total_shots_against: round(sum("opponent_total_shots") / n),

    avg_cards_for: round(sum("cards") / n),
    avg_cards_against: round(sum("opponent_cards") / n),
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

    team_over_75_shots_pct: pct(count((x) => x.total_shots >= 8), n),
    team_over_95_shots_pct: pct(count((x) => x.total_shots >= 10), n),
    team_over_115_shots_pct: pct(count((x) => x.total_shots >= 12), n),

    team_over_25_sot_pct: pct(count((x) => x.shots_on_goal >= 3), n),
    team_over_35_sot_pct: pct(count((x) => x.shots_on_goal >= 4), n),
    team_over_45_sot_pct: pct(count((x) => x.shots_on_goal >= 5), n),

    cards_over_15_pct: pct(count((x) => x.total_cards >= 2), n),
    cards_over_25_pct: pct(count((x) => x.total_cards >= 3), n),
    cards_over_35_pct: pct(count((x) => x.total_cards >= 4), n),
    cards_under_55_pct: pct(count((x) => x.total_cards <= 5), n),
  }
}

function getFixtureBase(fixture) {
  const homeTeam = fixture.teams?.home?.name
  const awayTeam = fixture.teams?.away?.name
  const leagueName = fixture.league?.name
  const country = fixture.league?.country

  return {
    fixture_id: fixture.fixture?.id,
    match_id: fixture.fixture?.id,
    id: fixture.fixture?.id,

    date: fixture.fixture?.date,
    kickoff: fixture.fixture?.date,
    timestamp: fixture.fixture?.timestamp,

    status: fixture.fixture?.status?.short,
    status_long: fixture.fixture?.status?.long,

    league_id: fixture.league?.id,
    league_name: leagueName,
    league: leagueName,
    league_country: country,
    country,

    season: fixture.league?.season,

    home_team_id: fixture.teams?.home?.id,
    home_team_name: homeTeam,

    away_team_id: fixture.teams?.away?.id,
    away_team_name: awayTeam,

    home_goals: fixture.goals?.home,
    away_goals: fixture.goals?.away,

    updated_at: nowISO(),
  }
}

async function fetchTeamRecentForm(teamId, leagueId, season, sideFilter = null) {
  const fixtures = await apiGet("/fixtures", {
    team: teamId,
    season,
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

  const selected = finished.slice(0, sideFilter ? FORM_LIMIT_HOME_AWAY : FORM_LIMIT_GENERAL)
  const last5 = finished.slice(0, FORM_LIMIT_LAST5)

  const rows = []
  const rowsLast5 = []

  for (const fx of selected) {
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

  for (const fx of last5) {
    const stats = await apiGet("/fixtures/statistics", { fixture: fx.fixture?.id })

    const isHome = fx.teams?.home?.id === teamId
    const opponentId = isHome ? fx.teams?.away?.id : fx.teams?.home?.id

    rowsLast5.push(
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
    last5: rowsLast5,
    agg: aggregateTeamForm(rows),
    agg_last5: aggregateTeamForm(rowsLast5),
  }
}

function buildGameContext(fixture, homeGeneral, awayGeneral, homeSide, awaySide) {
  const leagueName = fixture.league_name || ""
  const country = fixture.country || ""
  const home = fixture.home_team_name || ""
  const away = fixture.away_team_name || ""

  const importantCompetition = isImportantCompetition(leagueName)
  const brazilCompetition = isBrazilCompetition(country, leagueName)
  const internationalCompetition = isInternationalCompetition(country, leagueName)
  const bigTeamGame = hasBigTeam(home, away)

  const avgGoalProfile =
    (safeNumber(homeGeneral.avg_total_goals) +
      safeNumber(awayGeneral.avg_total_goals)) / 2

  const avgCornerProfile =
    (safeNumber(homeGeneral.avg_total_corners) +
      safeNumber(awayGeneral.avg_total_corners)) / 2

  const avgCardProfile =
    (safeNumber(homeGeneral.avg_cards_total) +
      safeNumber(awayGeneral.avg_cards_total)) / 2

  const homeAttackStrength =
    safeNumber(homeSide.avg_goals_for) +
    safeNumber(homeSide.avg_shots_on_goal_for) * 0.4 +
    safeNumber(homeSide.avg_corners_for) * 0.2

  const awayAttackStrength =
    safeNumber(awaySide.avg_goals_for) +
    safeNumber(awaySide.avg_shots_on_goal_for) * 0.4 +
    safeNumber(awaySide.avg_corners_for) * 0.2

  const balanceGap = Math.abs(homeAttackStrength - awayAttackStrength)

  let gameProfile = "balanced"

  if (avgGoalProfile >= 2.7 && avgCornerProfile >= 8.5) gameProfile = "open_game"
  if (avgGoalProfile <= 2.1 && avgCardProfile >= 4) gameProfile = "tight_physical"
  if (balanceGap >= 1.5) gameProfile = "favorite_pressure"
  if (importantCompetition && avgCardProfile >= 3.5) gameProfile = "high_stakes"

  const contextBoost =
    (importantCompetition ? 10 : 0) +
    (bigTeamGame ? 8 : 0) +
    (brazilCompetition ? 4 : 0) +
    (internationalCompetition ? 4 : 0)

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

function buildTeamAverages(forms) {
  return {
    home: {
      matches: forms.homeGeneral.matches,
      goals_for: forms.homeGeneral.avg_goals_for,
      goals_against: forms.homeGeneral.avg_goals_against,
      shots_total: forms.homeGeneral.avg_total_shots_for,
      shots_on_goal: forms.homeGeneral.avg_shots_on_goal_for,
      corners: forms.homeGeneral.avg_corners_for,
      cards: forms.homeGeneral.avg_cards_for,
    },
    away: {
      matches: forms.awayGeneral.matches,
      goals_for: forms.awayGeneral.avg_goals_for,
      goals_against: forms.awayGeneral.avg_goals_against,
      shots_total: forms.awayGeneral.avg_total_shots_for,
      shots_on_goal: forms.awayGeneral.avg_shots_on_goal_for,
      corners: forms.awayGeneral.avg_corners_for,
      cards: forms.awayGeneral.avg_cards_for,
    },
  }
}

function buildLast5Summary(forms) {
  return {
    home: {
      matches: forms.homeLast5.matches,
      goals_for: forms.homeLast5.avg_goals_for,
      goals_against: forms.homeLast5.avg_goals_against,
      shots_total: forms.homeLast5.avg_total_shots_for,
      shots_on_goal: forms.homeLast5.avg_shots_on_goal_for,
      corners: forms.homeLast5.avg_corners_for,
      cards: forms.homeLast5.avg_cards_for,
      over_25_corners_pct: forms.homeLast5.team_over_25_corners_pct,
      over_35_corners_pct: forms.homeLast5.team_over_35_corners_pct,
      over_45_corners_pct: forms.homeLast5.team_over_45_corners_pct,
    },
    away: {
      matches: forms.awayLast5.matches,
      goals_for: forms.awayLast5.avg_goals_for,
      goals_against: forms.awayLast5.avg_goals_against,
      shots_total: forms.awayLast5.avg_total_shots_for,
      shots_on_goal: forms.awayLast5.avg_shots_on_goal_for,
      corners: forms.awayLast5.avg_corners_for,
      cards: forms.awayLast5.avg_cards_for,
      over_25_corners_pct: forms.awayLast5.team_over_25_corners_pct,
      over_35_corners_pct: forms.awayLast5.team_over_35_corners_pct,
      over_45_corners_pct: forms.awayLast5.team_over_45_corners_pct,
    },
  }
}

function weightedAverage(parts = []) {
  let totalWeight = 0
  let total = 0

  for (const item of parts) {
    const value = safeNumber(item.value)
    const weight = safeNumber(item.weight)

    if (weight <= 0) continue

    total += value * weight
    totalWeight += weight
  }

  if (!totalWeight) return 0
  return round(total / totalWeight)
}

function buildStatProjection(fixture, forms, context) {
  const hG = forms.homeGeneral
  const aG = forms.awayGeneral
  const hS = forms.homeSide
  const aS = forms.awaySide
  const h5 = forms.homeLast5
  const a5 = forms.awayLast5

  const home = {
    goals: weightedAverage([
      { value: hS.avg_goals_for, weight: 0.34 },
      { value: aS.avg_goals_against, weight: 0.24 },
      { value: hG.avg_goals_for, weight: 0.18 },
      { value: aG.avg_goals_against, weight: 0.12 },
      { value: h5.avg_goals_for, weight: 0.12 },
    ]),

    shots_total: weightedAverage([
      { value: hS.avg_total_shots_for, weight: 0.34 },
      { value: aS.avg_total_shots_against, weight: 0.26 },
      { value: hG.avg_total_shots_for, weight: 0.16 },
      { value: aG.avg_total_shots_against, weight: 0.12 },
      { value: h5.avg_total_shots_for, weight: 0.12 },
    ]),

    shots_on_goal: weightedAverage([
      { value: hS.avg_shots_on_goal_for, weight: 0.34 },
      { value: aS.avg_shots_on_goal_against, weight: 0.26 },
      { value: hG.avg_shots_on_goal_for, weight: 0.16 },
      { value: aG.avg_shots_on_goal_against, weight: 0.12 },
      { value: h5.avg_shots_on_goal_for, weight: 0.12 },
    ]),

    corners: weightedAverage([
      { value: hS.avg_corners_for, weight: 0.36 },
      { value: aS.avg_corners_against, weight: 0.26 },
      { value: hG.avg_corners_for, weight: 0.14 },
      { value: aG.avg_corners_against, weight: 0.12 },
      { value: h5.avg_corners_for, weight: 0.12 },
    ]),

    cards: weightedAverage([
      { value: hS.avg_cards_for, weight: 0.30 },
      { value: aS.avg_cards_against, weight: 0.24 },
      { value: hG.avg_cards_for, weight: 0.20 },
      { value: aG.avg_cards_against, weight: 0.14 },
      { value: h5.avg_cards_for, weight: 0.12 },
    ]),
  }

  const away = {
    goals: weightedAverage([
      { value: aS.avg_goals_for, weight: 0.34 },
      { value: hS.avg_goals_against, weight: 0.24 },
      { value: aG.avg_goals_for, weight: 0.18 },
      { value: hG.avg_goals_against, weight: 0.12 },
      { value: a5.avg_goals_for, weight: 0.12 },
    ]),

    shots_total: weightedAverage([
      { value: aS.avg_total_shots_for, weight: 0.34 },
      { value: hS.avg_total_shots_against, weight: 0.26 },
      { value: aG.avg_total_shots_for, weight: 0.16 },
      { value: hG.avg_total_shots_against, weight: 0.12 },
      { value: a5.avg_total_shots_for, weight: 0.12 },
    ]),

    shots_on_goal: weightedAverage([
      { value: aS.avg_shots_on_goal_for, weight: 0.34 },
      { value: hS.avg_shots_on_goal_against, weight: 0.26 },
      { value: aG.avg_shots_on_goal_for, weight: 0.16 },
      { value: hG.avg_shots_on_goal_against, weight: 0.12 },
      { value: a5.avg_shots_on_goal_for, weight: 0.12 },
    ]),

    corners: weightedAverage([
      { value: aS.avg_corners_for, weight: 0.36 },
      { value: hS.avg_corners_against, weight: 0.26 },
      { value: aG.avg_corners_for, weight: 0.14 },
      { value: hG.avg_corners_against, weight: 0.12 },
      { value: a5.avg_corners_for, weight: 0.12 },
    ]),

    cards: weightedAverage([
      { value: aS.avg_cards_for, weight: 0.30 },
      { value: hS.avg_cards_against, weight: 0.24 },
      { value: aG.avg_cards_for, weight: 0.20 },
      { value: hG.avg_cards_against, weight: 0.14 },
      { value: a5.avg_cards_for, weight: 0.12 },
    ]),
  }

  if (context.gameProfile === "open_game") {
    home.shots_total = round(home.shots_total * 1.05)
    away.shots_total = round(away.shots_total * 1.05)
    home.shots_on_goal = round(home.shots_on_goal * 1.04)
    away.shots_on_goal = round(away.shots_on_goal * 1.04)
    home.corners = round(home.corners * 1.04)
    away.corners = round(away.corners * 1.04)
  }

  if (context.gameProfile === "favorite_pressure") {
    if (context.homeAttackStrength > context.awayAttackStrength) {
      home.corners = round(home.corners * 1.08)
      home.shots_total = round(home.shots_total * 1.06)
      home.shots_on_goal = round(home.shots_on_goal * 1.04)
    } else {
      away.corners = round(away.corners * 1.08)
      away.shots_total = round(away.shots_total * 1.06)
      away.shots_on_goal = round(away.shots_on_goal * 1.04)
    }
  }

  if (context.gameProfile === "high_stakes" || context.gameProfile === "tight_physical") {
    home.cards = round(home.cards * 1.1)
    away.cards = round(away.cards * 1.1)
  }

  return {
    home,
    away,
    match: {
      goals: round(home.goals + away.goals),
      shots_total: round(home.shots_total + away.shots_total),
      shots_on_goal: round(home.shots_on_goal + away.shots_on_goal),
      corners: round(home.corners + away.corners),
      cards: round(home.cards + away.cards),
    },
  }
}

function buildAnalysisSections(fixture, teamAverages, statProjection, last5Summary, context) {
  return {
    comparison_rows: [
      { label: "Gols marcados", home: teamAverages.home.goals_for, away: teamAverages.away.goals_for },
      { label: "Gols sofridos", home: teamAverages.home.goals_against, away: teamAverages.away.goals_against },
      { label: "Finalizações", home: teamAverages.home.shots_total, away: teamAverages.away.shots_total },
      { label: "No gol", home: teamAverages.home.shots_on_goal, away: teamAverages.away.shots_on_goal },
      { label: "Escanteios", home: teamAverages.home.corners, away: teamAverages.away.corners },
      { label: "Cartões", home: teamAverages.home.cards, away: teamAverages.away.cards },
    ],

    projection_rows: [
      { label: "Gols", home: statProjection.home.goals, away: statProjection.away.goals, match: statProjection.match.goals },
      { label: "Finalizações", home: statProjection.home.shots_total, away: statProjection.away.shots_total, match: statProjection.match.shots_total },
      { label: "No gol", home: statProjection.home.shots_on_goal, away: statProjection.away.shots_on_goal, match: statProjection.match.shots_on_goal },
      { label: "Escanteios", home: statProjection.home.corners, away: statProjection.away.corners, match: statProjection.match.corners },
      { label: "Cartões", home: statProjection.home.cards, away: statProjection.away.cards, match: statProjection.match.cards },
    ],

    last5_rows: [
      { label: "Gols", home: last5Summary.home.goals_for, away: last5Summary.away.goals_for },
      { label: "Finalizações", home: last5Summary.home.shots_total, away: last5Summary.away.shots_total },
      { label: "No gol", home: last5Summary.home.shots_on_goal, away: last5Summary.away.shots_on_goal },
      { label: "Escanteios", home: last5Summary.home.corners, away: last5Summary.away.corners },
      { label: "Cartões", home: last5Summary.home.cards, away: last5Summary.away.cards },
    ],

    context: {
      profile: context.gameProfile,
      important: context.importantCompetition,
      big_team_game: context.bigTeamGame,
      brazil: context.brazilCompetition,
      international: context.internationalCompetition,
    },
  }
}

function pushPick(picks, pick) {
  if (!pick) return
  if (!pick.market_type || !pick.market) return
  if (!Number.isFinite(Number(pick.score))) return

  picks.push({
    ...pick,
    score: round(clamp(pick.score, 0, 100), 1),
    probability: probabilityFromScore(pick.score),
  })
}

function buildGoalsPicks(fixture, forms, context, projection) {
  const picks = []

  const over15Pct =
    (safeNumber(forms.homeGeneral.over_15_goals_pct) +
      safeNumber(forms.awayGeneral.over_15_goals_pct)) / 2

  const over25Pct =
    (safeNumber(forms.homeGeneral.over_25_goals_pct) +
      safeNumber(forms.awayGeneral.over_25_goals_pct)) / 2

  const under35Pct =
    (safeNumber(forms.homeGeneral.under_35_goals_pct) +
      safeNumber(forms.awayGeneral.under_35_goals_pct)) / 2

  const projectedGoals = safeNumber(projection.match.goals)
  const projectedSot = safeNumber(projection.match.shots_on_goal)

  if (over15Pct >= 60 && projectedGoals >= 2.05) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "goals",
      market: "Mais de 1.5 gols",
      side: "match",
      line: 1.5,
      direction: "over",
      score:
        50 +
        over15Pct * 0.32 +
        projectedGoals * 5 +
        projectedSot * 1.2 +
        (context.gameProfile === "open_game" ? 5 : 0) +
        context.contextBoost * 0.18,
      reason: `Projeção de ${round(projectedGoals)} gols e ${round(over15Pct)}% de consistência recente acima de 1.5 gols.`,
    })
  }

  if (over25Pct >= 54 && projectedGoals >= 2.55 && projectedSot >= 7) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "goals",
      market: "Mais de 2.5 gols",
      side: "match",
      line: 2.5,
      direction: "over",
      score:
        44 +
        over25Pct * 0.34 +
        projectedGoals * 6 +
        projectedSot * 1.1 +
        (context.gameProfile === "open_game" ? 6 : 0) +
        context.contextBoost * 0.14,
      reason: `Jogo com projeção ofensiva favorável: ${round(projectedGoals)} gols e ${round(projectedSot)} finalizações no gol.`,
    })
  }

  if (under35Pct >= 64 && projectedGoals <= 2.75 && context.gameProfile !== "open_game") {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "under_goals",
      market: "Menos de 3.5 gols",
      side: "match",
      line: 3.5,
      direction: "under",
      score:
        48 +
        under35Pct * 0.33 +
        (3.5 - projectedGoals) * 5 +
        (context.gameProfile === "tight_physical" ? 5 : 0) +
        context.contextBoost * 0.08,
      reason: `Linha conservadora: projeção de ${round(projectedGoals)} gols e boa taxa recente abaixo de 3.5.`,
    })
  }

  return picks
}

function buildTotalCornersPicks(fixture, forms, context, projection) {
  const picks = []

  const over75Pct =
    (safeNumber(forms.homeGeneral.over_75_corners_pct) +
      safeNumber(forms.awayGeneral.over_75_corners_pct)) / 2

  const over85Pct =
    (safeNumber(forms.homeGeneral.over_85_corners_pct) +
      safeNumber(forms.awayGeneral.over_85_corners_pct)) / 2

  const over95Pct =
    (safeNumber(forms.homeGeneral.over_95_corners_pct) +
      safeNumber(forms.awayGeneral.over_95_corners_pct)) / 2

  const projectedCorners = safeNumber(projection.match.corners)

  if (over75Pct >= 58 && projectedCorners >= 7.8) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "total_corners",
      market: "Mais de 7.5 escanteios",
      side: "match",
      line: 7.5,
      direction: "over",
      score:
        47 +
        over75Pct * 0.34 +
        projectedCorners * 3 +
        (context.gameProfile === "open_game" || context.gameProfile === "favorite_pressure" ? 5 : 0) +
        context.contextBoost * 0.18,
      reason: `Projeção de ${round(projectedCorners)} escanteios totais, com boa consistência recente acima da linha 7.5.`,
    })
  }

  if (over85Pct >= 54 && projectedCorners >= 8.6) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "total_corners",
      market: "Mais de 8.5 escanteios",
      side: "match",
      line: 8.5,
      direction: "over",
      score:
        44 +
        over85Pct * 0.35 +
        projectedCorners * 3.1 +
        (context.gameProfile === "favorite_pressure" ? 5 : 0) +
        context.contextBoost * 0.15,
      reason: `Jogo projeta volume forte de cantos: ${round(projectedCorners)} escanteios no total.`,
    })
  }

  if (over95Pct >= 50 && projectedCorners >= 9.5) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "total_corners",
      market: "Mais de 9.5 escanteios",
      side: "match",
      line: 9.5,
      direction: "over",
      score:
        42 +
        over95Pct * 0.35 +
        projectedCorners * 3.2 +
        (context.gameProfile === "favorite_pressure" ? 6 : 0) +
        context.contextBoost * 0.12,
      reason: `Linha mais forte de escanteios, sustentada por projeção próxima/acima de 10 cantos.`,
    })
  }

  return picks
}

function buildTeamCornersPicks(fixture, forms, context, projection) {
  const picks = []

  const homeCorners = safeNumber(projection.home.corners)
  const awayCorners = safeNumber(projection.away.corners)

  const homePct = safeNumber(forms.homeLast5.team_over_45_corners_pct)
  const awayPct = safeNumber(forms.awayLast5.team_over_45_corners_pct)

  if (homeCorners >= 4.5 && homePct >= 55) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "team_corners",
      market: `${fixture.home_team_name} mais de 4.5 escanteios`,
      side: "home",
      line: 4.5,
      direction: "over",
      score:
        46 +
        homeCorners * 6 +
        homePct * 0.35 +
        (context.homeAttackStrength > context.awayAttackStrength ? 6 : 0) +
        context.contextBoost * 0.15,
      reason: `Últimos 5 jogos mostram consistência e projeção de ${round(homeCorners)} escanteios.`,
    })
  }

  if (awayCorners >= 4.5 && awayPct >= 55) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "team_corners",
      market: `${fixture.away_team_name} mais de 4.5 escanteios`,
      side: "away",
      line: 4.5,
      direction: "over",
      score:
        46 +
        awayCorners * 6 +
        awayPct * 0.35 +
        (context.awayAttackStrength > context.homeAttackStrength ? 6 : 0) +
        context.contextBoost * 0.15,
      reason: `Time visitante consistente nos últimos jogos com projeção de ${round(awayCorners)}.`,
    })
  }

  return picks
}

function buildCardsPicks(fixture, forms, context, projection) {
  const picks = []

  const cards = safeNumber(projection.match.cards)

  const highCardContext =
    context.gameProfile === "high_stakes" ||
    context.gameProfile === "tight_physical"

  if (cards >= 3.5) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "cards",
      market: "Mais de 3.5 cartões",
      side: "match",
      line: 3.5,
      direction: "over",
      score:
        45 +
        cards * 6 +
        (highCardContext ? 8 : 0) +
        context.contextBoost * 0.15,
      reason: `Projeção de ${round(cards)} cartões com perfil físico.`,
    })
  }

  return picks
}

function buildShotsPicks(fixture, forms, context, projection) {
  const picks = []

  const homeSOT = safeNumber(projection.home.shots_on_goal)
  const awaySOT = safeNumber(projection.away.shots_on_goal)

  if (homeSOT >= 4) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "shots_on_target",
      market: `${fixture.home_team_name} mais de 3.5 chutes no gol`,
      side: "home",
      line: 3.5,
      direction: "over",
      score:
        44 +
        homeSOT * 7 +
        (context.homeAttackStrength > context.awayAttackStrength ? 6 : 0),
      reason: `Projeção ofensiva forte do mandante.`,
    })
  }

  if (awaySOT >= 4) {
    pushPick(picks, {
      fixture_id: fixture.fixture_id,
      market_type: "shots_on_target",
      market: `${fixture.away_team_name} mais de 3.5 chutes no gol`,
      side: "away",
      line: 3.5,
      direction: "over",
      score:
        44 +
        awaySOT * 7 +
        (context.awayAttackStrength > context.homeAttackStrength ? 6 : 0),
      reason: `Visitante com bom volume ofensivo.`,
    })
  }

  return picks
}

function applyMarketLimits(picks) {
  const countByMarket = {}
  const countByLeague = {}

  return picks.filter((pick) => {
    const market = pick.market_type
    const league = pick.league_id || "unknown"

    countByMarket[market] = (countByMarket[market] || 0) + 1
    countByLeague[league] = (countByLeague[league] || 0) + 1

    if (countByMarket[market] > MAX_SAME_MARKET_IN_DAILY) return false
    if (countByLeague[league] > MAX_SAME_LEAGUE_IN_DAILY) return false

    return true
  })
}

async function processFixture(fixture) {
  try {
    const base = getFixtureBase(fixture)

    const homeData = await fetchTeamRecentForm(base.home_team_id, base.league_id, base.season)
    const awayData = await fetchTeamRecentForm(base.away_team_id, base.league_id, base.season)

    const homeSide = await fetchTeamRecentForm(base.home_team_id, base.league_id, base.season, "home")
    const awaySide = await fetchTeamRecentForm(base.away_team_id, base.league_id, base.season, "away")

    if (
      homeData.raw.length < MIN_REQUIRED_RECENT_MATCHES ||
      awayData.raw.length < MIN_REQUIRED_RECENT_MATCHES
    ) {
      return null
    }

    const forms = {
      homeGeneral: homeData.agg,
      awayGeneral: awayData.agg,
      homeSide: homeSide.agg,
      awaySide: awaySide.agg,
      homeLast5: homeData.agg_last5,
      awayLast5: awayData.agg_last5,
    }

    const context = buildGameContext(
      base,
      forms.homeGeneral,
      forms.awayGeneral,
      forms.homeSide,
      forms.awaySide
    )

    const teamAverages = buildTeamAverages({
      homeGeneral: forms.homeGeneral,
      awayGeneral: forms.awayGeneral,
    })

    const last5Summary = buildLast5Summary({
      homeLast5: forms.homeLast5,
      awayLast5: forms.awayLast5,
    })

    const projection = buildStatProjection(base, forms, context)

    const analysis = buildAnalysisSections(
      base,
      teamAverages,
      projection,
      last5Summary,
      context
    )

    let picks = []

    picks = picks.concat(buildGoalsPicks(base, forms, context, projection))
    picks = picks.concat(buildTotalCornersPicks(base, forms, context, projection))
    picks = picks.concat(buildTeamCornersPicks(base, forms, context, projection))
    picks = picks.concat(buildCardsPicks(base, forms, context, projection))
    picks = picks.concat(buildShotsPicks(base, forms, context, projection))

    picks = applyMarketLimits(picks)
      .sort((a, b) => b.score - a.score)
      .slice(0, 5)

    return {
      base,
      analysis,
      picks,
      score: picks.length ? picks[0].score : 0,
    }
  } catch (err) {
    console.error("Erro ao processar fixture:", err)
    return null
  }
}

async function runSync() {
  console.log("🚀 SYNC V13.6 START")

  const fixtures = await apiGet("/fixtures", {
    next: WINDOW_HOURS,
    timezone: TIMEZONE,
  })

  const results = []

  for (const f of fixtures) {
    const processed = await processFixture(f)
    if (processed) results.push(processed)
  }

  const sorted = results
    .sort((a, b) => b.score - a.score)
    .slice(0, MAX_DAILY_PICKS)

  const daily = sorted.flatMap((r) =>
    r.picks.map((p) => ({
      ...p,
      fixture_id: r.base.fixture_id,
      league: r.base.league_name,
      match: `${r.base.home_team_name} x ${r.base.away_team_name}`,
      kickoff: r.base.date,
    }))
  )

  await supabase.from("match_analysis").upsert(
    results.map((r) => ({
      fixture_id: r.base.fixture_id,
      data: r.analysis,
      updated_at: nowISO(),
    })),
    { onConflict: "fixture_id" }
  )

  await supabase.from("daily_picks").delete().neq("id", 0)
  await supabase.from("daily_picks").insert(daily)

  console.log("✅ SYNC V13.6 FINALIZADO", daily.length)
}

runSync()
