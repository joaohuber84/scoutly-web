const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const API_KEY = process.env.API_FOOTBALL_KEY
const API_BASE = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

if (!API_KEY) {
  throw new Error("APISPORTS_KEY não encontrada nas variáveis de ambiente.")
}

const LEAGUES = [
  // EUROPA
  { id: 39, name: "Premier League", country: "England", season: 2025 },
  { id: 140, name: "La Liga", country: "Spain", season: 2025 },
  { id: 78, name: "Bundesliga", country: "Germany", season: 2025 },
  { id: 135, name: "Serie A", country: "Italy", season: 2025 },
  { id: 61, name: "Ligue 1", country: "France", season: 2025 },
  { id: 94, name: "Liga Portugal", country: "Portugal", season: 2025 },
  { id: 88, name: "Eredivisie", country: "Netherlands", season: 2025 },
  { id: 203, name: "Super Lig", country: "Turkey", season: 2025 },

  // EUROPA - COMPETIÇÕES
  { id: 2, name: "UEFA Champions League", country: "Europe", season: 2025 },
  { id: 3, name: "UEFA Europa League", country: "Europe", season: 2025 },
  { id: 848, name: "UEFA Europa Conference League", country: "Europe", season: 2025 },

  // COPAS
  { id: 45, name: "FA Cup", country: "England", season: 2025 },
  { id: 143, name: "Copa del Rey", country: "Spain", season: 2025 },
  { id: 137, name: "Coppa Italia", country: "Italy", season: 2025 },

  // BRASIL
  { id: 71, name: "Brasileirão Série A", country: "Brazil", season: 2026 },
  { id: 72, name: "Brasileirão Série B", country: "Brazil", season: 2026 },
  { id: 73, name: "Copa do Brasil", country: "Brazil", season: 2026 },

  // AMÉRICA DO SUL
  { id: 128, name: "Liga Profesional Argentina", country: "Argentina", season: 2026 },
  { id: 13, name: "CONMEBOL Libertadores", country: "South America", season: 2026 },
  { id: 11, name: "CONMEBOL Sudamericana", country: "South America", season: 2026 },

  // EUA
  { id: 253, name: "MLS", country: "USA", season: 2026 }
]

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 2) {
  return Number(Number(value || 0).toFixed(decimals))
}

function roundInt(value) {
  return Math.round(Number(value || 0))
}

function avg(arr) {
  if (!arr || !arr.length) return 0
  return arr.reduce((sum, n) => sum + (Number(n) || 0), 0) / arr.length
}

function sigmoid(x) {
  return 1 / (1 + Math.exp(-x))
}

function todayDateInTimezone(timezone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(new Date())

  const year = parts.find(p => p.type === "year")?.value
  const month = parts.find(p => p.type === "month")?.value
  const day = parts.find(p => p.type === "day")?.value

  return `${year}-${month}-${day}`
}

async function apiGet(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`)

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.append(key, value)
    }
  })

  const res = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "x-apisports-key": API_KEY
    }
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`API error ${res.status} em ${path}: ${text}`)
  }

  const json = await res.json()
  return json.response || []
}

function getStatValue(statistics = [], labels = []) {
  for (const label of labels) {
    const found = statistics.find(item => item.type === label)

    if (found) {
      const raw = found.value

      if (raw === null || raw === undefined) return 0

      if (typeof raw === "string") {
        const cleaned = raw.replace("%", "").trim()
        const num = Number(cleaned)
        return Number.isNaN(num) ? 0 : num
      }

      return Number(raw) || 0
    }
  }

  return 0
}

const statsCache = new Map()
const teamFixturesCache = new Map()

async function getFixtureStatistics(fixtureId) {
  if (statsCache.has(fixtureId)) return statsCache.get(fixtureId)

  const response = await apiGet("/fixtures/statistics", { fixture: fixtureId })
  statsCache.set(fixtureId, response)
  return response
}

async function getRecentFixtures(teamId, season) {
  const cacheKey = `${teamId}-${season}`

  if (teamFixturesCache.has(cacheKey)) return teamFixturesCache.get(cacheKey)

  const fixtures = await apiGet("/fixtures", {
    team: teamId,
    season,
    last: 10,
    status: "FT",
    timezone: TIMEZONE
  })

  teamFixturesCache.set(cacheKey, fixtures)
  return fixtures
}

function buildPoints(goalsFor, goalsAgainst) {
  if (goalsFor > goalsAgainst) return 3
  if (goalsFor === goalsAgainst) return 1
  return 0
}

async function buildTeamProfile(teamId, season, venue = "home") {
  const fixtures = await getRecentFixtures(teamId, season)
  const rows = []

  for (const fx of fixtures) {
    const fixtureId = fx.fixture?.id
    if (!fixtureId) continue

    const isHome = fx.teams?.home?.id === teamId
    const goalsFor = Number(isHome ? fx.goals?.home : fx.goals?.away) || 0
    const goalsAgainst = Number(isHome ? fx.goals?.away : fx.goals?.home) || 0

    const stats = await getFixtureStatistics(fixtureId)
    const teamStats = stats.find(s => s.team?.id === teamId)
    const oppStats = stats.find(s => s.team?.id !== teamId)

    const teamStatList = teamStats?.statistics || []
    const oppStatList = oppStats?.statistics || []

    rows.push({
      isHome,
      goalsFor,
      goalsAgainst,
      shotsFor: getStatValue(teamStatList, ["Total Shots"]),
      shotsOnTargetFor: getStatValue(teamStatList, ["Shots on Goal", "Shots on Target"]),
      cornersFor: getStatValue(teamStatList, ["Corner Kicks"]),
      yellowFor: getStatValue(teamStatList, ["Yellow Cards"]),
      shotsAgainst: getStatValue(oppStatList, ["Total Shots"]),
      shotsOnTargetAgainst: getStatValue(oppStatList, ["Shots on Goal", "Shots on Target"]),
      cornersAgainst: getStatValue(oppStatList, ["Corner Kicks"]),
      points: buildPoints(goalsFor, goalsAgainst),
      scored: goalsFor > 0 ? 1 : 0,
      conceded: goalsAgainst > 0 ? 1 : 0,
      btts: goalsFor > 0 && goalsAgainst > 0 ? 1 : 0
    })
  }

  if (!rows.length) {
    return {
      goalsFor: 1.2,
      goalsAgainst: 1.2,
      shotsFor: 11,
      shotsOnTargetFor: 4,
      cornersFor: 4.5,
      yellowFor: 2,
      shotsAgainst: 11,
      shotsOnTargetAgainst: 4,
      cornersAgainst: 4.5,
      formScore: 50,
      scoredRate: 0.6,
      concededRate: 0.6,
      bttsRate: 0.45,
      power: 1.2
    }
  }

  const recent5 = rows.slice(0, 5)
  const venueRows = rows.filter(r => r.isHome === (venue === "home"))
  const venue5 = venueRows.slice(0, 5)

  function weightedMetric(key) {
    const a = avg(recent5.map(r => r[key]))
    const b = avg(rows.map(r => r[key]))
    const c = avg(venue5.map(r => r[key]))

    const values = []
    if (a) values.push({ w: 0.45, v: a })
    if (b) values.push({ w: 0.25, v: b })
    if (c) values.push({ w: 0.30, v: c })

    if (!values.length) return 0

    const totalWeight = values.reduce((sum, x) => sum + x.w, 0)
    return values.reduce((sum, x) => sum + x.v * x.w, 0) / totalWeight
  }

  const goalsFor = weightedMetric("goalsFor")
  const goalsAgainst = weightedMetric("goalsAgainst")
  const shotsFor = weightedMetric("shotsFor")
  const shotsOnTargetFor = weightedMetric("shotsOnTargetFor")
  const cornersFor = weightedMetric("cornersFor")
  const yellowFor = weightedMetric("yellowFor")
  const shotsAgainst = weightedMetric("shotsAgainst")
  const shotsOnTargetAgainst = weightedMetric("shotsOnTargetAgainst")
  const cornersAgainst = weightedMetric("cornersAgainst")
  const formPoints = weightedMetric("points")
  const scoredRate = weightedMetric("scored")
  const concededRate = weightedMetric("conceded")
  const bttsRate = weightedMetric("btts")

  const formScore = clamp((formPoints / 3) * 100, 0, 100)

  const power =
    goalsFor * 0.34 +
    shotsOnTargetFor * 0.11 +
    shotsFor * 0.03 +
    cornersFor * 0.05 +
    (formScore / 100) * 0.55 -
    goalsAgainst * 0.18 -
    shotsOnTargetAgainst * 0.05

  return {
    goalsFor: round(goalsFor),
    goalsAgainst: round(goalsAgainst),
    shotsFor: round(shotsFor),
    shotsOnTargetFor: round(shotsOnTargetFor),
    cornersFor: round(cornersFor),
    yellowFor: round(yellowFor),
    shotsAgainst: round(shotsAgainst),
    shotsOnTargetAgainst: round(shotsOnTargetAgainst),
    cornersAgainst: round(cornersAgainst),
    formScore: round(formScore),
    scoredRate: round(scoredRate, 4),
    concededRate: round(concededRate, 4),
    bttsRate: round(bttsRate, 4),
    power: round(clamp(power, 0.4, 3.5), 4)
  }
}

function buildMatchMetrics(homeProfile, awayProfile) {
  const homeExpectedGoals =
    (homeProfile.goalsFor + awayProfile.goalsAgainst) / 2

  const awayExpectedGoals =
    (awayProfile.goalsFor + homeProfile.goalsAgainst) / 2

  const avgGoals = clamp(homeExpectedGoals + awayExpectedGoals, 0.8, 5.5)

  const homeExpectedShots =
    (homeProfile.shotsFor + awayProfile.shotsAgainst) / 2

  const awayExpectedShots =
    (awayProfile.shotsFor + homeProfile.shotsAgainst) / 2

  const avgShots = clamp(homeExpectedShots + awayExpectedShots, 6, 40)

  const homeExpectedShotsOnTarget =
    (homeProfile.shotsOnTargetFor + awayProfile.shotsOnTargetAgainst) / 2

  const awayExpectedShotsOnTarget =
    (awayProfile.shotsOnTargetFor + homeProfile.shotsOnTargetAgainst) / 2

  const avgShotsOnTarget = clamp(homeExpectedShotsOnTarget + awayExpectedShotsOnTarget, 2, 16)

  const homeExpectedCorners =
    (homeProfile.cornersFor + awayProfile.cornersAgainst) / 2

  const awayExpectedCorners =
    (awayProfile.cornersFor + homeProfile.cornersAgainst) / 2

  const avgCorners = clamp(homeExpectedCorners + awayExpectedCorners, 4, 18)

  const over15Prob = clamp(sigmoid((avgGoals - 1.5) / 0.35), 0.15, 0.97)
  const over25Prob = clamp(sigmoid((avgGoals - 2.5) / 0.40), 0.10, 0.95)
  const under25Prob = clamp(1 - over25Prob, 0.05, 0.90)
  const under35Prob = clamp(sigmoid((3.5 - avgGoals) / 0.45), 0.08, 0.97)

  const bttsProbRaw =
    (homeProfile.scoredRate +
      awayProfile.scoredRate +
      homeProfile.concededRate +
      awayProfile.concededRate +
      homeProfile.bttsRate +
      awayProfile.bttsRate) / 6

  const bttsProb = clamp(bttsProbRaw, 0.08, 0.90)
  const cornersOver85Prob = clamp(sigmoid((avgCorners - 8.5) / 1.05), 0.08, 0.95)

  const homePower = homeProfile.power + 0.18
  const awayPower = awayProfile.power
  const diff = homePower - awayPower

  let drawProb = clamp(0.30 - Math.abs(diff) * 0.08, 0.14, 0.30)
  let homeWinRaw = sigmoid(diff * 1.8)
  let awayWinRaw = 1 - homeWinRaw

  let homeResultProb = homeWinRaw * (1 - drawProb)
  let awayResultProb = awayWinRaw * (1 - drawProb)

  const total = homeResultProb + drawProb + awayResultProb

  homeResultProb /= total
  drawProb /= total
  awayResultProb /= total

  return {
    avgGoals: round(avgGoals),
    avgShots: round(avgShots),
    avgShotsOnTarget: round(avgShotsOnTarget),
    avgCorners: round(avgCorners),

    homeExpectedGoals: round(homeExpectedGoals),
    awayExpectedGoals: round(awayExpectedGoals),

    homeExpectedShots: round(homeExpectedShots),
    awayExpectedShots: round(awayExpectedShots),

    homeExpectedShotsOnTarget: round(homeExpectedShotsOnTarget),
    awayExpectedShotsOnTarget: round(awayExpectedShotsOnTarget),

    homeExpectedCorners: round(homeExpectedCorners),
    awayExpectedCorners: round(awayExpectedCorners),

    over15Prob: round(over15Prob, 4),
    over25Prob: round(over25Prob, 4),
    under25Prob: round(under25Prob, 4),
    under35Prob: round(under35Prob, 4),
    bttsProb: round(bttsProb, 4),
    cornersOver85Prob: round(cornersOver85Prob, 4),

    homeResultProb: round(homeResultProb, 4),
    drawProb: round(drawProb, 4),
    awayResultProb: round(awayResultProb, 4),

    homePower: round(homeProfile.power, 4),
    awayPower: round(awayProfile.power, 4)
  }
}

async function fetchTodayFixtures() {
  const date = todayDateInTimezone(TIMEZONE)
  const all = []

  for (const league of LEAGUES) {
    console.log(`Buscando ${league.name} (${league.country})`)

    const fixtures = await apiGet("/fixtures", {
      league: league.id,
      season: league.season,
      date,
      timezone: TIMEZONE
    })

    const valid = fixtures.filter(fx => {
      const status = fx.fixture?.status?.short
      return ["NS", "TBD", "PST"].includes(status)
    })

    for (const fx of valid) {
      all.push({
        leagueMeta: league,
        fixture: fx
      })
    }
  }

  return all
}

async function replaceTodayData(fixturesIds) {
  if (!fixturesIds.length) return

  const { error: dailyError } = await supabase
    .from("daily_picks")
    .delete()
    .in("match_id", fixturesIds)

  if (dailyError) {
    console.error("Erro ao limpar daily_picks:", dailyError)
  }

  const { error: statsError } = await supabase
    .from("match_stats")
    .delete()
    .in("match_id", fixturesIds)

  if (statsError) {
    console.error("Erro ao limpar match_stats:", statsError)
  }

  const { error: analysisError } = await supabase
    .from("match_analysis")
    .delete()
    .in("match_id", fixturesIds)

  if (analysisError) {
    console.error("Erro ao limpar match_analysis:", analysisError)
  }

  const { error: matchesError } = await supabase
    .from("matches")
    .delete()
    .in("id", fixturesIds)

  if (matchesError) {
    console.error("Erro ao limpar matches:", matchesError)
  }
}

async function runSyncV21() {
  console.log("Scoutly Sync V2.1 iniciado")

  const wrappedFixtures = await fetchTodayFixtures()
  console.log("Jogos elegíveis hoje:", wrappedFixtures.length)

  if (!wrappedFixtures.length) {
    console.log("Nenhum jogo elegível encontrado hoje.")
    return
  }

  const fixtureIds = wrappedFixtures.map(item => item.fixture.fixture.id)
  await replaceTodayData(fixtureIds)

  const matchRows = []
  const statsRows = []

  for (const item of wrappedFixtures) {
    const fx = item.fixture
    const league = item.leagueMeta

    const fixtureId = fx.fixture?.id
    const homeTeamId = fx.teams?.home?.id
    const awayTeamId = fx.teams?.away?.id

    if (!fixtureId || !homeTeamId || !awayTeamId) continue

    console.log(`Processando ${fx.teams?.home?.name} x ${fx.teams?.away?.name}`)

    const homeProfile = await buildTeamProfile(homeTeamId, league.season, "home")
    const awayProfile = await buildTeamProfile(awayTeamId, league.season, "away")
    const metrics = buildMatchMetrics(homeProfile, awayProfile)
    const nowIso = new Date().toISOString()

    matchRows.push({
      id: fixtureId,
      created_at: nowIso,
      home_team: fx.teams?.home?.name || null,
      away_team: fx.teams?.away?.name || null,
      league: league.name,
      match_date: todayDateInTimezone(TIMEZONE),
      kickoff: fx.fixture?.date || null,
      home_logo: fx.teams?.home?.logo || null,
      away_logo: fx.teams?.away?.logo || null,

      avg_goals: metrics.avgGoals,
      avg_corners: metrics.avgCorners,
      avg_shots: metrics.avgShots,
      insight: null,

      home_win_prob: metrics.homeResultProb,
      draw_prob: metrics.drawProb,
      away_win_prob: metrics.awayResultProb,

      home_form: String(homeProfile.formScore),
      away_form: String(awayProfile.formScore),

      over25_prob: metrics.over25Prob,
      btts_prob: metrics.bttsProb,
      corners_over85_prob: metrics.cornersOver85Prob,

      pick: null,

      power_home: metrics.homePower,
      power_away: metrics.awayPower,

      home_result_prob: metrics.homeResultProb,
      draw_result_prob: metrics.drawProb,
      away_result_prob: metrics.awayResultProb,

      market_odds_over25: null,
      market_odds_btts: null,
      market_odds_corners85: null,

      over15_prob: metrics.over15Prob,
      under25_prob: metrics.under25Prob,
      under35_prob: metrics.under35Prob
    })

    statsRows.push({
      created_at: nowIso,
      match_id: fixtureId,

      home_shots: roundInt(metrics.homeExpectedShots),
      home_shots_on_target: roundInt(metrics.homeExpectedShotsOnTarget),
      home_corners: roundInt(metrics.homeExpectedCorners),
      home_yellow_cards: roundInt(homeProfile.yellowFor),

      away_shots: roundInt(metrics.awayExpectedShots),
      away_shots_on_target: roundInt(metrics.awayExpectedShotsOnTarget),
      away_corners: roundInt(metrics.awayExpectedCorners),
      away_yellow_cards: roundInt(awayProfile.yellowFor)
    })
  }

  console.log("Total de rows para matches:", matchRows.length)
  console.log("Total de rows para match_stats:", statsRows.length)

  if (matchRows.length) {
    const { error } = await supabase
      .from("matches")
      .insert(matchRows)

    if (error) {
      console.error("Erro ao inserir matches:", error)
      throw error
    }
  }

  if (statsRows.length) {
    const { error } = await supabase
      .from("match_stats")
      .insert(statsRows)

    if (error) {
      console.error("Erro ao inserir match_stats:", error)
      throw error
    }
  }

  console.log("Scoutly Sync V2.1 finalizado com sucesso")
}

runSyncV21().catch(err => {
  console.error("Erro fatal no Scoutly Sync V2.1:", err)
  process.exit(1)
})
