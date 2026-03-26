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
if (!SUPABASE_KEY) {
  throw new Error("SUPABASE_SERVICE_ROLE_KEY / SUPABASE_SERVICE_KEY não encontrada.")
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"
const WINDOW_HOURS = 120
const REQUEST_DELAY_MS = 1100

const TARGET_COMPETITIONS = [
  // Inglaterra
  { mode: "country", country: "England", type: "league", names: ["Premier League"], display: "Premier League", region: "general", priority: 100 },
  { mode: "country", country: "England", type: "cup", names: ["FA Cup", "EFL Cup", "League Cup"], display: "England - Cup", region: "general", priority: 74 },

  // Espanha
  { mode: "country", country: "Spain", type: "league", names: ["La Liga"], display: "La Liga", region: "general", priority: 98 },
  { mode: "country", country: "Spain", type: "cup", names: ["Copa del Rey"], display: "Copa del Rey", region: "general", priority: 72 },

  // Itália
  { mode: "country", country: "Italy", type: "league", names: ["Serie A"], display: "Serie A", region: "general", priority: 97 },
  { mode: "country", country: "Italy", type: "cup", names: ["Coppa Italia"], display: "Coppa Italia", region: "general", priority: 73 },

  // Alemanha
  { mode: "country", country: "Germany", type: "league", names: ["Bundesliga"], display: "Bundesliga", region: "general", priority: 96 },
  { mode: "country", country: "Germany", type: "cup", names: ["DFB Pokal", "DFB-Pokal"], display: "DFB-Pokal", region: "general", priority: 70 },

  // França
  { mode: "country", country: "France", type: "league", names: ["Ligue 1"], display: "Ligue 1", region: "general", priority: 95 },
  { mode: "country", country: "France", type: "cup", names: ["Coupe de France"], display: "Coupe de France", region: "general", priority: 71 },

  // Holanda
  { mode: "country", country: "Netherlands", type: "league", names: ["Eredivisie"], display: "Eredivisie", region: "general", priority: 90 },
  { mode: "country", country: "Netherlands", type: "cup", names: ["KNVB Cup"], display: "KNVB Cup", region: "general", priority: 70 },

  // Portugal
  { mode: "country", country: "Portugal", type: "league", names: ["Primeira Liga", "Liga Portugal Betclic"], display: "Primeira Liga", region: "general", priority: 89 },
  { mode: "country", country: "Portugal", type: "cup", names: ["Taça de Portugal"], display: "Taça de Portugal", region: "general", priority: 68 },

  // Brasil
  { mode: "country", country: "Brazil", type: "league", names: ["Serie A", "Brasileirão Série A", "Campeonato Brasileiro Série A"], display: "Brasileirão Série A", region: "brazil", priority: 94 },
  { mode: "country", country: "Brazil", type: "league", names: ["Serie B", "Brasileirão Série B", "Campeonato Brasileiro Série B"], display: "Brasileirão Série B", region: "brazil", priority: 88 },
  { mode: "country", country: "Brazil", type: "cup", names: ["Copa do Brasil"], display: "Copa do Brasil", region: "brazil", priority: 91 },
  { mode: "country", country: "Brazil", type: "cup", names: ["Copa do Nordeste"], display: "Copa do Nordeste", region: "brazil", priority: 90 },
  { mode: "search", search: "Brasileiro Women", display: "Brasileirão Feminino", region: "brazil", priority: 84 },

  // Argentina
  { mode: "country", country: "Argentina", type: "league", names: ["Liga Profesional Argentina", "Primera División"], display: "Liga Profesional Argentina", region: "general", priority: 84 },
  { mode: "country", country: "Argentina", type: "cup", names: ["Copa Argentina"], display: "Copa Argentina", region: "general", priority: 70 },

  // México
  { mode: "country", country: "Mexico", type: "league", names: ["Liga MX"], display: "Liga MX", region: "general", priority: 79 },

  // Turquia / Grécia / Dinamarca
  { mode: "country", country: "Turkey", type: "league", names: ["Süper Lig", "Super Lig"], display: "Super Lig", region: "general", priority: 78 },
  { mode: "country", country: "Denmark", type: "league", names: ["Superliga", "Superligaen"], display: "Superliga", region: "general", priority: 75 },
  { mode: "country", country: "Greece", type: "league", names: ["Super League 1", "Super League"], display: "Super League Greece", region: "general", priority: 74 },

  // Bélgica / Áustria
  { mode: "country", country: "Belgium", type: "league", names: ["Pro League", "Jupiler Pro League"], display: "Belgian Pro League", region: "general", priority: 85 },
  { mode: "country", country: "Austria", type: "league", names: ["Bundesliga"], display: "Austrian Bundesliga", region: "general", priority: 84 },

  // USA / CONCACAF
  { mode: "country", country: "USA", type: "league", names: ["Major League Soccer"], display: "MLS", region: "america", priority: 90 },
  { mode: "search", search: "CONCACAF Champions", display: "CONCACAF Champions Cup", region: "america", priority: 88 },

  // Saudi Arabia
  { mode: "search", search: "Saudi League", display: "Saudi Pro League", region: "general", priority: 85 },

  // UEFA
  { mode: "search", search: "UEFA Champions League", display: "UEFA Champions League", region: "general", priority: 98 },
  { mode: "search", search: "UEFA Europa League", display: "UEFA Europa League", region: "general", priority: 93 },
  { mode: "search", search: "UEFA Conference League", display: "UEFA Conference League", region: "general", priority: 88 },

  // CONMEBOL
  { mode: "search", search: "CONMEBOL Libertadores", display: "Libertadores", region: "brazil", priority: 92 },
  { mode: "search", search: "Copa Libertadores", display: "Libertadores", region: "brazil", priority: 91 },
  { mode: "search", search: "CONMEBOL Sudamericana", display: "Sul-Americana", region: "brazil", priority: 86 },
  { mode: "search", search: "Copa Sudamericana", display: "Sul-Americana", region: "brazil", priority: 85 },

  // Seleções / Internacional
  { mode: "search", search: "International Friendlies", display: "Amistosos Internacionais", region: "international", priority: 85 },
  { mode: "search", search: "UEFA Nations League", display: "UEFA Nations League", region: "international", priority: 95 },
  { mode: "search", search: "CONMEBOL World Cup Qualifiers", display: "Eliminatórias Sul-Americanas", region: "international", priority: 96 },
  { mode: "search", search: "UEFA Euro Qualifiers", display: "Eliminatórias da Euro", region: "international", priority: 94 },
  { mode: "search", search: "FIFA World Cup", display: "Copa do Mundo", region: "international", priority: 100 },
  { mode: "search", search: "Copa America", display: "Copa América", region: "international", priority: 98 },
  { mode: "search", search: "UEFA European Championship", display: "Eurocopa", region: "international", priority: 98 },

  // Feminino internacional
  { mode: "search", search: "UEFA Women's Champions League", display: "Champions League Feminina", region: "international", priority: 82 },
]

const apiCache = new Map()
const fixtureStatsCache = new Map()
const teamFixturesCache = new Map()

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 2) {
  const n = Number(value)
  if (!Number.isFinite(n)) return 0
  return Number(n.toFixed(decimals))
}

function safeNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function avg(arr) {
  if (!arr.length) return 0
  return arr.reduce((sum, value) => sum + safeNumber(value), 0) / arr.length
}

function isoDate(date) {
  return date.toISOString().slice(0, 10)
}

function normalizeLeagueKey(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim()
}

function uniqBy(arr, getKey) {
  const seen = new Set()
  return arr.filter((item) => {
    const key = getKey(item)
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function buildWindow() {
  const now = new Date()
  const start = new Date(now)
  start.setHours(0, 0, 0, 0)

  const end = new Date(now.getTime() + WINDOW_HOURS * 60 * 60 * 1000)
  return { start, end }
}

function makeApiCacheKey(path, params = {}) {
  return `${path}?${Object.entries(params)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join("&")}`
}

async function api(path, params = {}) {
  const cacheKey = makeApiCacheKey(path, params)
  if (apiCache.has(cacheKey)) return apiCache.get(cacheKey)

  await sleep(REQUEST_DELAY_MS)

  const url = new URL(API + path)
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") {
      url.searchParams.set(k, String(v))
    }
  })

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-apisports-key": APISPORTS_KEY,
    },
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`API ${response.status} em ${path}: ${text}`)
  }

  const json = await response.json()

  if (json.errors && Object.keys(json.errors).length > 0) {
    throw new Error(`API error em ${path}: ${JSON.stringify(json.errors)}`)
  }

  const data = json.response || []
  apiCache.set(cacheKey, data)
  return data
}

function hasForbiddenMarker(value = "") {
  const v = normalizeLeagueKey(value)
  return (
    v.includes("u17") ||
    v.includes("u18") ||
    v.includes("u19") ||
    v.includes("u20") ||
    v.includes("u21") ||
    v.includes("u23") ||
    v.includes("under 17") ||
    v.includes("under 18") ||
    v.includes("under 19") ||
    v.includes("under 20") ||
    v.includes("under 21") ||
    v.includes("under 23") ||
    v.includes("sub 17") ||
    v.includes("sub 18") ||
    v.includes("sub 19") ||
    v.includes("sub 20") ||
    v.includes("sub 21") ||
    v.includes("sub 23") ||
    v.includes("women u") ||
    v.includes("female u") ||
    v.includes("youth") ||
    v.includes("reserve") ||
    v.includes("reserves")
  )
}

function normalizeCompetitionName(country, rawName, fallbackDisplay) {
  const name = String(rawName || "").trim()
  const c = String(country || "").trim()

  if (c === "England" && name === "Premier League") return "Premier League"
  if (c === "Spain" && name === "La Liga") return "La Liga"
  if (c === "Italy" && name === "Serie A") return "Serie A"
  if (c === "Germany" && name === "Bundesliga") return "Bundesliga"
  if (c === "Austria" && name === "Bundesliga") return "Austrian Bundesliga"
  if (c === "France" && name === "Ligue 1") return "Ligue 1"
  if (c === "Netherlands" && name === "Eredivisie") return "Eredivisie"
  if (c === "Portugal" && (name === "Primeira Liga" || name === "Liga Portugal Betclic")) return "Primeira Liga"
  if (c === "Brazil" && name === "Serie A") return "Brasileirão Série A"
  if (c === "Brazil" && name === "Serie B") return "Brasileirão Série B"
  if (c === "Brazil" && name === "Copa do Brasil") return "Copa do Brasil"
  if (c === "Brazil" && name === "Copa do Nordeste") return "Copa do Nordeste"
  if (c === "Argentina" && (name === "Liga Profesional Argentina" || name === "Primera División")) return "Liga Profesional Argentina"
  if (c === "USA" && (name === "Major League Soccer" || name === "MLS")) return "MLS"
  if (c === "Mexico" && name === "Liga MX") return "Liga MX"
  if (c === "Turkey" && (name === "Süper Lig" || name === "Super Lig")) return "Super Lig"
  if (c === "Greece" && (name === "Super League 1" || name === "Super League")) return "Super League Greece"
  if (c === "Denmark" && (name === "Superliga" || name === "Superligaen")) return "Superliga"
  if (c === "Saudi Arabia" && name.toLowerCase().includes("league")) return "Saudi Pro League"

  if (name === "UEFA Champions League") return "UEFA Champions League"
  if (name === "UEFA Europa League") return "UEFA Europa League"
  if (name === "UEFA Conference League") return "UEFA Conference League"
  if (name === "UEFA Europa Conference League") return "UEFA Conference League"
  if (name === "CONMEBOL Libertadores") return "Libertadores"
  if (name === "CONMEBOL Sudamericana") return "Sul-Americana"

  return fallbackDisplay || name || c || "Competição"
}

async function resolveCountryCompetitions(target) {
  const leagues = await api("/leagues", {
    country: target.country,
    current: true,
  })

  const wanted = new Set((target.names || []).map(normalizeLeagueKey))

  return leagues
    .filter((item) => {
      const rawName = String(item?.league?.name || "")
      const rawKey = normalizeLeagueKey(rawName)
      const leagueType = String(item?.league?.type || "").toLowerCase()
      const currentSeason = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]

      if (!currentSeason) return false
      if (target.type && leagueType !== target.type) return false
      if (hasForbiddenMarker(rawName)) return false

      return Array.from(wanted).some((n) => rawKey === n)
    })
    .map((item) => {
      const currentSeason = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]
      return {
        leagueId: item.league.id,
        season: currentSeason.year,
        country: item.country?.name || target.country,
        rawName: item.league.name,
        display: normalizeCompetitionName(
          item.country?.name || target.country,
          item.league.name,
          target.display
        ),
        region: target.region,
        priority: target.priority,
      }
    })
}

async function resolveSearchCompetition(target) {
  const leagues = await api("/leagues", {
    search: target.search,
  })

  return uniqBy(
    leagues
      .map((item) => {
        const currentSeason = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]
        if (!currentSeason) return null

        const country = item?.country?.name || null
        const rawName = String(item?.league?.name || "").trim()
        const haystack = `${country || ""} ${rawName}`.toLowerCase()

        if (hasForbiddenMarker(rawName)) return null
        if (rawName.toLowerCase().includes("open cup")) return null

        return {
          leagueId: item.league.id,
          season: currentSeason.year,
          country,
          rawName,
          display: normalizeCompetitionName(country, rawName, target.display),
          region: target.region,
          priority: target.priority,
          haystack,
        }
      })
      .filter(Boolean),
    (item) => `${item.leagueId}:${item.season}`
  )
}

async function resolveTargetCompetitions() {
  const resolved = []

  for (const target of TARGET_COMPETITIONS) {
    try {
      const items =
        target.mode === "country"
          ? await resolveCountryCompetitions(target)
          : await resolveSearchCompetition(target)

      resolved.push(...items)
      console.log(`TARGET OK: ${target.display || target.search} -> ${items.length}`)
    } catch (error) {
      console.error(`Falha resolvendo ${target.display || target.search}:`, error.message)
    }
  }

  return uniqBy(resolved, (item) => `${item.leagueId}:${item.season}`)
}

async function fetchFixturesForCompetition(comp) {
  const { start, end } = buildWindow()
  const from = isoDate(start)
  const to = isoDate(end)

  try {
    const fixtures = await api("/fixtures", {
      league: comp.leagueId,
      season: comp.season,
      from,
      to,
      timezone: TIMEZONE,
    })

    const filtered = fixtures.filter((fixture) => {
      const home = fixture?.teams?.home?.name || ""
      const away = fixture?.teams?.away?.name || ""
      const league = fixture?.league?.name || ""

      if (hasForbiddenMarker(home)) return false
      if (hasForbiddenMarker(away)) return false
      if (hasForbiddenMarker(league)) return false

      const kickoff = fixture?.fixture?.date
      if (!kickoff) return false

      const dt = new Date(kickoff)
      if (Number.isNaN(dt.getTime())) return false

      return dt >= start && dt <= end
    })

    console.log(`FIXTURES ${comp.display}: ${filtered.length}`)
    return filtered.map((f) => ({ ...f, __comp: comp }))
  } catch (error) {
    console.error(`Falha buscando fixtures de ${comp.display}:`, error.message)
    return []
  }
}

async function getFixtureStatistics(fixtureId) {
  if (fixtureStatsCache.has(fixtureId)) return fixtureStatsCache.get(fixtureId)

  try {
    const stats = await api("/fixtures/statistics", { fixture: fixtureId })
    fixtureStatsCache.set(fixtureId, stats)
    return stats
  } catch (error) {
    console.error(`Falha buscando stats do fixture ${fixtureId}:`, error.message)
    fixtureStatsCache.set(fixtureId, [])
    return []
  }
}

function extractStatValue(statistics = [], type) {
  const found = statistics.find((x) => x.type === type)
  if (!found) return 0

  const value = found.value
  if (value === null || value === undefined) return 0

  if (typeof value === "string") {
    const cleaned = value.replace("%", "").trim()
    const n = Number(cleaned)
    return Number.isFinite(n) ? n : 0
  }

  return safeNumber(value)
}

function isCompletedFixture(fixture) {
  const short = fixture?.fixture?.status?.short || ""
  return ["FT", "AET", "PEN"].includes(short)
}

async function fetchTeamFinishedFixtures(teamId, limit = 20) {
  const key = `team:${teamId}:last:${limit}`
  if (teamFixturesCache.has(key)) return teamFixturesCache.get(key)

  try {
    const fixtures = await api("/fixtures", {
      team: teamId,
      last: limit,
      timezone: TIMEZONE,
    })

    const filtered = fixtures
      .filter(isCompletedFixture)
      .sort((a, b) => new Date(b.fixture.date).getTime() - new Date(a.fixture.date).getTime())
      .slice(0, limit)

    teamFixturesCache.set(key, filtered)
    return filtered
  } catch (error) {
    console.error(`Falha buscando histórico do time ${teamId}:`, error.message)
    teamFixturesCache.set(key, [])
    return []
  }
}

function buildFormString(fixtures, teamId, limit = 5) {
  return fixtures
    .slice(0, limit)
    .map((fixture) => {
      const isHome = fixture?.teams?.home?.id === teamId
      const gf = isHome ? safeNumber(fixture?.goals?.home) : safeNumber(fixture?.goals?.away)
      const ga = isHome ? safeNumber(fixture?.goals?.away) : safeNumber(fixture?.goals?.home)
      return `${gf}-${ga}`
    })
}

async function buildFixtureStatSnapshot(fixture, teamId) {
  const stats = await getFixtureStatistics(fixture?.fixture?.id)
  const teamStats = stats.find((s) => s?.team?.id === teamId)?.statistics || []

  return {
    shots: extractStatValue(teamStats, "Total Shots"),
    sot: extractStatValue(teamStats, "Shots on Goal"),
    corners: extractStatValue(teamStats, "Corner Kicks"),
    fouls: extractStatValue(teamStats, "Fouls"),
    yellow: extractStatValue(teamStats, "Yellow Cards"),
    red: extractStatValue(teamStats, "Red Cards"),
  }
}

async function buildTeamSplits(teamId) {
  const fixtures = await fetchTeamFinishedFixtures(teamId, 20)

  const overall = fixtures.slice(0, 10)
  const home = fixtures.filter((f) => f?.teams?.home?.id === teamId).slice(0, 5)
  const away = fixtures.filter((f) => f?.teams?.away?.id === teamId).slice(0, 5)

  async function aggregate(group) {
    const goalsFor = []
    const goalsAgainst = []
    const shots = []
    const shotsOnTarget = []
    const corners = []
    const fouls = []
    const cards = []

    for (const fixture of group) {
      const isHome = fixture?.teams?.home?.id === teamId
      const gf = isHome ? safeNumber(fixture?.goals?.home) : safeNumber(fixture?.goals?.away)
      const ga = isHome ? safeNumber(fixture?.goals?.away) : safeNumber(fixture?.goals?.home)

      goalsFor.push(gf)
      goalsAgainst.push(ga)

      const snap = await buildFixtureStatSnapshot(fixture, teamId)
      shots.push(snap.shots)
      shotsOnTarget.push(snap.sot)
      corners.push(snap.corners)
      fouls.push(snap.fouls)
      cards.push(snap.yellow + snap.red)
    }

    return {
      matches: group.length,
      avgGoalsFor: round(avg(goalsFor)),
      avgGoalsAgainst: round(avg(goalsAgainst)),
      avgShots: round(avg(shots)),
      avgShotsOnTarget: round(avg(shotsOnTarget)),
      avgCorners: round(avg(corners)),
      avgFouls: round(avg(fouls)),
      avgCards: round(avg(cards)),
      recentScores: buildFormString(group, teamId, 5),
    }
  }

  const overallAgg = await aggregate(overall)
  const homeAgg = await aggregate(home)
  const awayAgg = await aggregate(away)

  return {
    overall: overallAgg,
    home: homeAgg,
    away: awayAgg,
  }
}

function weightedMetric(...items) {
  let totalWeight = 0
  let totalValue = 0

  for (const item of items) {
    const value = item?.value
    const weight = item?.weight || 0

    if (value === null || value === undefined || !Number.isFinite(Number(value))) continue
    totalWeight += weight
    totalValue += Number(value) * weight
  }

  if (!totalWeight) return 0
  return totalValue / totalWeight
}

function buildMatchProjection(homeSplits, awaySplits) {
  const homeGoalsFor = weightedMetric(
    { value: homeSplits.home.avgGoalsFor, weight: 0.55 },
    { value: homeSplits.overall.avgGoalsFor, weight: 0.45 }
  )

  const homeGoalsAgainst = weightedMetric(
    { value: homeSplits.home.avgGoalsAgainst, weight: 0.55 },
    { value: homeSplits.overall.avgGoalsAgainst, weight: 0.45 }
  )

  const awayGoalsFor = weightedMetric(
    { value: awaySplits.away.avgGoalsFor, weight: 0.55 },
    { value: awaySplits.overall.avgGoalsFor, weight: 0.45 }
  )

  const awayGoalsAgainst = weightedMetric(
    { value: awaySplits.away.avgGoalsAgainst, weight: 0.55 },
    { value: awaySplits.overall.avgGoalsAgainst, weight: 0.45 }
  )

  const expectedHomeGoals = clamp((homeGoalsFor + awayGoalsAgainst) / 2, 0.35, 3.8)
  const expectedAwayGoals = clamp((awayGoalsFor + homeGoalsAgainst) / 2, 0.25, 3.5)

  const homeShots = weightedMetric(
    { value: homeSplits.home.avgShots, weight: 0.6 },
    { value: homeSplits.overall.avgShots, weight: 0.4 }
  )

  const awayShots = weightedMetric(
    { value: awaySplits.away.avgShots, weight: 0.6 },
    { value: awaySplits.overall.avgShots, weight: 0.4 }
  )

  const homeSot = weightedMetric(
    { value: homeSplits.home.avgShotsOnTarget, weight: 0.6 },
    { value: homeSplits.overall.avgShotsOnTarget, weight: 0.4 }
  )

  const awaySot = weightedMetric(
    { value: awaySplits.away.avgShotsOnTarget, weight: 0.6 },
    { value: awaySplits.overall.avgShotsOnTarget, weight: 0.4 }
  )

  const homeCorners = weightedMetric(
    { value: homeSplits.home.avgCorners, weight: 0.6 },
    { value: homeSplits.overall.avgCorners, weight: 0.4 }
  )

  const awayCorners = weightedMetric(
    { value: awaySplits.away.avgCorners, weight: 0.6 },
    { value: awaySplits.overall.avgCorners, weight: 0.4 }
  )

  const homeCards = weightedMetric(
    { value: homeSplits.home.avgCards, weight: 0.6 },
    { value: homeSplits.overall.avgCards, weight: 0.4 }
  )

  const awayCards = weightedMetric(
    { value: awaySplits.away.avgCards, weight: 0.6 },
    { value: awaySplits.overall.avgCards, weight: 0.4 }
  )

  return {
    expected_home_goals: round(expectedHomeGoals),
    expected_away_goals: round(expectedAwayGoals),
    expected_home_shots: round(homeShots),
    expected_away_shots: round(awayShots),
    expected_home_sot: round(homeSot),
    expected_away_sot: round(awaySot),
    expected_corners: round(homeCorners + awayCorners),
    expected_cards: round(homeCards + awayCards),
  }
}

function poisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0
  let factorial = 1
  for (let i = 2; i <= k; i++) factorial *= i
  return (Math.exp(-lambda) * Math.pow(lambda, k)) / factorial
}

function resultProbabilities(homeXg, awayXg, maxGoals = 8) {
  let homeWin = 0
  let draw = 0
  let awayWin = 0
  let over15 = 0
  let over25 = 0
  let under25 = 0
  let under35 = 0
  let btts = 0

  for (let h = 0; h <= maxGoals; h++) {
    for (let a = 0; a <= maxGoals; a++) {
      const p = poisson(homeXg, h) * poisson(awayXg, a)
      const total = h + a

      if (h > a) homeWin += p
      else if (h === a) draw += p
      else awayWin += p

      if (total >= 2) over15 += p
      if (total >= 3) over25 += p
      if (total <= 2) under25 += p
      if (total <= 3) under35 += p
      if (h >= 1 && a >= 1) btts += p
    }
  }

  return {
    home: clamp(round(homeWin), 0.05, 0.9),
    draw: clamp(round(draw), 0.05, 0.5),
    away: clamp(round(awayWin), 0.05, 0.9),
    over15: clamp(round(over15), 0.2, 0.97),
    over25: clamp(round(over25), 0.1, 0.9),
    under25: clamp(round(under25), 0.1, 0.95),
    under35: clamp(round(under35), 0.2, 0.97),
    btts: clamp(round(btts), 0.1, 0.9),
  }
}

function deriveCornersProbability(expectedCorners) {
  return clamp(round(1 / (1 + Math.exp(-(expectedCorners - 8.5) / 1.25))), 0.08, 0.92)
}

function choosePrimaryPick(match) {
  const over25 = safeNumber(match.over25_prob)
  const over15 = safeNumber(match.over15_prob)
  const under25 = safeNumber(match.under25_prob)
  const under35 = safeNumber(match.under35_prob)
  const btts = safeNumber(match.btts_prob)
  const cornersOver85 = safeNumber(match.corners_over85_prob)
  const home = safeNumber(match.home_result_prob)
  const draw = safeNumber(match.draw_result_prob)
  const away = safeNumber(match.away_result_prob)
  const shots = safeNumber(match.avg_shots)
  const goals = safeNumber(match.avg_goals)

  const candidates = []

  function push(market, probability, bonus = 0) {
    candidates.push({
      market,
      probability,
      score: probability + bonus,
    })
  }

  if (cornersOver85 >= 0.72 && shots >= 21) {
    push("Mais de 8.5 escanteios", cornersOver85, 0.07)
  }

  if (over25 >= 0.67 && goals >= 2.6) {
    push("Mais de 2.5 gols", over25, 0.06)
  }

  if (over15 >= 0.82 && goals >= 2.1) {
    push("Mais de 1.5 gols", over15, 0.02)
  }

  if (under25 >= 0.72 && goals <= 2.0) {
    push("Menos de 2.5 gols", under25, 0.05)
  }

  if (under35 >= 0.80 && goals <= 2.5) {
    push("Menos de 3.5 gols", under35, 0.04)
  }

  if (btts >= 0.64 && goals >= 2.5) {
    push("Ambas marcam", btts, 0.02)
  }

  if (1 - btts >= 0.72 && goals <= 2.1) {
    push("Ambas não marcam", 1 - btts, 0.03)
  }

  if (home >= 0.52 && home + draw >= 0.78) {
    push(`Dupla chance ${match.home_team} ou empate`, home + draw, 0.03)
  }

  if (away >= 0.52 && away + draw >= 0.78) {
    push(`Dupla chance ${match.away_team} ou empate`, away + draw, 0.03)
  }

  if (!candidates.length) {
    push("Menos de 3.5 gols", Math.max(under35, 0.68), 0)
  }

  candidates.sort((a, b) => b.score - a.score)
  return candidates[0]
}

function buildInsight(match) {
  const pick = match.pick || "Mercado em revisão"
  const goals = round(match.avg_goals, 1)
  const corners = round(match.avg_corners, 1)
  const shots = round(match.avg_shots, 0)

  if (pick.includes("escanteios")) {
    return `Leitura baseada em projeção ofensiva e volume pelos lados. O confronto chega com estimativa de ${corners} escanteios e ${shots} finalizações totais.`
  }

  if (pick.includes("Mais de 2.5 gols")) {
    return `O modelo identifica um confronto mais aberto, com expectativa de ${goals} gols e tendência ofensiva acima da média.`
  }

  if (pick.includes("Mais de 1.5 gols")) {
    return `A projeção sugere um jogo com boa chance de pelo menos 2 gols. A média esperada está em ${goals} gols.`
  }

  if (pick.includes("Menos de 2.5 gols")) {
    return `Leitura de jogo mais travado, com menor volume ofensivo e expectativa de placar controlado.`
  }

  if (pick.includes("Menos de 3.5 gols")) {
    return `O modelo identifica um cenário mais controlado, com boa sustentação estatística para até 3 gols no jogo.`
  }

  if (pick.includes("Ambas")) {
    return `A leitura do Scoutly cruza equilíbrio ofensivo, forma recente e produção esperada para destacar esse mercado.`
  }

  if (pick.includes("Dupla chance")) {
    return `Há vantagem estatística para um dos lados, mas com proteção ao empate como abordagem mais segura.`
  }

  return `Leitura construída com base em gols, escanteios, finalizações e forma recente dos dois times.`
}

async function clearWindowData() {
  const { error: dailyError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (dailyError) throw new Error(`Erro limpando daily_picks: ${dailyError.message}`)

  const { error: analysisError } = await supabase
    .from("match_analysis")
    .delete()
    .neq("id", 0)

  if (analysisError) {
    console.warn("Aviso limpando match_analysis:", analysisError.message)
  }

  const { error: statsError } = await supabase
    .from("match_stats")
    .delete()
    .neq("id", 0)

  if (statsError) {
    console.warn("Aviso limpando match_stats:", statsError.message)
  }

  const { start, end } = buildWindow()
  const startIso = start.toISOString()
  const endIso = end.toISOString()

  const { error: matchesDeleteError } = await supabase
    .from("matches")
    .delete()
    .gte("kickoff", startIso)
    .lte("kickoff", endIso)

  if (matchesDeleteError) {
    throw new Error(`Erro limpando matches da janela: ${matchesDeleteError.message}`)
  }
}

async function upsertMatchRow(payload) {
  const { error } = await supabase
    .from("matches")
    .upsert(payload, { onConflict: "id" })

  if (error) throw new Error(`Supabase matches upsert: ${error.message}`)
}

async function insertMatchStatsRow(payload) {
  const { error } = await supabase
    .from("match_stats")
    .insert(payload)

  if (error) {
    console.warn("Aviso match_stats insert:", error.message)
  }
}

async function insertMatchAnalysisRow(payload) {
  const { error } = await supabase
    .from("match_analysis")
    .insert(payload)

  if (error) {
    console.warn("Aviso match_analysis insert:", error.message)
  }
}

async function rebuildDailyPicks(matches) {
  const sorted = [...matches]
    .filter((m) => m.id && m.pick)
    .sort((a, b) => {
      const pb = safeNumber(b.probability)
      const pa = safeNumber(a.probability)
      if (pb !== pa) return pb - pa
      return safeNumber(b.priority) - safeNumber(a.priority)
    })
    .slice(0, 12)

  if (!sorted.length) return 0

  const rows = sorted.map((m, index) => ({
    rank: index,
    match_id: m.id,
    home_team: m.home_team,
    away_team: m.away_team,
    league: m.league,
    market: m.pick,
    probability: round(m.probability),
    is_opportunity: true,
    home_logo: m.home_logo || null,
    away_logo: m.away_logo || null,
    kickoff: m.kickoff || null,
    created_at: new Date().toISOString(),
  }))

  const { error } = await supabase.from("daily_picks").insert(rows)
  if (error) throw new Error(`Supabase daily_picks insert: ${error.message}`)

  return rows.length
}

async function buildAndStoreMatches(fixtures) {
  const stored = []

  for (const fixture of fixtures) {
    try {
      const comp = fixture.__comp
      if (!comp) continue

      const fixtureId = fixture?.fixture?.id
      const homeTeamId = fixture?.teams?.home?.id
      const awayTeamId = fixture?.teams?.away?.id

      if (!fixtureId || !homeTeamId || !awayTeamId) continue

      const homeTeam = fixture?.teams?.home?.name || null
      const awayTeam = fixture?.teams?.away?.name || null
      const homeLogo = fixture?.teams?.home?.logo || null
      const awayLogo = fixture?.teams?.away?.logo || null
      const kickoff = fixture?.fixture?.date || null
      const country = comp.country || fixture?.league?.country || null

      const homeSplits = await buildTeamSplits(homeTeamId)
      const awaySplits = await buildTeamSplits(awayTeamId)

      const projection = buildMatchProjection(homeSplits, awaySplits)
      const probs = resultProbabilities(
        projection.expected_home_goals,
        projection.expected_away_goals
      )

      const avgGoals = round(projection.expected_home_goals + projection.expected_away_goals)
      const avgCorners = round(projection.expected_corners)
      const avgShots = round(projection.expected_home_shots + projection.expected_away_shots)
      const avgShotsOnTarget = round(projection.expected_home_sot + projection.expected_away_sot)
      const avgCards = round(projection.expected_cards)
      const cornersOver85Prob = deriveCornersProbability(avgCorners)

      const matchBase = {
        id: fixtureId,
        fixture_id: fixtureId,
        kickoff,
        match_date: kickoff ? kickoff.slice(0, 10) : null,
        league: comp.display,
        country,
        region: comp.region,
        priority: comp.priority,
        home_team: homeTeam,
        away_team: awayTeam,
        home_logo: homeLogo,
        away_logo: awayLogo,

        avg_goals: avgGoals,
        avg_corners: avgCorners,
        avg_shots: avgShots,
        over15_prob: probs.over15,
        over25_prob: probs.over25,
        under25_prob: probs.under25,
        under35_prob: probs.under35,
        btts_prob: probs.btts,
        corners_over85_prob: cornersOver85Prob,

        home_win_prob: probs.home,
        draw_prob: probs.draw,
        away_win_prob: probs.away,
        home_result_prob: probs.home,
        draw_result_prob: probs.draw,
        away_result_prob: probs.away,

        home_form: JSON.stringify(homeSplits.overall.recentScores || []),
        away_form: JSON.stringify(awaySplits.overall.recentScores || []),

        metrics: {
          goals: avgGoals,
          corners: avgCorners,
          shots: avgShots,
          shots_on_target: avgShotsOnTarget,
          cards: avgCards,
          fouls: round(
            weightedMetric(
              { value: homeSplits.home.avgFouls, weight: 0.6 },
              { value: homeSplits.overall.avgFouls, weight: 0.4 }
            ) +
              weightedMetric(
                { value: awaySplits.away.avgFouls, weight: 0.6 },
                { value: awaySplits.overall.avgFouls, weight: 0.4 }
              )
          ),
        },

        probabilities: {
          home: probs.home,
          draw: probs.draw,
          away: probs.away,
        },

        markets: {
          over15: probs.over15,
          over25: probs.over25,
          under25: probs.under25,
          under35: probs.under35,
          btts: probs.btts,
          corners: avgCorners,
          cards: avgCards,
        },

        updated_at: new Date().toISOString(),
      }

      const primary = choosePrimaryPick(matchBase)

      const finalMatch = {
        ...matchBase,
        pick: primary.market,
        probability: round(primary.probability),
        insight: buildInsight({
          ...matchBase,
          pick: primary.market,
        }),
      }

      await upsertMatchRow(finalMatch)

      await insertMatchStatsRow({
        match_id: fixtureId,
        home_shots: round(projection.expected_home_shots),
        home_shots_on_target: round(projection.expected_home_sot),
        home_corners: round(
          weightedMetric(
            { value: homeSplits.home.avgCorners, weight: 0.6 },
            { value: homeSplits.overall.avgCorners, weight: 0.4 }
          )
        ),
        home_yellow_cards: round(
          weightedMetric(
            { value: homeSplits.home.avgCards, weight: 0.6 },
            { value: homeSplits.overall.avgCards, weight: 0.4 }
          )
        ),
        away_shots: round(projection.expected_away_shots),
        away_shots_on_target: round(projection.expected_away_sot),
        away_corners: round(
          weightedMetric(
            { value: awaySplits.away.avgCorners, weight: 0.6 },
            { value: awaySplits.overall.avgCorners, weight: 0.4 }
          )
        ),
        away_yellow_cards: round(
          weightedMetric(
            { value: awaySplits.away.avgCards, weight: 0.6 },
            { value: awaySplits.overall.avgCards, weight: 0.4 }
          )
        ),
        created_at: new Date().toISOString(),
      })

      await insertMatchAnalysisRow({
        match_id: fixtureId,
        home_strength: round(projection.expected_home_goals + projection.expected_home_sot * 0.25 + projection.expected_home_shots * 0.03),
        away_strength: round(projection.expected_away_goals + projection.expected_away_sot * 0.25 + projection.expected_away_shots * 0.03),
        expected_home_goals: round(projection.expected_home_goals),
        expected_away_goals: round(projection.expected_away_goals),
        expected_home_shots: round(projection.expected_home_shots),
        expected_away_shots: round(projection.expected_away_shots),
        expected_home_sot: round(projection.expected_home_sot),
        expected_away_sot: round(projection.expected_away_sot),
        expected_corners: round(projection.expected_corners),
        expected_cards: round(projection.expected_cards),
        prob_over25: probs.over25,
        prob_btts: probs.btts,
        prob_corners: cornersOver85Prob,
        prob_shots: clamp(round(avgShots / 28), 0.1, 0.95),
        prob_sot: clamp(round(avgShotsOnTarget / 10), 0.1, 0.95),
        prob_cards: clamp(round(avgCards / 6), 0.1, 0.95),
        best_pick_1: primary.market,
        best_pick_2: probs.over15 >= 0.78 ? "Mais de 1.5 gols" : null,
        best_pick_3: cornersOver85Prob >= 0.68 ? "Mais de 8.5 escanteios" : null,
        created_at: new Date().toISOString(),
      })

      stored.push(finalMatch)

      console.log(
        `✅ ${finalMatch.league} | ${finalMatch.home_team} x ${finalMatch.away_team} | ${finalMatch.pick} | prob=${finalMatch.probability}`
      )
    } catch (error) {
      console.error(`❌ Falha processando fixture ${fixture?.fixture?.id}:`, error.message)
    }
  }

  return stored
}

async function run() {
  console.log("🚀 Scoutly Sync V4 iniciado")

  const { start, end } = buildWindow()
  console.log(`📆 Janela ativa: ${start.toISOString()} -> ${end.toISOString()}`)

  const competitions = await resolveTargetCompetitions()
  console.log(`🏆 Competições resolvidas: ${competitions.length}`)

  const fixtureLists = []
  for (const comp of competitions) {
    const list = await fetchFixturesForCompetition(comp)
    fixtureLists.push(list)
  }

  const allFixtures = uniqBy(
    fixtureLists.flat(),
    (item) => item?.fixture?.id
  )

  console.log(`📦 Fixtures únicos na janela: ${allFixtures.length}`)

  await clearWindowData()
  console.log("🧹 Janela limpa")

  const storedMatches = await buildAndStoreMatches(allFixtures)
  console.log(`💾 Matches gravados: ${storedMatches.length}`)

  const picksCount = await rebuildDailyPicks(storedMatches)
  console.log(`🎯 Daily picks gerados: ${picksCount}`)

  console.log("✅ Scoutly Sync V4 concluído")
}

run().catch((error) => {
  console.error("❌ Erro fatal no Scoutly Sync V4:", error)
  process.exit(1)
})
