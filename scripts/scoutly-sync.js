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
const WINDOW_HOURS = 120
const REQUEST_DELAY_MS = 900

const FORM_LIMIT_GENERAL = 10
const FORM_LIMIT_HOME_AWAY = 5
const MAX_DAILY_PICKS = 20

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
  { mode: "search", search: "Brazil Women", display: "Brasileirão Feminino", region: "brazil", priority: 84 },
  { mode: "search", search: "Serie A Women Brazil", display: "Brasileirão Feminino", region: "brazil", priority: 84 },

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

  // Saudi
  { mode: "search", search: "Saudi League", display: "Saudi Pro League", region: "general", priority: 85 },
  { mode: "search", search: "Saudi Pro League", display: "Saudi Pro League", region: "general", priority: 85 },

  // UEFA
  { mode: "search", search: "UEFA Champions League", display: "UEFA Champions League", region: "general", priority: 98 },
  { mode: "search", search: "UEFA Europa League", display: "UEFA Europa League", region: "general", priority: 93 },
  { mode: "search", search: "UEFA Conference League", display: "UEFA Conference League", region: "general", priority: 88 },

  // CONMEBOL
  { mode: "search", search: "CONMEBOL Libertadores", display: "Libertadores", region: "brazil", priority: 92 },
  { mode: "search", search: "Copa Libertadores", display: "Libertadores", region: "brazil", priority: 92 },
  { mode: "search", search: "CONMEBOL Sudamericana", display: "Sul-Americana", region: "brazil", priority: 86 },
  { mode: "search", search: "Copa Sudamericana", display: "Sul-Americana", region: "brazil", priority: 86 },

  // Seleções / Internacional
  { mode: "search", search: "UEFA Nations League", display: "UEFA Nations League", region: "international", priority: 95 },
  { mode: "search", search: "International Friendlies", display: "Amistosos Internacionais", region: "international", priority: 90 },
  { mode: "search", search: "Friendlies", display: "Amistosos Internacionais", region: "international", priority: 88 },
  { mode: "search", search: "World Cup - Qualification Europe", display: "Eliminatórias Europeias", region: "international", priority: 94 },
  { mode: "search", search: "UEFA Euro Qualifiers", display: "Eliminatórias da Euro", region: "international", priority: 94 },
  { mode: "search", search: "CONMEBOL World Cup Qualifiers", display: "Eliminatórias Sul-Americanas", region: "international", priority: 96 },
  { mode: "search", search: "World Cup - Qualification South America", display: "Eliminatórias Sul-Americanas", region: "international", priority: 96 },
  { mode: "search", search: "World Cup - Qualification Africa", display: "Eliminatórias Africanas", region: "international", priority: 88 },
  { mode: "search", search: "World Cup - Qualification Asia", display: "Eliminatórias Asiáticas", region: "international", priority: 88 },
  { mode: "search", search: "World Cup - Qualification CONCACAF", display: "Eliminatórias CONCACAF", region: "international", priority: 88 },
  { mode: "search", search: "Copa America", display: "Copa América", region: "international", priority: 98 },
  { mode: "search", search: "UEFA European Championship", display: "Eurocopa", region: "international", priority: 98 },
  { mode: "search", search: "FIFA World Cup", display: "Copa do Mundo", region: "international", priority: 100 },
]

const apiCache = new Map()
const fixtureStatsCache = new Map()
const teamContextCache = new Map()

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function safeNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function round(value, decimals = 2) {
  return Number(Number(value || 0).toFixed(decimals))
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function avg(arr) {
  if (!arr.length) return 0
  return arr.reduce((a, b) => a + b, 0) / arr.length
}

function sum(arr) {
  return arr.reduce((a, b) => a + b, 0)
}

function isoDate(date) {
  return date.toISOString().slice(0, 10)
}

function normalizeText(value) {
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

function getSyncWindowRange() {
  const now = new Date()
  const start = new Date(now)
  start.setHours(0, 0, 0, 0)

  const end = new Date(now.getTime() + WINDOW_HOURS * 60 * 60 * 1000)
  return { start, end }
}

function hasForbiddenMarker(value = "") {
  const v = normalizeText(value)

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
    v.includes("women reserve") ||
    v.includes("reserve") ||
    v.includes("reserves") ||
    v.includes("youth")
  )
}

function normalizeCompetitionName(country, rawName, fallbackDisplay) {
  const name = String(rawName || "").trim()
  const c = String(country || "").trim()

  if (c === "Brazil" && name === "Serie A") return "Brasileirão Série A"
  if (c === "Brazil" && name === "Serie B") return "Brasileirão Série B"
  if (c === "Brazil" && normalizeText(name).includes("nordeste")) return "Copa do Nordeste"
  if (c === "Brazil" && normalizeText(name).includes("women")) return "Brasileirão Feminino"

  if (c === "Argentina" && (name === "Liga Profesional Argentina" || name === "Primera División")) {
    return "Liga Profesional Argentina"
  }

  if (c === "Portugal" && (name === "Primeira Liga" || name === "Liga Portugal Betclic")) {
    return "Primeira Liga"
  }

  if (c === "USA" && (name === "Major League Soccer" || name === "MLS")) return "MLS"
  if (c === "Turkey" && (name === "Süper Lig" || name === "Super Lig")) return "Super Lig"
  if (c === "Greece" && (name === "Super League 1" || name === "Super League")) return "Super League Greece"
  if (c === "Austria" && name === "Bundesliga") return "Austrian Bundesliga"
  if (c === "Belgium" && (name === "Pro League" || name === "Jupiler Pro League")) return "Belgian Pro League"
  if (c === "Denmark" && (name === "Superliga" || name === "Superligaen")) return "Superliga"
  if (c === "Saudi Arabia" && normalizeText(name).includes("pro league")) return "Saudi Pro League"

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

  const normalizedNames = new Set((target.names || []).map((x) => normalizeText(x)))

  return leagues
    .filter((item) => {
      const rawName = String(item?.league?.name || "")
      const leagueType = String(item?.league?.type || "").toLowerCase()
      const seasonCurrent = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]

      if (!seasonCurrent) return false
      if (target.type && leagueType !== target.type) return false
      if (hasForbiddenMarker(rawName)) return false

      const rawKey = normalizeText(rawName)
      return Array.from(normalizedNames).some((n) => rawKey === n)
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
  const leagues = await api("/leagues", { search: target.search })

  const items = leagues
    .map((item) => {
      const currentSeason = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]
      if (!currentSeason) return null

      const country = item?.country?.name || null
      const rawName = String(item?.league?.name || "").trim()
      const haystack = normalizeText(`${country || ""} ${rawName}`)

      if (hasForbiddenMarker(rawName)) return null
      if (haystack.includes("open cup")) return null

      if (target.display === "Saudi Pro League") {
        if (!normalizeText(country).includes("saudi")) return null
      }

      if (target.display === "Brasileirão Feminino") {
        const ok =
          normalizeText(country) === "brazil" &&
          (haystack.includes("women") ||
            haystack.includes("feminino") ||
            haystack.includes("feminina"))
        if (!ok) return null
      }

      if (target.display === "Copa do Nordeste") {
        if (!haystack.includes("nordeste")) return null
      }

      return {
        leagueId: item.league.id,
        season: currentSeason.year,
        country,
        rawName,
        display: normalizeCompetitionName(country, rawName, target.display),
        region: target.region,
        priority: target.priority,
      }
    })
    .filter(Boolean)

  return uniqBy(items, (x) => `${x.leagueId}:${x.season}`)
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
    } catch (error) {
      console.error(
        `Falha resolvendo competição ${target.display || target.search || target.country}:`,
        error.message
      )
    }
  }

  return uniqBy(resolved, (x) => `${x.leagueId}:${x.season}`)
}

async function fetchFixturesForCompetition(comp) {
  const { start, end } = getSyncWindowRange()
  const startDate = isoDate(start)
  const endDate = isoDate(end)

  try {
    const fixtures = await api("/fixtures", {
      league: comp.leagueId,
      season: comp.season,
      from: startDate,
      to: endDate,
      timezone: TIMEZONE,
    })

    return fixtures
      .filter((f) => {
        const home = f?.teams?.home?.name || ""
        const away = f?.teams?.away?.name || ""
        const league = f?.league?.name || ""
        if (hasForbiddenMarker(home)) return false
        if (hasForbiddenMarker(away)) return false
        if (hasForbiddenMarker(league)) return false
        if (normalizeText(league).includes("open cup")) return false
        return true
      })
      .map((fixture) => ({ ...fixture, __comp: comp }))
  } catch (error) {
    console.error(`Falha buscando fixtures de ${comp.display}:`, error.message)
    return []
  }
}

function isCompletedFixture(fixture) {
  const short = fixture?.fixture?.status?.short || ""
  return ["FT", "AET", "PEN"].includes(short)
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
    const num = Number(cleaned)
    return Number.isFinite(num) ? num : 0
  }

  return safeNumber(value)
}

async function fetchRecentFinishedFixtures(teamId, limit = 20) {
  try {
    const fixtures = await api("/fixtures", {
      team: teamId,
      last: limit,
      timezone: TIMEZONE,
    })

    return fixtures
      .filter((f) => isCompletedFixture(f))
      .sort((a, b) => new Date(b.fixture.date).getTime() - new Date(a.fixture.date).getTime())
  } catch (error) {
    console.error(`Falha buscando histórico do time ${teamId}:`, error.message)
    return []
  }
}

function buildScoreLabelForTeam(fixture, teamId) {
  const isHome = fixture?.teams?.home?.id === teamId
  const goalsFor = isHome ? safeNumber(fixture?.goals?.home) : safeNumber(fixture?.goals?.away)
  const goalsAgainst = isHome ? safeNumber(fixture?.goals?.away) : safeNumber(fixture?.goals?.home)
  return `${goalsFor}-${goalsAgainst}`
}

function weightedAverage(list, key, defaultValue = 0) {
  if (!list.length) return defaultValue

  const weights = list.map((_, index) => Math.max(1, list.length - index))
  const totalWeight = sum(weights)
  if (!totalWeight) return defaultValue

  const value = list.reduce((acc, item, index) => {
    return acc + safeNumber(item[key]) * weights[index]
  }, 0)

  return value / totalWeight
}

async function buildTeamContext(teamId) {
  if (teamContextCache.has(teamId)) return teamContextCache.get(teamId)

  const allFixtures = await fetchRecentFinishedFixtures(teamId, 20)

  const generalFixtures = allFixtures.slice(0, FORM_LIMIT_GENERAL)
  const homeFixtures = generalFixtures
    .filter((f) => f?.teams?.home?.id === teamId)
    .slice(0, FORM_LIMIT_HOME_AWAY)

  const awayFixtures = generalFixtures
    .filter((f) => f?.teams?.away?.id === teamId)
    .slice(0, FORM_LIMIT_HOME_AWAY)

  async function collectProfile(fixturesSubset) {
    if (!fixturesSubset.length) {
      return {
        matches: 0,
        avgGoalsFor: 0,
        avgGoalsAgainst: 0,
        avgShots: 0,
        avgShotsOnTarget: 0,
        avgCorners: 0,
        avgCards: 0,
        avgFouls: 0,
        recentScores: [],
      }
    }

    const rows = []

    for (const fixture of fixturesSubset) {
      const isHome = fixture?.teams?.home?.id === teamId
      const gf = isHome ? safeNumber(fixture?.goals?.home) : safeNumber(fixture?.goals?.away)
      const ga = isHome ? safeNumber(fixture?.goals?.away) : safeNumber(fixture?.goals?.home)

      const stats = await getFixtureStatistics(fixture.fixture.id)
      const teamStats = stats.find((s) => s.team.id === teamId)?.statistics || []

      rows.push({
        goalsFor: gf,
        goalsAgainst: ga,
        shots: extractStatValue(teamStats, "Total Shots"),
        shotsOnTarget: extractStatValue(teamStats, "Shots on Goal"),
        corners: extractStatValue(teamStats, "Corner Kicks"),
        fouls: extractStatValue(teamStats, "Fouls"),
        cards:
          extractStatValue(teamStats, "Yellow Cards") +
          extractStatValue(teamStats, "Red Cards"),
        scoreLabel: buildScoreLabelForTeam(fixture, teamId),
      })
    }

    return {
      matches: rows.length,
      avgGoalsFor: round(weightedAverage(rows, "goalsFor")),
      avgGoalsAgainst: round(weightedAverage(rows, "goalsAgainst")),
      avgShots: round(weightedAverage(rows, "shots")),
      avgShotsOnTarget: round(weightedAverage(rows, "shotsOnTarget")),
      avgCorners: round(weightedAverage(rows, "corners")),
      avgCards: round(weightedAverage(rows, "cards")),
      avgFouls: round(weightedAverage(rows, "fouls")),
      recentScores: rows.map((r) => r.scoreLabel).slice(0, 5),
    }
  }

  const general = await collectProfile(generalFixtures)
  const home = await collectProfile(homeFixtures)
  const away = await collectProfile(awayFixtures)

  const payload = {
    general,
    home,
    away,
  }

  teamContextCache.set(teamId, payload)
  return payload
}

function blendValue(primary, fallback, wPrimary = 0.68) {
  const p = safeNumber(primary, 0)
  const f = safeNumber(fallback, 0)
  return p * wPrimary + f * (1 - wPrimary)
}

function buildSideProfile(teamContext, side) {
  const sideProfile = side === "home" ? teamContext.home : teamContext.away
  const general = teamContext.general

  return {
    avgGoalsFor: round(blendValue(sideProfile.avgGoalsFor, general.avgGoalsFor, sideProfile.matches ? 0.70 : 0)),
    avgGoalsAgainst: round(blendValue(sideProfile.avgGoalsAgainst, general.avgGoalsAgainst, sideProfile.matches ? 0.70 : 0)),
    avgShots: round(blendValue(sideProfile.avgShots, general.avgShots, sideProfile.matches ? 0.72 : 0)),
    avgShotsOnTarget: round(blendValue(sideProfile.avgShotsOnTarget, general.avgShotsOnTarget, sideProfile.matches ? 0.72 : 0)),
    avgCorners: round(blendValue(sideProfile.avgCorners, general.avgCorners, sideProfile.matches ? 0.72 : 0)),
    avgCards: round(blendValue(sideProfile.avgCards, general.avgCards, sideProfile.matches ? 0.66 : 0)),
    avgFouls: round(blendValue(sideProfile.avgFouls, general.avgFouls, sideProfile.matches ? 0.66 : 0)),
    recentScores: general.recentScores || [],
  }
}

function buildExpectedMetrics(homeProfile, awayProfile) {
  // mais agressivo que a versão antiga: menos achatamento
  const expectedHomeGoals = round(
    homeProfile.avgGoalsFor * 0.62 + awayProfile.avgGoalsAgainst * 0.38
  )
  const expectedAwayGoals = round(
    awayProfile.avgGoalsFor * 0.55 + homeProfile.avgGoalsAgainst * 0.45
  )

  const expectedGoals = round(expectedHomeGoals + expectedAwayGoals)

  const expectedHomeShots = round(
    homeProfile.avgShots * 0.64 + awayProfile.avgShots * 0.12 + expectedHomeGoals * 3.1
  )
  const expectedAwayShots = round(
    awayProfile.avgShots * 0.60 + homeProfile.avgShots * 0.10 + expectedAwayGoals * 2.9
  )

  const expectedHomeSOT = round(
    homeProfile.avgShotsOnTarget * 0.68 + expectedHomeGoals * 1.2
  )
  const expectedAwaySOT = round(
    awayProfile.avgShotsOnTarget * 0.66 + expectedAwayGoals * 1.15
  )

  const expectedCorners = round(
    homeProfile.avgCorners * 0.54 + awayProfile.avgCorners * 0.46
  )

  const expectedCards = round(
    homeProfile.avgCards * 0.50 + awayProfile.avgCards * 0.50
  )

  const expectedFouls = round(
    homeProfile.avgFouls * 0.52 + awayProfile.avgFouls * 0.48
  )

  const expectedShots = round(expectedHomeShots + expectedAwayShots)
  const expectedSOT = round(expectedHomeSOT + expectedAwaySOT)

  return {
    expectedGoals,
    expectedHomeGoals: clamp(expectedHomeGoals, 0.2, 4.2),
    expectedAwayGoals: clamp(expectedAwayGoals, 0.2, 4.0),
    expectedHomeShots: clamp(expectedHomeShots, 4, 28),
    expectedAwayShots: clamp(expectedAwayShots, 4, 26),
    expectedHomeSOT: clamp(expectedHomeSOT, 1, 12),
    expectedAwaySOT: clamp(expectedAwaySOT, 1, 11),
    expectedShots: clamp(expectedShots, 8, 44),
    expectedSOT: clamp(expectedSOT, 2, 18),
    expectedCorners: clamp(expectedCorners, 4.5, 15.5),
    expectedCards: clamp(expectedCards, 1.2, 8.5),
    expectedFouls: clamp(expectedFouls, 8, 32),
  }
}

function poisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0

  let factorial = 1
  for (let i = 2; i <= k; i++) factorial *= i
  return (Math.exp(-lambda) * Math.pow(lambda, k)) / factorial
}

function cumulativePoisson(lambda, maxK = 10) {
  const arr = []
  let total = 0

  for (let k = 0; k <= maxK; k++) {
    const p = poisson(lambda, k)
    total += p
    arr.push(p)
  }

  if (total < 0.999) arr.push(1 - total)
  return arr
}

function buildProbabilities(metrics) {
  const homeDist = cumulativePoisson(metrics.expectedHomeGoals, 8)
  const awayDist = cumulativePoisson(metrics.expectedAwayGoals, 8)

  let homeWin = 0
  let draw = 0
  let awayWin = 0
  let over15 = 0
  let over25 = 0
  let btts = 0
  let under35 = 0

  for (let h = 0; h < homeDist.length; h++) {
    for (let a = 0; a < awayDist.length; a++) {
      const p = homeDist[h] * awayDist[a]
      const total = h + a

      if (h > a) homeWin += p
      else if (h === a) draw += p
      else awayWin += p

      if (total >= 2) over15 += p
      if (total >= 3) over25 += p
      if (total <= 3) under35 += p
      if (h >= 1 && a >= 1) btts += p
    }
  }

  const probCorners = clamp((metrics.expectedCorners - 6.5) / 4.5, 0.08, 0.93)
  const probShots = clamp((metrics.expectedShots - 14) / 18, 0.08, 0.93)
  const probSOT = clamp((metrics.expectedSOT - 4) / 8, 0.08, 0.93)
  const probCards = clamp((metrics.expectedCards - 2) / 4.5, 0.08, 0.93)

  return {
    home: round(clamp(homeWin, 0.05, 0.9)),
    draw: round(clamp(draw, 0.05, 0.5)),
    away: round(clamp(awayWin, 0.05, 0.9)),
    over15: round(clamp(over15, 0.1, 0.97)),
    over25: round(clamp(over25, 0.08, 0.95)),
    btts: round(clamp(btts, 0.08, 0.94)),
    under35: round(clamp(under35, 0.1, 0.97)),
    corners: round(probCorners),
    shots: round(probShots),
    sot: round(probSOT),
    cards: round(probCards),
  }
}

function buildMarkets(metrics, probs) {
  return {
    over15: probs.over15,
    over25: probs.over25,
    btts: probs.btts,
    corners: round(metrics.expectedCorners),
    cards: round(metrics.expectedCards),
    under35: probs.under35,
  }
}

function buildMetrics(metrics) {
  return {
    goals: round(metrics.expectedGoals),
    corners: round(metrics.expectedCorners),
    shots: round(metrics.expectedShots),
    shots_on_target: round(metrics.expectedSOT),
    cards: round(metrics.expectedCards),
    fouls: round(metrics.expectedFouls),
  }
}

function lineScore(prob, family, market) {
  let score = safeNumber(prob, 0)

  if (market === "Mais de 1.5 gols") score += 0.04
  if (market === "Mais de 2.5 gols") score += 0.02
  if (market === "Menos de 3.5 gols") score += 0.03
  if (family === "dupla_chance") score += 0.02
  if (family === "escanteios") score += 0.02
  if (market === "Empate") score -= 0.10
  if (score > 0.92) score -= 0.03

  return score
}

function buildCandidateMarkets(payload) {
  const { homeTeam, awayTeam, metrics, probabilities } = payload

  const candidates = []

  function add(market, probability, family) {
    candidates.push({
      market,
      probability: round(probability),
      score: round(lineScore(probability, family, market)),
      family,
    })
  }

  const homeOrDraw = probabilities.home + probabilities.draw
  const awayOrDraw = probabilities.away + probabilities.draw
  const under25 = clamp(1 - probabilities.over25, 0, 1)
  const bttsNo = clamp(1 - probabilities.btts, 0, 1)
  const cornersOver85 = clamp((metrics.expectedCorners - 6.7) / 3.1, 0.08, 0.93)
  const cornersUnder105 = clamp((11 - metrics.expectedCorners) / 3.5, 0.08, 0.93)

  if (probabilities.over25 >= 0.65) add("Mais de 2.5 gols", probabilities.over25, "gols")
  if (probabilities.over15 >= 0.76) add("Mais de 1.5 gols", probabilities.over15, "gols")
  if (under25 >= 0.75) add("Menos de 2.5 gols", under25, "gols")
  if (probabilities.under35 >= 0.80) add("Menos de 3.5 gols", probabilities.under35, "gols")
  if (probabilities.btts >= 0.63) add("Ambas marcam", probabilities.btts, "ambas")
  if (bttsNo >= 0.72) add("Ambas não marcam", bttsNo, "ambas")
  if (cornersOver85 >= 0.64) add("Mais de 8.5 escanteios", cornersOver85, "escanteios")
  if (cornersUnder105 >= 0.68) add("Menos de 10.5 escanteios", cornersUnder105, "escanteios")

  if (probabilities.home >= 0.62) add("Vitória do mandante", probabilities.home, "resultado")
  if (probabilities.away >= 0.62) add("Vitória do visitante", probabilities.away, "resultado")
  if (homeOrDraw >= 0.74) add(`Dupla chance ${homeTeam} ou empate`, homeOrDraw, "dupla_chance")
  if (awayOrDraw >= 0.74) add(`Dupla chance ${awayTeam} ou empate`, awayOrDraw, "dupla_chance")

  return candidates.sort((a, b) => b.score - a.score)
}

function chooseMainPick(candidates) {
  return candidates[0] || { market: "Menos de 3.5 gols", probability: 0.60, score: 0.60 }
}

function normalizeLeagueByTeams(comp, fixture) {
  let leagueDisplay = comp.display
  let country = comp.country || fixture?.league?.country || null

  const leagueNameRaw = fixture?.league?.name || ""
  const leagueId = fixture?.league?.id || comp?.leagueId || null
  const teams = `${fixture?.teams?.home?.name || ""} ${fixture?.teams?.away?.name || ""}`

  if (leagueId === 218) {
    leagueDisplay = "Austrian Bundesliga"
    country = "Austria"
  }

  if (leagueId === 203) {
    leagueDisplay = "Super Lig"
    country = "Turkey"
  }

  if (normalizeText(leagueNameRaw).includes("nordeste")) {
    leagueDisplay = "Copa do Nordeste"
    country = "Brazil"
  }

  if (
    normalizeText(leagueNameRaw).includes("women") &&
    normalizeText(country) === "brazil"
  ) {
    leagueDisplay = "Brasileirão Feminino"
  }

  if (normalizeText(teams).includes("fluminense w") || normalizeText(teams).includes("corinthians w")) {
    if (normalizeText(country) === "brazil" && normalizeText(leagueNameRaw).includes("women")) {
      leagueDisplay = "Brasileirão Feminino"
    }
  }

  return { leagueDisplay, country }
}

async function clearFutureWindow() {
  const now = new Date().toISOString()
  const { start, end } = getSyncWindowRange()
  const startIso = start.toISOString()
  const endIso = end.toISOString()

  const { error: dailyError } = await supabase.from("daily_picks").delete().neq("id", 0)
  if (dailyError) throw new Error(`Supabase delete daily_picks: ${dailyError.message}`)

  const { data: oldRows, error: oldError } = await supabase
    .from("matches")
    .select("id")
    .lte("kickoff", now)

  if (oldError) throw new Error(`Supabase select old matches: ${oldError.message}`)

  const oldIds = (oldRows || []).map((x) => x.id)

  if (oldIds.length) {
    await supabase.from("match_stats").delete().in("match_id", oldIds)
    await supabase.from("match_analysis").delete().in("match_id", oldIds)

    const { error: deleteOldError } = await supabase.from("matches").delete().in("id", oldIds)
    if (deleteOldError) throw new Error(`Supabase delete old matches: ${deleteOldError.message}`)
  }

  const { data: futureRows, error: futureError } = await supabase
    .from("matches")
    .select("id")
    .gte("kickoff", startIso)
    .lte("kickoff", endIso)

  if (futureError) throw new Error(`Supabase select future matches: ${futureError.message}`)

  const futureIds = (futureRows || []).map((x) => x.id)

  if (futureIds.length) {
    await supabase.from("match_stats").delete().in("match_id", futureIds)
    await supabase.from("match_analysis").delete().in("match_id", futureIds)

    const { error: deleteFutureError } = await supabase.from("matches").delete().in("id", futureIds)
    if (deleteFutureError) throw new Error(`Supabase delete future matches: ${deleteFutureError.message}`)
  }

  return oldIds.length + futureIds.length
}

async function upsertMatch(match) {
  const payload = {
    id: match.id,
    kickoff: match.kickoff,
    league: match.league,
    country: match.country || null,
    region: match.region || null,
    priority: match.priority || null,
    home_team: match.home_team || null,
    away_team: match.away_team || null,
    home_logo: match.home_logo || null,
    away_logo: match.away_logo || null,
    probabilities: match.probabilities || null,
    markets: match.markets || null,
    metrics: match.metrics || null,
    pick: match.pick || null,
    probability: match.probability || null,
    insight: match.insight || null,
    updated_at: new Date().toISOString(),
  }

  const { error } = await supabase.from("matches").upsert(payload, { onConflict: "id" })
  if (error) throw error
}

async function upsertMatchStats(row) {
  const payload = {
    match_id: row.match_id,
    home_shots: row.home_shots,
    home_shots_on_target: row.home_shots_on_target,
    home_corners: row.home_corners,
    home_yellow_cards: row.home_yellow_cards,
    away_shots: row.away_shots,
    away_shots_on_target: row.away_shots_on_target,
    away_corners: row.away_corners,
    away_yellow_cards: row.away_yellow_cards,
  }

  const { error } = await supabase.from("match_stats").upsert(payload, { onConflict: "match_id" })
  if (error) throw error
}

async function upsertMatchAnalysis(row) {
  const payload = {
    match_id: row.match_id,
    home_strength: row.home_strength,
    away_strength: row.away_strength,
    expected_home_goals: row.expected_home_goals,
    expected_away_goals: row.expected_away_goals,
    expected_home_shots: row.expected_home_shots,
    expected_away_shots: row.expected_away_shots,
    expected_home_sot: row.expected_home_sot,
    expected_away_sot: row.expected_away_sot,
    expected_corners: row.expected_corners,
    expected_cards: row.expected_cards,
    prob_over25: row.prob_over25,
    prob_btts: row.prob_btts,
    prob_corners: row.prob_corners,
    prob_shots: row.prob_shots,
    prob_sot: row.prob_sot,
    prob_cards: row.prob_cards,
    best_pick_1: row.best_pick_1,
    best_pick_2: row.best_pick_2,
    best_pick_3: row.best_pick_3,
    home_form: row.home_form,
    away_form: row.away_form,
    over25_prob: row.over25_prob,
    btts_prob: row.btts_prob,
    under25_prob: row.under25_prob,
    under35_prob: row.under35_prob,
    corners_over85_prob: row.corners_over85_prob,
    draw_result_prob: row.draw_result_prob,
    away_result_prob: row.away_result_prob,
    home_result_prob: row.home_result_prob,
    game_profile: row.game_profile,
    confidence_score: row.confidence_score,
    analysis_text: row.analysis_text,
    country: row.country,
    fixture_id: row.fixture_id,
    markets: row.markets,
    value_pick: row.value_pick,
    safe_pick: row.safe_pick,
    balanced_pick: row.balanced_pick,
    aggressive_pick: row.aggressive_pick,
  }

  const { error } = await supabase.from("match_analysis").upsert(payload, { onConflict: "match_id" })
  if (error) throw error
}

function buildGameProfile(metrics, probabilities) {
  if (metrics.expectedGoals >= 2.9 || probabilities.over25 >= 0.66) return "ofensivo"
  if (metrics.expectedCorners >= 9.3 && metrics.expectedShots >= 22) return "corners"
  if (metrics.expectedGoals <= 2.0 && probabilities.under35 >= 0.80) return "controlado"
  if (metrics.expectedGoals <= 1.7 && metrics.expectedShots <= 17) return "defensivo"
  return "equilibrado"
}

function buildInsight(mainPick, metrics, profile) {
  const goals = round(metrics.expectedGoals, 1)
  const corners = round(metrics.expectedCorners, 1)
  const shots = Math.round(metrics.expectedShots)

  if (mainPick === "Mais de 2.5 gols") {
    return `A leitura Scoutly projeta um jogo mais aberto, com produção ofensiva suficiente para 3 ou mais gols. O cenário combina ${goals} gols esperados e ${shots} finalizações projetadas.`
  }

  if (mainPick === "Mais de 1.5 gols") {
    return `A leitura Scoutly projeta um confronto com boa chance de pelo menos 2 gols. A média esperada está em ${goals} gols, com cenário ofensivo sustentável para essa linha.`
  }

  if (mainPick === "Menos de 2.5 gols") {
    return `O modelo identifica um cenário mais travado, com baixa explosão ofensiva e bom suporte estatístico para até 2 gols no jogo.`
  }

  if (mainPick === "Menos de 3.5 gols") {
    return `O modelo identifica um cenário mais controlado, com boa sustentação estatística para até 3 gols no jogo.`
  }

  if (mainPick.includes("escanteios")) {
    return `A leitura Scoutly projeta cerca de ${corners} escanteios, com volume ofensivo suficiente para transformar esse mercado em uma oportunidade estatística relevante.`
  }

  if (mainPick.includes("Dupla chance")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate. O equilíbrio da partida ainda pede segurança, e por isso a dupla chance aparece como leitura mais sólida.`
  }

  if (mainPick === "Ambas não marcam") {
    return `A leitura Scoutly vê um confronto com menor troca ofensiva entre os lados, sustentando o cenário de uma equipe passar em branco.`
  }

  if (mainPick === "Ambas marcam") {
    return `A leitura Scoutly identifica espaço para gols dos dois lados, combinando projeção ofensiva e comportamento recente das equipes.`
  }

  return `A leitura Scoutly classifica este confronto como ${profile}, cruzando projeção de gols, escanteios e finalizações para destacar a melhor oportunidade.`
}

async function buildAndStoreMatches(fixtureLists) {
  const { start, end } = getSyncWindowRange()

  const allFixtures = uniqBy(
    fixtureLists
      .flat()
      .filter((f) => {
        const kickoff = f?.fixture?.date
        if (!kickoff) return false
        const dt = new Date(kickoff)
        if (Number.isNaN(dt.getTime())) return false
        return dt >= start && dt <= end
      }),
    (x) => x?.fixture?.id
  )

  console.log(`📅 Fixtures na janela ativa: ${allFixtures.length}`)

  const cleared = await clearFutureWindow()
  console.log(`🧹 Limpeza prévia concluída: ${cleared}`)

  const stored = []

  for (const fixture of allFixtures) {
    try {
      const comp = fixture.__comp
      if (!comp) continue

      const homeTeamId = fixture?.teams?.home?.id
      const awayTeamId = fixture?.teams?.away?.id
      if (!homeTeamId || !awayTeamId) continue

      const homeContext = await buildTeamContext(homeTeamId)
      const awayContext = await buildTeamContext(awayTeamId)

      const homeProfile = buildSideProfile(homeContext, "home")
      const awayProfile = buildSideProfile(awayContext, "away")

      const metricsExp = buildExpectedMetrics(homeProfile, awayProfile)
      const probabilities = buildProbabilities(metricsExp)
      const markets = buildMarkets(metricsExp, probabilities)
      const metrics = buildMetrics(metricsExp)

      const candidates = buildCandidateMarkets({
        homeTeam: fixture?.teams?.home?.name || "Mandante",
        awayTeam: fixture?.teams?.away?.name || "Visitante",
        metrics: metricsExp,
        probabilities,
      })

      const mainPick = chooseMainPick(candidates)
      const pick2 = candidates[1]?.market || null
      const pick3 = candidates[2]?.market || null

      const { leagueDisplay, country } = normalizeLeagueByTeams(comp, fixture)
      const gameProfile = buildGameProfile(metricsExp, probabilities)
      const insight = buildInsight(mainPick.market, metricsExp, gameProfile)

      const matchPayload = {
        id: fixture?.fixture?.id,
        kickoff: fixture?.fixture?.date || null,
        league: leagueDisplay,
        country,
        region: comp.region,
        priority: comp.priority || 70,
        home_team: fixture?.teams?.home?.name || null,
        away_team: fixture?.teams?.away?.name || null,
        home_logo: fixture?.teams?.home?.logo || null,
        away_logo: fixture?.teams?.away?.logo || null,
        probabilities,
        markets,
        metrics,
        pick: mainPick.market,
        probability: mainPick.probability,
        insight,
      }

      await upsertMatch(matchPayload)

      await upsertMatchStats({
        match_id: matchPayload.id,
        home_shots: Math.round(metricsExp.expectedHomeShots),
        home_shots_on_target: Math.round(metricsExp.expectedHomeSOT),
        home_corners: Math.round(homeProfile.avgCorners),
        home_yellow_cards: Math.round(homeProfile.avgCards),
        away_shots: Math.round(metricsExp.expectedAwayShots),
        away_shots_on_target: Math.round(metricsExp.expectedAwaySOT),
        away_corners: Math.round(awayProfile.avgCorners),
        away_yellow_cards: Math.round(awayProfile.avgCards),
      })

      await upsertMatchAnalysis({
        match_id: matchPayload.id,
        home_strength: round(
          homeProfile.avgGoalsFor * 1.4 +
          homeProfile.avgShotsOnTarget * 0.55 +
          homeProfile.avgCorners * 0.25
        ),
        away_strength: round(
          awayProfile.avgGoalsFor * 1.35 +
          awayProfile.avgShotsOnTarget * 0.52 +
          awayProfile.avgCorners * 0.23
        ),
        expected_home_goals: round(metricsExp.expectedHomeGoals),
        expected_away_goals: round(metricsExp.expectedAwayGoals),
        expected_home_shots: round(metricsExp.expectedHomeShots),
        expected_away_shots: round(metricsExp.expectedAwayShots),
        expected_home_sot: round(metricsExp.expectedHomeSOT),
        expected_away_sot: round(metricsExp.expectedAwaySOT),
        expected_corners: round(metricsExp.expectedCorners),
        expected_cards: round(metricsExp.expectedCards),
        prob_over25: round(probabilities.over25),
        prob_btts: round(probabilities.btts),
        prob_corners: round(probabilities.corners),
        prob_shots: round(probabilities.shots),
        prob_sot: round(probabilities.sot),
        prob_cards: round(probabilities.cards),
        best_pick_1: mainPick.market,
        best_pick_2: pick2,
        best_pick_3: pick3,
        home_form: (homeProfile.recentScores || []).join("|"),
        away_form: (awayProfile.recentScores || []).join("|"),
        over25_prob: round(probabilities.over25),
        btts_prob: round(probabilities.btts),
        under25_prob: round(clamp(1 - probabilities.over25, 0, 1)),
        under35_prob: round(probabilities.under35),
        corners_over85_prob: round(clamp((metricsExp.expectedCorners - 6.7) / 3.1, 0.08, 0.93)),
        draw_result_prob: round(probabilities.draw),
        away_result_prob: round(probabilities.away),
        home_result_prob: round(probabilities.home),
        game_profile: gameProfile,
        confidence_score: round(mainPick.score),
        analysis_text: insight,
        country,
        fixture_id: matchPayload.id,
        markets: markets,
        value_pick: candidates[0]?.market || null,
        safe_pick: candidates.find((x) => x.family === "dupla_chance" || x.market.includes("Menos de 3.5"))?.market || null,
        balanced_pick: candidates[1]?.market || null,
        aggressive_pick: candidates.find((x) => x.market === "Mais de 2.5 gols" || x.market === "Ambas marcam")?.market || null,
      })

      stored.push(matchPayload)

      console.log(`✅ ${matchPayload.league} | ${matchPayload.home_team} x ${matchPayload.away_team} | ${mainPick.market}`)
    } catch (error) {
      console.error(`❌ Falha processando fixture ${fixture?.fixture?.id}:`, error.message)
    }
  }

  return stored
}

async function rebuildDailyPicks(matches) {
  if (!matches.length) return 0

  const rows = [...matches]
    .filter((m) => m.id && m.pick)
    .sort((a, b) => {
      const pa = safeNumber(a.probability, 0)
      const pb = safeNumber(b.probability, 0)
      if (pb !== pa) return pb - pa
      return safeNumber(b.priority, 0) - safeNumber(a.priority, 0)
    })
    .slice(0, MAX_DAILY_PICKS)
    .map((m, index) => ({
      match_id: m.id,
      rank: index,
      league: m.league,
      home_team: m.home_team,
      away_team: m.away_team,
      market: m.pick,
      probability: round(m.probability),
      kickoff: m.kickoff,
      is_opportunity: true,
      home_logo: m.home_logo || null,
      away_logo: m.away_logo || null,
      created_at: new Date().toISOString(),
    }))

  if (!rows.length) return 0

  const { error } = await supabase.from("daily_picks").insert(rows)
  if (error) throw new Error(`Supabase daily_picks: ${error.message}`)

  return rows.length
}

async function run() {
  console.log("🚀 Scoutly Sync V4 iniciado")

  const { start, end } = getSyncWindowRange()
  console.log(`📆 Janela ativa: ${start.toISOString()} -> ${end.toISOString()}`)

  const competitions = await resolveTargetCompetitions()
  console.log(`🏆 Competições resolvidas: ${competitions.length}`)

  const fixtureLists = []
  for (const comp of competitions) {
    const list = await fetchFixturesForCompetition(comp)
    fixtureLists.push(list)
    console.log(`📌 ${comp.display}: ${list.length} fixture(s)`)
  }

  const storedMatches = await buildAndStoreMatches(fixtureLists)
  const picksCount = await rebuildDailyPicks(storedMatches)

  console.log(`🏁 Daily picks gerados: ${picksCount}`)
  console.log("✅ Scoutly Sync V4 concluído")
}

run().catch((error) => {
  console.error("❌ Erro fatal no Scoutly Sync V4:", error)
  process.exit(1)
})
