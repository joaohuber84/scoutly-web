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

/**
 * MAIS LEVE:
 * - janela de 5 dias
 * - histórico geral: 8 jogos
 * - histórico por mando: 4 jogos
 * - stats detalhadas: últimos 4 jogos
 */
const WINDOW_HOURS = 120
const REQUEST_DELAY_MS = 350
const RECENT_MATCH_LIMIT = 8
const VENUE_MATCH_LIMIT = 4
const STATS_MATCH_LIMIT = 4
const DAILY_PICKS_LIMIT = 20

const TARGET_COMPETITIONS = [
  // ===== PRINCIPAIS EUROPA =====
  { mode: "country", country: "England", type: "league", names: ["Premier League"], display: "Premier League", region: "general", priority: 100 },
  { mode: "country", country: "England", type: "cup", names: ["FA Cup", "EFL Cup", "League Cup"], display: "England - Cup", region: "general", priority: 80 },

  { mode: "country", country: "Spain", type: "league", names: ["La Liga"], display: "La Liga", region: "general", priority: 98 },
  { mode: "country", country: "Spain", type: "cup", names: ["Copa del Rey"], display: "Copa del Rey", region: "general", priority: 78 },

  { mode: "country", country: "Italy", type: "league", names: ["Serie A"], display: "Serie A", region: "general", priority: 97 },
  { mode: "country", country: "Italy", type: "cup", names: ["Coppa Italia"], display: "Coppa Italia", region: "general", priority: 77 },

  { mode: "country", country: "Germany", type: "league", names: ["Bundesliga"], display: "Bundesliga", region: "general", priority: 96 },
  { mode: "country", country: "Germany", type: "cup", names: ["DFB Pokal", "DFB-Pokal"], display: "DFB-Pokal", region: "general", priority: 76 },

  { mode: "country", country: "France", type: "league", names: ["Ligue 1"], display: "Ligue 1", region: "general", priority: 95 },
  { mode: "country", country: "France", type: "cup", names: ["Coupe de France"], display: "Coupe de France", region: "general", priority: 75 },

  { mode: "country", country: "Netherlands", type: "league", names: ["Eredivisie"], display: "Eredivisie", region: "general", priority: 90 },
  { mode: "country", country: "Portugal", type: "league", names: ["Primeira Liga", "Liga Portugal Betclic"], display: "Primeira Liga", region: "general", priority: 89 },

  // ===== AMÉRICA DO SUL =====
  { mode: "country", country: "Brazil", type: "league", names: ["Serie A", "Brasileirão Série A", "Campeonato Brasileiro Série A"], display: "Brasileirão Série A", region: "brazil", priority: 94 },
  { mode: "country", country: "Brazil", type: "league", names: ["Serie B", "Brasileirão Série B", "Campeonato Brasileiro Série B"], display: "Brasileirão Série B", region: "brazil", priority: 88 },
  { mode: "country", country: "Brazil", type: "cup", names: ["Copa do Brasil"], display: "Copa do Brasil", region: "brazil", priority: 91 },

  // NOVOS IMPORTANTES
  { mode: "search", search: "Copa do Nordeste", display: "Copa do Nordeste", region: "brazil", priority: 92 },
  { mode: "search", search: "Brasileiro Women", display: "Brasileirão Feminino", region: "brazil", priority: 86 },
  { mode: "search", search: "Brazil Women", display: "Brasileirão Feminino", region: "brazil", priority: 85 },

  { mode: "country", country: "Argentina", type: "league", names: ["Liga Profesional Argentina", "Primera División"], display: "Liga Profesional Argentina", region: "general", priority: 84 },
  { mode: "country", country: "Argentina", type: "cup", names: ["Copa Argentina"], display: "Copa Argentina", region: "general", priority: 79 },

  // ===== OUTRAS LIGAS =====
  { mode: "country", country: "Mexico", type: "league", names: ["Liga MX"], display: "Liga MX", region: "general", priority: 79 },
  { mode: "country", country: "Turkey", type: "league", names: ["Süper Lig", "Super Lig"], display: "Super Lig", region: "general", priority: 78 },
  { mode: "country", country: "Denmark", type: "league", names: ["Superliga", "Superligaen"], display: "Superliga", region: "general", priority: 75 },
  { mode: "country", country: "Greece", type: "league", names: ["Super League 1", "Super League"], display: "Super League Greece", region: "general", priority: 74 },
  { mode: "country", country: "Belgium", type: "league", names: ["Pro League", "Jupiler Pro League"], display: "Belgian Pro League", region: "general", priority: 85 },
  { mode: "country", country: "Austria", type: "league", names: ["Bundesliga"], display: "Austrian Bundesliga", region: "general", priority: 84 },
  { mode: "country", country: "USA", type: "league", names: ["Major League Soccer"], display: "MLS", region: "america", priority: 82 },
  { mode: "search", search: "Saudi League", display: "Saudi Pro League", region: "general", priority: 80 },

  // ===== UEFA / CONMEBOL =====
  { mode: "search", search: "UEFA Champions League", display: "UEFA Champions League", region: "general", priority: 98 },
  { mode: "search", search: "UEFA Europa League", display: "UEFA Europa League", region: "general", priority: 93 },
  { mode: "search", search: "UEFA Conference League", display: "UEFA Conference League", region: "general", priority: 88 },
  { mode: "search", search: "CONMEBOL Libertadores", display: "Libertadores", region: "brazil", priority: 92 },
  { mode: "search", search: "Copa Libertadores", display: "Libertadores", region: "brazil", priority: 91 },
  { mode: "search", search: "CONMEBOL Sudamericana", display: "Sul-Americana", region: "brazil", priority: 86 },
  { mode: "search", search: "Copa Sudamericana", display: "Sul-Americana", region: "brazil", priority: 85 },

  // ===== SELEÇÕES / DATA FIFA =====
  { mode: "search", search: "UEFA Nations League", display: "UEFA Nations League", region: "international", priority: 95 },
  { mode: "search", search: "International Friendlies", display: "Amistosos Internacionais", region: "international", priority: 90 },
  { mode: "search", search: "Friendlies", display: "Amistosos Internacionais", region: "international", priority: 84 },

  { mode: "search", search: "CONMEBOL World Cup Qualifiers", display: "Eliminatórias Sul-Americanas", region: "international", priority: 96 },
  { mode: "search", search: "World Cup - Qualification Europe", display: "Eliminatórias Europeias", region: "international", priority: 94 },
  { mode: "search", search: "World Cup - Qualification Africa", display: "Eliminatórias Africanas", region: "international", priority: 88 },
  { mode: "search", search: "World Cup - Qualification Asia", display: "Eliminatórias Asiáticas", region: "international", priority: 88 },
  { mode: "search", search: "World Cup - Qualification CONCACAF", display: "Eliminatórias CONCACAF", region: "international", priority: 88 },

  { mode: "search", search: "UEFA European Championship", display: "Eurocopa", region: "international", priority: 98 },
  { mode: "search", search: "Copa America", display: "Copa América", region: "international", priority: 98 },
  { mode: "search", search: "FIFA World Cup", display: "Copa do Mundo", region: "international", priority: 100 },
]

const apiCache = new Map()
const fixtureStatsCache = new Map()
const teamRecentFixturesCache = new Map()
const teamProfileCache = new Map()

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 2) {
  return Number(Number(value || 0).toFixed(decimals))
}

function safeNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
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
    v.includes("women reserve") ||
    v.includes("reserves") ||
    v.includes("reserve") ||
    v.includes("youth")
  )
}

function normalizeCompetitionName(country, rawName, fallbackDisplay) {
  const name = String(rawName || "").trim()
  const c = String(country || "").trim()

  if (c === "Brazil" && name === "Serie A") return "Brasileirão Série A"
  if (c === "Brazil" && name === "Serie B") return "Brasileirão Série B"
  if (c === "Brazil" && name === "Copa do Brasil") return "Copa do Brasil"
  if (c === "Argentina" && (name === "Liga Profesional Argentina" || name === "Primera División")) return "Liga Profesional Argentina"
  if (c === "Portugal" && (name === "Primeira Liga" || name === "Liga Portugal Betclic")) return "Primeira Liga"
  if (c === "USA" && (name === "Major League Soccer" || name === "MLS")) return "MLS"
  if (c === "Turkey" && (name === "Süper Lig" || name === "Super Lig")) return "Super Lig"
  if (c === "Greece" && (name === "Super League 1" || name === "Super League")) return "Super League Greece"
  if (c === "Austria" && name === "Bundesliga") return "Austrian Bundesliga"

  if (
    c === "Saudi Arabia" &&
    (
      name === "Pro League" ||
      name === "Saudi Pro League" ||
      name === "Saudi League" ||
      name === "ROSHN Saudi League"
    )
  ) {
    return "Saudi Pro League"
  }

  if (normalizeLeagueKey(name).includes("nations league")) return "UEFA Nations League"
  if (normalizeLeagueKey(name).includes("friendlies")) return "Amistosos Internacionais"
  if (normalizeLeagueKey(name).includes("copa do nordeste")) return "Copa do Nordeste"
  if (normalizeLeagueKey(name).includes("women") && c === "Brazil") return "Brasileirão Feminino"

  return fallbackDisplay || name || c || "Competição"
}

async function resolveCountryCompetitions(target) {
  const leagues = await api("/leagues", {
    country: target.country,
    current: true,
  })

  const normalizedNames = new Set(
    (target.names || []).map((x) => normalizeLeagueKey(x))
  )

  return leagues
    .filter((item) => {
      const rawName = String(item?.league?.name || "")
      const rawNameKey = normalizeLeagueKey(rawName)
      const leagueType = String(item?.league?.type || "").toLowerCase()
      const seasonCurrent = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]

      if (!seasonCurrent) return false
      if (target.type && leagueType !== target.type) return false
      if (hasForbiddenMarker(rawName)) return false

      return Array.from(normalizedNames).some((n) => rawNameKey === n)
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
    current: true,
  })

  const items = leagues
    .map((item) => {
      const currentSeason = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]
      if (!currentSeason) return null

      const country = item?.country?.name || null
      const rawName = String(item?.league?.name || "").trim()
      const countryText = (country || "").toLowerCase()
      const rawNameText = rawName.toLowerCase()
      const haystack = `${country || ""} ${rawName}`.toLowerCase().trim()

      if (hasForbiddenMarker(rawName)) return null
      if (rawNameText.includes("open cup")) return null

      if (target.display === "Saudi Pro League") {
        if (!countryText.includes("saudi")) return null
      }

      if (target.display === "Copa do Nordeste") {
        if (!haystack.includes("nordeste")) return null
      }

      if (target.display === "Brasileirão Feminino") {
        if (!(countryText.includes("brazil") && rawNameText.includes("women"))) return null
      }

      if (target.display === "UEFA Nations League" && !haystack.includes("nations")) return null
      if (target.display === "Amistosos Internacionais" && !haystack.includes("friend")) return null

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

  const unique = new Map()
  items.forEach((item) => {
    const key = item.leagueId
    if (!unique.has(key)) unique.set(key, item)
  })

  return Array.from(unique.values())
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

  const seen = new Set()
  return resolved.filter((item) => {
    const key = `${item.leagueId}:${item.season}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
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
        return true
      })
      .map((fixture) => ({
        ...fixture,
        __comp: comp,
      }))
  } catch (error) {
    console.error(
      `Falha buscando fixtures de ${comp.display} entre ${startDate} e ${endDate}:`,
      error.message
    )
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
    const num = Number(cleaned)
    return Number.isFinite(num) ? num : 0
  }

  return safeNumber(value)
}

function isCompletedFixture(fixture) {
  const short = fixture?.fixture?.status?.short || ""
  return ["FT", "AET", "PEN"].includes(short)
}

async function fetchRecentFinishedFixtures(teamId, limit = RECENT_MATCH_LIMIT) {
  const cacheKey = `${teamId}:${limit}`
  if (teamRecentFixturesCache.has(cacheKey)) {
    return teamRecentFixturesCache.get(cacheKey)
  }

  try {
    const fixtures = await api("/fixtures", {
      team: teamId,
      last: limit,
      timezone: TIMEZONE,
    })

    const cleaned = fixtures
      .filter((f) => isCompletedFixture(f))
      .sort(
        (a, b) =>
          new Date(b.fixture.date).getTime() -
          new Date(a.fixture.date).getTime()
      )
      .slice(0, limit)

    teamRecentFixturesCache.set(cacheKey, cleaned)
    return cleaned
  } catch (error) {
    console.error(`Falha buscando histórico do time ${teamId}:`, error.message)
    teamRecentFixturesCache.set(cacheKey, [])
    return []
  }
}

function splitVenueFixtures(fixtures, teamId, wantHome, limit = VENUE_MATCH_LIMIT) {
  return fixtures
    .filter((f) => {
      const isHome = f?.teams?.home?.id === teamId
      return wantHome ? isHome : !isHome
    })
    .slice(0, limit)
}

function getGoalsForAgainst(fixture, teamId) {
  const isHome = fixture?.teams?.home?.id === teamId
  const gf = isHome ? fixture?.goals?.home ?? 0 : fixture?.goals?.away ?? 0
  const ga = isHome ? fixture?.goals?.away ?? 0 : fixture?.goals?.home ?? 0
  return {
    gf: safeNumber(gf),
    ga: safeNumber(ga),
  }
}

async function buildTeamProfile(teamId, wantHomeProfile) {
  const cacheKey = `${teamId}:${wantHomeProfile ? "home" : "away"}`
  if (teamProfileCache.has(cacheKey)) {
    return teamProfileCache.get(cacheKey)
  }

  const recentFixtures = await fetchRecentFinishedFixtures(teamId, RECENT_MATCH_LIMIT)
  const venueFixtures = splitVenueFixtures(recentFixtures, teamId, wantHomeProfile, VENUE_MATCH_LIMIT)
  const statFixtures = recentFixtures.slice(0, STATS_MATCH_LIMIT)

  const goalsForOverall = []
  const goalsAgainstOverall = []
  const goalsForVenue = []
  const goalsAgainstVenue = []

  for (const f of recentFixtures) {
    const { gf, ga } = getGoalsForAgainst(f, teamId)
    goalsForOverall.push(gf)
    goalsAgainstOverall.push(ga)
  }

  for (const f of venueFixtures) {
    const { gf, ga } = getGoalsForAgainst(f, teamId)
    goalsForVenue.push(gf)
    goalsAgainstVenue.push(ga)
  }

  const shots = []
  const shotsOnTarget = []
  const corners = []
  const fouls = []
  const cards = []

  for (const f of statFixtures) {
    const stats = await getFixtureStatistics(f.fixture.id)
    const teamStats =
      stats.find((s) => s.team.id === teamId)?.statistics || []

    shots.push(extractStatValue(teamStats, "Total Shots"))
    shotsOnTarget.push(extractStatValue(teamStats, "Shots on Goal"))
    corners.push(extractStatValue(teamStats, "Corner Kicks"))
    fouls.push(extractStatValue(teamStats, "Fouls"))
    cards.push(
      extractStatValue(teamStats, "Yellow Cards") +
      extractStatValue(teamStats, "Red Cards")
    )
  }

  const overallGF = avg(goalsForOverall)
  const overallGA = avg(goalsAgainstOverall)
  const venueGF = goalsForVenue.length ? avg(goalsForVenue) : overallGF
  const venueGA = goalsAgainstVenue.length ? avg(goalsAgainstVenue) : overallGA

  const profile = {
    matches: recentFixtures.length,
    venueMatches: venueFixtures.length,
    avgGoalsFor: round(overallGF * 0.45 + venueGF * 0.55),
    avgGoalsAgainst: round(overallGA * 0.45 + venueGA * 0.55),
    avgShots: round(avg(shots)),
    avgShotsOnTarget: round(avg(shotsOnTarget)),
    avgCorners: round(avg(corners)),
    avgFouls: round(avg(fouls)),
    avgCards: round(avg(cards)),
    recentScores: recentFixtures
      .slice(0, 5)
      .map((f) => {
        const { gf, ga } = getGoalsForAgainst(f, teamId)
        return `${gf}-${ga}`
      }),
  }

  teamProfileCache.set(cacheKey, profile)
  return profile
}

function buildExpectedFromProfiles(homeProfile, awayProfile) {
  const expectedHomeGoals = clamp(
    (homeProfile.avgGoalsFor * 0.62) + (awayProfile.avgGoalsAgainst * 0.38),
    0.35,
    3.6
  )

  const expectedAwayGoals = clamp(
    (awayProfile.avgGoalsFor * 0.56) + (homeProfile.avgGoalsAgainst * 0.44),
    0.25,
    3.1
  )

  const expectedHomeShots = clamp(
    (homeProfile.avgShots * 0.58) + (awayProfile.avgShots * 0.22) + expectedHomeGoals * 2.2,
    5,
    24
  )

  const expectedAwayShots = clamp(
    (awayProfile.avgShots * 0.56) + (homeProfile.avgShots * 0.18) + expectedAwayGoals * 2.0,
    4,
    22
  )

  const expectedHomeSOT = clamp(
    (homeProfile.avgShotsOnTarget * 0.62) + expectedHomeGoals * 0.9,
    1,
    9
  )

  const expectedAwaySOT = clamp(
    (awayProfile.avgShotsOnTarget * 0.60) + expectedAwayGoals * 0.85,
    1,
    8
  )

  const expectedCorners = clamp(
    (homeProfile.avgCorners + awayProfile.avgCorners) * 0.92,
    4.5,
    13.5
  )

  const expectedCards = clamp(
    (homeProfile.avgCards + awayProfile.avgCards),
    1.5,
    7.5
  )

  const expectedFouls = clamp(
    (homeProfile.avgFouls + awayProfile.avgFouls),
    8,
    32
  )

  return {
    expectedHomeGoals: round(expectedHomeGoals),
    expectedAwayGoals: round(expectedAwayGoals),
    expectedHomeShots: round(expectedHomeShots),
    expectedAwayShots: round(expectedAwayShots),
    expectedHomeSOT: round(expectedHomeSOT),
    expectedAwaySOT: round(expectedAwaySOT),
    expectedCorners: round(expectedCorners),
    expectedCards: round(expectedCards),
    expectedFouls: round(expectedFouls),
  }
}

function buildProbabilities(expected) {
  const totalGoals = expected.expectedHomeGoals + expected.expectedAwayGoals

  const homeStrength =
    expected.expectedHomeGoals * 1.25 +
    expected.expectedHomeSOT * 0.18 +
    expected.expectedHomeShots * 0.03

  const awayStrength =
    expected.expectedAwayGoals * 1.22 +
    expected.expectedAwaySOT * 0.18 +
    expected.expectedAwayShots * 0.03

  const totalStrength = homeStrength + awayStrength || 1

  let homeProb = clamp(homeStrength / totalStrength, 0.10, 0.82)
  let awayProb = clamp(awayStrength / totalStrength, 0.10, 0.82)

  let drawProb = clamp(
    0.28 - Math.abs(homeProb - awayProb) * 0.55,
    0.12,
    0.32
  )

  const normalize = homeProb + drawProb + awayProb
  homeProb /= normalize
  drawProb /= normalize
  awayProb /= normalize

  const over15 = clamp(totalGoals / 2.15, 0.18, 0.94)
  const over25 = clamp((totalGoals - 0.8) / 2.3, 0.08, 0.88)
  const btts = clamp(
    (Math.min(expected.expectedHomeGoals, expected.expectedAwayGoals) / 1.15) * 0.78 +
      (totalGoals / 4.4) * 0.22,
    0.10,
    0.82
  )

  const cornersProb = clamp(expected.expectedCorners / 10.5, 0.15, 0.92)
  const shotsProb = clamp((expected.expectedHomeShots + expected.expectedAwayShots) / 25, 0.15, 0.92)
  const sotProb = clamp((expected.expectedHomeSOT + expected.expectedAwaySOT) / 8.5, 0.15, 0.92)
  const cardsProb = clamp(expected.expectedCards / 4.8, 0.15, 0.92)

  return {
    probabilities: {
      home: round(homeProb),
      draw: round(drawProb),
      away: round(awayProb),
    },
    markets: {
      over15: round(over15),
      over25: round(over25),
      btts: round(btts),
      corners: round(expected.expectedCorners),
      cards: round(expected.expectedCards),
      shots: round(expected.expectedHomeShots + expected.expectedAwayShots),
      shots_on_target: round(expected.expectedHomeSOT + expected.expectedAwaySOT),
      fouls: round(expected.expectedFouls),
    },
    extraProbabilities: {
      corners85: round(cornersProb),
      shots: round(shotsProb),
      sot: round(sotProb),
      cards: round(cardsProb),
    },
  }
}

function buildCoreMetrics(expected) {
  return {
    goals: round(expected.expectedHomeGoals + expected.expectedAwayGoals),
    shots: round(expected.expectedHomeShots + expected.expectedAwayShots),
    shots_on_target: round(expected.expectedHomeSOT + expected.expectedAwaySOT),
    corners: round(expected.expectedCorners),
    fouls: round(expected.expectedFouls),
    cards: round(expected.expectedCards),
  }
}

function buildPrimaryMarket(probabilities, markets, fixture) {
  const homeProb = safeNumber(probabilities.home)
  const awayProb = safeNumber(probabilities.away)
  const drawProb = safeNumber(probabilities.draw)
  const over15 = safeNumber(markets.over15)
  const over25 = safeNumber(markets.over25)
  const btts = safeNumber(markets.btts)
  const corners = safeNumber(markets.corners)
  const cards = safeNumber(markets.cards)
  const shots = safeNumber(markets.shots)
  const shotsOnTarget = safeNumber(markets.shots_on_target)
  const goals = safeNumber(markets.goals || 0)

  const homeTeam = fixture?.teams?.home?.name || "mandante"
  const awayTeam = fixture?.teams?.away?.name || "visitante"

  const strongFavorite = Math.max(homeProb, awayProb) >= 0.62
  const balanced = Math.abs(homeProb - awayProb) <= 0.10
  const veryOpen = over25 >= 0.70 && btts >= 0.58 && shots >= 24
  const open = over15 >= 0.80 && shots >= 20
  const controlled = over25 <= 0.44 && btts <= 0.48 && shotsOnTarget <= 7
  const veryControlled = over25 <= 0.34 && shotsOnTarget <= 5.5
  const cornerGame = corners >= 9.1 && shots >= 22
  const cardGame = cards >= 4.8

  if (veryControlled) return "Menos de 2.5 gols"
  if (controlled) return "Menos de 3.5 gols"
  if (veryOpen) return "Mais de 2.5 gols"
  if (open && balanced && btts >= 0.60) return "Ambas marcam"
  if (open) return "Mais de 1.5 gols"
  if (strongFavorite && homeProb > awayProb && homeProb + drawProb >= 0.78) return `Dupla chance ${homeTeam} ou empate`
  if (strongFavorite && awayProb > homeProb && awayProb + drawProb >= 0.78) return `Dupla chance ${awayTeam} ou empate`
  if (cornerGame) return "Mais de 8.5 escanteios"
  if (cardGame) return "Mais de 3.5 cartões"

  return "Menos de 3.5 gols"
}

function buildPrimaryProbability(primaryMarket, probabilities, markets) {
  const homeProb = safeNumber(probabilities.home)
  const awayProb = safeNumber(probabilities.away)
  const drawProb = safeNumber(probabilities.draw)
  const over15 = safeNumber(markets.over15)
  const over25 = safeNumber(markets.over25)
  const btts = safeNumber(markets.btts)
  const corners = safeNumber(markets.corners)
  const cards = safeNumber(markets.cards)
  const shotsOnTarget = safeNumber(markets.shots_on_target)

  if (primaryMarket === "Mais de 2.5 gols") {
    return clamp(round(over25 + (shotsOnTarget >= 8 ? 0.04 : 0)), 0.20, 0.93)
  }

  if (primaryMarket === "Mais de 1.5 gols") {
    return clamp(round(over15 + 0.03), 0.20, 0.94)
  }

  if (primaryMarket === "Ambas marcam") {
    return clamp(round(btts + 0.03), 0.15, 0.90)
  }

  if (primaryMarket === "Menos de 2.5 gols") {
    return clamp(round((1 - over25) + 0.05), 0.20, 0.91)
  }

  if (primaryMarket === "Menos de 3.5 gols") {
    return clamp(round((1 - Math.max(over25 - 0.18, 0)) + 0.02), 0.20, 0.92)
  }

  if (primaryMarket === "Mais de 8.5 escanteios") {
    return clamp(round(corners / 10.5), 0.20, 0.90)
  }

  if (primaryMarket === "Mais de 3.5 cartões") {
    return clamp(round(cards / 4.6), 0.20, 0.88)
  }

  if (primaryMarket.includes("ou empate")) {
    if (homeProb > awayProb) return clamp(round(homeProb + drawProb), 0.20, 0.93)
    return clamp(round(awayProb + drawProb), 0.20, 0.93)
  }

  return 0.66
}

async function clearFutureWindow() {
  const now = new Date().toISOString()

  const { error: dailyError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (dailyError) {
    throw new Error(`Supabase delete daily_picks: ${dailyError.message}`)
  }

  const { data: oldRows, error: oldSelectError } = await supabase
    .from("matches")
    .select("id")
    .lte("kickoff", now)

  if (oldSelectError) {
    throw new Error(`Supabase select old matches: ${oldSelectError.message}`)
  }

  const oldIds = (oldRows || []).map((x) => x.id)
  if (oldIds.length) {
    await supabase.from("match_stats").delete().in("match_id", oldIds)
    await supabase.from("match_analysis").delete().in("match_id", oldIds).catch(() => {})
    const { error: deleteOldError } = await supabase
      .from("matches")
      .delete()
      .in("id", oldIds)

    if (deleteOldError) {
      throw new Error(`Supabase delete old matches: ${deleteOldError.message}`)
    }
  }

  const { start, end } = getSyncWindowRange()
  const startIso = start.toISOString()
  const endIso = end.toISOString()

  const { data: futureRows, error: futureSelectError } = await supabase
    .from("matches")
    .select("id")
    .gte("kickoff", startIso)
    .lte("kickoff", endIso)

  if (futureSelectError) {
    throw new Error(`Supabase select future matches: ${futureSelectError.message}`)
  }

  const futureIds = (futureRows || []).map((x) => x.id)

  if (futureIds.length) {
    await supabase.from("match_stats").delete().in("match_id", futureIds)
    await supabase.from("match_analysis").delete().in("match_id", futureIds).catch(() => {})
    const { error: futureDeleteError } = await supabase
      .from("matches")
      .delete()
      .in("id", futureIds)

    if (futureDeleteError) {
      throw new Error(`Supabase delete future matches: ${futureDeleteError.message}`)
    }
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

  const { error } = await supabase
    .from("matches")
    .upsert(payload, { onConflict: "id" })

  if (error) {
    console.error("Erro ao salvar match:", error.message)
  }
}

async function upsertMatchStats(row) {
  const payload = {
    match_id: row.match_id,
    home_shots: Math.round(row.home_shots || 0),
    home_shots_on_target: Math.round(row.home_shots_on_target || 0),
    home_corners: Math.round(row.home_corners || 0),
    home_yellow_cards: Math.round(row.home_yellow_cards || 0),
    away_shots: Math.round(row.away_shots || 0),
    away_shots_on_target: Math.round(row.away_shots_on_target || 0),
    away_corners: Math.round(row.away_corners || 0),
    away_yellow_cards: Math.round(row.away_yellow_cards || 0),
    created_at: new Date().toISOString(),
  }

  const { error } = await supabase
    .from("match_stats")
    .upsert(payload, { onConflict: "match_id" })

  if (error) {
    console.error("Erro ao salvar match_stats:", error.message)
  }
}

function buildInsightText(primaryMarket, metrics, fixture) {
  const goals = round(metrics.goals, 1)
  const corners = round(metrics.corners, 1)
  const shots = Math.round(metrics.shots)
  const sot = Math.round(metrics.shots_on_target)

  if (primaryMarket.includes("escanteios")) {
    return `A leitura Scoutly projeta cerca de ${corners} escanteios, com bom volume ofensivo (${shots} finalizações e ${sot} no alvo somados).`
  }

  if (primaryMarket.includes("Mais de 2.5 gols")) {
    return `A leitura Scoutly aponta jogo aberto, com projeção de ${goals} gols e boa produção ofensiva dos dois lados.`
  }

  if (primaryMarket.includes("Mais de 1.5 gols")) {
    return `A leitura Scoutly projeta confronto com boa chance de pelo menos 2 gols. A média esperada está em ${goals} gols.`
  }

  if (primaryMarket.includes("Menos de 2.5 gols")) {
    return `A leitura Scoutly indica jogo travado, com projeção ofensiva mais controlada. A expectativa está em ${goals} gols.`
  }

  if (primaryMarket.includes("Menos de 3.5 gols")) {
    return `O modelo identifica cenário controlado, com boa sustentação estatística para até 3 gols no jogo.`
  }

  if (primaryMarket.includes("Ambas marcam")) {
    return `A leitura Scoutly vê espaço para produção ofensiva dos dois lados, com projeção total de ${goals} gols.`
  }

  if (primaryMarket.includes("ou empate")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate como opção mais segura.`
  }

  if (primaryMarket.includes("cartões")) {
    return `A leitura Scoutly projeta partida mais intensa no aspecto disciplinar, com tendência de cartões acima da média.`
  }

  return `A leitura Scoutly projeta um jogo com ${goals} gols, ${corners} escanteios e ${shots} finalizações totais.`
}

async function buildAndStoreMatches(fixtureLists) {
  const { start, end } = getSyncWindowRange()

  const allFixtures = uniqBy(
    fixtureLists
      .flat()
      .filter((x) => {
        const kickoff = x?.fixture?.date
        if (!kickoff) return false

        const dt = new Date(kickoff)
        if (Number.isNaN(dt.getTime())) return false

        return dt >= start && dt <= end
      }),
    (x) => x?.fixture?.id
  )

  console.log(`📅 Fixtures na janela ativa: ${allFixtures.length}`)

  const cleared = await clearFutureWindow()
  console.log(`🧹 Matches futuros limpos antes do rebuild: ${cleared}`)

  const stored = []

  for (const fixture of allFixtures) {
    try {
      const comp = fixture.__comp
      if (!comp) continue

      const homeTeamId = fixture?.teams?.home?.id
      const awayTeamId = fixture?.teams?.away?.id
      if (!homeTeamId || !awayTeamId) continue

      const homeProfile = await buildTeamProfile(homeTeamId, true)
      const awayProfile = await buildTeamProfile(awayTeamId, false)

      const expected = buildExpectedFromProfiles(homeProfile, awayProfile)
      const built = buildProbabilities(expected)
      const metrics = buildCoreMetrics(expected)

      const primaryMarket = buildPrimaryMarket(
        built.probabilities,
        {
          ...built.markets,
          goals: metrics.goals,
        },
        fixture
      )

      const primaryProbability = buildPrimaryProbability(
        primaryMarket,
        built.probabilities,
        {
          ...built.markets,
          goals: metrics.goals,
        }
      )

      const payload = {
        id: fixture?.fixture?.id,
        kickoff: fixture?.fixture?.date || null,
        league: comp.display,
        country: comp.country || fixture?.league?.country || null,
        region: comp.region,
        priority: comp.priority,
        home_team: fixture?.teams?.home?.name || null,
        away_team: fixture?.teams?.away?.name || null,
        home_logo: fixture?.teams?.home?.logo || null,
        away_logo: fixture?.teams?.away?.logo || null,
        probabilities: built.probabilities,
        markets: built.markets,
        metrics,
        pick: primaryMarket,
        probability: primaryProbability,
        insight: buildInsightText(primaryMarket, metrics, fixture),
      }

      await upsertMatch(payload)

      await upsertMatchStats({
        match_id: payload.id,
        home_shots: expected.expectedHomeShots,
        home_shots_on_target: expected.expectedHomeSOT,
        home_corners: expected.expectedCorners / 2,
        home_yellow_cards: expected.expectedCards / 2,
        away_shots: expected.expectedAwayShots,
        away_shots_on_target: expected.expectedAwaySOT,
        away_corners: expected.expectedCorners / 2,
        away_yellow_cards: expected.expectedCards / 2,
      })

      stored.push(payload)

      console.log(
        `✅ ${payload.league} | ${payload.home_team} x ${payload.away_team} | ${payload.pick}`
      )
    } catch (error) {
      console.error(
        `❌ Falha processando fixture ${fixture?.fixture?.id}:`,
        error.message
      )
    }
  }

  return stored
}

async function rebuildDailyPicks(matches) {
  if (!matches.length) return 0

  const sorted = [...matches]
    .filter((m) => m.id && m.pick)
    .sort((a, b) => {
      const pa = Number(a.probability || 0)
      const pb = Number(b.probability || 0)

      if (pb !== pa) return pb - pa
      return Number(b.priority || 0) - Number(a.priority || 0)
    })
    .slice(0, DAILY_PICKS_LIMIT)

  if (!sorted.length) return 0

  const rows = sorted.map((m, index) => ({
    match_id: m.id,
    rank: index + 1,
    league: m.league,
    home_team: m.home_team,
    away_team: m.away_team,
    market: m.pick,
    probability: m.probability,
    kickoff: m.kickoff,
    is_opportunity: true,
    created_at: new Date().toISOString(),
  }))

  const { error } = await supabase.from("daily_picks").insert(rows)

  if (error) {
    throw new Error(`Supabase daily_picks: ${error.message}`)
  }

  return rows.length
}

async function run() {
  console.log("🚀 Scoutly Sync Light V1 iniciado")

  const { start, end } = getSyncWindowRange()
  console.log(`📆 Janela ativa: ${start.toISOString()} -> ${end.toISOString()}`)

  const competitions = await resolveTargetCompetitions()
  console.log(
    "🏟️ Competições resolvidas:",
    competitions.map((c) => ({
      leagueId: c.leagueId,
      display: c.display,
      country: c.country,
      season: c.season,
    }))
  )

  const fixtureLists = []
  for (const comp of competitions) {
    const list = await fetchFixturesForCompetition(comp)
    fixtureLists.push(list)
  }

  console.log(
    "📦 Fixtures por competição:",
    fixtureLists.map((list, i) => ({
      competition: competitions[i]?.display,
      total: list.length,
    }))
  )

  const storedMatches = await buildAndStoreMatches(fixtureLists)
  const picksCount = await rebuildDailyPicks(storedMatches)

  console.log(`🏁 Daily picks gerados: ${picksCount}`)
  console.log(`✅ Matches gravados: ${storedMatches.length}`)
  console.log("✅ Scoutly Sync Light V1 concluído")
}

run().catch((error) => {
  console.error("❌ Erro fatal no Scoutly Sync Light V1:", error)
  process.exit(1)
})
