const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY || ""
const SUPABASE_URL = process.env.SUPABASE_URL || ""
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_KEY ||
  process.env.SUPABASE_KEY ||
  ""

if (!APISPORTS_KEY) {
  throw new Error("APISPORTS_KEY não encontrada.")
}

if (!SUPABASE_URL) {
  throw new Error("SUPABASE_URL não encontrada.")
}

if (!SUPABASE_KEY) {
  throw new Error("SUPABASE_SERVICE_ROLE_KEY não encontrada.")
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

/**
 * SCOUTLY SYNC V13
 * - mantém schema atual
 * - melhora base de shots / SOT / cards
 * - reduz privilégio estrutural de corners
 * - entrega probabilidades mais úteis para o Brain
 */
const WINDOW_HOURS = 168
const REQUEST_DELAY_MS = 350

const FORM_LIMIT_GENERAL = 10
const FORM_LIMIT_HOME_AWAY = 5
const MAX_RECENT_FIXTURES_FETCH = 20

const MIN_REQUIRED_RECENT_MATCHES = 3
const MIN_REQUIRED_STATS_MATCHES = 2

const MAX_DAILY_PICKS = 20
const MAX_SAME_MARKET_IN_DAILY = 2
const MAX_SAME_LEAGUE_IN_DAILY = 4
const MAX_INTERNATIONAL_IN_DAILY = 8
const MAX_BRAZIL_IN_DAILY = 6

const NATIONAL_TEAM_DOUBLE_CHANCE_CAP = 0.82
const NATIONAL_TEAM_WIN_CAP = 0.74
const STRONG_MISMATCH_DOUBLE_CHANCE_BLOCK = 0.18

const apiCache = new Map()
const fixtureStatsCache = new Map()
const teamRecentFixturesCache = new Map()
const teamContextCache = new Map()
const competitionFixturesCache = new Map()

const TARGET_COMPETITIONS = [
  {
    mode: "country",
    country: "Brazil",
    type: "league",
    names: ["Serie A", "Brasileirão Série A", "Campeonato Brasileiro Série A"],
    display: "Brasileirão Série A",
    region: "brazil",
    priority: 94,
  },
  {
    mode: "country",
    country: "Brazil",
    type: "league",
    names: ["Serie B", "Brasileirão Série B", "Campeonato Brasileiro Série B"],
    display: "Brasileirão Série B",
    region: "brazil",
    priority: 88,
  },
  {
    mode: "country",
    country: "Brazil",
    type: "cup",
    names: ["Copa do Brasil"],
    display: "Copa do Brasil",
    region: "brazil",
    priority: 91,
  },
  {
    mode: "search",
    search: "Copa do Nordeste",
    display: "Copa do Nordeste",
    region: "brazil",
    priority: 90,
  },
  {
    mode: "search",
    search: "Nordeste",
    display: "Copa do Nordeste",
    region: "brazil",
    priority: 90,
  },
  {
    mode: "search",
    search: "Copa Verde",
    display: "Copa Verde",
    region: "brazil",
    priority: 82,
  },
  {
    mode: "search",
    search: "Verde",
    display: "Copa Verde",
    region: "brazil",
    priority: 82,
  },
  {
    mode: "search",
    search: "Copa Sul-Sudeste",
    display: "Copa Sul-Sudeste",
    region: "brazil",
    priority: 80,
  },
  {
    mode: "search",
    search: "Copa Sul Sudeste",
    display: "Copa Sul-Sudeste",
    region: "brazil",
    priority: 80,
  },
  {
    mode: "search",
    search: "Brasileiro Women",
    display: "Brasileirão Feminino",
    region: "brazil",
    priority: 84,
  },
  {
    mode: "search",
    search: "Brazil Women",
    display: "Brasileirão Feminino",
    region: "brazil",
    priority: 84,
  },
  {
    mode: "search",
    search: "Serie A Women Brazil",
    display: "Brasileirão Feminino",
    region: "brazil",
    priority: 84,
  },
  {
    mode: "search",
    search: "Campeonato Brasileiro Feminino",
    display: "Brasileirão Feminino",
    region: "brazil",
    priority: 84,
  },

  {
    mode: "country",
    country: "Argentina",
    type: "league",
    names: ["Liga Profesional Argentina", "Primera División"],
    display: "Liga Argentina",
    region: "general",
    priority: 84,
  },
  {
    mode: "country",
    country: "Argentina",
    type: "cup",
    names: ["Copa Argentina"],
    display: "Copa Argentina",
    region: "general",
    priority: 78,
  },

  {
    mode: "country",
    country: "England",
    type: "league",
    names: ["Premier League"],
    display: "Premier League",
    region: "general",
    priority: 100,
  },
  {
    mode: "country",
    country: "England",
    type: "cup",
    names: ["FA Cup", "EFL Cup", "League Cup"],
    display: "England - Cup",
    region: "general",
    priority: 74,
  },
  {
    mode: "country",
    country: "Spain",
    type: "league",
    names: ["La Liga"],
    display: "La Liga",
    region: "general",
    priority: 98,
  },
  {
    mode: "country",
    country: "Spain",
    type: "cup",
    names: ["Copa del Rey"],
    display: "Copa del Rey",
    region: "general",
    priority: 72,
  },
  {
    mode: "country",
    country: "Italy",
    type: "league",
    names: ["Serie A"],
    display: "Serie A",
    region: "general",
    priority: 97,
  },
  {
    mode: "country",
    country: "Italy",
    type: "cup",
    names: ["Coppa Italia"],
    display: "Coppa Italia",
    region: "general",
    priority: 73,
  },
  {
    mode: "country",
    country: "Germany",
    type: "league",
    names: ["Bundesliga"],
    display: "Bundesliga",
    region: "general",
    priority: 96,
  },
  {
    mode: "country",
    country: "Germany",
    type: "cup",
    names: ["DFB Pokal", "DFB-Pokal"],
    display: "DFB-Pokal",
    region: "general",
    priority: 70,
  },
  {
    mode: "country",
    country: "France",
    type: "league",
    names: ["Ligue 1"],
    display: "Ligue 1",
    region: "general",
    priority: 95,
  },
  {
    mode: "country",
    country: "France",
    type: "cup",
    names: ["Coupe de France"],
    display: "Coupe de France",
    region: "general",
    priority: 71,
  },
  {
    mode: "country",
    country: "Netherlands",
    type: "league",
    names: ["Eredivisie"],
    display: "Eredivisie",
    region: "general",
    priority: 90,
  },
  {
    mode: "country",
    country: "Portugal",
    type: "league",
    names: ["Primeira Liga", "Liga Portugal Betclic"],
    display: "Primeira Liga",
    region: "general",
    priority: 89,
  },
  {
    mode: "country",
    country: "Turkey",
    type: "league",
    names: ["Süper Lig", "Super Lig"],
    display: "Super Lig",
    region: "general",
    priority: 78,
  },
  {
    mode: "country",
    country: "Denmark",
    type: "league",
    names: ["Superliga", "Superligaen"],
    display: "Superliga",
    region: "general",
    priority: 75,
  },
  {
    mode: "country",
    country: "Greece",
    type: "league",
    names: ["Super League 1", "Super League"],
    display: "Super League Greece",
    region: "general",
    priority: 74,
  },
  {
    mode: "country",
    country: "Belgium",
    type: "league",
    names: ["Pro League", "Jupiler Pro League"],
    display: "Belgian Pro League",
    region: "general",
    priority: 85,
  },
  {
    mode: "country",
    country: "Austria",
    type: "league",
    names: ["Bundesliga"],
    display: "Austrian Bundesliga",
    region: "general",
    priority: 84,
  },
  {
    mode: "search",
    search: "Saudi Pro League",
    display: "Saudi Pro League",
    region: "general",
    priority: 85,
  },

  {
    mode: "country",
    country: "USA",
    type: "league",
    names: ["Major League Soccer"],
    display: "MLS",
    region: "america",
    priority: 90,
  },
  {
    mode: "country",
    country: "Mexico",
    type: "league",
    names: ["Liga MX"],
    display: "Liga MX",
    region: "general",
    priority: 79,
  },
  {
    mode: "search",
    search: "CONCACAF Champions",
    display: "CONCACAF Champions Cup",
    region: "america",
    priority: 88,
  },

  {
    mode: "search",
    search: "UEFA Champions League",
    display: "UEFA Champions League",
    region: "general",
    priority: 98,
  },
  {
    mode: "search",
    search: "UEFA Europa League",
    display: "UEFA Europa League",
    region: "general",
    priority: 93,
  },
  {
    mode: "search",
    search: "UEFA Conference League",
    display: "UEFA Conference League",
    region: "general",
    priority: 88,
  },

  {
    mode: "search",
    search: "CONMEBOL Libertadores",
    display: "Libertadores",
    region: "brazil",
    priority: 92,
  },
  {
    mode: "search",
    search: "Copa Libertadores",
    display: "Libertadores",
    region: "brazil",
    priority: 92,
  },
  {
    mode: "search",
    search: "CONMEBOL Sudamericana",
    display: "Sul-Americana",
    region: "brazil",
    priority: 86,
  },
  {
    mode: "search",
    search: "Copa Sudamericana",
    display: "Sul-Americana",
    region: "brazil",
    priority: 86,
  },

  {
    mode: "search",
    search: "UEFA Nations League",
    display: "Nations League",
    region: "international",
    priority: 95,
  },
  {
    mode: "search",
    search: "International Friendlies",
    display: "Amistosos Internacionais",
    region: "international",
    priority: 90,
  },
  {
    mode: "search",
    search: "Friendlies",
    display: "Amistosos Internacionais",
    region: "international",
    priority: 88,
  },
  {
    mode: "search",
    search: "World Cup - Qualification Europe",
    display: "Eliminatórias Europeias",
    region: "international",
    priority: 94,
  },
  {
    mode: "search",
    search: "UEFA Euro Qualifiers",
    display: "Eliminatórias da Euro",
    region: "international",
    priority: 94,
  },
  {
    mode: "search",
    search: "CONMEBOL World Cup Qualifiers",
    display: "Eliminatórias Sul-Americanas",
    region: "international",
    priority: 96,
  },
  {
    mode: "search",
    search: "World Cup - Qualification South America",
    display: "Eliminatórias Sul-Americanas",
    region: "international",
    priority: 96,
  },
  {
    mode: "search",
    search: "World Cup - Qualification Africa",
    display: "Eliminatórias Africanas",
    region: "international",
    priority: 88,
  },
  {
    mode: "search",
    search: "World Cup - Qualification Asia",
    display: "Eliminatórias Asiáticas",
    region: "international",
    priority: 88,
  },
  {
    mode: "search",
    search: "World Cup - Qualification CONCACAF",
    display: "Eliminatórias CONCACAF",
    region: "international",
    priority: 88,
  },
  {
    mode: "search",
    search: "Copa America",
    display: "Copa América",
    region: "international",
    priority: 98,
  },
  {
    mode: "search",
    search: "UEFA European Championship",
    display: "Eurocopa",
    region: "international",
    priority: 98,
  },
  {
    mode: "search",
    search: "FIFA World Cup",
    display: "Copa do Mundo",
    region: "international",
    priority: 100,
  },
]

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

function clampNum(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function sum(arr) {
  return arr.reduce((acc, value) => acc + value, 0)
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

  if (apiCache.has(cacheKey)) {
    return apiCache.get(cacheKey)
  }

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

function isLikelyClubName(name = "") {
  const v = normalizeText(name)

  const clubMarkers = [
    "fc",
    "sc",
    "afc",
    "cf",
    "ac",
    "club",
    "united",
    "city",
    "rovers",
    "athletic",
    "atletico",
    "deportivo",
    "sporting",
    "jk",
    "fk",
    "bk",
    "if",
  ]

  return clubMarkers.some((marker) => v.includes(marker))
}

function isClubFriendlyFixture(fixture) {
  const homeName = fixture?.teams?.home?.name || ""
  const awayName = fixture?.teams?.away?.name || ""

  if (isLikelyClubName(homeName) || isLikelyClubName(awayName)) {
    return true
  }

  const homeNational = fixture?.teams?.home?.national === true
  const awayNational = fixture?.teams?.away?.national === true

  if (homeNational && awayNational) return false
  return false
}

function isInternationalCompetition(comp, fixture = null) {
  const region = normalizeText(comp?.region || "")
  const display = normalizeText(comp?.display || "")
  const leagueName = normalizeText(fixture?.league?.name || "")
  const country = normalizeText(comp?.country || fixture?.league?.country || "")

  return (
    region === "international" ||
    display.includes("amistosos internacionais") ||
    display.includes("nations league") ||
    display.includes("eliminatorias") ||
    display.includes("eurocopa") ||
    display.includes("copa america") ||
    display.includes("copa do mundo") ||
    leagueName.includes("friendlies") ||
    country === "world"
  )
}

function isLikelyNationalTeamMatch(fixture, comp) {
  if (!isInternationalCompetition(comp, fixture)) return false

  const homeNational = fixture?.teams?.home?.national === true
  const awayNational = fixture?.teams?.away?.national === true

  if (homeNational && awayNational) return true
  if (isClubFriendlyFixture(fixture)) return false

  return true
}

function normalizeCompetitionName(country, rawName, fallbackDisplay) {
  const name = String(rawName || "").trim()
  const c = String(country || "").trim()
  const norm = normalizeText(name)

  if (c === "Brazil" && name === "Serie A") return "Brasileirão Série A"
  if (c === "Brazil" && name === "Serie B") return "Brasileirão Série B"
  if (c === "Brazil" && norm.includes("copa do brasil")) return "Copa do Brasil"
  if (c === "Brazil" && norm.includes("nordeste")) return "Copa do Nordeste"
  if (c === "Brazil" && norm.includes("verde")) return "Copa Verde"
  if (c === "Brazil" && (norm.includes("sul-sudeste") || norm.includes("sul sudeste"))) {
    return "Copa Sul-Sudeste"
  }
  if (
    c === "Brazil" &&
    (norm.includes("women") || norm.includes("feminino") || norm.includes("feminina"))
  ) {
    return "Brasileirão Feminino"
  }

  if (c === "Argentina" && (name === "Liga Profesional Argentina" || name === "Primera División")) {
    return "Liga Argentina"
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
  if (c === "Saudi Arabia" && norm.includes("pro league")) return "Saudi Pro League"

  if (name === "UEFA Europa Conference League") return "UEFA Conference League"
  if (name === "CONMEBOL Libertadores") return "Libertadores"
  if (name === "CONMEBOL Sudamericana") return "Sul-Americana"
  if (norm.includes("nations league")) return "Nations League"
  if (norm.includes("friendlies")) return "Amistosos Internacionais"

  return fallbackDisplay || name || c || "Competição"
}

function isExactBrazilRegionalMatch(targetDisplay, country, rawName) {
  const norm = normalizeText(`${country || ""} ${rawName || ""}`)

  if (targetDisplay === "Copa do Nordeste") {
    return norm.includes("brazil") && norm.includes("nordeste")
  }

  if (targetDisplay === "Copa Verde") {
    return norm.includes("brazil") && norm.includes("verde")
  }

  if (targetDisplay === "Copa Sul-Sudeste") {
    return norm.includes("brazil") && (norm.includes("sul-sudeste") || norm.includes("sul sudeste"))
  }

  return true
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

      if (target.country === "Italy" && normalizeText(rawName).includes("women")) return false

      const rawKey = normalizeText(rawName)

      return Array.from(normalizedNames).some((n) => {
        return rawKey === n || rawKey.includes(n) || n.includes(rawKey)
      })
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
        const isBrazil = normalizeText(country) === "brazil"
        if (!isBrazil) return null
      }

      if (!isExactBrazilRegionalMatch(target.display, country, rawName)) {
        return null
      }

      if (target.display === "Amistosos Internacionais") {
        if (!haystack.includes("friend")) return null
      }

      if (target.display === "Nations League") {
        if (!haystack.includes("nations")) return null
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

function isCompletedFixture(fixture) {
  const short = String(fixture?.fixture?.status?.short || "").toUpperCase()
  return ["FT", "AET", "PEN"].includes(short)
}

async function getFixtureStatistics(fixtureId) {
  try {
    const res = await apiRequest("fixtures/statistics", {
      fixture: fixtureId
    })

    return res?.response || []
  } catch (err) {
    console.error("Erro ao buscar estatísticas do fixture", fixtureId, err.message)
    return []
  }
}

async function fetchRecentFinishedFixtures(teamId, limit = MAX_RECENT_FIXTURES_FETCH) {
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
      .filter((fixture) => isCompletedFixture(fixture))
      .sort(
        (a, b) =>
          new Date(b.fixture.date).getTime() - new Date(a.fixture.date).getTime()
      )

    teamRecentFixturesCache.set(cacheKey, cleaned)
    return cleaned
  } catch (error) {
    console.error(`Falha buscando histórico do time ${teamId}:`, error.message)
    teamRecentFixturesCache.set(cacheKey, [])
    return []
  }
}

function buildScoreLabelForTeam(fixture, teamId) {
  const isHome = fixture?.teams?.home?.id === teamId

  const goalsFor = isHome
    ? safeNumber(fixture?.goals?.home)
    : safeNumber(fixture?.goals?.away)

  const goalsAgainst = isHome
    ? safeNumber(fixture?.goals?.away)
    : safeNumber(fixture?.goals?.home)

  return `${goalsFor}-${goalsAgainst}`
}

function getGoalsForAgainst(fixture, teamId) {
  const isHome = fixture?.teams?.home?.id === teamId

  const gf = isHome
    ? safeNumber(fixture?.goals?.home)
    : safeNumber(fixture?.goals?.away)

  const ga = isHome
    ? safeNumber(fixture?.goals?.away)
    : safeNumber(fixture?.goals?.home)

  return { gf, ga, isHome }
}

function weightedAverage(rows, key, fallback = 0) {
  if (!rows.length) return fallback

  const weights = rows.map((_, index) => Math.max(1, rows.length - index))
  const totalWeight = sum(weights)
  if (!totalWeight) return fallback

  const weightedSum = rows.reduce((acc, row, index) => {
    return acc + safeNumber(row[key], 0) * weights[index]
  }, 0)

  return weightedSum / totalWeight
}

function splitVenueFixtures(fixtures, teamId, wantHome, limit = FORM_LIMIT_HOME_AWAY) {
  return fixtures
    .filter((fixture) => {
      const isHome = fixture?.teams?.home?.id === teamId
      return wantHome ? isHome : !isHome
    })
    .slice(0, limit)
}

async function collectProfileFromFixtures(teamId, fixturesSubset) {
  if (!fixturesSubset.length) {
    return {
      matches: 0,
      statsMatches: 0,
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
    const { gf, ga } = getGoalsForAgainst(fixture, teamId)
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

  const statsRows = rows.filter((row) => {
    return (
      row.shots > 0 ||
      row.shotsOnTarget > 0 ||
      row.corners > 0 ||
      row.fouls > 0 ||
      row.cards > 0
    )
  })

  return {
    matches: rows.length,
    statsMatches: statsRows.length,
    avgGoalsFor: round(weightedAverage(rows, "goalsFor")),
    avgGoalsAgainst: round(weightedAverage(rows, "goalsAgainst")),
    avgShots: round(weightedAverage(statsRows, "shots")),
    avgShotsOnTarget: round(weightedAverage(statsRows, "shotsOnTarget")),
    avgCorners: round(weightedAverage(statsRows, "corners")),
    avgCards: round(weightedAverage(statsRows, "cards")),
    avgFouls: round(weightedAverage(statsRows, "fouls")),
    recentScores: rows.map((row) => row.scoreLabel).slice(0, 5),
  }
}

async function buildTeamContext(teamId) {
  if (teamContextCache.has(teamId)) {
    return teamContextCache.get(teamId)
  }

  const allFixtures = await fetchRecentFinishedFixtures(teamId, MAX_RECENT_FIXTURES_FETCH)

  const generalFixtures = allFixtures.slice(0, FORM_LIMIT_GENERAL)
  const homeFixtures = splitVenueFixtures(allFixtures, teamId, true, FORM_LIMIT_HOME_AWAY)
  const awayFixtures = splitVenueFixtures(allFixtures, teamId, false, FORM_LIMIT_HOME_AWAY)

  const general = await collectProfileFromFixtures(teamId, generalFixtures)
  const home = await collectProfileFromFixtures(teamId, homeFixtures)
  const away = await collectProfileFromFixtures(teamId, awayFixtures)

  const payload = { general, home, away }
  teamContextCache.set(teamId, payload)
  return payload
}

function blendValue(primary, fallback, primaryWeight = 0.68) {
  const p = safeNumber(primary, 0)
  const f = safeNumber(fallback, 0)
  return p * primaryWeight + f * (1 - primaryWeight)
}

function buildSideProfile(teamContext, side) {
  const sideProfile = side === "home" ? teamContext.home : teamContext.away
  const general = teamContext.general

  const hasSideData = sideProfile.matches >= 2
  const hasSideStats = sideProfile.statsMatches >= 2

  return {
    matches: sideProfile.matches,
    statsMatches: sideProfile.statsMatches,
    avgGoalsFor: round(
      hasSideData
        ? blendValue(sideProfile.avgGoalsFor, general.avgGoalsFor, 0.70)
        : general.avgGoalsFor
    ),
    avgGoalsAgainst: round(
      hasSideData
        ? blendValue(sideProfile.avgGoalsAgainst, general.avgGoalsAgainst, 0.70)
        : general.avgGoalsAgainst
    ),
    avgShots: round(
      hasSideStats
        ? blendValue(sideProfile.avgShots, general.avgShots, 0.72)
        : general.avgShots
    ),
    avgShotsOnTarget: round(
      hasSideStats
        ? blendValue(sideProfile.avgShotsOnTarget, general.avgShotsOnTarget, 0.72)
        : general.avgShotsOnTarget
    ),
    avgCorners: round(
      hasSideStats
        ? blendValue(sideProfile.avgCorners, general.avgCorners, 0.72)
        : general.avgCorners
    ),
    avgCards: round(
      hasSideStats
        ? blendValue(sideProfile.avgCards, general.avgCards, 0.66)
        : general.avgCards
    ),
    avgFouls: round(
      hasSideStats
        ? blendValue(sideProfile.avgFouls, general.avgFouls, 0.66)
        : general.avgFouls
    ),
    recentScores: sideProfile.recentScores?.length
      ? sideProfile.recentScores
      : general.recentScores || [],
  }
}

function isUsableTeamProfile(sideProfile, generalProfile) {
  const recentOk = safeNumber(generalProfile.matches, 0) >= MIN_REQUIRED_RECENT_MATCHES
  const statsOk = safeNumber(generalProfile.statsMatches, 0) >= MIN_REQUIRED_STATS_MATCHES

  return recentOk && statsOk
}

function buildExpectedMetrics(homeProfile, awayProfile) {
  const expectedHomeGoals = clamp(
    round(homeProfile.avgGoalsFor * 0.60 + awayProfile.avgGoalsAgainst * 0.40),
    0.25,
    3.8
  )

  const expectedAwayGoals = clamp(
    round(awayProfile.avgGoalsFor * 0.56 + homeProfile.avgGoalsAgainst * 0.44),
    0.20,
    3.4
  )

  const expectedGoals = round(expectedHomeGoals + expectedAwayGoals)

  const expectedHomeShots = clamp(
    round(homeProfile.avgShots * 0.68 + expectedHomeGoals * 2.8),
    4,
    24
  )

  const expectedAwayShots = clamp(
    round(awayProfile.avgShots * 0.64 + expectedAwayGoals * 2.5),
    4,
    22
  )

  const expectedHomeSOT = clamp(
    round(homeProfile.avgShotsOnTarget * 0.70 + expectedHomeGoals * 0.95),
    1,
    9
  )

  const expectedAwaySOT = clamp(
    round(awayProfile.avgShotsOnTarget * 0.67 + expectedAwayGoals * 0.90),
    1,
    8
  )

  const expectedShots = clamp(round(expectedHomeShots + expectedAwayShots), 8, 42)
  const expectedSOT = clamp(round(expectedHomeSOT + expectedAwaySOT), 2, 16)

  const pressureFactor =
    expectedShots >= 24
      ? 0.45
      : expectedShots >= 20
        ? 0.25
        : 0.10

  const expectedCorners = clamp(
    round(
      homeProfile.avgCorners * 0.50 +
      awayProfile.avgCorners * 0.46 +
      expectedShots * 0.050 +
      expectedSOT * 0.045 +
      pressureFactor
    ),
    4.5,
    13.2
  )

  const expectedCards = clamp(
    round(
      homeProfile.avgCards * 0.50 +
      awayProfile.avgCards * 0.50 +
      (homeProfile.avgFouls + awayProfile.avgFouls) * 0.025
    ),
    1.2,
    7.0
  )

  const expectedFouls = clamp(
    round(homeProfile.avgFouls * 0.52 + awayProfile.avgFouls * 0.48),
    8,
    30
  )

  return {
    expectedGoals,
    expectedHomeGoals,
    expectedAwayGoals,
    expectedHomeShots,
    expectedAwayShots,
    expectedHomeSOT,
    expectedAwaySOT,
    expectedShots,
    expectedSOT,
    expectedCorners,
    expectedCards,
    expectedFouls,
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

  if (total < 0.999) {
    arr.push(1 - total)
  }

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

  const cornersProb = clamp((metrics.expectedCorners - 6.8) / 4.0, 0.08, 0.93)
  const shotsProb = clamp((metrics.expectedShots - 15) / 17, 0.08, 0.93)
  const sotProb = clamp((metrics.expectedSOT - 4.5) / 7.5, 0.08, 0.93)
  const cardsProb = clamp((metrics.expectedCards - 2.2) / 3.8, 0.08, 0.93)

  return {
    home: round(clamp(homeWin, 0.05, 0.88)),
    draw: round(clamp(draw, 0.06, 0.42)),
    away: round(clamp(awayWin, 0.05, 0.88)),
    over15: round(clamp(over15, 0.10, 0.97)),
    over25: round(clamp(over25, 0.08, 0.94)),
    btts: round(clamp(btts, 0.08, 0.92)),
    under35: round(clamp(under35, 0.12, 0.97)),
    corners: round(cornersProb),
    shots: round(shotsProb),
    sot: round(sotProb),
    cards: round(cardsProb),
  }
}

function buildMarkets(metrics, probs) {
  return {
    over15: probs.over15,
    over25: probs.over25,
    btts: probs.btts,
    under35: probs.under35,
    corners: round(metrics.expectedCorners),
    cards: round(metrics.expectedCards),
    shots: round(metrics.expectedShots),
    shots_on_target: round(metrics.expectedSOT),
    fouls: round(metrics.expectedFouls),
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
  if (market === "Menos de 2.5 gols") score += 0.01
  if (family === "dupla_chance") score += 0.015
  if (family === "resultado") score += 0.008
  if (market === "Empate") score -= 0.10
  if (score > 0.90) score -= 0.04

  return round(score)
}

function detectMarketFamily(market = "") {
  const m = normalizeText(market)
  if (m.includes("escanteio")) return "escanteios"
  if (m.includes("finalizacoes") || m.includes("finalizações")) return "shots"
  if (m.includes("no gol")) return "sot"
  if (m.includes("cart")) return "cards"
  if (m.includes("ambas")) return "ambas"
  if (m.includes("dupla chance")) return "dupla_chance"
  if (m.includes("vitoria") || m.includes("vitória") || m.includes("empate")) return "resultado"
  if (m.includes("gol")) return "gols"
  return "outro"
}

function detectDirection(market = "") {
  const m = normalizeText(market)
  if (m.includes("mais de")) return "over"
  if (m.includes("menos de")) return "under"
  return null
}

function extractLine(market = "") {
  const m = String(market || "").replace(",", ".")
  const match = m.match(/(\d+(\.\d+)?)/)
  return match ? Number(match[1]) : null
}

function buildCornerCandidates(metrics) {
  const candidates = []

  function add(market, probability) {
    if (!market) return
    candidates.push({
      market,
      probability: round(probability),
      score: lineScore(probability, "escanteios", market),
      family: "escanteios",
    })
  }

  const c = safeNumber(metrics.expectedCorners, 0)
  const shots = safeNumber(metrics.expectedShots, 0)

  if (c >= 6.5) {
    add("Mais de 6.5 escanteios", clamp((c - 5.5) / 2.0 + (shots >= 20 ? 0.02 : 0), 0.60, 0.90))
  }

  if (c >= 7.4) {
    add("Mais de 7.5 escanteios", clamp((c - 6.2) / 2.1 + (shots >= 22 ? 0.02 : 0), 0.58, 0.87))
  }

  if (c >= 8.6) {
    add("Mais de 8.5 escanteios", clamp((c - 7.1) / 2.3 + (shots >= 24 ? 0.02 : 0), 0.55, 0.83))
  }

  if (c <= 9.0) {
    add("Menos de 12.5 escanteios", clamp((13.2 - c) / 3.8, 0.63, 0.91))
  }

  if (c <= 8.2) {
    add("Menos de 11.5 escanteios", clamp((12.0 - c) / 3.3, 0.60, 0.88))
  }

  if (c <= 7.2) {
    add("Menos de 10.5 escanteios", clamp((10.8 - c) / 3.0, 0.56, 0.84))
  }

  return candidates.sort((a, b) => b.score - a.score)
}

function buildShotsCandidates(metrics, probs) {
  const candidates = []

  function add(market, probability) {
    if (!market) return
    candidates.push({
      market,
      probability: round(probability),
      score: lineScore(probability, "shots", market),
      family: "shots",
    })
  }

  const shots = safeNumber(metrics.expectedShots, 0)
  const base = safeNumber(probs.shots, 0)

  if (shots >= 18.5) {
    add("Mais de 17.5 finalizações", clamp(Math.max(base, 0.61) + (shots - 18.5) * 0.02, 0.61, 0.90))
  }

  if (shots >= 20.0) {
    add("Mais de 19.5 finalizações", clamp(Math.max(base - 0.01, 0.58) + (shots - 20.0) * 0.02, 0.58, 0.87))
  }

  if (shots >= 22.0) {
    add("Mais de 21.5 finalizações", clamp(Math.max(base - 0.03, 0.55) + (shots - 22.0) * 0.02, 0.55, 0.83))
  }

  if (shots <= 28.0) {
    add("Menos de 29.5 finalizações", clamp(0.62 + (28.0 - shots) * 0.018, 0.62, 0.89))
  }

  if (shots <= 25.0) {
    add("Menos de 26.5 finalizações", clamp(0.58 + (25.0 - shots) * 0.018, 0.58, 0.85))
  }

  return candidates.sort((a, b) => b.score - a.score)
}

function buildSOTCandidates(metrics, probs) {
  const candidates = []

  function add(market, probability) {
    if (!market) return
    candidates.push({
      market,
      probability: round(probability),
      score: lineScore(probability, "sot", market),
      family: "sot",
    })
  }

  const sot = safeNumber(metrics.expectedSOT, 0)
  const base = safeNumber(probs.sot, 0)

  if (sot >= 5.8) {
    add("Mais de 5.5 finalizações no gol", clamp(Math.max(base, 0.61) + (sot - 5.8) * 0.03, 0.61, 0.91))
  }

  if (sot >= 6.8) {
    add("Mais de 6.5 finalizações no gol", clamp(Math.max(base - 0.01, 0.58) + (sot - 6.8) * 0.03, 0.58, 0.88))
  }

  if (sot >= 7.8) {
    add("Mais de 7.5 finalizações no gol", clamp(Math.max(base - 0.03, 0.55) + (sot - 7.8) * 0.03, 0.55, 0.84))
  }

  if (sot <= 10.0) {
    add("Menos de 10.5 finalizações no gol", clamp(0.61 + (10.0 - sot) * 0.022, 0.61, 0.89))
  }

  if (sot <= 8.8) {
    add("Menos de 9.5 finalizações no gol", clamp(0.58 + (8.8 - sot) * 0.022, 0.58, 0.85))
  }

  return candidates.sort((a, b) => b.score - a.score)
}

function buildCardsCandidates(metrics, probs) {
  const candidates = []

  function add(market, probability) {
    if (!market) return
    candidates.push({
      market,
      probability: round(probability),
      score: lineScore(probability, "cards", market),
      family: "cards",
    })
  }

  const cards = safeNumber(metrics.expectedCards, 0)
  const base = safeNumber(probs.cards, 0)

  if (cards >= 2.8) {
    add("Mais de 2.5 cartões", clamp(Math.max(base, 0.60) + (cards - 2.8) * 0.03, 0.60, 0.90))
  }

  if (cards >= 3.6) {
    add("Mais de 3.5 cartões", clamp(Math.max(base - 0.01, 0.57) + (cards - 3.6) * 0.03, 0.57, 0.87))
  }

  if (cards >= 4.6) {
    add("Mais de 4.5 cartões", clamp(Math.max(base - 0.03, 0.54) + (cards - 4.6) * 0.03, 0.54, 0.83))
  }

  if (cards <= 5.8) {
    add("Menos de 6.5 cartões", clamp(0.62 + (5.8 - cards) * 0.02, 0.62, 0.88))
  }

  if (cards <= 4.8) {
    add("Menos de 5.5 cartões", clamp(0.58 + (4.8 - cards) * 0.02, 0.58, 0.84))
  }

  return candidates.sort((a, b) => b.score - a.score)
}

function buildCandidateMarkets(payload) {
  const { homeTeam, awayTeam, metrics, probabilities, isNationalTeamsGame } = payload
  const candidates = []

  function add(market, probability, family) {
    if (!market) return
    candidates.push({
      market,
      probability: round(probability),
      score: lineScore(probability, family, market),
      family,
    })
  }

  let homeProb = safeNumber(probabilities.home, 0)
  let drawProb = safeNumber(probabilities.draw, 0)
  let awayProb = safeNumber(probabilities.away, 0)

  if (isNationalTeamsGame) {
    homeProb = Math.min(homeProb, NATIONAL_TEAM_WIN_CAP)
    awayProb = Math.min(awayProb, NATIONAL_TEAM_WIN_CAP)
  }

  const homeOrDraw = clamp(homeProb + drawProb, 0, 1)
  const awayOrDraw = clamp(awayProb + drawProb, 0, 1)
  const under25 = clamp(1 - probabilities.over25, 0, 1)
  const bttsNo = clamp(1 - probabilities.btts, 0, 1)

  if (probabilities.over25 >= 0.66) add("Mais de 2.5 gols", probabilities.over25, "gols")
  if (probabilities.over15 >= 0.76) add("Mais de 1.5 gols", probabilities.over15, "gols")
  if (under25 >= 0.74) add("Menos de 2.5 gols", under25, "gols")
  if (probabilities.under35 >= 0.80) add("Menos de 3.5 gols", probabilities.under35, "gols")
  if (probabilities.btts >= 0.63) add("Ambas marcam", probabilities.btts, "ambas")
  if (bttsNo >= 0.72) add("Ambas não marcam", bttsNo, "ambas")

  buildShotsCandidates(metrics, probabilities).forEach((item) => candidates.push(item))
  buildSOTCandidates(metrics, probabilities).forEach((item) => candidates.push(item))
  buildCardsCandidates(metrics, probabilities).forEach((item) => candidates.push(item))
  buildCornerCandidates(metrics).forEach((item) => candidates.push(item))

  const mismatch = Math.abs(homeProb - awayProb)

  if (homeProb >= 0.62) {
    add("Vitória do mandante", homeProb, "resultado")
  }

  if (awayProb >= 0.62) {
    add("Vitória do visitante", awayProb, "resultado")
  }

  if (homeOrDraw >= 0.74) {
    const allowed =
      !isNationalTeamsGame ||
      mismatch <= STRONG_MISMATCH_DOUBLE_CHANCE_BLOCK ||
      homeProb >= awayProb

    if (allowed) {
      add(
        `Dupla chance ${homeTeam} ou empate`,
        isNationalTeamsGame ? Math.min(homeOrDraw, NATIONAL_TEAM_DOUBLE_CHANCE_CAP) : homeOrDraw,
        "dupla_chance"
      )
    }
  }

  if (awayOrDraw >= 0.74) {
    const allowed =
      !isNationalTeamsGame ||
      mismatch <= STRONG_MISMATCH_DOUBLE_CHANCE_BLOCK ||
      awayProb >= homeProb

    if (allowed) {
      add(
        `Dupla chance ${awayTeam} ou empate`,
        isNationalTeamsGame ? Math.min(awayOrDraw, NATIONAL_TEAM_DOUBLE_CHANCE_CAP) : awayOrDraw,
        "dupla_chance"
      )
    }
  }

  const profile = buildGameProfile(metrics, probabilities)

  candidates.forEach((item) => {
    if (profile === "volume" && item.family === "shots") {
      item.score = round(item.score + 0.03)
    }

    if (profile === "precisao" && item.family === "sot") {
      item.score = round(item.score + 0.03)
    }

    if (profile === "disciplinar" && item.family === "cards") {
      item.score = round(item.score + 0.03)
    }

    if ((profile === "volume" || profile === "precisao") && item.family === "escanteios") {
      item.score = round(item.score - 0.02)
    }
  })

  return candidates.sort((a, b) => b.score - a.score)
}

function chooseMainPick(candidates) {
  if (!candidates.length) {
    return {
      market: "Menos de 3.5 gols",
      probability: 0.6,
      score: 0.6,
      family: "gols",
    }
  }

  const sorted = [...candidates].sort((a, b) => b.score - a.score)
  return sorted[0]
}

function chooseExtraPicks(candidates, mainPick) {
  const filtered = candidates.filter((item) => item.market !== mainPick.market)

  const picked = []
  const usedMarketKeys = new Set([normalizeText(mainPick.market)])

  const mainFamily = detectMarketFamily(mainPick.market)
  const mainDirection = detectDirection(mainPick.market)
  const mainLine = extractLine(mainPick.market)

  for (const item of filtered) {
    const marketKey = normalizeText(item.market)
    if (usedMarketKeys.has(marketKey)) continue

    const family = detectMarketFamily(item.market)

    if (family === mainFamily && !["escanteios", "shots", "sot", "cards"].includes(family)) {
      continue
    }

    if (["escanteios", "shots", "sot", "cards"].includes(family) && family === mainFamily) {
      const direction = detectDirection(item.market)
      const line = extractLine(item.market)

      if (!direction || line === null || !mainDirection || mainLine === null) {
        continue
      }

      if (direction !== mainDirection) continue
      if (direction === "under" && line >= mainLine) continue
      if (direction === "over" && line <= mainLine) continue
    }

    picked.push(item)
    usedMarketKeys.add(marketKey)

    if (picked.length === 2) break
  }

  return picked
}

function normalizeLeagueByTeams(comp, fixture) {
  let leagueDisplay = comp.display
  let country = comp.country || fixture?.league?.country || null

  const leagueNameRaw = fixture?.league?.name || ""
  const leagueId = fixture?.league?.id || comp?.leagueId || null
  const teams = `${fixture?.teams?.home?.name || ""} ${fixture?.teams?.away?.name || ""}`
  const normLeague = normalizeText(leagueNameRaw)
  const normCountry = normalizeText(country)

  if (leagueId === 218) {
    leagueDisplay = "Austrian Bundesliga"
    country = "Austria"
  }

  if (leagueId === 203) {
    leagueDisplay = "Super Lig"
    country = "Turkey"
  }

  if (normLeague.includes("nordeste")) {
    leagueDisplay = "Copa do Nordeste"
    country = "Brazil"
  }

  if (normLeague.includes("verde")) {
    leagueDisplay = "Copa Verde"
    country = "Brazil"
  }

  if (normLeague.includes("sul-sudeste") || normLeague.includes("sul sudeste")) {
    leagueDisplay = "Copa Sul-Sudeste"
    country = "Brazil"
  }

  if (
    (normLeague.includes("women") || normLeague.includes("feminino") || normLeague.includes("feminina")) &&
    normCountry === "brazil"
  ) {
    leagueDisplay = "Brasileirão Feminino"
  }

  if (
    normalizeText(teams).includes("fluminense w") ||
    normalizeText(teams).includes("corinthians w") ||
    normalizeText(teams).includes("palmeiras w")
  ) {
    if (normCountry === "brazil") {
      leagueDisplay = "Brasileirão Feminino"
    }
  }

  return { leagueDisplay, country }
}

function buildGameProfile(metrics, probabilities) {
  if (metrics.expectedGoals >= 2.9 || probabilities.over25 >= 0.66) return "ofensivo"
  if (metrics.expectedCorners >= 9.3 && metrics.expectedShots >= 22) return "estatistico"
  if (metrics.expectedShots >= 22 && metrics.expectedSOT >= 7 && metrics.expectedCorners < 9.2) return "volume"
  if (metrics.expectedSOT >= 7 && metrics.expectedGoals >= 2.2 && metrics.expectedShots <= 23) return "precisao"
  if (metrics.expectedCards >= 4.2 && metrics.expectedGoals <= 2.8) return "disciplinar"
  if (metrics.expectedGoals <= 2.0 && probabilities.under35 >= 0.8) return "controlado"
  if (metrics.expectedGoals <= 1.7 && metrics.expectedShots <= 17) return "defensivo"
  return "equilibrado"
}

function buildInsight(mainPick, metrics, profile) {
  const goals = round(metrics.expectedGoals, 1)
  const corners = round(metrics.expectedCorners, 1)
  const shots = Math.round(metrics.expectedShots)
  const sot = Math.round(metrics.expectedSOT)
  const cards = round(metrics.expectedCards, 1)

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

  if (normalizeText(mainPick).includes("escanteios")) {
    if (normalizeText(mainPick).includes("mais de")) {
      return `A leitura Scoutly projeta cerca de ${corners} escanteios, com contexto de jogo favorável para uma linha de cantos mais alta sem forçar uma projeção exagerada.`
    }

    if (normalizeText(mainPick).includes("menos de")) {
      return `A leitura Scoutly projeta cerca de ${corners} escanteios, indicando um cenário mais controlado para o mercado de cantos, sem necessidade de esticar demais a linha.`
    }

    return `A leitura Scoutly projeta cerca de ${corners} escanteios, com suporte estatístico suficiente para transformar esse mercado em uma oportunidade relevante.`
  }

  if (normalizeText(mainPick).includes("finalizações") && !normalizeText(mainPick).includes("no gol")) {
    return `A leitura Scoutly projeta cerca de ${shots} finalizações totais, sugerindo um cenário de volume ofensivo consistente para transformar produção em oportunidade real de mercado.`
  }

  if (normalizeText(mainPick).includes("no gol")) {
    return `A leitura Scoutly projeta cerca de ${sot} finalizações no gol, indicando um cenário de boa produção ofensiva com precisão suficiente para sustentar esse mercado.`
  }

  if (normalizeText(mainPick).includes("cart")) {
    return `A leitura Scoutly projeta cerca de ${cards} cartões, sugerindo um confronto com nível de contato e tensão suficiente para transformar disciplina em oportunidade de mercado.`
  }

  if (normalizeText(mainPick).includes("dupla chance")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate. O equilíbrio da partida ainda pede segurança, e por isso a dupla chance aparece como leitura mais sólida.`
  }

  if (mainPick === "Ambas não marcam") {
    return `A leitura Scoutly vê um confronto com menor troca ofensiva entre os lados, sustentando o cenário de uma equipe passar em branco.`
  }

  if (mainPick === "Ambas marcam") {
    return `A leitura Scoutly identifica espaço para gols dos dois lados, combinando projeção ofensiva e comportamento recente das equipes.`
  }

  return `A leitura Scoutly classifica este confronto como ${profile}, cruzando projeção de gols, escanteios, finalizações, no gol e cartões para destacar a melhor oportunidade.`
}

function isInternationalNationalTeamsFixture(fixture, comp) {
  if (!isInternationalCompetition(comp, fixture)) return true

  if (
    normalizeText(comp?.display || "").includes("amistosos internacionais") ||
    normalizeText(fixture?.league?.name || "").includes("friend")
  ) {
    if (isClubFriendlyFixture(fixture)) return false
  }

  return true
}

function hasMinimumMatchData(homeContext, awayContext) {
  const homeOk = isUsableTeamProfile(
    buildSideProfile(homeContext, "home"),
    homeContext.general
  )

  const awayOk = isUsableTeamProfile(
    buildSideProfile(awayContext, "away"),
    awayContext.general
  )

  return homeOk && awayOk
}

// ===============================
// PIPELINE PRINCIPAL
// ===============================

async function processFixture(fixture, comp) {
  try {
    const homeTeam = fixture.teams.home.name
    const awayTeam = fixture.teams.away.name

    const homeId = fixture.teams.home.id
    const awayId = fixture.teams.away.id

    const homeContext = await buildTeamContext(homeId)
    const awayContext = await buildTeamContext(awayId)

    if (!hasMinimumMatchData(homeContext, awayContext)) {
      return null
    }

    const homeProfile = buildSideProfile(homeContext, "home")
    const awayProfile = buildSideProfile(awayContext, "away")

    const metrics = buildExpectedMetrics(homeProfile, awayProfile)
    const probabilities = buildProbabilities(metrics)

    const isNationalTeamsGame = isInternationalNationalTeamsFixture(fixture, comp)

    const payload = {
      homeTeam,
      awayTeam,
      metrics,
      probabilities,
      isNationalTeamsGame,
    }

    const candidates = buildCandidateMarkets(payload)

    if (!candidates.length) return null

    const mainPick = chooseMainPick(candidates)
    const extraPicks = chooseExtraPicks(candidates, mainPick)

    const profile = buildGameProfile(metrics, probabilities)

    const insight = buildInsight(mainPick.market, metrics, profile)

    const normalizedLeague = normalizeLeagueByTeams(comp, fixture)

    return {
      fixtureId: fixture.fixture.id,
      date: fixture.fixture.date,
      timestamp: new Date(fixture.fixture.date).getTime(),

      homeTeam,
      awayTeam,

      league: normalizedLeague.leagueDisplay,
      country: normalizedLeague.country,

      metrics: buildMetrics(metrics),
      probabilities,
      markets: buildMarkets(metrics, probabilities),

      best_pick: mainPick.market,
      best_pick_probability: mainPick.probability,
      best_pick_score: mainPick.score,

      best_pick_2: extraPicks[0]?.market || null,
      best_pick_2_probability: extraPicks[0]?.probability || null,

      best_pick_3: extraPicks[1]?.market || null,
      best_pick_3_probability: extraPicks[1]?.probability || null,

      confidence_score: round(mainPick.score),

      game_profile: profile,
      insight,

      created_at: new Date().toISOString(),
    }
  } catch (err) {
    console.error("Erro processando fixture:", err.message)
    return null
  }
}

// ===============================
// UPSERT NO BANCO
// ===============================

async function saveMatchData(row) {
  if (!row) return

  try {
    // MATCHES
    await supabase.from("matches").upsert({
      fixture_id: row.fixtureId,
      date: row.date,
      timestamp: row.timestamp,
      home_team: row.homeTeam,
      away_team: row.awayTeam,
      league: row.league,
      country: row.country,
      created_at: row.created_at,
    })

    // MATCH STATS
    await supabase.from("match_stats").upsert({
      fixture_id: row.fixtureId,
      goals: row.metrics.goals,
      corners: row.metrics.corners,
      shots: row.metrics.shots,
      shots_on_target: row.metrics.shots_on_target,
      cards: row.metrics.cards,
      fouls: row.metrics.fouls,
    })

    // MATCH ANALYSIS
    await supabase.from("match_analysis").upsert({
      fixture_id: row.fixtureId,

      expected_home_goals: row.metrics.goals / 2,
      expected_away_goals: row.metrics.goals / 2,

      expected_home_shots: row.metrics.shots / 2,
      expected_away_shots: row.metrics.shots / 2,

      expected_home_sot: row.metrics.shots_on_target / 2,
      expected_away_sot: row.metrics.shots_on_target / 2,

      expected_corners: row.metrics.corners,
      expected_cards: row.metrics.cards,
      expected_fouls: row.metrics.fouls,

      prob_home: row.probabilities.home,
      prob_draw: row.probabilities.draw,
      prob_away: row.probabilities.away,

      prob_over15: row.probabilities.over15,
      prob_over25: row.probabilities.over25,
      prob_btts: row.probabilities.btts,
      prob_under35: row.probabilities.under35,

      prob_corners: row.probabilities.corners,
      prob_shots: row.probabilities.shots,
      prob_sot: row.probabilities.sot,
      prob_cards: row.probabilities.cards,

      best_pick: row.best_pick,
      best_pick_probability: row.best_pick_probability,
      best_pick_score: row.best_pick_score,

      best_pick_2: row.best_pick_2,
      best_pick_2_probability: row.best_pick_2_probability,

      best_pick_3: row.best_pick_3,
      best_pick_3_probability: row.best_pick_3_probability,

      confidence_score: row.confidence_score,
      game_profile: row.game_profile,
      insight: row.insight,

      created_at: row.created_at,
    })
  } catch (error) {
    console.error("Erro ao salvar no banco:", error.message)
  }
}

// ===============================
// DAILY PICKS (MELHORADO)
// ===============================

function rebuildDailyPicks(rows) {
  if (!rows.length) return []

  const sorted = [...rows].sort((a, b) => b.confidence_score - a.confidence_score)

  const picks = []
  const usedMatches = new Set()
  const usedFamilies = new Set()

  for (const row of sorted) {
    if (usedMatches.has(row.fixtureId)) continue

    const family = detectMarketFamily(row.best_pick)

    // evita repetir só escanteios
    if (family === "escanteios" && usedFamilies.has("escanteios")) continue

    picks.push({
      fixture_id: row.fixtureId,
      match: `${row.homeTeam} x ${row.awayTeam}`,
      league: row.league,
      market: row.best_pick,
      probability: row.best_pick_probability,
      confidence: row.confidence_score,
    })

    usedMatches.add(row.fixtureId)
    usedFamilies.add(family)

    if (picks.length >= 10) break
  }

  return picks
}

async function saveDailyPicks(picks) {
  try {
    await supabase.from("daily_picks").delete().neq("fixture_id", 0)

    if (!picks.length) return

    await supabase.from("daily_picks").insert(picks)
  } catch (error) {
    console.error("Erro ao salvar daily picks:", error.message)
  }
}

// ===============================
// EXECUÇÃO PRINCIPAL
// ===============================

function getTodayDate() {
  const now = new Date()

  const year = now.getFullYear()
  const month = String(now.getMonth() + 1).padStart(2, "0")
  const day = String(now.getDate()).padStart(2, "0")

  return `${year}-${month}-${day}`
}

function isValidFixture(fixture) {
  if (!fixture) return false

  const fixtureId = fixture?.fixture?.id
  const fixtureDate = fixture?.fixture?.date
  const statusShort = String(fixture?.fixture?.status?.short || "").toUpperCase()

  const homeId = fixture?.teams?.home?.id
  const awayId = fixture?.teams?.away?.id
  const homeName = String(fixture?.teams?.home?.name || "").trim()
  const awayName = String(fixture?.teams?.away?.name || "").trim()

  const leagueName = String(fixture?.league?.name || "").trim()

  if (!fixtureId) return false
  if (!fixtureDate) return false
  if (!homeId || !awayId) return false
  if (!homeName || !awayName) return false
  if (!leagueName) return false

  if (homeId === awayId) return false
  if (homeName.toLowerCase() === awayName.toLowerCase()) return false

  if (hasForbiddenMarker(homeName)) return false
  if (hasForbiddenMarker(awayName)) return false
  if (hasForbiddenMarker(leagueName)) return false
  if (normalizeText(leagueName).includes("open cup")) return false

  // ignora jogos já encerrados/cancelados
  if (["FT", "AET", "PEN", "CANC", "PST", "ABD", "AWD", "WO"].includes(statusShort)) {
    return false
  }

  return true
}

function resolveCompetition(fixture) {
  if (!fixture) return null

  const leagueId = fixture?.league?.id || null
  const rawLeagueName = String(fixture?.league?.name || "").trim()
  const country = String(fixture?.league?.country || "").trim() || null

  if (!rawLeagueName) return null

  const normalizedDisplay = normalizeCompetitionName(
    country,
    rawLeagueName,
    rawLeagueName
  )

  const normLeague = normalizeText(rawLeagueName)
  const normCountry = normalizeText(country)

  let region = "general"

  if (
    normCountry === "brazil" ||
    normLeague.includes("brasileir") ||
    normLeague.includes("copa do brasil") ||
    normLeague.includes("nordeste") ||
    normLeague.includes("verde") ||
    normLeague.includes("sul-americana") ||
    normLeague.includes("sudamericana") ||
    normLeague.includes("libertadores")
  ) {
    region = "brazil"
  } else if (
    normCountry === "usa" ||
    normCountry === "mexico" ||
    normLeague.includes("mls") ||
    normLeague.includes("liga mx") ||
    normLeague.includes("concacaf")
  ) {
    region = "america"
  } else if (
    normCountry === "world" ||
    normLeague.includes("friendlies") ||
    normLeague.includes("nations league") ||
    normLeague.includes("qualification") ||
    normLeague.includes("euro") ||
    normLeague.includes("copa america") ||
    normLeague.includes("world cup")
  ) {
    region = "international"
  }

  return {
    leagueId,
    country,
    rawName: rawLeagueName,
    display: normalizedDisplay,
    region,
    priority: 70,
  }
}

async function runSync() {
  console.log("🚀 Scoutly Sync iniciado...")

  const fixtures = await api("/fixtures", {
    date: getTodayDate(),
    timezone: TIMEZONE,
  })

  const validFixtures = fixtures.filter((fixture) =>
    isValidFixture(fixture)
  )

  console.log(`📊 ${validFixtures.length} jogos encontrados`)

  const results = []

  for (const fixture of validFixtures) {
    const comp = resolveCompetition(fixture)

    if (!comp) continue

    const row = await processFixture(fixture, comp)

    if (!row) continue

    await saveMatchData(row)
    results.push(row)
  }

  const daily = rebuildDailyPicks(results)
  await saveDailyPicks(daily)

  console.log("✅ Sync finalizado")
}

// ===============================
// START
// ===============================

runSync()
