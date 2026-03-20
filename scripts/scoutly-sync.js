const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY || ""
const SUPABASE_URL = process.env.SUPABASE_URL || ""
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY || ""

if (!APISPORTS_KEY) throw new Error("APISPORTS_KEY não encontrada.")
if (!SUPABASE_URL) throw new Error("SUPABASE_URL não encontrada.")
if (!SUPABASE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY não encontrada.")

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"
const WINDOW_HOURS = 72
const WINDOW_MODE = "UNTIL_SUNDAY"
const REQUEST_DELAY_MS = 1200

const TARGET_COMPETITIONS = [
  // Inglaterra
  { mode: "country", country: "England", type: "league", names: ["Premier League"], display: "Premier League", region: "general", priority: 100 },
  { mode: "country", country: "England", type: "cup", names: ["FA Cup", "EFL Cup"], display: "England - Cup", region: "general", priority: 74 },

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

  // Argentina
  { mode: "country", country: "Argentina", type: "league", names: ["Liga Profesional Argentina", "Primera División"], display: "Liga Profesional Argentina", region: "general", priority: 84 },
  { mode: "country", country: "Argentina", type: "cup", names: ["Copa Argentina"], display: "Copa Argentina", region: "general", priority: 70 },

  // México
  { mode: "country", country: "Mexico", type: "league", names: ["Liga MX"], display: "Liga MX", region: "general", priority: 79 },

  // Turquia / Grécia / Dinamarca
  { mode: "country", country: "Turkey", type: "league", names: ["Süper Lig", "Super Lig"], display: "Super Lig", region: "general", priority: 78 },
  { mode: "country", country: "Denmark", type: "league", names: ["Superliga", "Superligaen"], display: "Danish Superliga", region: "general", priority: 75 },
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
]

const apiCache = new Map()
const teamProfileCache = new Map()
const fixtureStatsCache = new Map()

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

function isoDate(date) {
  return date.toISOString().slice(0, 10)
}

function toDateOnly(iso) {
  if (!iso) return null
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return null
  return d.toISOString().slice(0, 10)
}

function normalizeLeagueKey(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim()
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

function normalizeCompetitionName(country, rawName, fallbackDisplay) {
  const name = String(rawName || "").trim()
  const c = String(country || "").trim()

  if (c === "England" && name === "Premier League") return "Premier League"
  if (c === "Russia" && name === "Premier League") return "Russian Premier League"
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
  if (c === "Argentina" && (name === "Liga Profesional Argentina" || name === "Primera División")) return "Liga Profesional Argentina"
  if (c === "USA" && (name === "Major League Soccer" || name === "MLS")) return "MLS"
  if (c === "Mexico" && name === "Liga MX") return "Liga MX"
  if (c === "Turkey" && (name === "Süper Lig" || name === "Super Lig")) return "Super Lig"
  if (c === "Greece" && (name === "Super League 1" || name === "Super League")) return "Super League Greece"
  if (
    c === "Saudi Arabia" &&
    (name === "Pro League" ||
      name === "Saudi Pro League" ||
      name === "Saudi League" ||
      name === "ROSHN Saudi League")
  ) {
    return "Saudi Pro League"
  }
  if (c === "Denmark" && name === "Superliga") return "Superliga"

  if (name === "UEFA Champions League") return "UEFA Champions League"
  if (name === "UEFA Europa League") return "UEFA Europa League"
  if (name === "UEFA Conference League") return "UEFA Conference League"
  if (name === "UEFA Europa Conference League") return "UEFA Conference League"
  if (name === "CONMEBOL Libertadores") return "CONMEBOL Libertadores"
  if (name === "CONMEBOL Sudamericana") return "CONMEBOL Sudamericana"

  if (fallbackDisplay && name.toLowerCase() === fallbackDisplay.toLowerCase()) {
    return fallbackDisplay
  }

  return fallbackDisplay || name || c || "Competição"
}

function leagueScorePriority(leagueName) {
  const name = String(leagueName || "")
  if (name === "UEFA Champions League") return 100
  if (name === "Premier League") return 99
  if (name === "La Liga") return 98
  if (name === "Serie A") return 97
  if (name === "Bundesliga") return 96
  if (name === "Ligue 1") return 95
  if (name === "Brasileirão Série A") return 94
  if (name === "UEFA Europa League") return 93
  if (name === "CONMEBOL Libertadores" || name === "Libertadores") return 92
  if (name === "Copa do Brasil") return 91
  if (name === "Eredivisie") return 90
  if (name === "Primeira Liga") return 89
  if (name === "Brasileirão Série B") return 88
  if (name === "UEFA Conference League") return 87
  if (name === "CONMEBOL Sudamericana" || name === "Sul-Americana") return 86
  return 70
}

function getSyncWindowRange() {
  const now = new Date()
  const start = new Date(now)
  start.setHours(0, 0, 0, 0)

  if (WINDOW_MODE === "UNTIL_SUNDAY") {
    const end = new Date(now)
    const day = end.getDay()
    const daysUntilSunday = (7 - day) % 7
    end.setDate(end.getDate() + daysUntilSunday)
    end.setHours(23, 59, 59, 999)
    return { start, end }
  }

  const end = new Date(now.getTime() + WINDOW_HOURS * 60 * 60 * 1000)
  return { start, end }
}

function buildWindowDates() {
  const { start, end } = getSyncWindowRange()

  const dates = new Set()
  const cursor = new Date(start)

  while (cursor <= end) {
    dates.add(isoDate(cursor))
    cursor.setUTCDate(cursor.getUTCDate() + 1)
  }

  return Array.from(dates)
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

      if (
        rawNameKey.includes("u17") ||
        rawNameKey.includes("u18") ||
        rawNameKey.includes("u19") ||
        rawNameKey.includes("u20") ||
        rawNameKey.includes("u21") ||
        rawNameKey.includes("u23") ||
        rawNameKey.includes("under 17") ||
        rawNameKey.includes("under 18") ||
        rawNameKey.includes("under 19") ||
        rawNameKey.includes("under 20") ||
        rawNameKey.includes("under 21") ||
        rawNameKey.includes("under 23") ||
        rawNameKey.includes("women") ||
        rawNameKey.includes("feminina") ||
        rawNameKey.includes("feminino") ||
        rawNameKey.includes("female") ||
        rawNameKey.includes("youth") ||
        rawNameKey.includes("reserve") ||
        rawNameKey.includes("reserves")
      ) {
        return false
      }

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

      if (
        rawNameText.includes("u17") ||
        rawNameText.includes("u18") ||
        rawNameText.includes("u19") ||
        rawNameText.includes("u20") ||
        rawNameText.includes("u21") ||
        rawNameText.includes("u23") ||
        rawNameText.includes("under 17") ||
        rawNameText.includes("under 18") ||
        rawNameText.includes("under 19") ||
        rawNameText.includes("under 20") ||
        rawNameText.includes("under 21") ||
        rawNameText.includes("under 23") ||
        rawNameText.includes("sub 17") ||
        rawNameText.includes("sub 18") ||
        rawNameText.includes("sub 19") ||
        rawNameText.includes("sub 20") ||
        rawNameText.includes("sub 21") ||
        rawNameText.includes("sub 23") ||
        rawNameText.includes("women") ||
        rawNameText.includes("female") ||
        rawNameText.includes("feminina") ||
        rawNameText.includes("feminino") ||
        rawNameText.includes("youth") ||
        rawNameText.includes("reserve") ||
        rawNameText.includes("reserves") ||
        rawNameText.includes("division 1") ||
        rawNameText.includes("division 2") ||
        rawNameText.includes("lower") ||
        rawNameText.includes("open cup")
      ) {
        return null
      }

      if (target.display === "Saudi Pro League") {
        if (
          !countryText.includes("saudi") ||
          !(
            rawNameText.includes("pro league") ||
            rawNameText.includes("professional league") ||
            rawNameText.includes("saudi league") ||
            rawNameText.includes("roshn")
          )
        ) {
          return null
        }
      }

      if (target.display === "UEFA Champions League" && !haystack.includes("champions")) return null
      if (target.display === "UEFA Europa League" && !haystack.includes("europa")) return null
      if (target.display === "UEFA Conference League" && !haystack.includes("conference")) return null
      if (target.display === "MLS" && rawNameText !== "major league soccer") return null
      if (target.display === "CONCACAF Champions Cup" && !haystack.includes("concacaf champions")) return null
      if (target.display === "Austrian Bundesliga" && !countryText.includes("austria")) return null
      if (target.display === "Libertadores" && !haystack.includes("libertadores")) return null
      if (target.display === "Sul-Americana" && !haystack.includes("sudamericana")) return null

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

      console.log("TARGET:", target.display || target.search, "ITEMS:", items)
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
  const startDate = start.toISOString().slice(0, 10)
  const endDate = end.toISOString().slice(0, 10)

  const all = []

  const hasForbiddenMarker = (value = "") => {
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
      v.includes("women") ||
      v.includes("female") ||
      v.includes("feminina") ||
      v.includes("feminino") ||
      v.includes("youth") ||
      v.includes("reserve") ||
      v.includes("reserves")
    )
  }

  try {
    console.log("comp:", comp)

    const fixtures = await api("/fixtures", {
      league: comp.leagueId,
      season: comp.season,
      from: startDate,
      to: endDate,
      timezone: TIMEZONE,
    })

    const filteredFixtures = fixtures.filter((f) => {
      const home = f?.teams?.home?.name || ""
      const away = f?.teams?.away?.name || ""
      const league = f?.league?.name || ""
      const country = f?.league?.country || ""

      if (hasForbiddenMarker(home)) return false
      if (hasForbiddenMarker(away)) return false
      if (hasForbiddenMarker(league)) return false

      if (normalizeLeagueKey(league).includes("open cup")) return false

      if (
        comp.display === "Saudi Pro League" &&
        normalizeLeagueKey(country) !== "saudi arabia"
      ) {
        return false
      }

      return true
    })

    console.log(
      "LEAGUE:", comp.display,
      "FROM:", startDate,
      "TO:", endDate,
      "RAW:", fixtures.length,
      "FILTERED:", filteredFixtures.length
    )

    for (const fixture of filteredFixtures) {
      const kickoff = fixture?.fixture?.date
      if (!kickoff) continue

      const dt = new Date(kickoff)
      if (Number.isNaN(dt.getTime())) continue

      if (dt >= start && dt <= end) {
        all.push({
          ...fixture,
          __comp: comp,
        })
      }
    }
  } catch (error) {
    console.error(
      `Falha buscando fixtures de ${comp.display} entre ${startDate} e ${endDate}:`,
      error.message
    )
  }

  return all
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
function collectRecentFixtures(allFixtures, teamId, limit = 10) {
  return allFixtures
    .filter(
      (f) =>
        isCompletedFixture(f) &&
        (f?.teams?.home?.id === teamId || f?.teams?.away?.id === teamId)
    )
    .sort(
      (a, b) =>
        new Date(b.fixture.date).getTime() -
        new Date(a.fixture.date).getTime()
    )
    .slice(0, limit)
}

async function buildTeamProfile(teamId, leagueId, season, allFixtures) {
  const cacheKey = `${teamId}-${leagueId}-${season}`
  if (teamProfileCache.has(cacheKey)) {
    return teamProfileCache.get(cacheKey)
  }

  const recentFixtures = collectRecentFixtures(allFixtures, teamId, 10)

  const goalsFor = []
  const goalsAgainst = []
  const shots = []
  const shotsOnTarget = []
  const corners = []
  const fouls = []
  const cards = []

  for (const f of recentFixtures) {
    const isHome = f?.teams?.home?.id === teamId

    const gf = isHome
      ? f?.goals?.home ?? 0
      : f?.goals?.away ?? 0

    const ga = isHome
      ? f?.goals?.away ?? 0
      : f?.goals?.home ?? 0

    goalsFor.push(safeNumber(gf))
    goalsAgainst.push(safeNumber(ga))

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

  const profile = {
    matches: recentFixtures.length,
    avgGoalsFor: round(avg(goalsFor)),
    avgGoalsAgainst: round(avg(goalsAgainst)),
    avgShots: round(avg(shots)),
    avgShotsOnTarget: round(avg(shotsOnTarget)),
    avgCorners: round(avg(corners)),
    avgFouls: round(avg(fouls)),
    avgCards: round(avg(cards)),
  }

  teamProfileCache.set(cacheKey, profile)
  return profile
}

function buildMatchAnalysis(fixture, homeProfile, awayProfile) {
  const homeStrength =
    homeProfile.avgGoalsFor +
    homeProfile.avgShotsOnTarget * 0.3 +
    homeProfile.avgCorners * 0.2

  const awayStrength =
    awayProfile.avgGoalsFor +
    awayProfile.avgShotsOnTarget * 0.3 +
    awayProfile.avgCorners * 0.2

  const totalStrength = homeStrength + awayStrength || 1

  const homeProb = clamp(homeStrength / totalStrength, 0.05, 0.9)
  const awayProb = clamp(awayStrength / totalStrength, 0.05, 0.9)
  const drawProb = clamp(1 - (homeProb + awayProb), 0.05, 0.5)

  const expectedGoals =
    (homeProfile.avgGoalsFor + awayProfile.avgGoalsFor) / 2

  const expectedCorners =
    (homeProfile.avgCorners + awayProfile.avgCorners) / 2

  const expectedCards =
    (homeProfile.avgCards + awayProfile.avgCards) / 2

  const over25 = clamp(expectedGoals / 3, 0.1, 0.85)
  const over15 = clamp(expectedGoals / 2, 0.2, 0.95)
  const btts = clamp(expectedGoals / 2.5, 0.15, 0.85)

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
      corners: round(expectedCorners),
      cards: round(expectedCards),
    },
  }
}

function buildCoreMetrics(fixture, homeProfile, awayProfile) {
  return {
    goals: round(
      (homeProfile.avgGoalsFor + awayProfile.avgGoalsFor) / 2
    ),
    shots: round(
      (homeProfile.avgShots + awayProfile.avgShots) / 2
    ),
    shots_on_target: round(
      (homeProfile.avgShotsOnTarget + awayProfile.avgShotsOnTarget) / 2
    ),
    corners: round(
      (homeProfile.avgCorners + awayProfile.avgCorners) / 2
    ),
    fouls: round(
      (homeProfile.avgFouls + awayProfile.avgFouls) / 2
    ),
    cards: round(
      (homeProfile.avgCards + awayProfile.avgCards) / 2
    ),
  }
}

async function upsertMatch(match) {
  const payload = {
    fixture_id: match.fixture_id,
    kickoff: match.kickoff,
    league: match.league,
    region: match.region,
    priority: match.priority,
    home_team: match.home_team,
    away_team: match.away_team,
    probabilities: match.probabilities,
    markets: match.markets,
    metrics: match.metrics,
    updated_at: new Date().toISOString(),
  }

  const { error } = await supabase
    .from("matches")
    .upsert(payload, { onConflict: "fixture_id" })

  if (error) {
    console.error("Erro ao salvar match:", error.message)
  }
}
function normalizeLeagueByTeams(comp, fixture) {
  let leagueDisplay = comp.display
  let country = comp.country || fixture?.league?.country || null

  const leagueNameRaw = fixture?.league?.name || ""
  const leagueId = fixture?.league?.id || comp?.leagueId || null

  const home = fixture?.teams?.home?.name || ""
  const away = fixture?.teams?.away?.name || ""
  const teams = `${home} ${away}`

  if (
    leagueId === 218 ||
    /salzburg|sturm graz|rapid vienna|austria vienna|altach|bw linz|wolfsberger|wsg wattens|grazer ak/i.test(
      teams
    )
  ) {
    leagueDisplay = "Austrian Bundesliga"
    country = "Austria"
  }

  if (
    leagueId === 235 ||
    /rubin|lokomotiv|krylia sovetov|nizhny novgorod|zenit|spartak|rostov|sochi|krasnodar|dinamo/i.test(
      teams
    )
  ) {
    leagueDisplay = "Russian Premier League"
    country = "Russia"
  }

  if (
    leagueId === 203 ||
    /fenerbahce|fenerbahçe|besiktas|beşiktaş|galatasaray|trabzonspor|samsunspor|gaziantep|kasimpasa|kasımpaşa|eyupspor|eyüpspor|konyaspor|rizespor|kayserispor|goztepe|göztepe|alanyaspor|basaksehir|başakşehir|genclerbirligi|gençlerbirliği/i.test(
      teams
    )
  ) {
    leagueDisplay = "Super Lig"
    country = "Turkey"
  }

  if (
    /silkeborg|vejle|brondby|midtjylland|fc copenhagen|nordsjaelland/i.test(
      teams
    )
  ) {
    leagueDisplay = "Superliga"
    country = "Denmark"
  }

  if (
    /olympiacos|paok|panathinaikos|aek athens|aris|volos|kifisia/i.test(
      teams
    )
  ) {
    leagueDisplay = "Super League Greece"
    country = "Greece"
  }

  if (
    /al nassr|al hilal|al ittihad|al ahli|al shabab|al taawoun|al ettifaq/i.test(
      teams
    )
  ) {
    leagueDisplay = "Saudi Pro League"
    country = "Saudi Arabia"
  }

  if (leagueNameRaw === "Bundesliga" && country === "Austria") {
    leagueDisplay = "Austrian Bundesliga"
  }

  if (leagueNameRaw === "Premier League" && country === "Russia") {
    leagueDisplay = "Russian Premier League"
  }

  if (
    (leagueNameRaw === "Süper Lig" || leagueNameRaw === "Super Lig") &&
    country === "Turkey"
  ) {
    leagueDisplay = "Super Lig"
  }

  if (leagueNameRaw === "Superliga" && country === "Denmark") {
    leagueDisplay = "Superliga"
  }

  if (
    (leagueNameRaw === "Super League 1" ||
      leagueNameRaw === "Super League") &&
    country === "Greece"
  ) {
    leagueDisplay = "Super League Greece"
  }

  return { leagueDisplay, country }
}

function buildPrimaryMarket(analysis) {
  const { probabilities, markets } = analysis

  if (markets.over25 >= 0.72) return "Mais de 2.5 gols"
  if (markets.over15 >= 0.82) return "Mais de 1.5 gols"
  if (markets.btts >= 0.68) return "Ambas marcam"
  if (markets.corners >= 9.2) return "Mais de 8.5 escanteios"
  if (probabilities.home >= 0.62) return "Vitória do mandante"
  if (probabilities.away >= 0.62) return "Vitória do visitante"
  return "Menos de 3.5 gols"
}

function buildPrimaryProbability(analysis, market) {
  const { probabilities, markets } = analysis

  if (market === "Mais de 2.5 gols") return markets.over25
  if (market === "Mais de 1.5 gols") return markets.over15
  if (market === "Ambas marcam") return markets.btts
  if (market === "Mais de 8.5 escanteios") return clamp(markets.corners / 12, 0.1, 0.9)
  if (market === "Vitória do mandante") return probabilities.home
  if (market === "Vitória do visitante") return probabilities.away
  if (market === "Menos de 3.5 gols") return clamp(1 - markets.over25 / 1.2, 0.2, 0.92)

  return 0.55
}

async function clearFutureWindow() {
  const { start, end } = getSyncWindowRange()
  const nowIso = start.toISOString()
  const endIso = end.toISOString()

  const { data: rows, error: selectError } = await supabase
    .from("matches")
    .select("fixture_id")
    .gte("kickoff", nowIso)
    .lte("kickoff", endIso)

  if (selectError) {
    throw new Error(`Supabase select matches window: ${selectError.message}`)
  }

  const ids = (rows || []).map((x) => x.fixture_id)

  const { error: dailyError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("match_id", -1)

  if (dailyError) {
    throw new Error(`Supabase delete daily_picks window: ${dailyError.message}`)
  }

  if (!ids.length) return 0

  const { error: matchesError } = await supabase
    .from("matches")
    .delete()
    .in("fixture_id", ids)

  if (matchesError) {
    throw new Error(`Supabase delete matches window: ${matchesError.message}`)
  }

  return ids.length
}

async function buildAndStoreMatches(competitions, fixtureLists) {
  const allFixtures = uniqBy(
    fixtureLists.flat(),
    (x) => x?.fixture?.id
  )

  console.log(
    "FIXTURES POR LIGA:",
    allFixtures.reduce((acc, f) => {
      const league = f?.__comp?.display || f?.league?.name || "SEM LIGA"
      acc[league] = (acc[league] || 0) + 1
      return acc
    }, {})
  )

  console.log("ALL FIXTURES:", allFixtures)
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

      const homeProfile = await buildTeamProfile(
        homeTeamId,
        comp.leagueId,
        comp.season,
        allFixtures
      )

      const awayProfile = await buildTeamProfile(
        awayTeamId,
        comp.leagueId,
        comp.season,
        allFixtures
      )

      const analysis = buildMatchAnalysis(fixture, homeProfile, awayProfile)
      const metrics = buildCoreMetrics(fixture, homeProfile, awayProfile)
      const primaryMarket = buildPrimaryMarket(analysis)
      const primaryProbability = buildPrimaryProbability(
        analysis,
        primaryMarket
      )

      const { leagueDisplay, country } = normalizeLeagueByTeams(comp, fixture)

      const payload = {
        fixture_id: fixture?.fixture?.id,
        kickoff: fixture?.fixture?.date || null,
        league: leagueDisplay,
        country,
        region: comp.region,
        priority: comp.priority || leagueScorePriority(leagueDisplay),
        home_team: fixture?.teams?.home?.name || null,
        away_team: fixture?.teams?.away?.name || null,
        home_logo: fixture?.teams?.home?.logo || null,
        away_logo: fixture?.teams?.away?.logo || null,
        probabilities: analysis.probabilities,
        markets: analysis.markets,
        metrics,
        pick: primaryMarket,
        probability: primaryProbability,
        updated_at: new Date().toISOString(),
      }

      await upsertMatch(payload)

      stored.push(payload)

      console.log(
        `✅ ${payload.league} | ${payload.home_team} x ${payload.away_team}`
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
    .filter((m) => m.fixture_id && m.pick)
    .sort((a, b) => {
      const pa = Number(a.probability || 0)
      const pb = Number(b.probability || 0)

      if (pb !== pa) return pb - pa
      return Number(b.priority || 0) - Number(a.priority || 0)
    })
    .slice(0, 20)

  if (!sorted.length) return 0

  const rows = sorted.map((m, index) => ({
    match_id: m.fixture_id,
    rank: index + 1,
    league: m.league,
    home_team: m.home_team,
    away_team: m.away_team,
    market: m.pick,
    probability: m.probability,
    kickoff: m.kickoff,
    created_at: new Date().toISOString(),
  }))

  const { error } = await supabase.from("daily_picks").insert(rows)

  if (error) {
    throw new Error(`Supabase daily_picks: ${error.message}`)
  }

  return rows.length
}
async function run() {
  console.log("🚀 Scoutly Sync V3 iniciado")

  const { start, end } = getSyncWindowRange()
  console.log(`📆 Janela ativa: ${start.toISOString()} -> ${end.toISOString()}`)

  const competitions = await resolveTargetCompetitions()
  console.log("COMPETITIONS:", competitions)

  const fixtureLists = []
  for (const comp of competitions) {
    const list = await fetchFixturesForCompetition(comp)
    fixtureLists.push(list)
  }

  console.log(
    "COMPETITIONS RESOLVIDAS:",
    competitions.map((c) => ({
      leagueId: c.leagueId,
      display: c.display,
      country: c.country,
      season: c.season,
    }))
  )

  console.log(
    "FIXTURE LISTS COUNT:",
    fixtureLists.map((list, i) => ({
      competition: competitions[i]?.display,
      total: list.length,
    }))
  )

  const storedMatches = await buildAndStoreMatches(competitions, fixtureLists)

  const picksCount = await rebuildDailyPicks(storedMatches)
  console.log(`🏁 Daily picks gerados: ${picksCount}`)
  console.log("✅ Scoutly Sync V3 concluído")
}

run().catch((error) => {
  console.error("❌ Erro fatal no Scoutly Sync V3:", error)
  process.exit(1)
})
