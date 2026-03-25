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
const WINDOW_HOURS = 96
const REQUEST_DELAY_MS = 1100

/**
 * Estratégia do novo sync:
 * 1) Mantém as ligas/copas já cobertas
 * 2) Adiciona seleções, Copa do Nordeste, Copa Verde e feminino
 * 3) Perfil do time = últimos 10 jogos ponderados por recência
 * 4) Usa split casa/fora quando possível
 * 5) Não conta estatística ausente como zero
 * 6) Salva colunas flat + JSONB em matches
 */

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

  // Brasil - adições
  { mode: "search", search: "Copa do Nordeste", display: "Copa do Nordeste", region: "brazil", priority: 87 },
  { mode: "search", search: "Copa Verde", display: "Copa Verde", region: "brazil", priority: 80 },
  { mode: "search", search: "Brasileiro Women", display: "Brasileirão Feminino", region: "brazil", priority: 82, allowWomen: true },

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
  { mode: "search", search: "UEFA Women Champions League", display: "UEFA Women's Champions League", region: "general", priority: 84, allowWomen: true },

  // CONMEBOL
  { mode: "search", search: "CONMEBOL Libertadores", display: "Libertadores", region: "brazil", priority: 92 },
  { mode: "search", search: "Copa Libertadores", display: "Libertadores", region: "brazil", priority: 91 },
  { mode: "search", search: "CONMEBOL Sudamericana", display: "Sul-Americana", region: "brazil", priority: 86 },
  { mode: "search", search: "Copa Sudamericana", display: "Sul-Americana", region: "brazil", priority: 85 },

  // Seleções / internacional
  { mode: "search", search: "International Friendlies", display: "Amistosos Internacionais", region: "international", priority: 85 },
  { mode: "search", search: "UEFA Nations League", display: "UEFA Nations League", region: "international", priority: 95 },
  { mode: "search", search: "CONMEBOL World Cup Qualifiers", display: "Eliminatórias Sul-Americanas", region: "international", priority: 96 },
  { mode: "search", search: "UEFA Euro Qualifiers", display: "Eliminatórias da Euro", region: "international", priority: 94 },
  { mode: "search", search: "FIFA World Cup", display: "Copa do Mundo", region: "international", priority: 100 },
  { mode: "search", search: "Copa America", display: "Copa América", region: "international", priority: 98 },
  { mode: "search", search: "UEFA European Championship", display: "Eurocopa", region: "international", priority: 98 },
]

const apiCache = new Map()
const fixtureStatsCache = new Map()
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
  if (c === "Saudi Arabia" && ["Pro League", "Saudi Pro League", "Saudi League", "ROSHN Saudi League"].includes(name)) {
    return "Saudi Pro League"
  }
  if (c === "Denmark" && name === "Superliga") return "Superliga"
  if (name === "UEFA Europa Conference League") return "UEFA Conference League"

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
  if (name === "Libertadores" || name === "CONMEBOL Libertadores") return 92
  if (name === "Copa do Brasil") return 91
  if (name === "Eredivisie") return 90
  if (name === "Primeira Liga") return 89
  if (name === "Brasileirão Série B") return 88
  if (name === "UEFA Conference League") return 87
  if (name === "Sul-Americana" || name === "CONMEBOL Sudamericana") return 86
  return 70
}

function getSyncWindowRange() {
  const now = new Date()
  const start = new Date(now)
  start.setHours(0, 0, 0, 0)
  const end = new Date(now.getTime() + WINDOW_HOURS * 60 * 60 * 1000)
  return { start, end }
}

function hasForbiddenMarker(value = "", allowWomen = false) {
  const v = normalizeLeagueKey(value)

  if (
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
    v.includes("youth") ||
    v.includes("reserve") ||
    v.includes("reserves")
  ) {
    return true
  }

  if (!allowWomen) {
    if (
      v.includes("women") ||
      v.includes("female") ||
      v.includes("feminina") ||
      v.includes("feminino")
    ) {
      return true
    }
  }

  return false
}

async function resolveCountryCompetitions(target) {
  const leagues = await api("/leagues", {
    country: target.country,
    current: true,
  })

  const normalizedNames = new Set((target.names || []).map((x) => normalizeLeagueKey(x)))

  return leagues
    .filter((item) => {
      const rawName = String(item?.league?.name || "")
      const rawNameKey = normalizeLeagueKey(rawName)
      const leagueType = String(item?.league?.type || "").toLowerCase()
      const seasonCurrent = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]

      if (!seasonCurrent) return false
      if (target.type && leagueType !== target.type) return false
      if (hasForbiddenMarker(rawName, !!target.allowWomen)) return false

      return Array.from(normalizedNames).some((n) => rawNameKey === n)
    })
    .map((item) => {
      const currentSeason = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]
      return {
        leagueId: item.league.id,
        season: currentSeason.year,
        country: item.country?.name || target.country,
        rawName: item.league.name,
        display: normalizeCompetitionName(item.country?.name || target.country, item.league.name, target.display),
        region: target.region,
        priority: target.priority,
        allowWomen: !!target.allowWomen,
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
      const haystack = `${country || ""} ${rawName}`.toLowerCase().trim()

      if (hasForbiddenMarker(rawName, !!target.allowWomen)) return null
      if (haystack.includes("open cup")) return null

      return {
        leagueId: item.league.id,
        season: currentSeason.year,
        country,
        rawName,
        display: normalizeCompetitionName(country, rawName, target.display),
        region: target.region,
        priority: target.priority,
        allowWomen: !!target.allowWomen,
      }
    })
    .filter(Boolean)

  const unique = new Map()
  items.forEach((item) => {
    if (!unique.has(item.leagueId)) unique.set(item.leagueId, item)
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
      console.error(`Falha resolvendo competição ${target.display || target.search || target.country}:`, error.message)
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

  try {
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

      if (hasForbiddenMarker(home, comp.allowWomen)) return false
      if (hasForbiddenMarker(away, comp.allowWomen)) return false
      if (hasForbiddenMarker(league, comp.allowWomen)) return false
      if (normalizeLeagueKey(league).includes("open cup")) return false

      if (comp.display === "Saudi Pro League" && normalizeLeagueKey(country) !== "saudi arabia") {
        return false
      }

      return true
    })

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
    console.error(`Falha buscando fixtures de ${comp.display}:`, error.message)
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
  if (!found) return null

  const value = found.value
  if (value === null || value === undefined) return null

  if (typeof value === "string") {
    const cleaned = value.replace("%", "").trim()
    const num = Number(cleaned)
    return Number.isFinite(num) ? num : null
  }

  return Number.isFinite(Number(value)) ? Number(value) : null
}

function isCompletedFixture(fixture) {
  const short = fixture?.fixture?.status?.short || ""
  return ["FT", "AET", "PEN"].includes(short)
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
      .slice(0, limit)
  } catch (error) {
    console.error(`Falha buscando histórico do time ${teamId}:`, error.message)
    return []
  }
}

function getRecencyWeight(index) {
  const weights = [1.0, 0.95, 0.90, 0.85, 0.80, 0.75, 0.70, 0.65, 0.60, 0.55]
  return weights[index] || 0.5
}

function weightedAverage(entries) {
  if (!entries.length) return null
  const totalWeight = entries.reduce((acc, e) => acc + e.weight, 0)
  if (!totalWeight) return null
  return entries.reduce((acc, e) => acc + e.value * e.weight, 0) / totalWeight
}

function blendValues(primary, secondary, tertiary, w1 = 0.6, w2 = 0.25, w3 = 0.15) {
  const parts = []
  if (primary != null) parts.push({ value: primary, weight: w1 })
  if (secondary != null) parts.push({ value: secondary, weight: w2 })
  if (tertiary != null) parts.push({ value: tertiary, weight: w3 })

  const totalWeight = parts.reduce((acc, p) => acc + p.weight, 0)
  if (!parts.length || totalWeight === 0) return null

  return parts.reduce((acc, p) => acc + p.value * p.weight, 0) / totalWeight
}

function makeScoreLabel(fixture, teamId) {
  const isHome = fixture?.teams?.home?.id === teamId
  const gf = isHome ? safeNumber(fixture?.goals?.home, 0) : safeNumber(fixture?.goals?.away, 0)
  const ga = isHome ? safeNumber(fixture?.goals?.away, 0) : safeNumber(fixture?.goals?.home, 0)
  return `${gf}-${ga}`
}

function buildRecentForm(fixtures, teamId) {
  return fixtures
    .slice(0, 5)
    .map((f) => makeScoreLabel(f, teamId))
    .join("|")
}

async function buildTeamProfile(teamId) {
  const cacheKey = `${teamId}`
  if (teamProfileCache.has(cacheKey)) return teamProfileCache.get(cacheKey)

  const recentFixtures = await fetchRecentFinishedFixtures(teamId, 20)

  const overall = {
    goalsFor: [],
    goalsAgainst: [],
    shots: [],
    shotsOnTarget: [],
    corners: [],
    cards: [],
  }

  const homeOnly = {
    goalsFor: [],
    goalsAgainst: [],
    shots: [],
    shotsOnTarget: [],
    corners: [],
    cards: [],
  }

  const awayOnly = {
    goalsFor: [],
    goalsAgainst: [],
    shots: [],
    shotsOnTarget: [],
    corners: [],
    cards: [],
  }

  const recent10 = []
  let seasonBucketGoalsFor = []
  let seasonBucketGoalsAgainst = []
  let seasonBucketShots = []
  let seasonBucketShotsOnTarget = []
  let seasonBucketCorners = []
  let seasonBucketCards = []

  for (const [index, f] of recentFixtures.entries()) {
    const isHome = f?.teams?.home?.id === teamId
    const gf = isHome ? safeNumber(f?.goals?.home, 0) : safeNumber(f?.goals?.away, 0)
    const ga = isHome ? safeNumber(f?.goals?.away, 0) : safeNumber(f?.goals?.home, 0)
    const weight = getRecencyWeight(index)

    overall.goalsFor.push({ value: gf, weight })
    overall.goalsAgainst.push({ value: ga, weight })

    if (isHome) {
      homeOnly.goalsFor.push({ value: gf, weight })
      homeOnly.goalsAgainst.push({ value: ga, weight })
    } else {
      awayOnly.goalsFor.push({ value: gf, weight })
      awayOnly.goalsAgainst.push({ value: ga, weight })
    }

    if (index < 10) recent10.push(f)

    const stats = await getFixtureStatistics(f.fixture.id)
    const teamStats = stats.find((s) => s.team.id === teamId)?.statistics || []

    const shots = extractStatValue(teamStats, "Total Shots")
    const shotsOnTarget = extractStatValue(teamStats, "Shots on Goal")
    const corners = extractStatValue(teamStats, "Corner Kicks")
    const yellow = extractStatValue(teamStats, "Yellow Cards")
    const red = extractStatValue(teamStats, "Red Cards")
    const cards = (yellow ?? 0) + (red ?? 0)

    // só entra em média se existir de verdade
    if (shots != null) {
      overall.shots.push({ value: shots, weight })
      seasonBucketShots.push(shots)
      if (isHome) homeOnly.shots.push({ value: shots, weight })
      else awayOnly.shots.push({ value: shots, weight })
    }

    if (shotsOnTarget != null) {
      overall.shotsOnTarget.push({ value: shotsOnTarget, weight })
      seasonBucketShotsOnTarget.push(shotsOnTarget)
      if (isHome) homeOnly.shotsOnTarget.push({ value: shotsOnTarget, weight })
      else awayOnly.shotsOnTarget.push({ value: shotsOnTarget, weight })
    }

    if (corners != null) {
      overall.corners.push({ value: corners, weight })
      seasonBucketCorners.push(corners)
      if (isHome) homeOnly.corners.push({ value: corners, weight })
      else awayOnly.corners.push({ value: corners, weight })
    }

    if (yellow != null || red != null) {
      overall.cards.push({ value: cards, weight })
      seasonBucketCards.push(cards)
      if (isHome) homeOnly.cards.push({ value: cards, weight })
      else awayOnly.cards.push({ value: cards, weight })
    }

    seasonBucketGoalsFor.push(gf)
    seasonBucketGoalsAgainst.push(ga)
  }

  const profile = {
    recentCount: recent10.length,
    form: buildRecentForm(recent10, teamId),

    overall: {
      avgGoalsFor: weightedAverage(overall.goalsFor),
      avgGoalsAgainst: weightedAverage(overall.goalsAgainst),
      avgShots: weightedAverage(overall.shots),
      avgShotsOnTarget: weightedAverage(overall.shotsOnTarget),
      avgCorners: weightedAverage(overall.corners),
      avgCards: weightedAverage(overall.cards),
    },

    home: {
      avgGoalsFor: weightedAverage(homeOnly.goalsFor),
      avgGoalsAgainst: weightedAverage(homeOnly.goalsAgainst),
      avgShots: weightedAverage(homeOnly.shots),
      avgShotsOnTarget: weightedAverage(homeOnly.shotsOnTarget),
      avgCorners: weightedAverage(homeOnly.corners),
      avgCards: weightedAverage(homeOnly.cards),
    },

    away: {
      avgGoalsFor: weightedAverage(awayOnly.goalsFor),
      avgGoalsAgainst: weightedAverage(awayOnly.goalsAgainst),
      avgShots: weightedAverage(awayOnly.shots),
      avgShotsOnTarget: weightedAverage(awayOnly.shotsOnTarget),
      avgCorners: weightedAverage(awayOnly.corners),
      avgCards: weightedAverage(awayOnly.cards),
    },

    season: {
      avgGoalsFor: seasonBucketGoalsFor.length ? avg(seasonBucketGoalsFor) : null,
      avgGoalsAgainst: seasonBucketGoalsAgainst.length ? avg(seasonBucketGoalsAgainst) : null,
      avgShots: seasonBucketShots.length ? avg(seasonBucketShots) : null,
      avgShotsOnTarget: seasonBucketShotsOnTarget.length ? avg(seasonBucketShotsOnTarget) : null,
      avgCorners: seasonBucketCorners.length ? avg(seasonBucketCorners) : null,
      avgCards: seasonBucketCards.length ? avg(seasonBucketCards) : null,
    },

    samples: {
      goals: seasonBucketGoalsFor.length,
      shots: seasonBucketShots.length,
      shotsOnTarget: seasonBucketShotsOnTarget.length,
      corners: seasonBucketCorners.length,
      cards: seasonBucketCards.length,
    }
  }

  teamProfileCache.set(cacheKey, profile)
  return profile
}

function buildProjectedMetrics(homeProfile, awayProfile) {
  const homeGoalsFor = blendValues(homeProfile.home.avgGoalsFor, homeProfile.overall.avgGoalsFor, homeProfile.season.avgGoalsFor)
  const homeGoalsAgainst = blendValues(homeProfile.home.avgGoalsAgainst, homeProfile.overall.avgGoalsAgainst, homeProfile.season.avgGoalsAgainst)

  const awayGoalsFor = blendValues(awayProfile.away.avgGoalsFor, awayProfile.overall.avgGoalsFor, awayProfile.season.avgGoalsFor)
  const awayGoalsAgainst = blendValues(awayProfile.away.avgGoalsAgainst, awayProfile.overall.avgGoalsAgainst, awayProfile.season.avgGoalsAgainst)

  const homeShots = blendValues(homeProfile.home.avgShots, homeProfile.overall.avgShots, homeProfile.season.avgShots)
  const awayShots = blendValues(awayProfile.away.avgShots, awayProfile.overall.avgShots, awayProfile.season.avgShots)

  const homeShotsOnTarget = blendValues(homeProfile.home.avgShotsOnTarget, homeProfile.overall.avgShotsOnTarget, homeProfile.season.avgShotsOnTarget)
  const awayShotsOnTarget = blendValues(awayProfile.away.avgShotsOnTarget, awayProfile.overall.avgShotsOnTarget, awayProfile.season.avgShotsOnTarget)

  const homeCorners = blendValues(homeProfile.home.avgCorners, homeProfile.overall.avgCorners, homeProfile.season.avgCorners)
  const awayCorners = blendValues(awayProfile.away.avgCorners, awayProfile.overall.avgCorners, awayProfile.season.avgCorners)

  const homeCards = blendValues(homeProfile.home.avgCards, homeProfile.overall.avgCards, homeProfile.season.avgCards)
  const awayCards = blendValues(awayProfile.away.avgCards, awayProfile.overall.avgCards, awayProfile.season.avgCards)

  const xgHome = clamp(
    ((homeGoalsFor ?? 1.2) * 0.62) + ((awayGoalsAgainst ?? 1.1) * 0.38) + 0.18,
    0.25,
    3.6
  )

  const xgAway = clamp(
    ((awayGoalsFor ?? 1.0) * 0.62) + ((homeGoalsAgainst ?? 1.1) * 0.38),
    0.20,
    3.2
  )

  return {
    xgHome,
    xgAway,
    goals: round(xgHome + xgAway),
    corners: round((homeCorners ?? 4.2) + (awayCorners ?? 4.0)),
    shots: round((homeShots ?? 10.5) + (awayShots ?? 9.5)),
    shotsOnTarget: round((homeShotsOnTarget ?? 3.8) + (awayShotsOnTarget ?? 3.2)),
    cards: round((homeCards ?? 2.0) + (awayCards ?? 2.0)),
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
  let sum = 0

  for (let k = 0; k <= maxK; k++) {
    const p = poisson(lambda, k)
    sum += p
    arr.push(p)
  }

  if (sum < 0.999) arr.push(1 - sum)
  return arr
}

function buildProbabilitySet(xgHome, xgAway) {
  const homeDist = cumulativePoisson(xgHome, 10)
  const awayDist = cumulativePoisson(xgAway, 10)

  let homeWin = 0
  let draw = 0
  let awayWin = 0

  let over15 = 0
  let over25 = 0
  let under25 = 0
  let under35 = 0
  let bttsYes = 0

  for (let h = 0; h < homeDist.length; h++) {
    for (let a = 0; a < awayDist.length; a++) {
      const p = homeDist[h] * awayDist[a]
      const total = h + a

      if (h > a) homeWin += p
      else if (h === a) draw += p
      else awayWin += p

      if (total >= 2) over15 += p
      if (total >= 3) over25 += p
      if (total <= 2) under25 += p
      if (total <= 3) under35 += p
      if (h >= 1 && a >= 1) bttsYes += p
    }
  }

  return {
    probabilities: {
      home: round(clamp(homeWin, 0.05, 0.92)),
      draw: round(clamp(draw, 0.05, 0.45)),
      away: round(clamp(awayWin, 0.05, 0.92)),
    },
    markets: {
      over15: round(clamp(over15, 0.10, 0.97)),
      over25: round(clamp(over25, 0.08, 0.95)),
      under25: round(clamp(under25, 0.08, 0.95)),
      under35: round(clamp(under35, 0.10, 0.97)),
      btts: round(clamp(bttsYes, 0.08, 0.94)),
    }
  }
}

function buildCornersOver85Prob(expectedCorners) {
  const logistic = 1 / (1 + Math.exp(-(expectedCorners - 8.5) / 1.15))
  return round(clamp(logistic, 0.08, 0.92))
}

function buildGameProfile(avgGoals, avgShots, avgCorners, bttsProb, over25Prob, under25Prob) {
  if (avgGoals >= 2.8 || over25Prob >= 0.67 || (avgShots >= 24 && bttsProb >= 0.62)) return "ofensivo"
  if (avgGoals <= 2.1 && under25Prob >= 0.72 && (1 - bttsProb) >= 0.70 && avgShots <= 18) return "defensivo"
  if (avgCorners >= 9.2 && avgShots >= 21 && avgGoals >= 2.2) return "corners"
  return "equilibrado"
}

function buildInsight(match, gameProfile) {
  if (match.pick === "Mais de 2.5 gols") {
    return `A leitura Scoutly projeta um jogo mais aberto, com potencial real para 3 ou mais gols. A média esperada está em ${match.avg_goals} gols e o ritmo ofensivo sustenta essa linha.`
  }

  if (match.pick === "Mais de 1.5 gols") {
    return `A leitura Scoutly projeta um confronto com boa chance de pelo menos 2 gols. A base recente e a produção ofensiva favorecem esse cenário.`
  }

  if (match.pick === "Menos de 2.5 gols") {
    return `A leitura Scoutly indica um jogo travado, com baixa projeção ofensiva e controle no placar.`
  }

  if (match.pick === "Menos de 3.5 gols") {
    return `A leitura Scoutly indica um jogo controlado, sem expectativa de explosão ofensiva.`
  }

  if (match.pick === "Ambas marcam") {
    return `A leitura Scoutly identifica espaço para gols dos dois lados a partir do equilíbrio ofensivo recente.`
  }

  if (match.pick === "Ambas não marcam") {
    return `A leitura Scoutly vê um confronto com baixa tendência de gols dos dois lados e maior chance de um dos times passar em branco.`
  }

  if (match.pick?.includes("escanteios")) {
    return `A leitura Scoutly projeta aproximadamente ${match.avg_corners} escanteios e enxerga esse mercado como a melhor oportunidade estatística.`
  }

  if (match.pick?.includes("empate") || match.pick?.includes("Dupla chance")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção adequada ao contexto do confronto.`
  }

  return `A leitura Scoutly classifica este confronto como ${gameProfile}, combinando projeção de ${match.avg_goals} gols, ${match.avg_corners} escanteios e ${match.avg_shots} finalizações totais.`
}

function buildPrimaryMarket(analysis) {
  const {
    avg_goals,
    avg_corners,
    avg_shots,
    avg_shots_on_target,
    home_win_prob,
    away_win_prob,
    draw_prob,
    over15_prob,
    over25_prob,
    under25_prob,
    under35_prob,
    btts_prob,
    corners_over85_prob,
  } = analysis

  const strongestSide = Math.max(home_win_prob, away_win_prob)
  const balancedGame = Math.abs(home_win_prob - away_win_prob) <= 0.12

  const veryLowTempoGame =
    avg_goals <= 1.8 &&
    avg_shots <= 18 &&
    avg_shots_on_target <= 5.5 &&
    btts_prob <= 0.48

  const lowTempoGame =
    avg_goals <= 2.05 &&
    avg_shots <= 22 &&
    avg_shots_on_target <= 7 &&
    btts_prob <= 0.56

  const openGame =
    avg_goals >= 2.7 &&
    avg_shots >= 24 &&
    avg_shots_on_target >= 8 &&
    over25_prob >= 0.68

  const veryOpenGame =
    avg_goals >= 3.0 &&
    avg_shots >= 27 &&
    avg_shots_on_target >= 9 &&
    over25_prob >= 0.74 &&
    btts_prob >= 0.60

  const oneSidedGame =
    strongestSide >= 0.64 &&
    Math.abs(home_win_prob - away_win_prob) >= 0.22

  if (veryLowTempoGame) return "Menos de 2.5 gols"
  if (lowTempoGame && draw_prob <= 0.34) return "Menos de 3.5 gols"
  if (veryOpenGame) return "Mais de 2.5 gols"
  if (openGame && balancedGame && btts_prob >= 0.62) return "Ambas marcam"
  if (openGame && over15_prob >= 0.80) return "Mais de 1.5 gols"
  if (oneSidedGame && home_win_prob > away_win_prob) return "Vitória do mandante"
  if (oneSidedGame && away_win_prob > home_win_prob) return "Vitória do visitante"
  if (avg_corners >= 9.6 && avg_shots >= 23 && corners_over85_prob >= 0.62) return "Mais de 8.5 escanteios"
  if (btts_prob <= 0.50 && avg_goals <= 2.1 && avg_shots_on_target <= 7) return "Ambas não marcam"
  if (over15_prob >= 0.84 && avg_goals >= 2.2) return "Mais de 1.5 gols"
  if (under25_prob >= 0.77) return "Menos de 2.5 gols"
  if (under35_prob >= 0.81) return "Menos de 3.5 gols"

  return "Menos de 3.5 gols"
}

function buildPrimaryProbability(match, market) {
  if (market === "Mais de 2.5 gols") return round(match.over25_prob)
  if (market === "Mais de 1.5 gols") return round(match.over15_prob)
  if (market === "Menos de 2.5 gols") return round(match.under25_prob)
  if (market === "Menos de 3.5 gols") return round(match.under35_prob)
  if (market === "Ambas marcam") return round(match.btts_prob)
  if (market === "Ambas não marcam") return round(clamp(1 - match.btts_prob, 0.08, 0.94))
  if (market === "Mais de 8.5 escanteios") return round(match.corners_over85_prob)
  if (market === "Vitória do mandante") return round(match.home_win_prob)
  if (market === "Vitória do visitante") return round(match.away_win_prob)
  return 0.55
}

async function upsertMatch(match) {
  const payload = {
    id: match.id,
    match_date: match.match_date,
    kickoff: match.kickoff,
    league: match.league,
    country: match.country || null,
    region: match.region || null,
    priority: match.priority || null,
    home_team: match.home_team || null,
    away_team: match.away_team || null,
    home_logo: match.home_logo || null,
    away_logo: match.away_logo || null,

    avg_goals: match.avg_goals,
    avg_corners: match.avg_corners,
    avg_shots: match.avg_shots,

    home_form: match.home_form || null,
    away_form: match.away_form || null,

    insight: match.insight || null,
    pick: match.pick || null,

    home_win_prob: match.home_win_prob,
    draw_prob: match.draw_prob,
    away_win_prob: match.away_win_prob,
    over15_prob: match.over15_prob,
    over25_prob: match.over25_prob,
    under25_prob: match.under25_prob,
    under35_prob: match.under35_prob,
    btts_prob: match.btts_prob,
    corners_over85_prob: match.corners_over85_prob,

    metrics: match.metrics || null,
    markets: match.markets || null,
    probabilities: match.probabilities || null,

    probability: match.probability || null,
    updated_at: new Date().toISOString(),
  }

  const { error } = await supabase
    .from("matches")
    .upsert(payload, { onConflict: "id" })

  if (error) {
    throw new Error(`Erro ao salvar match ${match.id}: ${error.message}`)
  }
}

async function clearFutureWindow() {
  const now = new Date().toISOString()

  const { error: dailyError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("match_id", -1)

  if (dailyError) {
    throw new Error(`Supabase delete daily_picks: ${dailyError.message}`)
  }

  const { data: rows, error: selectError } = await supabase
    .from("matches")
    .select("id")
    .lte("kickoff", now)

  if (selectError) {
    throw new Error(`Supabase select old matches: ${selectError.message}`)
  }

  const oldIds = (rows || []).map((x) => x.id)

  if (oldIds.length) {
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
    /salzburg|sturm graz|rapid vienna|austria vienna|altach|bw linz|wolfsberger|wsg wattens|grazer ak/i.test(teams)
  ) {
    leagueDisplay = "Austrian Bundesliga"
    country = "Austria"
  }

  if (
    leagueId === 235 ||
    /rubin|lokomotiv|krylia sovetov|nizhny novgorod|zenit|spartak|rostov|sochi|krasnodar|dinamo/i.test(teams)
  ) {
    leagueDisplay = "Russian Premier League"
    country = "Russia"
  }

  if (
    leagueId === 203 ||
    /fenerbahce|fenerbahçe|besiktas|beşiktaş|galatasaray|trabzonspor|samsunspor|gaziantep|kasimpasa|kasımpaşa|eyupspor|eyüpspor|konyaspor|rizespor|kayserispor|goztepe|göztepe|alanyaspor|basaksehir|başakşehir|genclerbirligi|gençlerbirliği/i.test(teams)
  ) {
    leagueDisplay = "Super Lig"
    country = "Turkey"
  }

  if (/silkeborg|vejle|brondby|midtjylland|fc copenhagen|nordsjaelland/i.test(teams)) {
    leagueDisplay = "Superliga"
    country = "Denmark"
  }

  if (/olympiacos|paok|panathinaikos|aek athens|aris|volos|kifisia/i.test(teams)) {
    leagueDisplay = "Super League Greece"
    country = "Greece"
  }

  if (/al nassr|al hilal|al ittihad|al ahli|al shabab|al taawoun|al ettifaq/i.test(teams)) {
    leagueDisplay = "Saudi Pro League"
    country = "Saudi Arabia"
  }

  if (leagueNameRaw === "Bundesliga" && country === "Austria") leagueDisplay = "Austrian Bundesliga"
  if (leagueNameRaw === "Premier League" && country === "Russia") leagueDisplay = "Russian Premier League"
  if ((leagueNameRaw === "Süper Lig" || leagueNameRaw === "Super Lig") && country === "Turkey") leagueDisplay = "Super Lig"
  if (leagueNameRaw === "Superliga" && country === "Denmark") leagueDisplay = "Superliga"
  if ((leagueNameRaw === "Super League 1" || leagueNameRaw === "Super League") && country === "Greece") leagueDisplay = "Super League Greece"

  return { leagueDisplay, country }
}

async function buildAndStoreMatches(fixtureLists) {
  const { start, end } = getSyncWindowRange()

  const allFixtures = uniqBy(
    fixtureLists.flat().filter((x) => {
      const kickoff = x?.fixture?.date
      if (!kickoff) return false

      const dt = new Date(kickoff)
      if (Number.isNaN(dt.getTime())) return false

      return dt >= start && dt <= end
    }),
    (x) => x?.fixture?.id
  )

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

      const homeProfile = await buildTeamProfile(homeTeamId)
      const awayProfile = await buildTeamProfile(awayTeamId)

      const projected = buildProjectedMetrics(homeProfile, awayProfile)
      const probSet = buildProbabilitySet(projected.xgHome, projected.xgAway)
      const corners_over85_prob = buildCornersOver85Prob(projected.corners)

      const { leagueDisplay, country } = normalizeLeagueByTeams(comp, fixture)

      const avg_goals = round(projected.goals)
      const avg_corners = round(projected.corners)
      const avg_shots = round(projected.shots)
      const avg_shots_on_target = round(projected.shotsOnTarget)
      const avg_cards = round(projected.cards)

      const home_win_prob = round(probSet.probabilities.home)
      const draw_prob = round(probSet.probabilities.draw)
      const away_win_prob = round(probSet.probabilities.away)

      const over15_prob = round(probSet.markets.over15)
      const over25_prob = round(probSet.markets.over25)
      const under25_prob = round(probSet.markets.under25)
      const under35_prob = round(probSet.markets.under35)
      const btts_prob = round(probSet.markets.btts)

      const gameProfile = buildGameProfile(
        avg_goals,
        avg_shots,
        avg_corners,
        btts_prob,
        over25_prob,
        under25_prob
      )

      const tempMatch = {
        id: fixture?.fixture?.id,
        match_date: toDateOnly(fixture?.fixture?.date),
        kickoff: fixture?.fixture?.date || null,
        league: leagueDisplay,
        country,
        region: comp.region,
        priority: comp.priority || leagueScorePriority(leagueDisplay),

        home_team: fixture?.teams?.home?.name || null,
        away_team: fixture?.teams?.away?.name || null,
        home_logo: fixture?.teams?.home?.logo || null,
        away_logo: fixture?.teams?.away?.logo || null,

        avg_goals,
        avg_corners,
        avg_shots,

        home_form: homeProfile.form,
        away_form: awayProfile.form,

        home_win_prob,
        draw_prob,
        away_win_prob,
        over15_prob,
        over25_prob,
        under25_prob,
        under35_prob,
        btts_prob,
        corners_over85_prob,

        metrics: {
          goals: avg_goals,
          corners: avg_corners,
          shots: avg_shots,
          shots_on_target: avg_shots_on_target,
          cards: avg_cards,
          xg_home: round(projected.xgHome),
          xg_away: round(projected.xgAway),
          game_profile: gameProfile,
          samples: {
            home: homeProfile.samples,
            away: awayProfile.samples,
          }
        },

        markets: {
          over15: over15_prob,
          over25: over25_prob,
          under25: under25_prob,
          under35: under35_prob,
          btts: btts_prob,
          corners: avg_corners,
          corners_over85: corners_over85_prob,
        },

        probabilities: {
          home: home_win_prob,
          draw: draw_prob,
          away: away_win_prob,
        },
      }

      const pick = buildPrimaryMarket({
        avg_goals,
        avg_corners,
        avg_shots,
        avg_shots_on_target,
        home_win_prob,
        draw_prob,
        away_win_prob,
        over15_prob,
        over25_prob,
        under25_prob,
        under35_prob,
        btts_prob,
        corners_over85_prob,
      })

      tempMatch.pick = pick
      tempMatch.probability = buildPrimaryProbability(
        {
          avg_goals,
          avg_corners,
          avg_shots,
          avg_shots_on_target,
          home_win_prob,
          away_win_prob,
          over15_prob,
          over25_prob,
          under25_prob,
          under35_prob,
          btts_prob,
          corners_over85_prob,
        },
        pick
      )

      tempMatch.insight = buildInsight(tempMatch, gameProfile)

      await upsertMatch(tempMatch)
      stored.push(tempMatch)

      console.log(`✅ ${tempMatch.league} | ${tempMatch.home_team} x ${tempMatch.away_team} -> ${tempMatch.pick}`)
    } catch (error) {
      console.error(`❌ Falha processando fixture ${fixture?.fixture?.id}:`, error.message)
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
    .slice(0, 20)

  if (!sorted.length) return 0

  const rows = sorted.map((m, index) => ({
    match_id: m.id,
    rank: index + 1,
    league: m.league,
    home_team: m.home_team,
    away_team: m.away_team,
    home_logo: m.home_logo,
    away_logo: m.away_logo,
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
  console.log("🚀 Scoutly Sync V4 iniciado")

  const { start, end } = getSyncWindowRange()
  console.log(`📆 Janela ativa: ${start.toISOString()} -> ${end.toISOString()}`)

  const competitions = await resolveTargetCompetitions()
  console.log(`🏆 Competições resolvidas: ${competitions.length}`)

  const fixtureLists = []
  for (const comp of competitions) {
    const list = await fetchFixturesForCompetition(comp)
    fixtureLists.push(list)
  }

  const storedMatches = await buildAndStoreMatches(fixtureLists)
  const picksCount = await rebuildDailyPicks(storedMatches)

  console.log(`🏁 Matches salvos: ${storedMatches.length}`)
  console.log(`🔥 Daily picks gerados: ${picksCount}`)
  console.log("✅ Scoutly Sync V4 concluído")
}

run().catch((error) => {
  console.error("❌ Erro fatal no Scoutly Sync V4:", error)
  process.exit(1)
})
