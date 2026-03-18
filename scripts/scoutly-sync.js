const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = "705f52b4ec1295f3c369365c2d71cb71"

// Supabase será configurado depois
const SUPABASE_URL = "https://zngdcatlxlxamceqpxij.supabase.co"
const SUPABASE_KEY = "sb_publishable_gw3BGCEcwhidS2hFbiwxIA_W28KkZ2a"

if (!APISPORTS_KEY) throw new Error("APISPORTS_KEY não encontrada.")

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)
const API = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"
const WINDOW_HOURS = 72
const REQUEST_DELAY_MS = 350

const TARGET_COMPETITIONS = [
  // Inglaterra
  { mode: "country", country: "England", type: "league", names: ["Premier League"], display: "Premier League", region: "general", priority: 100 },
  { mode: "country", country: "England", type: "cup", names: ["FA Cup", "League Cup", "EFL Cup"], display: "England - Cup", region: "general", priority: 74 },

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

  // Portugal
  { mode: "country", country: "Portugal", type: "league", names: ["Primeira Liga", "Liga Portugal Betclic"], display: "Primeira Liga", region: "general", priority: 89 },
  { mode: "country", country: "Portugal", type: "cup", names: ["Taça de Portugal"], display: "Taça de Portugal", region: "general", priority: 68 },

  // Brasil
  { mode: "country", country: "Brazil", type: "league", names: ["Serie A", "Brasileirão Série A", "Campeonato Brasileiro Série A"], display: "Brasileirão Série A", region: "brazil", priority: 94 },
  { mode: "country", country: "Brazil", type: "league", names: ["Serie B", "Brasileirão Série B", "Campeonato Brasileiro Série B"], display: "Brasileirão Série B", region: "brazil", priority: 88 },
  { mode: "country", country: "Brazil", type: "cup", names: ["Copa do Brasil"], display: "Copa do Brasil", region: "brazil", priority: 91 },

  // Argentina
  { mode: "country", country: "Argentina", type: "league", names: ["Liga Profesional Argentina", "Primera División"], display: "Liga Profesional Argentina", region: "general", priority: 84 },

  // EUA / México
  { mode: "country", country: "USA", type: "league", names: ["Major League Soccer", "MLS"], display: "MLS", region: "general", priority: 80 },
  { mode: "country", country: "Mexico", type: "league", names: ["Liga MX"], display: "Liga MX", region: "general", priority: 79 },

  // Turquia / Grécia / Saudi / Dinamarca
  { mode: "country", country: "Turkey", type: "league", names: ["Süper Lig", "Super Lig"], display: "Super Lig", region: "general", priority: 78 },
  { mode: "country", country: "Greece", type: "league", names: ["Super League 1", "Super League"], display: "Super League Greece", region: "general", priority: 77 },
  { mode: "country", country: "Saudi Arabia", type: "league", names: ["Pro League", "Saudi Pro League"], display: "Saudi Pro League", region: "general", priority: 76 },
  { mode: "country", country: "Denmark", type: "league", names: ["Superliga"], display: "Superliga", region: "general", priority: 75 },

  // UEFA / CONMEBOL
  { mode: "search", search: "UEFA Champions League", display: "UEFA Champions League", region: "general", priority: 99 },
  { mode: "search", search: "UEFA Europa League", display: "UEFA Europa League", region: "general", priority: 93 },
  { mode: "search", search: "UEFA Europa Conference League", display: "UEFA Europa Conference League", region: "general", priority: 87 },
  { mode: "search", search: "CONMEBOL Libertadores", display: "CONMEBOL Libertadores", region: "brazil", priority: 92 },
  { mode: "search", search: "CONMEBOL Sudamericana", display: "CONMEBOL Sudamericana", region: "brazil", priority: 86 },
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

function makeApiCacheKey(path, params = {}) {
  return `${path}?${Object.entries(params)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join("&")}`
}

async function api(path, params = {}) {
  const cacheKey = makeApiCacheKey(path, params)
  if (apiCache.has(cacheKey)) return apiCache.get(cacheKey)

  const url = new URL(API + path)
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") {
      url.searchParams.set(k, String(v))
    }
  })

  await sleep(REQUEST_DELAY_MS)

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
  if (c === "Saudi Arabia" && (name === "Pro League" || name === "Saudi Pro League")) return "Saudi Pro League"
  if (c === "Denmark" && name === "Superliga") return "Superliga"

  if (name === "UEFA Champions League") return "UEFA Champions League"
  if (name === "UEFA Europa League") return "UEFA Europa League"
  if (name === "UEFA Europa Conference League") return "UEFA Europa Conference League"
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
  if (name === "CONMEBOL Libertadores") return 92
  if (name === "Copa do Brasil") return 91
  if (name === "Eredivisie") return 90
  if (name === "Primeira Liga") return 89
  if (name === "Brasileirão Série B") return 88
  if (name === "UEFA Europa Conference League") return 87
  if (name === "CONMEBOL Sudamericana") return 86
  return 70
}

async function resolveCountryCompetitions(target) {
  const leagues = await api("/leagues", {
    country: target.country,
    current: true,
  })

  const normalizedNames = new Set(target.names.map((x) => x.toLowerCase()))

  return leagues
    .filter((item) => {
      const rawName = String(item?.league?.name || "").toLowerCase()
      const leagueType = String(item?.league?.type || "").toLowerCase()
      const seasonCurrent = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]

      if (!seasonCurrent) return false
      if (target.type && leagueType !== target.type) return false

      return Array.from(normalizedNames).some((n) => rawName === n)
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
      }
    })
}

async function resolveSearchCompetition(target) {
  const leagues = await api("/leagues", {
    search: target.search,
    current: true,
  })

  const searchNeedle = String(target.search || "").toLowerCase().trim()

  return leagues
    .map((item) => {
      const currentSeason = item?.seasons?.find((s) => s.current) || item?.seasons?.[0]
      if (!currentSeason) return null

      const country = item?.country?.name || null
      const rawName = item?.league?.name || null

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
    .filter((x) => {
      const haystack = `${x.country || ""} ${x.rawName || ""}`.toLowerCase().trim()
      return haystack.includes(searchNeedle)
    })
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

function buildWindowDates() {
  const now = new Date()
  const end = new Date(now.getTime() + WINDOW_HOURS * 60 * 60 * 1000)

  const dates = new Set()
  const cursor = new Date(now)

  while (cursor <= end) {
    dates.add(isoDate(cursor))
    cursor.setUTCDate(cursor.getUTCDate() + 1)
  }

  return Array.from(dates)
}

async function fetchFixturesForCompetition(comp) {
const today = new Date()

const dates = [
  new Date(today.getTime() - 24 * 60 * 60 * 1000),
  today,
  new Date(today.getTime() + 24 * 60 * 60 * 1000)
].map(d => d.toISOString().split('T')[0])
  const all = []

  for (const date of dates) {
    try {
      console.log("comp:", comp)
      const fixtures = await api("/fixtures", {
        league: comp.leagueId,
        season: comp.season,
        date,
        timezone: TIMEZONE,
      })
  console.log("DATA:", date, "LEAGUE:", comp.display, "FIXTURES:", fixtures.length)
      
      for (const fixture of fixtures) {
        const kickoff = fixture?.fixture?.date
        if (!kickoff) continue

        const dt = new Date(kickoff)
        const now = new Date()
        const end = new Date(now.getTime() + WINDOW_HOURS * 60 * 60 * 1000)

        if (dt <= end) {
          all.push({
            ...fixture,
            __comp: comp,
          })
        }
      }
    } catch (error) {
      console.error(`Falha buscando fixtures de ${comp.display} em ${date}:`, error.message)
    }
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

async function buildTeamRecentProfile(teamId, leagueId, season, beforeIso) {
  const cacheKey = `${teamId}:${leagueId}:${season}:${toDateOnly(beforeIso)}`
  if (teamProfileCache.has(cacheKey)) return teamProfileCache.get(cacheKey)

  const fixtures = await api("/fixtures", {
    team: teamId,
    league: leagueId,
    season,
    last: 10,
    timezone: TIMEZONE,
  })

  const beforeDate = new Date(beforeIso)

  const completed = fixtures
    .filter(isCompletedFixture)
    .filter((f) => {
      const d = new Date(f?.fixture?.date)
      return !Number.isNaN(d.getTime()) && d < beforeDate
    })
    .sort((a, b) => new Date(b.fixture.date) - new Date(a.fixture.date))
    .slice(0, 5)

  const goalsFor = []
  const goalsAgainst = []
  const corners = []
  const shots = []
  const shotsOnTarget = []
  const cards = []
  const form = []

  for (const game of completed) {
    const fixtureId = game?.fixture?.id
    const isHome = game?.teams?.home?.id === teamId

    const gf = isHome ? safeNumber(game?.goals?.home) : safeNumber(game?.goals?.away)
    const ga = isHome ? safeNumber(game?.goals?.away) : safeNumber(game?.goals?.home)

    goalsFor.push(gf)
    goalsAgainst.push(ga)

    if (gf > ga) form.push("W")
    else if (gf === ga) form.push("D")
    else form.push("L")

    const stats = await getFixtureStatistics(fixtureId)
    const teamStats = stats.find((s) => s?.team?.id === teamId)

    if (teamStats?.statistics) {
      corners.push(extractStatValue(teamStats.statistics, "Corner Kicks"))
      shots.push(extractStatValue(teamStats.statistics, "Total Shots"))
      shotsOnTarget.push(extractStatValue(teamStats.statistics, "Shots on Goal"))
      cards.push(extractStatValue(teamStats.statistics, "Yellow Cards"))
    }
  }

  const points = form.reduce((acc, cur) => {
    if (cur === "W") return acc + 3
    if (cur === "D") return acc + 1
    return acc
  }, 0)

  const profile = {
    games: completed.length,
    goalsFor: round(avg(goalsFor), 2),
    goalsAgainst: round(avg(goalsAgainst), 2),
    corners: round(avg(corners), 2),
    shots: round(avg(shots), 2),
    shotsOnTarget: round(avg(shotsOnTarget), 2),
    cards: round(avg(cards), 2),
    formString: form.join(" "),
    formPointsPct: completed.length ? round(points / (completed.length * 3), 4) : 0,
  }

  teamProfileCache.set(cacheKey, profile)
  return profile
}
function computeMatchMetrics(homeProfile, awayProfile) {

  const expectedHomeGoals = clamp(((homeProfile.goalsFor + awayProfile.goalsAgainst) / 2) * 1.05, 0.2, 4.2)
  const expectedAwayGoals = clamp(((awayProfile.goalsFor + homeProfile.goalsAgainst) / 2) * 0.95, 0.2, 4.2)

  const expectedHomeShots = clamp(((homeProfile.shots + awayProfile.shots) / 2) * 1.02, 4, 25)
  const expectedAwayShots = clamp(((awayProfile.shots + homeProfile.shots) / 2) * 0.98, 4, 25)

  const expectedHomeSOT = clamp(((homeProfile.shotsOnTarget + awayProfile.shotsOnTarget) / 2) * 1.02, 1.5, 12)
  const expectedAwaySOT = clamp(((awayProfile.shotsOnTarget + homeProfile.shotsOnTarget) / 2) * 0.98, 1.5, 12)

  const expectedCorners = clamp(homeProfile.corners + awayProfile.corners, 4, 16)
  const expectedCards = clamp(homeProfile.cards + awayProfile.cards, 1.5, 8)

  const avgGoals = round(expectedHomeGoals + expectedAwayGoals, 2)
  const avgCorners = round(expectedCorners, 2)
  const avgShots = round(expectedHomeShots + expectedAwayShots, 2)
  const expectedTotalSOT = round(expectedHomeSOT + expectedAwaySOT, 2)

  const homeStrengthRaw =
    homeProfile.goalsFor * 1.3 +
    homeProfile.shotsOnTarget * 0.35 +
    homeProfile.corners * 0.12 +
    homeProfile.formPointsPct * 1.2 -
    homeProfile.goalsAgainst * 0.45

  const awayStrengthRaw =
    awayProfile.goalsFor * 1.25 +
    awayProfile.shotsOnTarget * 0.33 +
    awayProfile.corners * 0.10 +
    awayProfile.formPointsPct * 1.1 -
    awayProfile.goalsAgainst * 0.42

  const powerHome = round(clamp(homeStrengthRaw, 0.2, 5), 4)
  const powerAway = round(clamp(awayStrengthRaw, 0.2, 5), 4)

  const totalPower = Math.max(powerHome + powerAway, 0.01)

  let homeWinProb = powerHome / totalPower
  let awayWinProb = powerAway / totalPower

  let drawProb = clamp(
    0.18 + (1 - Math.abs(homeWinProb - awayWinProb)) * 0.12,
    0.18,
    0.32
  )

  const remain = 1 - drawProb
  const pair = Math.max(homeWinProb + awayWinProb, 0.01)

  homeWinProb = (homeWinProb / pair) * remain
  awayWinProb = (awayWinProb / pair) * remain

  const over15Prob = clamp(avgGoals / 2.7, 0.28, 0.95)
  const over25Prob = clamp((avgGoals - 1.2) / 1.8, 0.10, 0.90)
  const under25Prob = clamp(1 - over25Prob, 0.10, 0.90)
  const under35Prob = clamp(1 - ((avgGoals - 1.8) / 2.0), 0.15, 0.95)

  const bttsProb = clamp(
    ((expectedHomeGoals - 0.4) + (expectedAwayGoals - 0.4)) / 3.2,
    0.12,
    0.88
  )

  const cornersOver85Prob = clamp(avgCorners / 10.8, 0.15, 0.92)

  const probShots = clamp(avgShots / 24, 0.20, 0.90)
  const probSot = clamp(expectedTotalSOT / 8.5, 0.20, 0.90)
  const probCards = clamp(expectedCards / 5.2, 0.20, 0.85)

  let gameProfile = "Jogo equilibrado"

  if (avgGoals >= 2.8 && avgShots >= 24) {
    gameProfile = "Jogo de gols"
  } else if (avgCorners >= 10.2) {
    gameProfile = "Jogo de escanteios"
  } else if (avgGoals <= 2.0 && avgShots <= 18) {
    gameProfile = "Jogo travado"
  }

  const confidenceScore = round(
    clamp(
      over25Prob * 40 +
      bttsProb * 20 +
      cornersOver85Prob * 20 +
      Math.max(homeWinProb, awayWinProb) * 20,
      0,
      100
    ),
    2
  )

  const safePick =
    avgGoals >= 2.15
      ? "Mais de 1.5 gols"
      : avgCorners >= 8.7
      ? "Mais de 7.5 escanteios"
      : homeWinProb >= 0.58
      ? "Dupla chance mandante ou empate"
      : awayWinProb >= 0.58
      ? "Dupla chance visitante ou empate"
      : "Menos de 3.5 gols"

  const balancedPick =
    over25Prob >= 0.66 && avgGoals >= 2.5
      ? "Mais de 2.5 gols"
      : bttsProb >= 0.61
      ? "Ambas marcam"
      : cornersOver85Prob >= 0.67
      ? "Mais de 8.5 escanteios"
      : homeWinProb >= 0.68
      ? "Vitória do mandante"
      : awayWinProb >= 0.68
      ? "Vitória do visitante"
      : "Mais de 1.5 gols"

  const aggressivePick =
    avgGoals >= 3.1
      ? "Mais de 3.5 gols"
      : cornersOver85Prob >= 0.76 && avgCorners >= 10.8
      ? "Mais de 9.5 escanteios"
      : homeWinProb >= 0.72
      ? "Vitória do mandante"
      : awayWinProb >= 0.72
      ? "Vitória do visitante"
      : bttsProb >= 0.64
      ? "Ambas marcam"
      : "Ambas não marcam"

  const valuePick =
    avgCorners >= 10.4
      ? "Mais de 9.5 escanteios"
      : avgShots >= 24
      ? "Mais de 10.5 finalizações"
      : avgGoals >= 2.7
      ? "Mais de 2.5 gols"
      : "Mais de 8.5 escanteios"

  let analysisText =
    "O confronto apresenta equilíbrio estatístico, com oportunidades distribuídas entre mercados de gols e escanteios."

  if (gameProfile === "Jogo de gols") {
    analysisText =
      "As equipes apresentam média ofensiva elevada e bom volume de finalizações, indicando tendência favorável para mercados de gols."
  }

  if (gameProfile === "Jogo de escanteios") {
    analysisText =
      "O confronto apresenta alto volume ofensivo e tendência de geração de escanteios, sugerindo valor nas linhas de corners."
  }

  if (gameProfile === "Jogo travado") {
    analysisText =
      "Os números recentes indicam um jogo mais controlado e com menor produção ofensiva."
  }

  return {
    avgGoals,
    avgCorners,
    avgShots,

    expectedHomeGoals: round(expectedHomeGoals, 2),
    expectedAwayGoals: round(expectedAwayGoals, 2),

    expectedHomeShots: round(expectedHomeShots, 2),
    expectedAwayShots: round(expectedAwayShots, 2),

    expectedHomeSOT: round(expectedHomeSOT, 2),
    expectedAwaySOT: round(expectedAwaySOT, 2),

    expectedCards: round(expectedCards, 2),

    powerHome,
    powerAway,

    homeWinProb: round(homeWinProb, 4),
    drawProb: round(drawProb, 4),
    awayWinProb: round(awayWinProb, 4),

    over15Prob: round(over15Prob, 4),
    over25Prob: round(over25Prob, 4),
    under25Prob: round(under25Prob, 4),
    under35Prob: round(under35Prob, 4),

    bttsProb: round(bttsProb, 4),
    cornersOver85Prob: round(cornersOver85Prob, 4),

    probShots: round(probShots, 4),
    probSot: round(probSot, 4),
    probCards: round(probCards, 4),

    gameProfile,
    confidenceScore,

    safePick,
    balancedPick,
    aggressivePick,
    valuePick,

    analysisText
  }
}
function buildPrimaryMarket(metrics) {
  return metrics.balancedPick
}

function buildPrimaryProbability(metrics) {
  const market = metrics.balancedPick

  if (market === "Mais de 2.5 gols") return metrics.over25Prob
  if (market === "Mais de 1.5 gols") return metrics.over15Prob
  if (market === "Ambas marcam") return metrics.bttsProb
  if (market.includes("8.5 escanteios") || market.includes("9.5 escanteios")) return metrics.cornersOver85Prob
  if (market === "Vitória do mandante") return metrics.homeWinProb
  if (market === "Vitória do visitante") return metrics.awayWinProb
  if (market.includes("Dupla chance")) return round(Math.max(metrics.homeWinProb, metrics.awayWinProb) + metrics.drawProb, 4)
  if (market === "Menos de 3.5 gols") return metrics.under35Prob

  return 0.60
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

  if (
    /silkeborg|vejle|brondby|midtjylland|fc copenhagen|nordsjaelland/i.test(teams)
  ) {
    leagueDisplay = "Superliga"
    country = "Denmark"
  }

  if (
    /olympiacos|paok|panathinaikos|aek athens|aris|volos|kifisia/i.test(teams)
  ) {
    leagueDisplay = "Super League Greece"
    country = "Greece"
  }

  if (
    /al nassr|al hilal|al ittihad|al ahli|al shabab|al taawoun|al ettifaq/i.test(teams)
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

  if ((leagueNameRaw === "Süper Lig" || leagueNameRaw === "Super Lig") && country === "Turkey") {
    leagueDisplay = "Super Lig"
  }

  if (leagueNameRaw === "Superliga" && country === "Denmark") {
    leagueDisplay = "Superliga"
  }

  if ((leagueNameRaw === "Super League 1" || leagueNameRaw === "Super League") && country === "Greece") {
    leagueDisplay = "Super League Greece"
  }

  return { leagueDisplay, country }
}

async function upsertMatchAndAnalysis(fixture) {
  const fixtureId = fixture?.fixture?.id
  const kickoff = fixture?.fixture?.date
  const homeTeamId = fixture?.teams?.home?.id
  const awayTeamId = fixture?.teams?.away?.id
  const comp = fixture.__comp

  const homeProfile = await buildTeamRecentProfile(homeTeamId, comp.leagueId, comp.season, kickoff)
  const awayProfile = await buildTeamRecentProfile(awayTeamId, comp.leagueId, comp.season, kickoff)

  const metrics = computeMatchMetrics(homeProfile, awayProfile)
  const { leagueDisplay, country } = normalizeLeagueByTeams(comp, fixture)

  const matchRow = {
    id: fixtureId,
    home_team: fixture?.teams?.home?.name || null,
    away_team: fixture?.teams?.away?.name || null,
    league: leagueDisplay,
    League: leagueDisplay,
    country: country,
    match_date: toDateOnly(kickoff),
    kickoff,
    Kickoff: kickoff,
    home_logo: fixture?.teams?.home?.logo || null,
    away_logo: fixture?.teams?.away?.logo || null,

    avg_goals: metrics.avgGoals,
    avg_corners: metrics.avgCorners,
    avg_shots: metrics.avgShots,

    home_win_prob: metrics.homeWinProb,
    draw_prob: metrics.drawProb,
    away_win_prob: metrics.awayWinProb,

    home_result_prob: metrics.homeWinProb,
    draw_result_prob: metrics.drawProb,
    away_result_prob: metrics.awayWinProb,

    power_home: metrics.powerHome,
    power_away: metrics.powerAway,

    over15_prob: metrics.over15Prob,
    over25_prob: metrics.over25Prob,
    under25_prob: metrics.under25Prob,
    under35_prob: metrics.under35Prob,
    btts_prob: metrics.bttsProb,
    corners_over85_prob: metrics.cornersOver85Prob,

    home_form: homeProfile.formString || null,
    away_form: awayProfile.formString || null,

    pick: buildPrimaryMarket(metrics),

    game_profile: metrics.gameProfile,
    confidence_score: metrics.confidenceScore,
    safe_pick: metrics.safePick,
    balanced_pick: metrics.balancedPick,
    aggressive_pick: metrics.aggressivePick,
    value_pick: metrics.valuePick,
    analysis_text: metrics.analysisText,
  }

  const analysisRow = {
    match_id: fixtureId,
    expected_home_goals: metrics.expectedHomeGoals,
    expected_away_goals: metrics.expectedAwayGoals,
    expected_home_shots: metrics.expectedHomeShots,
    expected_away_shots: metrics.expectedAwayShots,
    expected_home_sot: metrics.expectedHomeSOT,
    expected_away_sot: metrics.expectedAwaySOT,
    expected_corners: metrics.avgCorners,
    expected_cards: metrics.expectedCards,
    prob_over25: metrics.over25Prob,
    prob_btts: metrics.bttsProb,
    prob_corners: metrics.cornersOver85Prob,
    prob_shots: metrics.probShots,
    prob_sot: metrics.probSot,
    prob_cards: metrics.probCards,
    best_pick_1: metrics.safePick,
    best_pick_2: metrics.balancedPick,
    best_pick_3: metrics.aggressivePick,
  }

  const statsRow = {
    match_id: fixtureId,
    home_shots: round(homeProfile.shots, 2),
    home_shots_on_target: round(homeProfile.shotsOnTarget, 2),
    home_corners: round(homeProfile.corners, 2),
    home_yellow_cards: round(homeProfile.cards, 2),
    away_shots: round(awayProfile.shots, 2),
    away_shots_on_target: round(awayProfile.shotsOnTarget, 2),
    away_corners: round(awayProfile.corners, 2),
    away_yellow_cards: round(awayProfile.cards, 2),
  }

  const { error: matchError } = await supabase
    .from("matches")
    .upsert(matchRow, { onConflict: "id" })

  if (matchError) throw new Error(`Supabase matches: ${matchError.message}`)

  const { error: analysisError } = await supabase
    .from("match_analysis")
    .upsert(analysisRow, { onConflict: "match_id" })

  if (analysisError) throw new Error(`Supabase match_analysis: ${analysisError.message}`)

  const { error: statsError } = await supabase
    .from("match_stats")
    .upsert(statsRow, { onConflict: "match_id" })

  if (statsError) throw new Error(`Supabase match_stats: ${statsError.message}`)

  return {
    fixtureId,
    league: leagueDisplay,
    market: buildPrimaryMarket(metrics),
    probability: buildPrimaryProbability(metrics),
    homeTeam: matchRow.home_team,
    awayTeam: matchRow.away_team,
    homeLogo: matchRow.home_logo,
    awayLogo: matchRow.away_logo,
    kickoff: matchRow.kickoff,
    region: comp.region,
    priority: comp.priority || leagueScorePriority(leagueDisplay),
  }
}

function uniqBy(arr, keyFn) {
  const seen = new Set()
  return arr.filter((item) => {
    const key = keyFn(item)
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

/*
async function clearFutureWindow() {
  const nowIso = new Date().toISOString()
  const endIso = new Date(Date.now() + WINDOW_HOURS * 60 * 60 * 1000).toISOString()

  const { data: rows, error: selectError } = await supabase
    .from("matches")
    .select("id")
    .gte("kickoff", nowIso)
    .lte("kickoff", endIso)
}

  if (selectError) throw new Error(`Supabase select matches window: ${selectError.message}`)

  const ids = (rows || []).map((x) => x.id)
  if (!ids.length) return 0

  const { error: dailyError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("match_id", -1)

  if (dailyError) throw new Error(`Supabase delete daily_picks window: ${dailyError.message}`)

  const { error: analysisError } = await supabase
    .from("match_analysis")
    .delete()
    .in("match_id", ids)

  if (analysisError) throw new Error(`Supabase delete match_analysis window: ${analysisError.message}`)

  const { error: statsError } = await supabase
    .from("match_stats")
    .delete()
    .in("match_id", ids)

  if (statsError) throw new Error(`Supabase delete match_stats window: ${statsError.message}`)

  const { error: matchesError } = await supabase
    .from("matches")
    .delete()
    .in("id", ids)

  if (matchesError) throw new Error(`Supabase delete matches window: ${matchesError.message}`)

  return ids.length
}
/*
async function rebuildDailyPicks(candidates) {
  const future = candidates
    .filter((x) => x.kickoff)
    .filter((x) => new Date(x.kickoff) > new Date())
    .sort((a, b) => new Date(a.kickoff) - new Date(b.kickoff))

  const uniquePerLeague = uniqBy(
    future.sort((a, b) => {
      if (b.probability !== a.probability) return b.probability - a.probability
      if (b.priority !== a.priority) return b.priority - a.priority
      return new Date(a.kickoff) - new Date(b.kickoff)
    }),
    (x) => x.league
  )

  const brazilCandidate = uniquePerLeague
    .filter((x) => x.region === "brazil")
    .sort((a, b) => b.probability - a.probability)[0] || null

  const withoutBrazilDup = uniquePerLeague.filter((x) => x.fixtureId !== brazilCandidate?.fixtureId)

  const generalTop = withoutBrazilDup
    .sort((a, b) => {
      if (b.probability !== a.probability) return b.probability - a.probability
      if (b.priority !== a.priority) return b.priority - a.priority
      return new Date(a.kickoff) - new Date(b.kickoff)
    })
    .slice(0, 5)

  const pool = brazilCandidate
    ? [brazilCandidate, ...generalTop].slice(0, 6)
    : generalTop.slice(0, 6)

  const finalSorted = pool
    .sort((a, b) => {
      if (b.probability !== a.probability) return b.probability - a.probability
      if (b.priority !== a.priority) return b.priority - a.priority
      return new Date(a.kickoff) - new Date(b.kickoff)
    })
    .slice(0, 6)

  const rows = finalSorted.map((item, index) => ({
    rank: index,
    match_id: item.fixtureId,
    home_team: item.homeTeam,
    away_team: item.awayTeam,
    league: item.league,
    market: item.market,
    probability: round(item.probability, 4),
    is_opportunity: index === 0,
    created_at: new Date().toISOString(),
    home_logo: item.homeLogo || null,
    away_logo: item.awayLogo || null,
  }))

  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("match_id", -1)

  if (deleteError) throw new Error(`Supabase delete daily_picks: ${deleteError.message}`)

  if (!rows.length) return 0

  const { error: insertError } = await supabase
    .from("daily_picks")
    .insert(rows)

  if (insertError) throw new Error(`Supabase insert daily_picks: ${insertError.message}`)

  return rows.length
}
*/
async function run() {
  console.log("🚀 Scoutly Sync V3 iniciado")

  const competitions = await resolveTargetCompetitions()
console.log("COMPETITIONS:", competitions)

const fixtureLists = await Promise.all(
  competitions.map((comp) => fetchFixturesForCompetition(comp))
)

console.log("FIXTURE LISTS:", fixtureLists)

const allFixtures = uniqBy(
  fixtureLists.flat(),
  (x) => x?.fixture?.id
)

console.log("ALL FIXTURES:", allFixtures)
console.log(`📅 Fixtures na janela de ${WINDOW_HOURS}h: ${allFixtures.length}`)

  const cleared = 0 //await clearFutureWindow()
  console.log(`🧹 Matches futuros limpos antes do rebuild: ${cleared}`)

  const candidates = []

  for (const fixture of allFixtures) {
    try {
      const candidate = await upsertMatchAndAnalysis(fixture)
      candidates.push(candidate)
      console.log(`✅ ${candidate.league} | ${candidate.homeTeam} x ${candidate.awayTeam}`)
    } catch (error) {
      console.error(`❌ Falha processando fixture ${fixture?.fixture?.id}:`, error.message)
    }
  }

  const picksCount = 0 // await rebuildDailyPicks(candidates)
  console.log(`🏁 Daily picks gerados: ${picksCount}`)
  console.log("✅ Scoutly Sync V3 concluído")
}

run().catch((error) => {
  console.error("❌ Erro fatal no Scoutly Sync V3:", error)
  process.exit(1)
})
