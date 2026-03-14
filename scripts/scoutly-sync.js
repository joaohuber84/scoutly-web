const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY

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

const API_BASE = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

// ligas principais / mais confiáveis para o Scoutly
const LEAGUES = [
  { id: 39, season: 2025, name: "Premier League", country: "England" },
  { id: 140, season: 2025, name: "La Liga", country: "Spain" },
  { id: 135, season: 2025, name: "Serie A", country: "Italy" },
  { id: 78, season: 2025, name: "Bundesliga", country: "Germany" },
  { id: 61, season: 2025, name: "Ligue 1", country: "France" },
  { id: 94, season: 2025, name: "Liga Portugal", country: "Portugal" },
  { id: 128, season: 2025, name: "Liga Profesional Argentina", country: "Argentina" },
  { id: 71, season: 2025, name: "Brasileirão Série A", country: "Brazil" },
  { id: 88, season: 2025, name: "Eredivisie", country: "Netherlands" },
  { id: 203, season: 2025, name: "Super Lig", country: "Turkey" }
]

function isoDate(date) {
  return date.toISOString().slice(0, 10)
}

function toNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function safeText(value, fallback = "") {
  return typeof value === "string" && value.trim() ? value.trim() : fallback
}

function getInitials(name) {
  const clean = safeText(name)
  if (!clean) return "?"
  const parts = clean.split(" ").filter(Boolean)
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase()
  return (parts[0][0] + parts[1][0]).toUpperCase()
}

async function apiGet(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`)

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value))
    }
  }

  const response = await fetch(url.toString(), {
    headers: {
      "x-apisports-key": APISPORTS_KEY
    }
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`API-Football erro ${response.status}: ${text}`)
  }

  return response.json()
}

async function fetchLeagueFixtures(leagueId, season, from, to) {
  const data = await apiGet("/fixtures", {
    league: leagueId,
    season,
    from,
    to,
    timezone: TIMEZONE,
    status: "NS"
  })

  return Array.isArray(data.response) ? data.response : []
}

async function fetchFixtureStats(fixtureId) {
  try {
    const data = await apiGet("/fixtures/statistics", {
      fixture: fixtureId
    })

    return Array.isArray(data.response) ? data.response : []
  } catch (error) {
    console.error(`Erro ao buscar stats da fixture ${fixtureId}:`, error.message)
    return []
  }
}

function parseTeamStats(teamStats = []) {
  const map = {}

  for (const item of teamStats) {
    const type = item?.type
    const value = item?.value

    if (!type) continue

    if (value === null || value === undefined) {
      map[type] = 0
      continue
    }

    if (typeof value === "string") {
      const cleaned = value.replace("%", "").trim()
      const asNumber = Number(cleaned)
      map[type] = Number.isFinite(asNumber) ? asNumber : 0
      continue
    }

    map[type] = Number.isFinite(Number(value)) ? Number(value) : 0
  }

  return map
}

function buildMatchRow(fixture) {
  const fixtureId = fixture?.fixture?.id
  const kickoffRaw = fixture?.fixture?.date || null
  const leagueName =
    safeText(fixture?.league?.name) || "Liga não informada"

  const homeTeam = safeText(fixture?.teams?.home?.name, "Mandante")
  const awayTeam = safeText(fixture?.teams?.away?.name, "Visitante")

  const homeLogo = safeText(fixture?.teams?.home?.logo)
  const awayLogo = safeText(fixture?.teams?.away?.logo)

  const kickoff = kickoffRaw ? new Date(kickoffRaw).toISOString() : null
  const matchDate = kickoff ? kickoff.slice(0, 10) : null

  return {
    id: fixtureId,
    home_team: homeTeam,
    away_team: awayTeam,
    league: leagueName,
    match_date: matchDate,
    kickoff,
    home_logo: homeLogo,
    away_logo: awayLogo,

    // valores base neutros — o Brain recalcula depois
    avg_goals: null,
    avg_corners: null,
    avg_shots: null,
    insight: null,
    home_win_prob: null,
    draw_prob: null,
    away_win_prob: null,
    home_form: null,
    away_form: null,
    over25_prob: null,
    btts_prob: null,
    corners_over85_prob: null,
    pick: null,
    power_home: null,
    power_away: null,
    home_result_prob: null,
    draw_result_prob: null,
    away_result_prob: null,
    market_odds_over25: null,
    market_odds_btts: null,
    market_odds_corners85: null,
    over15_prob: null,
    under25_prob: null,
    under35_prob: null
  }
}

function buildStatsRow(fixtureId, statsResponse) {
  const home = statsResponse[0] || {}
  const away = statsResponse[1] || {}

  const homeStats = parseTeamStats(home.statistics || [])
  const awayStats = parseTeamStats(away.statistics || [])

  const homeShots =
    toNumber(homeStats["Total Shots"]) ||
    toNumber(homeStats["Shots on Goal"]) + toNumber(homeStats["Shots off Goal"])

  const awayShots =
    toNumber(awayStats["Total Shots"]) ||
    toNumber(awayStats["Shots on Goal"]) + toNumber(awayStats["Shots off Goal"])

  return {
    match_id: fixtureId,
    home_shots: toNumber(homeShots),
    home_shots_on_target: toNumber(homeStats["Shots on Goal"]),
    home_corners: toNumber(homeStats["Corner Kicks"]),
    home_yellow_cards: toNumber(homeStats["Yellow Cards"]),

    away_shots: toNumber(awayShots),
    away_shots_on_target: toNumber(awayStats["Shots on Goal"]),
    away_corners: toNumber(awayStats["Corner Kicks"]),
    away_yellow_cards: toNumber(awayStats["Yellow Cards"])
  }
}

async function upsertMatches(rows) {
  if (!rows.length) return

  const { error } = await supabase
    .from("matches")
    .upsert(rows, { onConflict: "id" })

  if (error) throw error
}

async function upsertMatchStats(rows) {
  if (!rows.length) return

  const { error } = await supabase
    .from("match_stats")
    .upsert(rows, { onConflict: "match_id" })

  if (error) throw error
}

async function cleanupOldData(todayIso) {
  // pega jogos antigos
  const { data: oldMatches, error: selectError } = await supabase
    .from("matches")
    .select("id")
    .lt("match_date", todayIso)

  if (selectError) throw selectError

  if (!oldMatches || !oldMatches.length) return

  const oldIds = oldMatches.map((m) => m.id).filter(Boolean)

  // ordem correta por causa da FK
  const { error: dailyPicksError } = await supabase
    .from("daily_picks")
    .delete()
    .in("match_id", oldIds)

  if (dailyPicksError) throw dailyPicksError

  const { error: analysisError } = await supabase
    .from("match_analysis")
    .delete()
    .in("match_id", oldIds)

  if (analysisError) throw analysisError

  const { error: statsError } = await supabase
    .from("match_stats")
    .delete()
    .in("match_id", oldIds)

  if (statsError) throw statsError

  const { error: matchesError } = await supabase
    .from("matches")
    .delete()
    .in("id", oldIds)

  if (matchesError) throw matchesError
}

async function runScoutlySync() {
  console.log("🚀 Scoutly Sync iniciado...")

  const today = new Date()
  const end = new Date()
  end.setDate(today.getDate() + 3)

  const from = isoDate(today)
  const to = isoDate(end)

  const allFixtures = []

  for (const league of LEAGUES) {
    try {
      const fixtures = await fetchLeagueFixtures(
        league.id,
        league.season,
        from,
        to
      )

      for (const fixture of fixtures) {
        allFixtures.push(fixture)
      }
    } catch (error) {
      console.error(`Erro ao buscar fixtures da liga ${league.name}:`, error.message)
    }
  }

  // deduplicação por fixture id
  const uniqueFixturesMap = new Map()
  for (const fixture of allFixtures) {
    const fixtureId = fixture?.fixture?.id
    if (fixtureId) uniqueFixturesMap.set(fixtureId, fixture)
  }

  const uniqueFixtures = [...uniqueFixturesMap.values()]

  const matchRows = uniqueFixtures
    .map(buildMatchRow)
    .filter((row) => row.id && row.home_team && row.away_team)

  await upsertMatches(matchRows)
  console.log(`✅ Matches sincronizados: ${matchRows.length}`)

  const statsRows = []

  for (const fixture of uniqueFixtures) {
    const fixtureId = fixture?.fixture?.id
    if (!fixtureId) continue

    const statsResponse = await fetchFixtureStats(fixtureId)
    const statsRow = buildStatsRow(fixtureId, statsResponse)
    statsRows.push(statsRow)
  }

  await upsertMatchStats(statsRows)
  console.log(`✅ Match stats garantidos: ${statsRows.length}`)

  await cleanupOldData(from)
  console.log("✅ Limpeza de dados antigos concluída")

  console.log("🏁 Scoutly Sync finalizado com sucesso.")
}

runScoutlySync().catch((error) => {
  console.error("❌ Erro no Scoutly Sync:", error)
  process.exit(1)
})
