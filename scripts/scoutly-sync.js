const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY

if (!APISPORTS_KEY) {
  throw new Error("APISPORTS_KEY não encontrada nas variáveis de ambiente.")
}

if (!SUPABASE_URL) {
  throw new Error("SUPABASE_URL não encontrada nas variáveis de ambiente.")
}

if (!SUPABASE_KEY) {
  throw new Error(
    "SUPABASE_SERVICE_ROLE_KEY não encontrada nas variáveis de ambiente."
  )
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API_BASE = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

/**
 * IMPORTANTE:
 * Aqui eu estou forçando nomes únicos de liga para evitar
 * confusão no front entre Premier League / Super League / Pro League.
 */
const LEAGUES = [
  { id: 39, season: 2025, leagueLabel: "England Premier League" },
  { id: 140, season: 2025, leagueLabel: "Spain La Liga" },
  { id: 78, season: 2025, leagueLabel: "Germany Bundesliga" },
  { id: 135, season: 2025, leagueLabel: "Italy Serie A" },
  { id: 61, season: 2025, leagueLabel: "France Ligue 1" },
  { id: 88, season: 2025, leagueLabel: "Netherlands Eredivisie" },
  { id: 94, season: 2025, leagueLabel: "Portugal Primeira Liga" },
  { id: 71, season: 2025, leagueLabel: "Brazil Serie A" },
  { id: 128, season: 2025, leagueLabel: "Argentina Liga Profesional" },
  { id: 203, season: 2025, leagueLabel: "Turkey Super Lig" },
  { id: 218, season: 2025, leagueLabel: "Austria Bundesliga" },

  // ligas que estavam aparecendo bagunçadas / genéricas
  { id: 307, season: 2025, leagueLabel: "Russia Premier League" },
  { id: 244, season: 2025, leagueLabel: "Bangladesh Premier League" },
  { id: 233, season: 2025, leagueLabel: "Egypt Premier League" },
  { id: 301, season: 2025, leagueLabel: "Kazakhstan Premier League" },
  { id: 197, season: 2025, leagueLabel: "Saudi Pro League" },
  { id: 179, season: 2025, leagueLabel: "Greece Super League" },
  { id: 299, season: 2025, leagueLabel: "Zambia Super League" },
  { id: 186, season: 2025, leagueLabel: "Algeria Ligue 1" }
]

function formatDateInTimezone(date, timeZone = TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(date)

  const year = parts.find((p) => p.type === "year")?.value
  const month = parts.find((p) => p.type === "month")?.value
  const day = parts.find((p) => p.type === "day")?.value

  return `${year}-${month}-${day}`
}

function addDays(date, days) {
  const d = new Date(date)
  d.setDate(d.getDate() + days)
  return d
}

async function apiGet(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`)

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value)
    }
  })

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "x-apisports-key": APISPORTS_KEY
    }
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Erro API-SPORTS ${response.status}: ${text}`)
  }

  const json = await response.json()
  return json.response || []
}

function safeString(value, fallback = null) {
  if (value === undefined || value === null) return fallback
  const text = String(value).trim()
  return text.length ? text : fallback
}

function buildMatchRow(fixture, leagueDef) {
  const fixtureId = fixture?.fixture?.id
  const kickoff = fixture?.fixture?.date || null

  const homeTeam = safeString(fixture?.teams?.home?.name, "Time da casa")
  const awayTeam = safeString(fixture?.teams?.away?.name, "Time visitante")

  const homeLogo = safeString(fixture?.teams?.home?.logo, null)
  const awayLogo = safeString(fixture?.teams?.away?.logo, null)

  const matchDate = kickoff
    ? formatDateInTimezone(new Date(kickoff), TIMEZONE)
    : null

  return {
    id: fixtureId,
    home_team: homeTeam,
    away_team: awayTeam,
    league: leagueDef.leagueLabel,
    match_date: matchDate,
    kickoff,
    home_logo: homeLogo,
    away_logo: awayLogo
  }
}

function buildEmptyMatchStatsRow(matchId) {
  return {
    match_id: matchId,
    home_shots: 0,
    home_shots_on_target: 0,
    home_corners: 0,
    home_yellow_cards: 0,
    away_shots: 0,
    away_shots_on_target: 0,
    away_corners: 0,
    away_yellow_cards: 0
  }
}

async function fetchUpcomingFixtures() {
  const today = new Date()
  const dates = [
    formatDateInTimezone(today, TIMEZONE),
    formatDateInTimezone(addDays(today, 1), TIMEZONE),
    formatDateInTimezone(addDays(today, 2), TIMEZONE)
  ]

  const allRows = []

  for (const league of LEAGUES) {
    for (const date of dates) {
      const fixtures = await apiGet("/fixtures", {
        league: league.id,
        season: league.season,
        date,
        timezone: TIMEZONE
      })

      for (const fixture of fixtures) {
        const statusShort = fixture?.fixture?.status?.short

        // só futuros / não iniciados / adiado
        const allowed = ["NS", "TBD", "PST", "CANC"]
        if (!allowed.includes(statusShort)) continue

        const row = buildMatchRow(fixture, league)
        if (!row.id || !row.home_team || !row.away_team || !row.league) continue

        allRows.push(row)
      }
    }
  }

  const uniqueMap = new Map()

  for (const row of allRows) {
    uniqueMap.set(row.id, row)
  }

  return Array.from(uniqueMap.values()).sort((a, b) => {
    if (!a.kickoff && !b.kickoff) return 0
    if (!a.kickoff) return 1
    if (!b.kickoff) return -1
    return new Date(a.kickoff) - new Date(b.kickoff)
  })
}

async function upsertMatches(matches) {
  if (!matches.length) {
    console.log("Nenhum jogo futuro encontrado para sincronizar.")
    return
  }

  const { error } = await supabase.from("matches").upsert(matches, {
    onConflict: "id"
  })

  if (error) {
    throw error
  }

  console.log(`✅ Matches sincronizados: ${matches.length}`)
}

async function ensureMatchStats(matches) {
  if (!matches.length) return

  const statsRows = matches.map((match) => buildEmptyMatchStatsRow(match.id))

  const { error } = await supabase.from("match_stats").upsert(statsRows, {
    onConflict: "match_id"
  })

  if (error) {
    throw error
  }

  console.log(`✅ Match stats garantidos: ${statsRows.length}`)
}

async function cleanupOldFutureMatches(validIds) {
  const today = formatDateInTimezone(new Date(), TIMEZONE)

  const { data: existing, error: existingError } = await supabase
    .from("matches")
    .select("id, match_date")
    .gte("match_date", today)

  if (existingError) {
    throw existingError
  }

  const idsToDelete = (existing || [])
    .map((row) => row.id)
    .filter((id) => !validIds.includes(id))

  if (!idsToDelete.length) {
    console.log("🧹 Nenhum jogo antigo para remover.")
    return
  }

  const { error: deleteError } = await supabase
    .from("matches")
    .delete()
    .in("id", idsToDelete)

  if (deleteError) {
    throw deleteError
  }

  console.log(`🧹 Jogos antigos removidos: ${idsToDelete.length}`)
}

async function runScoutlySync() {
  console.log("🚀 Scoutly Sync iniciado...")

  const matches = await fetchUpcomingFixtures()

  await upsertMatches(matches)
  await ensureMatchStats(matches)
  await cleanupOldFutureMatches(matches.map((m) => m.id))

  console.log("✅ Scoutly Sync finalizado com sucesso.")
}

runScoutlySync().catch((error) => {
  console.error("❌ Erro no Scoutly Sync:", {
    message: error.message,
    code: error.code || null,
    details: error.details || null,
    hint: error.hint || null
  })
  process.exit(1)
})
