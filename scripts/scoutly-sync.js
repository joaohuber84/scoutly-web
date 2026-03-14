const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY

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

// Ligas prioritárias do Scoutly
const LEAGUES = [
  // Inglaterra
  { id: 39, season: 2025 }, // Premier League

  // Espanha
  { id: 140, season: 2025 }, // La Liga

  // Itália
  { id: 135, season: 2025 }, // Serie A

  // Alemanha
  { id: 78, season: 2025 }, // Bundesliga

  // França
  { id: 61, season: 2025 }, // Ligue 1

  // Holanda
  { id: 88, season: 2025 }, // Eredivisie

  // Portugal
  { id: 94, season: 2025 }, // Liga Portugal

  // Brasil
  { id: 71, season: 2025 }, // Brasileirão Série A

  // Argentina
  { id: 128, season: 2025 }, // Liga Profesional Argentina

  // Turquia
  { id: 203, season: 2025 }, // Super Lig

  // Áustria
  { id: 218, season: 2025 }, // Bundesliga / Austrian league

  // Rússia
  { id: 235, season: 2025 }, // Premier League

  // Suíça
  { id: 207, season: 2025 }, // Super League

  // Bélgica
  { id: 144, season: 2025 }, // Jupiler Pro League

  // Escócia
  { id: 179, season: 2025 }, // Premiership

  // Dinamarca
  { id: 119, season: 2025 }, // Superliga

  // Noruega
  { id: 103, season: 2025 }, // Eliteserien

  // Suécia
  { id: 113, season: 2025 }, // Allsvenskan

  // Japão
  { id: 98, season: 2025 }, // J1 League

  // Arábia Saudita
  { id: 307, season: 2025 } // Pro League
]

function safeNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function toISOStringOrNull(value) {
  if (!value) return null
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? null : d.toISOString()
}

function toDateOnly(value) {
  if (!value) return null
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return null
  return d.toISOString().slice(0, 10)
}

async function apiGet(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`)

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value))
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
    throw new Error(`API-Football erro ${response.status}: ${text}`)
  }

  const json = await response.json()
  return json.response || []
}

async function fetchUpcomingFixtures() {
  const allMatches = []
  const seen = new Set()

  // hoje + próximos 2 dias
  const now = new Date()
  const dates = [0, 1, 2].map((offset) => {
    const d = new Date(now)
    d.setUTCDate(d.getUTCDate() + offset)
    return d.toISOString().slice(0, 10)
  })

  for (const league of LEAGUES) {
    for (const date of dates) {
      const fixtures = await apiGet("/fixtures", {
        league: league.id,
        season: league.season,
        date,
        timezone: TIMEZONE
      })

      for (const item of fixtures) {
        const fixtureId = item?.fixture?.id
        if (!fixtureId || seen.has(fixtureId)) continue

        const statusShort = item?.fixture?.status?.short || ""
        const isFuture = ["NS", "TBD", "PST"].includes(statusShort)

        if (!isFuture) continue

        seen.add(fixtureId)

        const kickoffIso = toISOStringOrNull(item?.fixture?.date)

        allMatches.push({
          id: fixtureId,
          created_at: new Date().toISOString(),
          home_team: item?.teams?.home?.name || "Time da casa",
          away_team: item?.teams?.away?.name || "Time visitante",
          league: item?.league?.name || null,
          match_date: toDateOnly(kickoffIso),
          kickoff: kickoffIso,
          home_logo: item?.teams?.home?.logo || null,
          away_logo: item?.teams?.away?.logo || null,

          // Campos que podem ser usados pelo Brain/frontend
          avg_goals: null,
          avg_corners: null,
          avg_shots: null,
          insight: null,
          home_win_prob: null,
          draw_prob: null,
          away_win_prob: null
        })
      }
    }
  }

  // ordena por kickoff
  allMatches.sort((a, b) => {
    const ta = a.kickoff ? new Date(a.kickoff).getTime() : 0
    const tb = b.kickoff ? new Date(b.kickoff).getTime() : 0
    return ta - tb
  })

  return allMatches
}

async function upsertMatches(matches) {
  if (!matches.length) {
    console.log("⚠️ Nenhum jogo futuro encontrado.")
    return
  }

  const { error } = await supabase
    .from("matches")
    .upsert(matches, { onConflict: "id" })

  if (error) throw error

  console.log(`✅ Matches sincronizados: ${matches.length}`)
}

async function ensureMatchStats(matches) {
  if (!matches.length) return

  const matchIds = matches.map((m) => m.id)

  const { data: existingStats, error: existingError } = await supabase
    .from("match_stats")
    .select("match_id")
    .in("match_id", matchIds)

  if (existingError) throw existingError

  const existingSet = new Set((existingStats || []).map((row) => row.match_id))

  const missingStats = matches
    .filter((m) => !existingSet.has(m.id))
    .map((m) => ({
      match_id: m.id,
      home_shots: 0,
      home_shots_on_target: 0,
      home_corners: 0,
      home_yellow_cards: 0,
      away_shots: 0,
      away_shots_on_target: 0,
      away_corners: 0,
      away_yellow_cards: 0
    }))

  if (missingStats.length > 0) {
    const { error: insertError } = await supabase
      .from("match_stats")
      .insert(missingStats)

    if (insertError) throw insertError
  }

  console.log(`✅ Match stats garantidos: ${matchIds.length}`)
}

async function runScoutlySync() {
  console.log("🚀 Scoutly Sync iniciado...")

  const matches = await fetchUpcomingFixtures()

  await upsertMatches(matches)
  await ensureMatchStats(matches)

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
