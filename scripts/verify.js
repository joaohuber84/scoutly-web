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

/**
 * SCOUTLY VERIFY V1.0
 * Roda após os jogos para verificar resultados reais
 * e gravar na tabela pick_results.
 * 
 * Fluxo:
 * 1. Busca picks das últimas 48h que ainda não foram verificados
 * 2. Consulta resultado real na API-Football
 * 3. Verifica se o pick acertou
 * 4. Grava em pick_results
 */

const REQUEST_DELAY_MS = 400
const VERIFY_WINDOW_HOURS = 48 // Verifica jogos das últimas 48h

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }
function safeNumber(v, fb = 0) { const n = Number(v); return Number.isFinite(n) ? n : fb }

async function api(path, params = {}) {
  await sleep(REQUEST_DELAY_MS)
  const url = new URL(`${API}${path}`)
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v))
  })
  const res = await fetch(url, {
    headers: { "x-apisports-key": APISPORTS_KEY }
  })
  if (!res.ok) throw new Error(`API ${res.status} em ${path}`)
  const json = await res.json()
  if (json.errors && Object.keys(json.errors).length > 0) throw new Error(`API error: ${JSON.stringify(json.errors)}`)
  return json.response || []
}

// ─── MARKET VERIFICATION LOGIC ────────────────────────────────────────────

function normalizeText(v) {
  return String(v || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim()
}

function extractLine(market) {
  const m = String(market || "").replace(",", ".")
  const match = m.match(/(\d+(\.\d+)?)/)
  return match ? Number(match[1]) : null
}

function detectDirection(market) {
  const m = normalizeText(market)
  if (m.includes("mais de")) return "over"
  if (m.includes("menos de")) return "under"
  return null
}

function detectFamily(market) {
  const m = normalizeText(market)
  if (m.includes("escanteio")) return "escanteios"
  if (m.includes("no gol")) return "sot"
  if (m.includes("finaliz")) return "shots"
  if (m.includes("cart")) return "cards"
  if (m.includes("ambas")) return "btts"
  if (m.includes("dupla chance")) return "dupla_chance"
  if (m.includes("vitoria") || m.includes("vitória")) return "resultado"
  if (m.includes("gol")) return "gols"
  return "outro"
}

async function getFixtureResult(fixtureId) {
  try {
    const data = await api("/fixtures", { id: fixtureId })
    if (!data.length) return null
    const fixture = data[0]
    const status = fixture?.fixture?.status?.short
    const finished = ["FT", "AET", "PEN"].includes(String(status || "").toUpperCase())
    if (!finished) return null

    // Estatísticas do jogo
    const statsData = await api("/fixtures/statistics", { fixture: fixtureId })

    const homeStats = statsData.find(s => s.team?.id === fixture?.teams?.home?.id)?.statistics || []
    const awayStats = statsData.find(s => s.team?.id === fixture?.teams?.away?.id)?.statistics || []

    function getStat(stats, type) {
      const found = stats.find(x => x.type === type)
      if (!found || found.value === null) return 0
      const v = String(found.value || "").replace("%", "").trim()
      return Number.isFinite(Number(v)) ? Number(v) : 0
    }

    return {
      finished: true,
      homeGoals: safeNumber(fixture?.goals?.home),
      awayGoals: safeNumber(fixture?.goals?.away),
      homeCorners: getStat(homeStats, "Corner Kicks"),
      awayCorners: getStat(awayStats, "Corner Kicks"),
      totalCorners: getStat(homeStats, "Corner Kicks") + getStat(awayStats, "Corner Kicks"),
      homeShots: getStat(homeStats, "Total Shots"),
      awayShots: getStat(awayStats, "Total Shots"),
      totalShots: getStat(homeStats, "Total Shots") + getStat(awayStats, "Total Shots"),
      homeSOT: getStat(homeStats, "Shots on Goal"),
      awaySOT: getStat(awayStats, "Shots on Goal"),
      totalSOT: getStat(homeStats, "Shots on Goal") + getStat(awayStats, "Shots on Goal"),
      homeCards: getStat(homeStats, "Yellow Cards") + getStat(homeStats, "Red Cards"),
      awayCards: getStat(awayStats, "Yellow Cards") + getStat(awayStats, "Red Cards"),
      totalCards: getStat(homeStats, "Yellow Cards") + getStat(homeStats, "Red Cards") +
                  getStat(awayStats, "Yellow Cards") + getStat(awayStats, "Red Cards"),
      homeTeamId: fixture?.teams?.home?.id,
      awayTeamId: fixture?.teams?.away?.id,
      homeName: fixture?.teams?.home?.name,
      awayName: fixture?.teams?.away?.name,
    }
  } catch (e) {
    console.error(`Erro ao buscar resultado do fixture ${fixtureId}:`, e.message)
    return null
  }
}

function evaluatePick(market, result) {
  const family = detectFamily(market)
  const direction = detectDirection(market)
  const line = extractLine(market)
  const m = normalizeText(market)

  const totalGoals = result.homeGoals + result.awayGoals

  // GOLS
  if (family === "gols") {
    if (line === null) return null
    if (direction === "over") return totalGoals > line
    if (direction === "under") return totalGoals < line
    return null
  }

  // BTTS
  if (family === "btts") {
    if (m.includes("ambas marcam") && !m.includes("nao") && !m.includes("não")) {
      return result.homeGoals >= 1 && result.awayGoals >= 1
    }
    if (m.includes("nao marcam") || m.includes("não marcam")) {
      return result.homeGoals === 0 || result.awayGoals === 0
    }
    return null
  }

  // ESCANTEIOS
  if (family === "escanteios") {
    if (line === null) return null
    // Mercados por time (ex: "Flamengo mais de 3.5 escanteios")
    if (m.includes(normalizeText(result.homeName || ""))) {
      return direction === "over" ? result.homeCorners > line : result.homeCorners < line
    }
    if (m.includes(normalizeText(result.awayName || ""))) {
      return direction === "over" ? result.awayCorners > line : result.awayCorners < line
    }
    // Total
    return direction === "over" ? result.totalCorners > line : result.totalCorners < line
  }

  // FINALIZAÇÕES NO GOL
  if (family === "sot") {
    if (line === null) return null
    return direction === "over" ? result.totalSOT > line : result.totalSOT < line
  }

  // FINALIZAÇÕES TOTAIS
  if (family === "shots") {
    if (line === null) return null
    return direction === "over" ? result.totalShots > line : result.totalShots < line
  }

  // CARTÕES
  if (family === "cards") {
    if (line === null) return null
    return direction === "over" ? result.totalCards > line : result.totalCards < line
  }

  // RESULTADO / DUPLA CHANCE
  if (family === "resultado" || family === "dupla_chance") {
    const homeWon = result.homeGoals > result.awayGoals
    const awayWon = result.awayGoals > result.homeGoals
    const draw = result.homeGoals === result.awayGoals

    if (m.includes("vitoria do mandante") || m.includes("vitória do mandante")) return homeWon
    if (m.includes("vitoria do visitante") || m.includes("vitória do visitante")) return awayWon
    if (m.includes("empate")) return draw

    if (m.includes("dupla chance")) {
      if (m.includes("ou empate") && (m.includes(normalizeText(result.homeName||"")))) return homeWon || draw
      if (m.includes("ou empate") && (m.includes(normalizeText(result.awayName||"")))) return awayWon || draw
    }
    return null
  }

  return null
}

// ─── MAIN ─────────────────────────────────────────────────────────────────

async function run() {
  console.log("🔍 Scoutly Verify V1.0 iniciado")

  const now = new Date()
  const windowStart = new Date(now.getTime() - VERIFY_WINDOW_HOURS * 60 * 60 * 1000)

  // 1. Busca picks das últimas 48h que ainda não foram verificados
  const { data: picks, error: picksError } = await supabase
    .from("daily_picks")
    .select("*")
    .gte("kickoff", windowStart.toISOString())
    .lte("kickoff", now.toISOString())

  if (picksError) throw new Error(`Erro ao buscar picks: ${picksError.message}`)
  if (!picks || !picks.length) {
    console.log("Nenhum pick para verificar na janela atual.")
    return
  }

  console.log(`📋 Picks para verificar: ${picks.length}`)

  // 2. Verifica quais já foram verificados
  const { data: existingResults } = await supabase
    .from("pick_results")
    .select("match_id, market")
    .gte("created_at", windowStart.toISOString())

  const alreadyChecked = new Set(
    (existingResults || []).map(r => `${r.match_id}::${r.market}`)
  )

  let verified = 0
  let correct = 0
  let incorrect = 0
  let skipped = 0

  for (const pick of picks) {
    const key = `${pick.match_id}::${pick.market}`
    if (alreadyChecked.has(key)) {
      skipped++
      continue
    }

    console.log(`🔎 Verificando: ${pick.home_team} x ${pick.away_team} — ${pick.market}`)

    const result = await getFixtureResult(pick.match_id)

    if (!result || !result.finished) {
      console.log(`⏳ Jogo ainda não finalizado: ${pick.home_team} x ${pick.away_team}`)
      continue
    }

    const isCorrect = evaluatePick(pick.market, result)

    if (isCorrect === null) {
      console.log(`⚠️ Não foi possível avaliar: ${pick.market}`)
      continue
    }

    const { error: insertError } = await supabase
      .from("pick_results")
      .insert({
        match_id: pick.match_id,
        home_team: pick.home_team,
        away_team: pick.away_team,
        league: pick.league,
        kickoff: pick.kickoff,
        market: pick.market,
        predicted: pick.market,
        result_home: result.homeGoals,
        result_away: result.awayGoals,
        correct: isCorrect,
        checked_at: new Date().toISOString(),
        created_at: new Date().toISOString(),
      })

    if (insertError) {
      console.error(`❌ Erro ao gravar resultado: ${insertError.message}`)
      continue
    }

    verified++
    if (isCorrect) { correct++; console.log(`✅ ACERTOU: ${pick.market}`) }
    else { incorrect++; console.log(`❌ ERROU: ${pick.market} | Placar: ${result.homeGoals}-${result.awayGoals}`) }
  }

  console.log(`\n📊 Resumo da verificação:`)
  console.log(`   Verificados: ${verified}`)
  console.log(`   Acertos: ${correct}`)
  console.log(`   Erros: ${incorrect}`)
  console.log(`   Já verificados (pulados): ${skipped}`)
  if (verified > 0) {
    const rate = Math.round((correct / verified) * 100)
    console.log(`   Taxa de acerto: ${rate}%`)
  }
  console.log("✅ Scoutly Verify V1.0 concluído")
}

run().catch(error => {
  console.error("❌ Erro fatal no Scoutly Verify V1.0:", error)
  process.exit(1)
})
