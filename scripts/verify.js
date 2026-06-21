const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY || ""
const SUPABASE_URL  = process.env.SUPABASE_URL   || ""
const SUPABASE_KEY  =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_KEY      ||
  process.env.SUPABASE_KEY              ||
  ""

if (!APISPORTS_KEY) throw new Error("APISPORTS_KEY não encontrada.")
if (!SUPABASE_URL)  throw new Error("SUPABASE_URL não encontrada.")
if (!SUPABASE_KEY)  throw new Error("SUPABASE_SERVICE_ROLE_KEY não encontrada.")

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)
const API_BASE = "https://v3.football.api-sports.io"

/**
 * SCOUTLY VERIFY V1.0
 *
 * Responsabilidade: avaliar picks do radar após o encerramento dos jogos.
 *
 * Fluxo:
 *   1. Busca picks na daily_picks com kickoff > 2h atrás
 *   2. Filtra os que ainda não foram avaliados (não existem em pick_results)
 *   3. Busca o resultado real na API-Football
 *   4. Avalia cada mercado previsto contra o resultado
 *   5. Salva em pick_results com correct = true/false (avaliado) ou null (não verificável —
 *      jogo encerrado mas o texto do mercado não foi reconhecido pelo parser; fica registrado
 *      para auditoria, mas não entra na taxa de acerto pública)
 *
 * Mercados suportados:
 *   - Mais de X gols / Menos de X gols
 *   - Mais de X escanteios / Menos de X escanteios
 *   - Mais de X finalizações / Menos de X finalizações
 *   - Mais de X finalizações no gol / Menos de X finalizações no gol
 *   - Mais de X cartões / Menos de X cartões
 *   - Ambas marcam / Ambas não marcam
 *   - Dupla chance [time] ou empate
 *   - Vitória do mandante / Vitória do visitante
 *   - [Time] mais de X escanteios (individual)
 *   - [Time] mais de X chutes (individual)
 *   - [Time] mais de X chutes no gol (individual)
 */

// Quanto tempo após o kickoff consideramos o jogo como "potencialmente encerrado"
const GRACE_AFTER_KICKOFF_HOURS = 2.5

// Não tenta avaliar jogos muito antigos (evita chamadas desnecessárias à API)
const MAX_LOOKBACK_DAYS = 14

const REQUEST_DELAY_MS = 400

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

function safeNum(v, fb = 0) {
  const n = Number(v)
  return Number.isFinite(n) ? n : fb
}

function normalizeText(v) {
  return String(v || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim()
}

function extractNumber(market) {
  const m = String(market || "").replace(",", ".")
  const match = m.match(/(\d+(\.\d+)?)/)
  return match ? Number(match[1]) : null
}

// ── API ─────────────────────────────────────────────────────────────

async function fetchFixture(fixtureId) {
  await sleep(REQUEST_DELAY_MS)
  const url = `${API_BASE}/fixtures?id=${fixtureId}`
  const res = await fetch(url, {
    headers: { "x-apisports-key": APISPORTS_KEY }
  })
  if (!res.ok) throw new Error(`API ${res.status} — fixture ${fixtureId}`)
  const json = await res.json()
  return (json.response || [])[0] || null
}

async function fetchFixtureStats(fixtureId) {
  await sleep(REQUEST_DELAY_MS)
  const url = `${API_BASE}/fixtures/statistics?fixture=${fixtureId}`
  const res = await fetch(url, {
    headers: { "x-apisports-key": APISPORTS_KEY }
  })
  if (!res.ok) throw new Error(`API stats ${res.status} — fixture ${fixtureId}`)
  const json = await res.json()
  return json.response || []
}

function isFixtureFinished(fixture) {
  const status = String(fixture?.fixture?.status?.short || "").toUpperCase()
  return ["FT", "AET", "PEN"].includes(status)
}

function extractStat(statsArray, teamIndex, type) {
  const teamStats = statsArray[teamIndex]?.statistics || []
  const found = teamStats.find(s => s.type === type)
  if (!found || found.value === null || found.value === undefined) return 0
  if (typeof found.value === "string") {
    const n = Number(found.value.replace("%", "").trim())
    return Number.isFinite(n) ? n : 0
  }
  return safeNum(found.value)
}

// ── AVALIADORES DE MERCADO ───────────────────────────────────────────

/**
 * Avalia se um pick foi correto dado o resultado real.
 * Retorna true, false, ou null (não conseguiu avaliar).
 */
function evaluateMarket(market, fixture, statsArray) {
  const m = normalizeText(market)
  const homeGoals = safeNum(fixture?.goals?.home)
  const awayGoals = safeNum(fixture?.goals?.away)
  const totalGoals = homeGoals + awayGoals
  const homeName = normalizeText(fixture?.teams?.home?.name || "")
  const awayName = normalizeText(fixture?.teams?.away?.name || "")
  const line = extractNumber(market)

  // ── GOLS ──────────────────────────────────────────────────────────
  if (m.includes("mais de") && m.includes("gol") && !m.includes("escanteio") && !m.includes("finaliz")) {
    if (line === null) return null
    return totalGoals > line
  }
  if (m.includes("menos de") && m.includes("gol") && !m.includes("escanteio") && !m.includes("finaliz")) {
    if (line === null) return null
    return totalGoals < line
  }

  // ── AMBAS MARCAM ──────────────────────────────────────────────────
  if (m.includes("ambas marcam") || m === "ambas marcam") {
    return homeGoals >= 1 && awayGoals >= 1
  }
  if (m.includes("ambas nao marcam") || m.includes("ambas não marcam")) {
    return homeGoals === 0 || awayGoals === 0
  }

  // ── RESULTADO ─────────────────────────────────────────────────────
  if (m.includes("vitoria do mandante") || m.includes("vitória do mandante")) {
    return homeGoals > awayGoals
  }
  if (m.includes("vitoria do visitante") || m.includes("vitória do visitante")) {
    return awayGoals > homeGoals
  }
  if (m.includes("dupla chance")) {
    // "Dupla chance [time] ou empate"
    if (m.includes(homeName) || m.includes("mandante") || m.includes("casa")) {
      return homeGoals >= awayGoals // vitória da casa OU empate
    }
    if (m.includes(awayName) || m.includes("visitante") || m.includes("fora")) {
      return awayGoals >= homeGoals // vitória do visitante OU empate
    }
    // Fallback: se não identificou o time, tenta pela posição no texto
    return null
  }

  // ── ESCANTEIOS TOTAIS ─────────────────────────────────────────────
  if ((m.includes("mais de") || m.includes("menos de")) && m.includes("escanteio")) {
    if (!statsArray.length) return null
    // Verifica se é mercado individual de time
    if (m.includes(homeName) || m.includes(awayName)) {
      return evaluateIndividualCorners(m, line, fixture, statsArray)
    }
    const homeCorners = extractStat(statsArray, 0, "Corner Kicks")
    const awayCorners = extractStat(statsArray, 1, "Corner Kicks")
    const totalCorners = homeCorners + awayCorners
    if (line === null) return null
    if (m.includes("mais de")) return totalCorners > line
    if (m.includes("menos de")) return totalCorners < line
    return null
  }

  // ── FINALIZAÇÕES NO GOL (deve vir antes de finalizações totais) ───
  if (m.includes("finaliz") && m.includes("no gol")) {
    if (!statsArray.length) return null
    if (m.includes(homeName) || m.includes(awayName)) {
      return evaluateIndividualSOT(m, line, fixture, statsArray)
    }
    const homeSOT = extractStat(statsArray, 0, "Shots on Goal")
    const awaySOT  = extractStat(statsArray, 1, "Shots on Goal")
    const totalSOT = homeSOT + awaySOT
    if (line === null) return null
    if (m.includes("mais de")) return totalSOT > line
    if (m.includes("menos de")) return totalSOT < line
    return null
  }

  // ── FINALIZAÇÕES TOTAIS ───────────────────────────────────────────
  if (m.includes("finaliz") && !m.includes("no gol")) {
    if (!statsArray.length) return null
    if (m.includes(homeName) || m.includes(awayName)) {
      return evaluateIndividualShots(m, line, fixture, statsArray)
    }
    const homeShots = extractStat(statsArray, 0, "Total Shots")
    const awayShots = extractStat(statsArray, 1, "Total Shots")
    const totalShots = homeShots + awayShots
    if (line === null) return null
    if (m.includes("mais de")) return totalShots > line
    if (m.includes("menos de")) return totalShots < line
    return null
  }

  // ── CHUTES (alias de finalizações, nomenclatura individual) ───────
  if ((m.includes("chutes") || m.includes("chute")) && !m.includes("no gol")) {
    if (!statsArray.length) return null
    return evaluateIndividualShots(m, line, fixture, statsArray)
  }
  if ((m.includes("chutes") || m.includes("chute")) && m.includes("no gol")) {
    if (!statsArray.length) return null
    return evaluateIndividualSOT(m, line, fixture, statsArray)
  }

  // ── CARTÕES ───────────────────────────────────────────────────────
  if (m.includes("cart")) {
    if (!statsArray.length) return null
    const homeYellow = extractStat(statsArray, 0, "Yellow Cards")
    const homeRed    = extractStat(statsArray, 0, "Red Cards")
    const awayYellow = extractStat(statsArray, 1, "Yellow Cards")
    const awayRed    = extractStat(statsArray, 1, "Red Cards")
    const totalCards = homeYellow + homeRed + awayYellow + awayRed
    if (line === null) return null
    if (m.includes("mais de")) return totalCards > line
    if (m.includes("menos de")) return totalCards < line
    return null
  }

  // Mercado não reconhecido
  console.log(`  ⚠️  Mercado não reconhecido: "${market}"`)
  return null
}

function evaluateIndividualCorners(m, line, fixture, statsArray) {
  if (line === null) return null
  const homeCorners = extractStat(statsArray, 0, "Corner Kicks")
  const awayCorners = extractStat(statsArray, 1, "Corner Kicks")
  const homeName = normalizeText(fixture?.teams?.home?.name || "")
  const awayName = normalizeText(fixture?.teams?.away?.name || "")
  let teamCorners = null
  if (m.includes(homeName)) teamCorners = homeCorners
  else if (m.includes(awayName)) teamCorners = awayCorners
  if (teamCorners === null) return null
  if (m.includes("mais de")) return teamCorners > line
  if (m.includes("menos de")) return teamCorners < line
  return null
}

function evaluateIndividualShots(m, line, fixture, statsArray) {
  if (line === null) return null
  const homeShots = extractStat(statsArray, 0, "Total Shots")
  const awayShots = extractStat(statsArray, 1, "Total Shots")
  const homeName = normalizeText(fixture?.teams?.home?.name || "")
  const awayName = normalizeText(fixture?.teams?.away?.name || "")
  let teamShots = null
  if (m.includes(homeName)) teamShots = homeShots
  else if (m.includes(awayName)) teamShots = awayShots
  if (teamShots === null) return null
  if (m.includes("mais de")) return teamShots > line
  if (m.includes("menos de")) return teamShots < line
  return null
}

function evaluateIndividualSOT(m, line, fixture, statsArray) {
  if (line === null) return null
  const homeSOT = extractStat(statsArray, 0, "Shots on Goal")
  const awaySOT  = extractStat(statsArray, 1, "Shots on Goal")
  const homeName = normalizeText(fixture?.teams?.home?.name || "")
  const awayName = normalizeText(fixture?.teams?.away?.name || "")
  let teamSOT = null
  if (m.includes(homeName)) teamSOT = homeSOT
  else if (m.includes(awayName)) teamSOT = awaySOT
  if (teamSOT === null) return null
  if (m.includes("mais de")) return teamSOT > line
  if (m.includes("menos de")) return teamSOT < line
  return null
}

// ── SUPABASE ─────────────────────────────────────────────────────────

async function loadPendingPicks() {
  const now = new Date()
  const graceMs = GRACE_AFTER_KICKOFF_HOURS * 60 * 60 * 1000
  const cutoffTime = new Date(now.getTime() - graceMs).toISOString()
  const maxLookback = new Date(now.getTime() - MAX_LOOKBACK_DAYS * 24 * 60 * 60 * 1000).toISOString()

  // Picks cujo kickoff já passou (com grace de 2.5h)
  const { data: allPicks, error } = await supabase
    .from("daily_picks")
    .select("id, match_id, home_team, away_team, league, kickoff, market")
    .lte("kickoff", cutoffTime)
    .gte("kickoff", maxLookback)
    .order("kickoff", { ascending: false })

  if (error) throw new Error(`Erro ao carregar picks: ${error.message}`)
  if (!allPicks || !allPicks.length) return []

  // Filtra os que já foram avaliados
  const { data: alreadyEvaluated, error: evalError } = await supabase
    .from("pick_results")
    .select("match_id")

  if (evalError) throw new Error(`Erro ao carregar avaliados: ${evalError.message}`)
  const evaluatedIds = new Set((alreadyEvaluated || []).map(r => String(r.match_id)))

  const pending = allPicks.filter(p => !evaluatedIds.has(String(p.match_id)))
  console.log(`📋 Picks pendentes de avaliação: ${pending.length} (de ${allPicks.length} totais)`)
  return pending
}

async function saveResult(pick, correct, resultHome, resultAway, checkedAt) {
  const { error } = await supabase.from("pick_results").insert({
    match_id:   pick.match_id,
    home_team:  pick.home_team,
    away_team:  pick.away_team,
    league:     pick.league,
    kickoff:    pick.kickoff,
    market:     pick.market,
    predicted:  pick.market,    // o que o radar previu
    result_home: resultHome,
    result_away: resultAway,
    correct:    correct,
    checked_at: checkedAt,
    created_at: checkedAt,
  })
  if (error) throw new Error(`Erro ao salvar result match ${pick.match_id}: ${error.message}`)
}

// ── MAIN ─────────────────────────────────────────────────────────────

async function run() {
  console.log("🔍 Scoutly Verify V1.0 iniciado")
  console.log(`📅 Grace: ${GRACE_AFTER_KICKOFF_HOURS}h após kickoff | Lookback: ${MAX_LOOKBACK_DAYS} dias`)

  const pending = await loadPendingPicks()
  if (!pending.length) {
    console.log("✅ Nenhum pick pendente. Tudo avaliado.")
    return
  }

  let evaluated = 0, correct = 0, incorrect = 0, skipped = 0, naoAvaliavel = 0

  for (const pick of pending) {
    console.log(`\n⚽ ${pick.home_team} x ${pick.away_team} [${pick.league}]`)
    console.log(`   📌 Pick: ${pick.market}`)
    console.log(`   🕐 Kickoff: ${pick.kickoff}`)

    try {
      // 1. Busca o resultado na API
      const fixture = await fetchFixture(pick.match_id)

      if (!fixture) {
        console.log(`   ⚠️  Fixture não encontrada na API — pulando`)
        skipped++
        continue
      }

      if (!isFixtureFinished(fixture)) {
        const status = fixture?.fixture?.status?.short || "?"
        console.log(`   ⏳ Jogo ainda não encerrado (status: ${status}) — pulando`)
        skipped++
        continue
      }

      const homeGoals = safeNum(fixture?.goals?.home)
      const awayGoals = safeNum(fixture?.goals?.away)
      console.log(`   📊 Resultado: ${homeGoals}-${awayGoals}`)

      // 2. Busca estatísticas (para mercados de escanteios, finalizações, cartões)
      let statsArray = []
      const m = normalizeText(pick.market)
      const needsStats = m.includes("escanteio") || m.includes("finaliz") ||
                         m.includes("chute") || m.includes("cart")
      if (needsStats) {
        statsArray = await fetchFixtureStats(pick.match_id)
        if (statsArray.length) {
          const homeCorners = extractStat(statsArray, 0, "Corner Kicks")
          const awayCorners = extractStat(statsArray, 1, "Corner Kicks")
          const homeShots = extractStat(statsArray, 0, "Total Shots")
          const awayShots = extractStat(statsArray, 1, "Total Shots")
          const homeSOT = extractStat(statsArray, 0, "Shots on Goal")
          const awaySOT = extractStat(statsArray, 1, "Shots on Goal")
          const homeCards = extractStat(statsArray, 0, "Yellow Cards") + extractStat(statsArray, 0, "Red Cards")
          const awayCards = extractStat(statsArray, 1, "Yellow Cards") + extractStat(statsArray, 1, "Red Cards")
          console.log(`   📈 Stats: escanteios ${homeCorners+awayCorners} | finalizações ${homeShots+awayShots} | no gol ${homeSOT+awaySOT} | cartões ${homeCards+awayCards}`)
        } else {
          console.log(`   ⚠️  Estatísticas não disponíveis na API`)
        }
      }

      // 3. Avalia o mercado
      const result = evaluateMarket(pick.market, fixture, statsArray)

      if (result === null) {
        // Jogo encerrado e com dados disponíveis, mas o mercado não pôde ser avaliado
        // (texto não reconhecido pelo parser). Diferente do "pulando" acima: aqui já
        // sabemos o resultado final, então registramos como NÃO VERIFICÁVEL (correct=null)
        // em vez de simplesmente sumir — fica auditável e não entra na taxa de acerto pública.
        console.log(`   ❓ Mercado não avaliável — registrado como NÃO VERIFICÁVEL (não conta na taxa de acerto)`)
        const checkedAt = new Date().toISOString()
        await saveResult(pick, null, homeGoals, awayGoals, checkedAt)
        naoAvaliavel++
        continue
      }

      // 4. Salva na pick_results
      const checkedAt = new Date().toISOString()
      await saveResult(pick, result, homeGoals, awayGoals, checkedAt)

      const emoji = result ? "✅" : "❌"
      console.log(`   ${emoji} Resultado: ${result ? "ACERTO" : "ERRO"}`)

      evaluated++
      if (result) correct++
      else incorrect++

    } catch (err) {
      console.error(`   ❌ Erro ao processar pick ${pick.match_id}:`, err.message)
      skipped++
    }
  }

  const accuracy = evaluated > 0 ? Math.round((correct / evaluated) * 100) : 0

  console.log(`\n📊 Resumo da rodada:`)
  console.log(`   Avaliados: ${evaluated}`)
  console.log(`   ✅ Acertos: ${correct}`)
  console.log(`   ❌ Erros: ${incorrect}`)
  console.log(`   ❓ Não verificáveis (registrados, fora da taxa de acerto): ${naoAvaliavel}`)
  console.log(`   ⏳ Pendentes (jogo não encerrado / fixture indisponível, tenta de novo depois): ${skipped}`)
  console.log(`   🎯 Taxa desta rodada: ${accuracy}%`)
  console.log(`\n✅ Scoutly Verify V1.0 concluído`)
}

run().catch(err => {
  console.error("❌ Erro fatal no Scoutly Verify V1.0:", err)
  process.exit(1)
})
