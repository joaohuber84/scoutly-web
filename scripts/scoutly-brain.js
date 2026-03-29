const { createClient } = require("@supabase/supabase-js")

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY

if (!SUPABASE_URL) {
  throw new Error("SUPABASE_URL não encontrada nas variáveis de ambiente.")
}

if (!SUPABASE_KEY) {
  throw new Error(
    "SUPABASE_SERVICE_KEY / SUPABASE_SERVICE_ROLE_KEY não encontrada nas variáveis de ambiente."
  )
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const TIMEZONE = "America/Sao_Paulo"
const RADAR_SIZE = 15
const TICKET_MIN_SIZE = 2
const TICKET_MAX_SIZE = 3
const UPCOMING_WINDOW_HOURS = 120
const RECENT_PAST_GRACE_HOURS = 3

function toNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function round1(value) {
  return Math.round(toNumber(value) * 10) / 10
}

function round2(value) {
  return Math.round(toNumber(value) * 100) / 100
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function maybeJson(value) {
  if (!value) return null
  if (typeof value === "object") return value

  if (typeof value === "string") {
    try {
      return JSON.parse(value)
    } catch (_) {
      return null
    }
  }

  return null
}

function formatDateInTZ(date, timeZone = TIMEZONE) {
  return new Intl.DateTimeFormat("sv-SE", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date)
}

function getNow() {
  return new Date()
}

function getTodayInTZ() {
  return formatDateInTZ(getNow(), TIMEZONE)
}

function getKickoffDateOnly(kickoff) {
  if (!kickoff) return null
  const d = new Date(kickoff)
  if (Number.isNaN(d.getTime())) return null
  return formatDateInTZ(d, TIMEZONE)
}

function getKickoffMs(kickoff) {
  if (!kickoff) return Number.MAX_SAFE_INTEGER
  const d = new Date(kickoff)
  if (Number.isNaN(d.getTime())) return Number.MAX_SAFE_INTEGER
  return d.getTime()
}

function buildMatchLabel(row) {
  return `${row.home_team} x ${row.away_team}`
}

function marketFamily(market) {
  const m = String(market || "").trim().toLowerCase()

  if (!m) return "outro"
  if (m.includes("escanteio")) return "escanteios"
  if (m.includes("ambas")) return "btts"
  if (m.includes("dupla chance") || m.includes("empate") || m.includes("vitória")) {
    return "resultado"
  }
  if (m.includes("gol")) return "gols"
  return "outro"
}

function marketMacroFromFamily(family, market = "") {
  const m = String(market || "").toLowerCase()

  if (family === "gols") {
    if (m.includes("mais de")) return "ofensivo"
    if (m.includes("menos de")) return "defensivo"
  }

  if (family === "btts") {
    if (m.includes("não")) return "defensivo"
    return "ofensivo"
  }

  if (family === "escanteios") return "estatistico"
  if (family === "resultado") return "protecao"

  return "equilibrado"
}

function getStrengthLabel(score) {
  if (score >= 0.84) return "Forte"
  if (score >= 0.72) return "Boa"
  return "Moderada"
}

function compareByFullKickoffAsc(a, b) {
  return getKickoffMs(a.kickoff) - getKickoffMs(b.kickoff)
}

function compareByScoreThenKickoff(a, b) {
  if (b.main_score !== a.main_score) return b.main_score - a.main_score
  if (b.main_probability !== a.main_probability) {
    return b.main_probability - a.main_probability
  }
  return compareByFullKickoffAsc(a, b)
}

function safeLeague(row) {
  return row.league || "Liga"
}

function getWindowBounds() {
  const now = getNow()
  const minTime = new Date(now.getTime() - RECENT_PAST_GRACE_HOURS * 60 * 60 * 1000)
  const maxTime = new Date(now.getTime() + UPCOMING_WINDOW_HOURS * 60 * 60 * 1000)

  return { now, minTime, maxTime }
}

function isKickoffInsideWindow(kickoff) {
  if (!kickoff) return false
  const d = new Date(kickoff)
  if (Number.isNaN(d.getTime())) return false

  const { minTime, maxTime } = getWindowBounds()
  return d >= minTime && d <= maxTime
}

function isTodayKickoff(kickoff) {
  return getKickoffDateOnly(kickoff) === getTodayInTZ()
}

function hasUsablePrimaryPick(row) {
  return Boolean(
    row.best_pick_1 ||
      row.pick ||
      row.safe_pick ||
      row.balanced_pick ||
      row.aggressive_pick ||
      row.value_pick
  )
}

function derivePrimaryPick(row) {
  return (
    row.best_pick_1 ||
    row.pick ||
    row.safe_pick ||
    row.balanced_pick ||
    row.aggressive_pick ||
    row.value_pick ||
    null
  )
}

function deriveSecondaryPicks(row) {
  const candidates = [
    row.best_pick_2,
    row.best_pick_3,
    row.safe_pick,
    row.balanced_pick,
    row.aggressive_pick,
    row.value_pick,
  ]
    .filter(Boolean)
    .map((x) => String(x).trim())
    .filter(Boolean)

  const primary = String(derivePrimaryPick(row) || "").trim().toLowerCase()
  const unique = []
  const seen = new Set(primary ? [primary] : [])

  for (const item of candidates) {
    const key = item.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    unique.push(item)
  }

  return unique.slice(0, 2)
}

function deriveProbabilities(row) {
  const probs = maybeJson(row.probabilities) || {}

  const home = clamp(
    toNumber(row.home_win_prob, NaN) || toNumber(row.home_result_prob, NaN) || toNumber(probs.home, 0),
    0,
    1
  )
  const draw = clamp(
    toNumber(row.draw_prob, NaN) || toNumber(row.draw_result_prob, NaN) || toNumber(probs.draw, 0),
    0,
    1
  )
  const away = clamp(
    toNumber(row.away_win_prob, NaN) || toNumber(row.away_result_prob, NaN) || toNumber(probs.away, 0),
    0,
    1
  )

  return { home, draw, away }
}

function deriveMetrics(row) {
  const metrics = maybeJson(row.metrics) || {}
  const markets = maybeJson(row.markets) || {}

  const avgGoals =
    toNumber(row.avg_goals, NaN) ||
    toNumber(metrics.goals, NaN) ||
    round1(toNumber(row.expected_home_goals, 0) + toNumber(row.expected_away_goals, 0))

  const avgCorners =
    toNumber(row.avg_corners, NaN) ||
    toNumber(metrics.corners, NaN) ||
    toNumber(markets.corners, NaN) ||
    toNumber(row.expected_corners, 0)

  const avgShots =
    toNumber(row.avg_shots, NaN) ||
    toNumber(metrics.shots, NaN) ||
    round1(toNumber(row.expected_home_shots, 0) + toNumber(row.expected_away_shots, 0))

  const avgShotsOnTarget =
    toNumber(row.avg_shots_on_target, NaN) ||
    toNumber(metrics.shots_on_target, NaN) ||
    toNumber(metrics.sot, NaN) ||
    round1(toNumber(row.expected_home_sot, 0) + toNumber(row.expected_away_sot, 0))

  const avgCards =
    toNumber(row.avg_cards, NaN) ||
    toNumber(metrics.cards, NaN) ||
    toNumber(markets.cards, NaN) ||
    toNumber(row.expected_cards, 0)

  const avgFouls =
    toNumber(row.avg_fouls, NaN) ||
    toNumber(metrics.fouls, NaN) ||
    toNumber(markets.fouls, 0)

  return {
    avgGoals: round1(avgGoals || 0),
    avgCorners: round1(avgCorners || 0),
    avgShots: round1(avgShots || 0),
    avgShotsOnTarget: round1(avgShotsOnTarget || 0),
    avgCards: round1(avgCards || 0),
    avgFouls: round1(avgFouls || 0),
  }
}

function deriveMainProbability(row, primaryPick) {
  const market = String(primaryPick || "").toLowerCase()
  const probs = maybeJson(row.probabilities) || {}
  const markets = maybeJson(row.markets) || {}

  const directMain =
    toNumber(row.main_probability, NaN) ||
    toNumber(row.probability, NaN) ||
    toNumber(row.confidence_score, NaN)

  if (Number.isFinite(directMain) && directMain > 0) {
    return clamp(directMain, 0, 1)
  }

  if (market.includes("mais de 1.5")) {
    return clamp(
      toNumber(row.over15_prob, NaN) || toNumber(markets.over15, 0),
      0,
      1
    )
  }

  if (market.includes("mais de 2.5")) {
    return clamp(
      toNumber(row.over25_prob, NaN) || toNumber(row.prob_over25, NaN) || toNumber(markets.over25, 0),
      0,
      1
    )
  }

  if (market.includes("menos de 2.5")) {
    const under25 =
      toNumber(row.under25_prob, NaN) ||
      (1 - (toNumber(row.over25_prob, NaN) || toNumber(row.prob_over25, NaN) || toNumber(markets.over25, 0)))
    return clamp(under25, 0, 1)
  }

  if (market.includes("menos de 3.5")) {
    return clamp(
      toNumber(row.under35_prob, NaN) || toNumber(markets.under35, 0),
      0,
      1
    )
  }

  if (market.includes("ambas marcam")) {
    return clamp(
      toNumber(row.btts_prob, NaN) || toNumber(row.prob_btts, NaN) || toNumber(markets.btts, 0),
      0,
      1
    )
  }

  if (market.includes("ambas não marcam")) {
    const btts =
      toNumber(row.btts_prob, NaN) || toNumber(row.prob_btts, NaN) || toNumber(markets.btts, 0)
    return clamp(1 - btts, 0, 1)
  }

  if (market.includes("escanteios")) {
    return clamp(
      toNumber(row.corners_over85_prob, NaN) ||
        toNumber(row.prob_corners, NaN) ||
        toNumber(markets.corners, 0),
      0,
      1
    )
  }

  const derived = deriveProbabilities(row)
  const homeOrDraw = clamp(derived.home + derived.draw, 0, 1)
  const awayOrDraw = clamp(derived.away + derived.draw, 0, 1)

  if (market.includes("dupla chance")) {
    if (market.includes(String(row.home_team || "").toLowerCase())) return homeOrDraw
    if (market.includes(String(row.away_team || "").toLowerCase())) return awayOrDraw
  }

  if (market.includes("mandante")) return derived.home
  if (market.includes("visitante")) return derived.away

  return clamp(toNumber(row.confidence_score, 0.65), 0, 1)
}

function deriveMainScore(row, mainProbability, primaryPick) {
  const explicit =
    toNumber(row.main_score, NaN) ||
    toNumber(row.confidence_score, NaN) ||
    toNumber(row.probability, NaN)

  let score = Number.isFinite(explicit) && explicit > 0 ? explicit : mainProbability

  const market = String(primaryPick || "").toLowerCase()

  if (market.includes("mais de 1.5")) score += 0.03
  if (market.includes("mais de 2.5")) score += 0.01
  if (market.includes("menos de 3.5")) score += 0.02
  if (market.includes("dupla chance")) score += 0.02
  if (market.includes("escanteio")) score -= 0.01

  return clamp(round2(score), 0, 1)
}

async function loadBaseTables() {
  const { data: matches, error: matchesError } = await supabase
    .from("matches")
    .select("*")
    .order("kickoff", { ascending: true, nullsFirst: false })

  if (matchesError) throw matchesError

  const { data: analysis, error: analysisError } = await supabase
    .from("match_analysis")
    .select("*")

  if (analysisError) throw analysisError

  return {
    matches: matches || [],
    analysis: analysis || [],
  }
}

function mergeMatchRow(matchRow, analysisMap) {
  const analysis = analysisMap.get(String(matchRow.id)) || {}
  const merged = {
    ...matchRow,
    ...analysis,
    id: matchRow.id,
    match_id: matchRow.id,
  }

  const primaryPick = derivePrimaryPick(merged)
  const secondaryPicks = deriveSecondaryPicks(merged)
  const mainFamily = marketFamily(primaryPick)
  const mainMacro = marketMacroFromFamily(mainFamily, primaryPick)
  const mainProbability = deriveMainProbability(merged, primaryPick)
  const mainScore = deriveMainScore(merged, mainProbability, primaryPick)
  const metrics = deriveMetrics(merged)
  const probs = deriveProbabilities(merged)

  return {
    ...merged,
    league: safeLeague(merged),
    home_team: merged.home_team,
    away_team: merged.away_team,
    kickoff: merged.kickoff,
    best_pick_1: primaryPick,
    best_pick_2: secondaryPicks[0] || null,
    best_pick_3: secondaryPicks[1] || null,
    main_pick: primaryPick,
    main_probability: round2(mainProbability),
    main_score: round2(mainScore),
    main_family: mainFamily,
    main_subfamily: String(primaryPick || "").toLowerCase().replace(/\s+/g, "_"),
    main_macro: mainMacro,
    strength: getStrengthLabel(mainScore),
    avg_goals: metrics.avgGoals,
    avg_corners: metrics.avgCorners,
    avg_shots: metrics.avgShots,
    avg_shots_on_target: metrics.avgShotsOnTarget,
    avg_cards: metrics.avgCards,
    avg_fouls: metrics.avgFouls,
    home_win_prob: round2(probs.home),
    draw_prob: round2(probs.draw),
    away_win_prob: round2(probs.away),
    insight:
      merged.analysis_text ||
      merged.insight ||
      "Leitura Scoutly disponível para esta partida.",
  }
}

async function loadWindowMatches() {
  const { matches, analysis } = await loadBaseTables()

  console.log("DEBUG TOTAL RAW MATCHES:", matches.length)
  console.log("DEBUG TOTAL ANALYSIS:", analysis.length)

  const analysisMap = new Map(
    analysis.map((row) => [String(row.match_id), row])
  )

  const merged = matches.map((row) => mergeMatchRow(row, analysisMap))

  const filtered = merged.filter((row) => {
    if (!row.home_team || !row.away_team || !row.league || !row.kickoff) return false
    if (!isKickoffInsideWindow(row.kickoff)) return false
    if (!hasUsablePrimaryPick(row)) return false
    return true
  })

  console.log("DEBUG TOTAL FILTRADOS JANELA:", filtered.length)
  return filtered
}

function chooseRadar(analyses) {
  const baseSorted = [...analyses].sort((a, b) => {
    const aToday = isTodayKickoff(a.kickoff) ? 1 : 0
    const bToday = isTodayKickoff(b.kickoff) ? 1 : 0

    if (bToday !== aToday) return bToday - aToday
    return compareByScoreThenKickoff(a, b)
  })

  const radar = []
  const usedMatchIds = new Set()
  const usedExactMarkets = {}
  const usedFamilies = {}
  const usedLeagues = {}

  for (const item of baseSorted) {
    if (usedMatchIds.has(item.match_id)) continue
    if (!item.main_pick) continue

    const exactMarketCount = usedExactMarkets[item.main_pick] || 0
    const familyCount = usedFamilies[item.main_family] || 0
    const leagueCount = usedLeagues[item.league] || 0

    if (exactMarketCount >= 2) continue
    if (leagueCount >= 3) continue

    if (item.main_family === "escanteios" && familyCount >= 3) continue
    if (item.main_family === "gols" && familyCount >= 6) continue
    if (item.main_family === "btts" && familyCount >= 3) continue
    if (item.main_family === "resultado" && familyCount >= 3) continue

    radar.push(item)
    usedMatchIds.add(item.match_id)
    usedExactMarkets[item.main_pick] = exactMarketCount + 1
    usedFamilies[item.main_family] = familyCount + 1
    usedLeagues[item.league] = leagueCount + 1

    if (radar.length === RADAR_SIZE) break
  }

  if (radar.length < RADAR_SIZE) {
    for (const item of baseSorted) {
      if (usedMatchIds.has(item.match_id)) continue
      radar.push(item)
      usedMatchIds.add(item.match_id)
      if (radar.length === RADAR_SIZE) break
    }
  }

  return radar.sort(compareByFullKickoffAsc)
}

function buildTicketFromRadar(radar) {
  const ranked = [...radar].sort(compareByScoreThenKickoff)

  const desiredSize =
    ranked.length >= 3 && Math.random() < 0.7
      ? TICKET_MAX_SIZE
      : TICKET_MIN_SIZE

  const ticket = []
  const usedMatchIds = new Set()
  const usedFamilies = new Set()

  for (const item of ranked) {
    if (usedMatchIds.has(item.match_id)) continue
    if (usedFamilies.has(item.main_family)) continue

    ticket.push(item)
    usedMatchIds.add(item.match_id)
    usedFamilies.add(item.main_family)

    if (ticket.length === desiredSize) break
  }

  if (ticket.length < desiredSize) {
    for (const item of ranked) {
      if (usedMatchIds.has(item.match_id)) continue

      ticket.push(item)
      usedMatchIds.add(item.match_id)

      if (ticket.length === desiredSize) break
    }
  }

  return ticket.sort(compareByFullKickoffAsc)
}

async function updateMatchAnalysisFromBrain(analyses) {
  for (const item of analyses) {
    const { error } = await supabase
      .from("match_analysis")
      .update({
        best_pick_1: item.best_pick_1,
        best_pick_2: item.best_pick_2,
        best_pick_3: item.best_pick_3,
        aggressive_pick: item.aggressive_pick || null,
        analysis_text: item.insight || null,
      })
      .eq("match_id", item.match_id)

    if (error) {
      console.error(
        `Erro ao atualizar match_analysis ${item.match_id}:`,
        error.message
      )
    }
  }
}

async function rebuildDailyPicks(radar, ticket) {
  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (deleteError) throw deleteError

  const rows = radar.map((item, index) => {
    const isInTicket = ticket.some(
      (t) => String(t.match_id) === String(item.match_id)
    )

    return {
      rank: index + 1,
      match_id: item.match_id,
      home_team: item.home_team,
      away_team: item.away_team,
      league: item.league,
      market: item.main_pick,
      probability: round2(item.main_probability),
      is_opportunity: isInTicket,
      home_logo: item.home_logo || null,
      away_logo: item.away_logo || null,
      kickoff: item.kickoff || null,
      created_at: new Date().toISOString(),
    }
  })

  if (!rows.length) {
    console.log("Nenhuma dica elegível para gravar em daily_picks.")
    return
  }

  const { error: insertError } = await supabase
    .from("daily_picks")
    .insert(rows)

  if (insertError) throw insertError
}

function printRadar(radar) {
  console.log("DEBUG RADAR FINAL:")
  radar.forEach((item, index) => {
    console.log(
      index + 1,
      buildMatchLabel(item),
      "->",
      item.main_pick,
      "| family:",
      item.main_family,
      "| macro:",
      item.main_macro,
      "| strength:",
      item.strength,
      "| score:",
      item.main_score,
      "| kickoff:",
      item.kickoff
    )
  })
}

function printTicket(ticket) {
  console.log("🎟️ BILHETE FINAL:")
  ticket.forEach((item, index) => {
    console.log(
      index + 1,
      buildMatchLabel(item),
      "->",
      item.main_pick,
      "| family:",
      item.main_family,
      "| strength:",
      item.strength,
      "| score:",
      item.main_score,
      "| kickoff:",
      item.kickoff
    )
  })
}

async function runScoutlyBrain() {
  console.log("🧠 Scoutly Brain V14 iniciado...")

  const matches = await loadWindowMatches()
  console.log(`📦 Jogos da janela carregados para análise: ${matches.length}`)

  const analyses = matches
    .filter((row) => row.main_pick && row.main_probability > 0)
    .map((row) => ({
      ...row,
      aggressive_pick: row.aggressive_pick || row.best_pick_3 || null,
      insight: row.insight || row.analysis_text || null,
    }))

  console.log(`🧪 Análises válidas geradas: ${analyses.length}`)

  if (!analyses.length) {
    console.log("⚠️ Nenhuma análise válida encontrada na janela.")
    await supabase.from("daily_picks").delete().neq("id", 0)
    return
  }

  await updateMatchAnalysisFromBrain(analyses)

  const radar = chooseRadar(analyses)
  const ticket = buildTicketFromRadar(radar)

  printRadar(radar)
  printTicket(ticket)

  await rebuildDailyPicks(radar, ticket)

  console.log("✅ Scoutly Brain V14 finalizado com sucesso.")
  console.log(`📡 Radar do dia gerado com ${radar.length} jogo(s).`)
  console.log(`🎫 Bilhete do dia definido com ${ticket.length} jogo(s).`)
}

runScoutlyBrain().catch((error) => {
  console.error("❌ Erro no Scoutly Brain V14:", error)
  process.exit(1)
})
