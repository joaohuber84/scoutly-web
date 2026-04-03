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
const PAST_GRACE_HOURS = 3
const RADAR_SIZE = 15
const TICKET_MIN_SIZE = 2
const TICKET_MAX_SIZE = 3

// ======================================
// PESO DAS FAMÍLIAS DE MERCADO
// gols / resultado / escanteios / cartões = mais fortes
// btts = forte, mas abaixo dos acima
// sot = intermediário
// shots = mais fraco
// ======================================
const FAMILY_SCORE_WEIGHT = {
  gols: 1.0,
  resultado: 0.98,
  escanteios: 0.97,
  cards: 0.95,
  btts: 0.92,
  sot: 0.88,
  shots: 0.80,
  outro: 0.70,
}

// ======================================
// LINHAS DINÂMICAS DISPONÍVEIS
// evita engessamento
// ======================================
const CORNER_OVER_LINES = [6.5, 7.5, 8.5]
const CORNER_UNDER_LINES = [11.5, 12.5, 13.5]

const CARDS_OVER_LINES = [1.5, 2.5, 3.5, 4.5, 5.5]

const SHOTS_OVER_LINES = [17.5, 19.5, 21.5, 23.5, 25.5]
const SOT_OVER_LINES = [5.5, 6.5, 7.5, 8.5]

// ======================================
// HELPERS
// ======================================

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

function formatDateInTZ(date, timeZone = TIMEZONE) {
  return new Intl.DateTimeFormat("sv-SE", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date)
}

function getTodayInTZ() {
  return formatDateInTZ(new Date(), TIMEZONE)
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

function getDayOffsetFromToday(kickoff) {
  if (!kickoff) return 999
  const kickoffDay = getKickoffDateOnly(kickoff)
  if (!kickoffDay) return 999

  const today = getTodayInTZ()
  const todayMs = new Date(`${today}T00:00:00`).getTime()
  const kickoffMs = new Date(`${kickoffDay}T00:00:00`).getTime()

  if (Number.isNaN(todayMs) || Number.isNaN(kickoffMs)) return 999

  return Math.round((kickoffMs - todayMs) / (24 * 60 * 60 * 1000))
}

function buildMatchLabel(row) {
  return `${row.home_team} x ${row.away_team}`
}

function compareByScoreThenKickoff(a, b) {
  if (b.main_score !== a.main_score) return b.main_score - a.main_score
  if (b.main_probability !== a.main_probability) {
    return b.main_probability - a.main_probability
  }
  return getKickoffMs(a.kickoff) - getKickoffMs(b.kickoff)
}

function compareByKickoff(a, b) {
  return getKickoffMs(a.kickoff) - getKickoffMs(b.kickoff)
}

function compareByRadarPriority(a, b) {
  const aDay = getDayOffsetFromToday(a.kickoff)
  const bDay = getDayOffsetFromToday(b.kickoff)

  if (aDay !== bDay) return aDay - bDay

  const aKickoff = getKickoffMs(a.kickoff)
  const bKickoff = getKickoffMs(b.kickoff)

  if (aKickoff !== bKickoff) return aKickoff - bKickoff

  if (b.main_score !== a.main_score) return b.main_score - a.main_score
  return String(a.league || "").localeCompare(String(b.league || ""))
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

function safeLeague(row) {
  return row.league || "Liga"
}

function hasMinimumAnalysis(row) {
  const values = [
    row.expected_home_goals,
    row.expected_away_goals,
    row.expected_home_shots,
    row.expected_away_shots,
    row.expected_home_sot,
    row.expected_away_sot,
    row.expected_corners,
    row.expected_cards,
    row.over25_prob,
    row.btts_prob,
    row.prob_shots,
    row.prob_sot,
    row.prob_cards,
  ]

  return values.some((v) => v !== null && Number(v) > 0)
}

function getStrengthLabel(score) {
  if (score >= 0.78) return "Forte"
  if (score >= 0.70) return "Boa"
  return "Moderada"
}

function getRhythmLabel(avgShots) {
  const shots = toNumber(avgShots)
  if (shots >= 24) return "Alto"
  if (shots >= 16) return "Moderado"
  return "Baixo"
}

// ======================================
// CLASSIFICAÇÃO DE MERCADO
// ======================================

function marketFamily(market) {
  const m = String(market || "").trim().toLowerCase()

  if (!m) return "outro"
  if (m.includes("escanteio")) return "escanteios"
  if (m.includes("finaliza") && m.includes("no gol")) return "sot"
  if (m.includes("finaliza")) return "shots"
  if (m.includes("cart")) return "cards"
  if (m.includes("ambas")) return "btts"

  if (
    m.includes("dupla chance") ||
    m.includes("empate") ||
    m.includes("vitória") ||
    m.includes("vitoria")
  ) {
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
    if (m.includes("não") || m.includes("nao")) return "defensivo"
    return "ofensivo"
  }

  if (family === "escanteios") return "estatistico"
  if (family === "shots") return "volume"
  if (family === "sot") return "precisao"
  if (family === "cards") return "disciplina"
  if (family === "resultado") return "protecao"

  return "equilibrado"
}

// ======================================
// SELEÇÃO DINÂMICA DE LINHAS
// ======================================

function pickDynamicOverLine(value, lines) {
  if (!lines.length) return null

  const eligible = lines.filter((line) => value >= line + 0.25)
  if (!eligible.length) return lines[0]

  return eligible[eligible.length - 1]
}

function pickDynamicUnderLine(value, lines) {
  if (!lines.length) return null

  const eligible = lines.filter((line) => value <= line - 0.25)
  if (!eligible.length) return lines[lines.length - 1]

  return eligible[0]
}

function buildSubfamily(prefix, side, line) {
  const clean = String(line).replace(".5", "5").replace(".", "")
  return `${prefix}_${side}${clean}`
}

// ======================================
// LOAD BASE TABLES
// ======================================

async function loadBaseTables() {
  const { data: matches, error: matchesError } = await supabase
    .from("matches")
    .select(`
      id,
      kickoff,
      league,
      country,
      region,
      priority,
      home_team,
      away_team,
      home_logo,
      away_logo,
      metrics,
      markets,
      probabilities,
      pick,
      probability,
      insight,
      game_profile,
      confidence_score,
      updated_at
    `)
    .order("kickoff", { ascending: true, nullsFirst: false })

  if (matchesError) {
    throw matchesError
  }

  const { data: analysis, error: analysisError } = await supabase
    .from("match_analysis")
    .select(`
      match_id,
      home_strength,
      away_strength,
      expected_home_goals,
      expected_away_goals,
      expected_home_shots,
      expected_away_shots,
      expected_home_sot,
      expected_away_sot,
      expected_corners,
      expected_cards,
      prob_over25,
      prob_btts,
      prob_corners,
      prob_shots,
      prob_sot,
      prob_cards,
      best_pick_1,
      best_pick_2,
      best_pick_3,
      aggressive_pick,
      analysis_text
    `)

  if (analysisError) throw analysisError

  return {
    matches: matches || [],
    analysis: analysis || [],
  }
}

// ======================================
// MERGE MATCH + ANALYSIS
// ======================================

function mergeMatchRow(matchRow, analysisMap) {
  const analysis = analysisMap.get(String(matchRow.id)) || {}

  const metrics = maybeJson(matchRow.metrics) || {}
  const markets = maybeJson(matchRow.markets) || {}
  const probabilities = maybeJson(matchRow.probabilities) || {}

  const expectedHomeGoals = toNumber(analysis.expected_home_goals, 0)
  const expectedAwayGoals = toNumber(analysis.expected_away_goals, 0)
  const expectedHomeShots = toNumber(analysis.expected_home_shots, 0)
  const expectedAwayShots = toNumber(analysis.expected_away_shots, 0)
  const expectedHomeSOT = toNumber(analysis.expected_home_sot, 0)
  const expectedAwaySOT = toNumber(analysis.expected_away_sot, 0)
  const expectedCorners = toNumber(analysis.expected_corners, 0)
  const expectedCards = toNumber(analysis.expected_cards, 0)

  const avgGoals =
    expectedHomeGoals > 0 || expectedAwayGoals > 0
      ? round1(expectedHomeGoals + expectedAwayGoals)
      : round1(toNumber(metrics.goals, 0))

  const avgShots =
    expectedHomeShots > 0 || expectedAwayShots > 0
      ? Math.round(expectedHomeShots + expectedAwayShots)
      : Math.round(toNumber(metrics.shots, 0))

  const avgShotsOnTarget =
    expectedHomeSOT > 0 || expectedAwaySOT > 0
      ? Math.round(expectedHomeSOT + expectedAwaySOT)
      : Math.round(
          toNumber(metrics.shots_on_target, 0) || toNumber(metrics.sot, 0)
        )

  const avgCorners =
    expectedCorners > 0
      ? round1(expectedCorners)
      : round1(toNumber(metrics.corners, 0) || toNumber(markets.corners, 0))

  const avgCards =
    expectedCards > 0
      ? round1(expectedCards)
      : round1(toNumber(metrics.cards, 0) || toNumber(markets.cards, 0))

  const avgFouls = round1(
    toNumber(metrics.fouls, 0) || toNumber(markets.fouls, 0)
  )

  const homeWinProb = clamp(toNumber(probabilities.home, 0), 0, 1)
  const drawProb = clamp(toNumber(probabilities.draw, 0), 0, 1)
  const awayWinProb = clamp(toNumber(probabilities.away, 0), 0, 1)

  const over15Prob = clamp(toNumber(markets.over15, 0), 0, 1)

  const over25Prob = clamp(
    toNumber(analysis.prob_over25, 0) || toNumber(markets.over25, 0),
    0,
    1
  )

  const bttsProb = clamp(
    toNumber(analysis.prob_btts, 0) || toNumber(markets.btts, 0),
    0,
    1
  )

  const shotsProb = clamp(
    toNumber(analysis.prob_shots, 0) || toNumber(markets.shots, 0),
    0,
    1
  )

  const sotProb = clamp(
    toNumber(analysis.prob_sot, 0) || toNumber(markets.shots_on_target, 0),
    0,
    1
  )

  const cardsProb = clamp(
    toNumber(analysis.prob_cards, 0) || toNumber(markets.cards, 0),
    0,
    1
  )

  const cornersProb = clamp(
    toNumber(analysis.prob_corners, 0) || toNumber(markets.corners, 0),
    0,
    1
  )

  const under25Prob = clamp(1 - over25Prob, 0, 1)

  const under35Prob = clamp(
    toNumber(markets.under35, 0) ||
      clamp(1 - Math.max(over25Prob - 0.18, 0), 0, 1),
    0,
    1
  )

  const confidenceScore = clamp(
    toNumber(matchRow.confidence_score, 0) ||
      toNumber(matchRow.probability, 0) ||
      over25Prob ||
      over15Prob,
    0,
    1
  )

  return {
    id: matchRow.id,
    kickoff: matchRow.kickoff,
    league: matchRow.league,
    country: matchRow.country,
    region: matchRow.region,
    priority: toNumber(matchRow.priority, 0),
    home_team: matchRow.home_team,
    away_team: matchRow.away_team,
    home_logo: matchRow.home_logo,
    away_logo: matchRow.away_logo,

    metrics,
    markets,
    probabilities,

    home_strength: round1(toNumber(analysis.home_strength, 0)),
    away_strength: round1(toNumber(analysis.away_strength, 0)),

    avg_goals: avgGoals,
    avg_corners: avgCorners,
    avg_shots: avgShots,
    avg_shots_on_target: avgShotsOnTarget,
    avg_cards: avgCards,
    avg_fouls: avgFouls,

    over15_prob: round2(over15Prob),
    over25_prob: round2(over25Prob),
    under25_prob: round2(under25Prob),
    under35_prob: round2(under35Prob),
    btts_prob: round2(bttsProb),
    prob_corners: round2(cornersProb),
    prob_shots: round2(shotsProb),
    prob_sot: round2(sotProb),
    prob_cards: round2(cardsProb),

    home_win_prob: round2(homeWinProb),
    draw_prob: round2(drawProb),
    away_win_prob: round2(awayWinProb),

    expected_home_goals: round2(expectedHomeGoals),
    expected_away_goals: round2(expectedAwayGoals),
    expected_home_shots: round2(expectedHomeShots),
    expected_away_shots: round2(expectedAwayShots),
    expected_home_sot: round2(expectedHomeSOT),
    expected_away_sot: round2(expectedAwaySOT),
    expected_corners: round2(expectedCorners),
    expected_cards: round2(expectedCards),

    confidence_score: round2(confidenceScore),

    best_pick_1: analysis.best_pick_1 || matchRow.pick || null,
    best_pick_2: analysis.best_pick_2 || null,
    best_pick_3: analysis.best_pick_3 || null,
    aggressive_pick: analysis.aggressive_pick || null,
    analysis_text: analysis.analysis_text || matchRow.insight || null,
    game_profile: matchRow.game_profile || null,
  }
}

async function loadActiveMatches() {
  const now = new Date()
  const minTime = new Date(now.getTime() - PAST_GRACE_HOURS * 60 * 60 * 1000)

  const { matches, analysis } = await loadBaseTables()

  console.log("DEBUG DATA HOJE:", getTodayInTZ())
  console.log("DEBUG TOTAL RAW MATCHES:", matches.length)
  console.log("DEBUG TOTAL ANALYSIS:", analysis.length)

  const analysisMap = new Map(
    analysis.map((row) => [String(row.match_id), row])
  )

  const merged = matches.map((row) => mergeMatchRow(row, analysisMap))

  const filtered = merged.filter((row) => {
    const kickoffDate = row.kickoff ? new Date(row.kickoff) : null
    const kickoffValid = kickoffDate && !Number.isNaN(kickoffDate.getTime())

    return kickoffValid && kickoffDate.getTime() >= minTime.getTime()
  })

  const finalRows = filtered
    .filter((row) => row.home_team && row.away_team && row.league)
    .filter((row) => hasMinimumAnalysis(row))

  console.log("DEBUG TOTAL FILTRADOS ATIVOS:", finalRows.length)

  return finalRows.sort(compareByKickoff)
}

// ======================================
// PERFIL DO JOGO
// ======================================

function getGameProfile(row) {
  if (row.game_profile && String(row.game_profile).trim()) {
    return String(row.game_profile).trim()
  }

  const avgGoals = toNumber(row.avg_goals)
  const avgShots = toNumber(row.avg_shots)
  const avgCorners = toNumber(row.avg_corners)
  const avgSOT = toNumber(row.avg_shots_on_target)
  const avgCards = toNumber(row.avg_cards)

  const over25Prob = toNumber(row.over25_prob)
  const under25Prob = toNumber(row.under25_prob)
  const under35Prob = toNumber(row.under35_prob)
  const bttsProb = toNumber(row.btts_prob)
  const bttsNoProb = clamp(1 - bttsProb, 0, 1)

  if (
    avgGoals >= 2.8 ||
    over25Prob >= 0.67 ||
    (avgShots >= 24 && bttsProb >= 0.62)
  ) {
    return "ofensivo"
  }

  if (
    avgCorners >= 8.8 &&
    avgShots >= 21 &&
    avgGoals >= 2.1
  ) {
    return "estatistico"
  }

  if (
    avgShots >= 22 &&
    avgSOT >= 7 &&
    avgCorners < 9.3
  ) {
    return "volume"
  }

  if (
    avgSOT >= 7 &&
    avgGoals >= 2.2 &&
    avgShots <= 24
  ) {
    return "precisao"
  }

  if (
    avgCards >= 3.8 &&
    avgGoals <= 3.0
  ) {
    return "disciplinar"
  }

  if (
    avgGoals <= 2.1 &&
    under25Prob >= 0.72 &&
    bttsNoProb >= 0.70 &&
    avgShots <= 18
  ) {
    return "defensivo"
  }

  if (under35Prob >= 0.79 && avgGoals <= 2.5 && avgShots <= 20) {
    return "controlado"
  }

  return "equilibrado"
}

// ======================================
// INSIGHT
// ======================================

function buildInsight(row, bestPick, profile) {
  const avgGoals = round1(row.avg_goals)
  const avgCorners = round1(row.avg_corners)
  const avgShots = Math.round(toNumber(row.avg_shots))
  const avgSOT = Math.round(toNumber(row.avg_shots_on_target))
  const avgCards = round1(row.avg_cards)
  const rhythm = getRhythmLabel(avgShots).toLowerCase()

  const market = String(bestPick.market || "").trim().toLowerCase()

  if (market.includes("mais de 2.5 gols")) {
    return `A leitura Scoutly projeta um jogo mais aberto, com potencial real para 3 ou mais gols. A média esperada está em ${avgGoals} gols, com ritmo ofensivo ${rhythm}, reforçando essa linha como uma leitura forte da partida.`
  }

  if (market.includes("mais de 1.5 gols")) {
    return `A leitura Scoutly projeta um confronto com boa chance de pelo menos 2 gols. A média esperada está em ${avgGoals} gols, com ritmo ofensivo ${rhythm} e cenário favorável para essa linha.`
  }

  if (market.includes("menos de 2.5 gols")) {
    return `A leitura Scoutly indica um jogo travado, com baixa projeção ofensiva e controle no placar. A expectativa está em ${avgGoals} gols, com ritmo ${rhythm}, sustentando a linha de menos de 2.5 gols.`
  }

  if (market.includes("menos de 3.5 gols")) {
    return `A leitura Scoutly indica um jogo controlado, sem expectativa de explosão ofensiva. A projeção está em ${avgGoals} gols, com ritmo ${rhythm}, tornando a linha de menos de 3.5 gols uma opção consistente.`
  }

  if (market.includes("ambas não marcam") || market.includes("ambas nao marcam")) {
    return `A leitura Scoutly vê um confronto com baixa tendência de gols dos dois lados. A projeção ofensiva é moderada, e o cenário sugere maior chance de uma das equipes passar em branco.`
  }

  if (market.includes("ambas marcam")) {
    return `A leitura Scoutly identifica espaço para gols dos dois lados. A expectativa ofensiva, o ritmo ${rhythm} e o equilíbrio do confronto criam um cenário interessante para ambas marcam.`
  }

  if (market.includes("escanteios")) {
    if (market.includes("mais de")) {
      return `A leitura Scoutly projeta cerca de ${avgCorners} escanteios, com ritmo ${rhythm}, pressão ofensiva e volume suficiente para sustentar uma linha de over corners com boa coerência estatística.`
    }

    return `A leitura Scoutly projeta cerca de ${avgCorners} escanteios, indicando um cenário mais controlado para o mercado de cantos, sem necessidade de esticar demais a linha.`
  }

  if (market.includes("finalizações") && market.includes("no gol")) {
    return `A leitura Scoutly projeta cerca de ${avgSOT} finalizações no gol, indicando um cenário de boa produção ofensiva com precisão suficiente para sustentar esse mercado.`
  }

  if (market.includes("finalizações")) {
    return `A leitura Scoutly projeta cerca de ${avgShots} finalizações totais, com ritmo ${rhythm} e sustentação estatística clara para transformar volume ofensivo em oportunidade real de mercado.`
  }

  if (market.includes("cart")) {
    return `A leitura Scoutly projeta cerca de ${avgCards} cartões, sugerindo um confronto com nível de contato e tensão suficiente para transformar disciplina em oportunidade de mercado.`
  }

  if (market.includes("dupla chance")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate. O equilíbrio da partida ainda pede segurança, e por isso a dupla chance aparece como uma leitura sólida.`
  }

  return `A leitura Scoutly classifica este confronto como ${profile}, combinando projeção de ${avgGoals} gols, ${avgCorners} escanteios, ${avgShots} finalizações e ${avgSOT} no gol para destacar essa oportunidade.`
}

// ======================================
// PUSH PADRONIZADO
// ======================================

function pushCandidate(candidates, item) {
  const family = item.family || marketFamily(item.market)
  const familyWeight = FAMILY_SCORE_WEIGHT[family] || 0.7
  const adjustedScore = clamp(toNumber(item.score) * familyWeight, 0, 1)

  candidates.push({
    ...item,
    family,
    probability: clamp(toNumber(item.probability), 0, 1),
    score: adjustedScore,
  })
}

// ======================================
// CANTOS
// menos engessado
// ======================================

function buildCornerCandidates(row, profile) {
  const candidates = []

  const avgCorners = toNumber(row.avg_corners)
  const avgShots = toNumber(row.avg_shots)
  const avgGoals = toNumber(row.avg_goals)
  const avgSOT = toNumber(row.avg_shots_on_target)

  function add(market, probability, score, subfamily, macro = "estatistico") {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "escanteios",
      subfamily,
      macro,
    })
  }

  // OVER CORNERS
  const boostedOverCorners =
    avgCorners +
    (avgShots >= 20 ? 0.35 : 0) +
    (avgShots >= 23 ? 0.25 : 0) +
    (avgSOT >= 7 ? 0.20 : 0) +
    (avgGoals >= 2.6 ? 0.15 : 0) +
    (profile === "estatistico" ? 0.20 : 0) +
    (profile === "volume" ? 0.18 : 0) +
    (profile === "precisao" ? 0.10 : 0)

  if (boostedOverCorners >= 6.0) {
    const line = pickDynamicOverLine(boostedOverCorners, CORNER_OVER_LINES)

    if (line !== null) {
      const probability = clamp(
        0.60 +
          (boostedOverCorners - line) * 0.11 +
          (avgShots >= 21 ? 0.02 : 0),
        0.60,
        0.91
      )

      const score = clamp(
        0.68 +
          (boostedOverCorners - line) * 0.09 +
          (profile === "estatistico" ? 0.03 : 0) +
          (avgSOT >= 7 ? 0.02 : 0),
        0.60,
        0.90
      )

      add(
        `Mais de ${line} escanteios`,
        probability,
        score,
        buildSubfamily("corners", "over", line)
      )
    }
  }

  // UNDER CORNERS
  const controlledCorners =
    avgCorners -
    (avgShots <= 18 ? 0.30 : 0) -
    (avgShots <= 16 ? 0.20 : 0) -
    (avgGoals <= 2.1 ? 0.15 : 0) -
    (profile === "defensivo" ? 0.18 : 0) -
    (profile === "controlado" ? 0.20 : 0)

  if (controlledCorners <= 10.6) {
    const referenceUnder = controlledCorners + 4.0
    const line = pickDynamicUnderLine(referenceUnder, CORNER_UNDER_LINES)

    if (line !== null) {
      const probability = clamp(
        0.61 +
          (line - referenceUnder) * 0.07 +
          (avgShots <= 18 ? 0.02 : 0),
        0.58,
        0.89
      )

      const score = clamp(
        0.66 +
          (line - referenceUnder) * 0.06 +
          (profile === "controlado" ? 0.03 : 0) +
          (profile === "defensivo" ? 0.02 : 0),
        0.58,
        0.87
      )

      add(
        `Menos de ${line} escanteios`,
        probability,
        score,
        buildSubfamily("corners", "under", line)
      )
    }
  }

  candidates.forEach((c) => {
    if (String(c.subfamily).includes("under") && avgShots >= 23) {
      c.score = clamp(c.score - 0.05, 0, 1)
      c.probability = clamp(c.probability - 0.04, 0, 1)
    }

    if (String(c.subfamily).includes("over") && avgShots <= 17) {
      c.score = clamp(c.score - 0.04, 0, 1)
      c.probability = clamp(c.probability - 0.03, 0, 1)
    }

    if (profile === "ofensivo" && String(c.subfamily).includes("over") && avgGoals >= 2.8) {
      c.score = clamp(c.score + 0.02, 0, 1)
    }

    if (
      (profile === "defensivo" || profile === "controlado") &&
      String(c.subfamily).includes("under")
    ) {
      c.score = clamp(c.score + 0.02, 0, 1)
    }
  })

  return candidates
}

// ======================================
// CARTÕES (APENAS OVER, MAIS NATURAL)
// ======================================

function buildCardsCandidates(row, profile) {
  const candidates = []

  const avgCards = toNumber(row.avg_cards)
  const avgGoals = toNumber(row.avg_goals)
  const avgShots = toNumber(row.avg_shots)

  function add(market, probability, score, subfamily) {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "cards",
      subfamily,
      macro: "disciplina",
    })
  }

  const adjustedCards =
    avgCards +
    (avgGoals <= 2.4 ? 0.25 : 0) +
    (avgGoals <= 2.0 ? 0.20 : 0) +
    (profile === "disciplinar" ? 0.30 : 0) +
    (profile === "defensivo" ? 0.15 : 0)

  if (adjustedCards >= 1.8) {
    const line = pickDynamicOverLine(adjustedCards, CARDS_OVER_LINES)

    if (line !== null) {
      const probability = clamp(
        0.60 + (adjustedCards - line) * 0.12,
        0.58,
        0.90
      )

      const score = clamp(
        0.66 +
          (adjustedCards - line) * 0.10 +
          (profile === "disciplinar" ? 0.03 : 0),
        0.60,
        0.88
      )

      add(
        `Mais de ${line} cartões`,
        probability,
        score,
        buildSubfamily("cards", "over", line)
      )
    }
  }

  return candidates
}

// ======================================
// FINALIZAÇÕES TOTAIS (MAIS FRACO)
// ======================================

function buildShotsCandidates(row, profile) {
  const candidates = []

  const avgShots = toNumber(row.avg_shots)
  const avgGoals = toNumber(row.avg_goals)

  function add(market, probability, score, subfamily) {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "shots",
      subfamily,
      macro: "volume",
    })
  }

  const adjustedShots =
    avgShots +
    (avgGoals >= 2.5 ? 0.6 : 0) +
    (profile === "volume" ? 0.5 : 0)

  if (adjustedShots >= 16.2) {
    const line = pickDynamicOverLine(adjustedShots, SHOTS_OVER_LINES)

    if (line !== null) {
      const probability = clamp(
        0.57 + (adjustedShots - line) * 0.085,
        0.54,
        0.84
      )

      const score = clamp(
        0.59 + (adjustedShots - line) * 0.065,
        0.54,
        0.80
      )

      add(
        `Mais de ${line} finalizações`,
        probability,
        score,
        buildSubfamily("shots", "over", line)
      )
    }
  }

  return candidates
}

// ======================================
// FINALIZAÇÕES NO GOL (MAIS FRACO AINDA)
// ======================================

function buildSOTCandidates(row, profile) {
  const candidates = []

  const avgSOT = toNumber(row.avg_shots_on_target)
  const avgGoals = toNumber(row.avg_goals)

  function add(market, probability, score, subfamily) {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "sot",
      subfamily,
      macro: "precisao",
    })
  }

  const adjustedSOT =
    avgSOT +
    (avgGoals >= 2.6 ? 0.25 : 0) +
    (profile === "precisao" ? 0.25 : 0)

  if (adjustedSOT >= 4.6) {
    const line = pickDynamicOverLine(adjustedSOT, SOT_OVER_LINES)

    if (line !== null) {
      const probability = clamp(
        0.57 + (adjustedSOT - line) * 0.08,
        0.54,
        0.83
      )

      const score = clamp(
        0.58 + (adjustedSOT - line) * 0.065,
        0.53,
        0.79
      )

      add(
        `Mais de ${line} finalizações no gol`,
        probability,
        score,
        buildSubfamily("sot", "over", line)
      )
    }
  }

  return candidates
}

// ======================================
// GOLS (CARRO-CHEFE)
// ======================================

function buildGoalsCandidates(row, profile) {
  const candidates = []

  const avgGoals = toNumber(row.avg_goals)
  const over25 = toNumber(row.over25_prob)
  const under25 = toNumber(row.under25_prob)

  function add(market, probability, score, subfamily, macro) {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "gols",
      subfamily,
      macro,
    })
  }

  // OVER 1.5
  if (avgGoals >= 1.6) {
    const prob = clamp(over25 + 0.18, 0.62, 0.92)

    add(
      "Mais de 1.5 gols",
      prob,
      prob,
      "gols_over15",
      "ofensivo"
    )
  }

  // OVER 2.5
  if (avgGoals >= 2.2) {
    const prob = clamp(over25, 0.60, 0.90)

    add(
      "Mais de 2.5 gols",
      prob,
      prob,
      "gols_over25",
      "ofensivo"
    )
  }

  // UNDER 2.5
  if (avgGoals <= 2.4) {
    const prob = clamp(under25, 0.60, 0.90)

    add(
      "Menos de 2.5 gols",
      prob,
      prob,
      "gols_under25",
      "defensivo"
    )
  }

  // UNDER 3.5
  if (avgGoals <= 3.0) {
    const prob = clamp(row.under35_prob, 0.62, 0.92)

    add(
      "Menos de 3.5 gols",
      prob,
      prob,
      "gols_under35",
      "defensivo"
    )
  }

  return candidates
}

// ======================================
// BTTS
// ======================================

function buildBTTS(row, profile) {
  const candidates = []

  const btts = toNumber(row.btts_prob)
  const avgGoals = toNumber(row.avg_goals)

  function add(market, probability, score, subfamily, macro) {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "btts",
      subfamily,
      macro,
    })
  }

  if (btts >= 0.58 && avgGoals >= 2.2) {
    add(
      "Ambas marcam",
      btts,
      btts,
      "btts_yes",
      "ofensivo"
    )
  }

  if (btts <= 0.45 && avgGoals <= 2.4) {
    add(
      "Ambas não marcam",
      1 - btts,
      1 - btts,
      "btts_no",
      "defensivo"
    )
  }

  return candidates
}

// ======================================
// RESULTADO / DUPLA CHANCE
// ======================================

function buildResultCandidates(row, profile) {
  const candidates = []

  const homeWin = toNumber(row.home_win_prob)
  const draw = toNumber(row.draw_prob)
  const awayWin = toNumber(row.away_win_prob)

  const homeOrDraw = clamp(homeWin + draw, 0, 1)
  const awayOrDraw = clamp(awayWin + draw, 0, 1)

  const diff = Math.abs(homeWin - awayWin)

  function add(market, probability, score, subfamily) {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "resultado",
      subfamily,
      macro: "protecao",
    })
  }

  if (homeWin >= awayWin && homeOrDraw >= 0.68) {
    const probability = clamp(
      homeOrDraw + (diff >= 0.10 ? 0.02 : 0),
      0.68,
      0.92
    )

    const score = clamp(
      probability + (profile === "equilibrado" ? 0.02 : 0),
      0.66,
      0.90
    )

    add(
      `Dupla chance ${row.home_team} ou empate`,
      probability,
      score,
      "resultado_home_draw"
    )
  }

  if (awayWin > homeWin && awayOrDraw >= 0.68) {
    const probability = clamp(
      awayOrDraw + (diff >= 0.10 ? 0.02 : 0),
      0.68,
      0.92
    )

    const score = clamp(
      probability + (profile === "equilibrado" ? 0.02 : 0),
      0.66,
      0.90
    )

    add(
      `Dupla chance ${row.away_team} ou empate`,
      probability,
      score,
      "resultado_away_draw"
    )
  }

  return candidates
}

// ======================================
// MOTOR PRINCIPAL DE MERCADOS
// ======================================

function buildMarketCandidates(row, options = {}) {
  const relaxed = options.relaxed === true
  const profile = getGameProfile(row)

  let candidates = []

  candidates.push(...buildGoalsCandidates(row, profile))
  candidates.push(...buildBTTS(row, profile))
  candidates.push(...buildResultCandidates(row, profile))
  candidates.push(...buildCornerCandidates(row, profile))
  candidates.push(...buildCardsCandidates(row, profile))
  candidates.push(...buildShotsCandidates(row, profile))
  candidates.push(...buildSOTCandidates(row, profile))

  candidates = candidates.map((item) => {
    let score = toNumber(item.score)

    // reforço extra da hierarquia
    if (item.family === "gols") score += 0.03
    if (item.family === "resultado") score += 0.025
    if (item.family === "escanteios") score += 0.02
    if (item.family === "cards") score += 0.015

    // shots / sot sempre abaixo dos carros-chefe
    if (item.family === "shots") score -= 0.035
    if (item.family === "sot") score -= 0.025

    // ajustes por perfil
    if (profile === "ofensivo" && item.macro === "ofensivo") score += 0.02
    if (profile === "defensivo" && item.macro === "defensivo") score += 0.02
    if (profile === "estatistico" && item.family === "escanteios") score += 0.02
    if (profile === "disciplinar" && item.family === "cards") score += 0.02
    if (profile === "equilibrado" && item.family === "resultado") score += 0.015

    return {
      ...item,
      score: clamp(score, 0, 1),
    }
  })

  const minProbability = relaxed ? 0.52 : 0.56
  const minScore = relaxed ? 0.53 : 0.58

  return candidates
    .filter((c) => c.probability >= minProbability && c.score >= minScore)
    .sort((a, b) => b.score - a.score)
}

// ======================================
// ESCOLHA DA PICK PRINCIPAL + ALTERNATIVAS
// ======================================

function chooseBestAndAlternatives(candidates, row) {
  if (!candidates.length) {
    return {
      best: null,
      alternatives: [],
    }
  }

  const sorted = [...candidates].sort((a, b) => b.score - a.score)
  const best = sorted[0]

  const alternatives = []
  const usedMarkets = new Set([best.market])
  const usedFamilies = new Set([best.family])

  for (const item of sorted.slice(1)) {
    if (!item?.market) continue
    if (usedMarkets.has(item.market)) continue
    if (usedFamilies.has(item.family)) continue

    alternatives.push(item)
    usedMarkets.add(item.market)
    usedFamilies.add(item.family)

    if (alternatives.length >= 2) break
  }

  if (alternatives.length < 2) {
    for (const item of sorted.slice(1)) {
      if (!item?.market) continue
      if (usedMarkets.has(item.market)) continue

      alternatives.push(item)
      usedMarkets.add(item.market)

      if (alternatives.length >= 2) break
    }
  }

  if (alternatives.length < 2 && row) {
    const relaxedCandidates = buildMarketCandidates(row, { relaxed: true })

    for (const item of relaxedCandidates) {
      if (!item?.market) continue
      if (usedMarkets.has(item.market)) continue

      alternatives.push(item)
      usedMarkets.add(item.market)

      if (alternatives.length >= 2) break
    }
  }

  return { best, alternatives }
}

// ======================================
// BUILD DA ANÁLISE FINAL DO JOGO
// ======================================

function buildAnalysisFromRow(row) {
  const candidates = buildMarketCandidates(row)
  if (!candidates.length) return null

  const profile = getGameProfile(row)
  const { best, alternatives } = chooseBestAndAlternatives(candidates, row)
  if (!best) return null

  const aggressivePick =
    alternatives.find((x) =>
      String(x.subfamily || "").includes("gols_over") ||
      String(x.subfamily || "").includes("corners_over") ||
      String(x.subfamily || "").includes("cards_over")
    )?.market || row.aggressive_pick || null

  return {
    match_id: row.id,
    home_team: row.home_team,
    away_team: row.away_team,
    league: safeLeague(row),
    kickoff: row.kickoff,
    home_logo: row.home_logo,
    away_logo: row.away_logo,

    avg_goals: round1(row.avg_goals),
    avg_corners: round1(row.avg_corners),
    avg_shots: Math.round(toNumber(row.avg_shots)),
    avg_shots_on_target: Math.round(toNumber(row.avg_shots_on_target)),
    avg_cards: round1(row.avg_cards),
    avg_fouls: round1(row.avg_fouls),

    home_strength: round1(row.home_strength),
    away_strength: round1(row.away_strength),

    home_win_prob: round2(row.home_win_prob),
    draw_prob: round2(row.draw_prob),
    away_win_prob: round2(row.away_win_prob),

    over25_prob: round2(row.over25_prob),
    under25_prob: round2(row.under25_prob),
    under35_prob: round2(row.under35_prob),
    btts_prob: round2(row.btts_prob),
    prob_corners: round2(row.prob_corners),
    prob_shots: round2(row.prob_shots),
    prob_sot: round2(row.prob_sot),
    prob_cards: round2(row.prob_cards),

    game_profile: profile,

    main_pick: best.market,
    main_probability: round2(best.probability),
    main_score: round2(best.score),
    main_family: best.family,
    main_subfamily: best.subfamily,
    main_macro: best.macro,

    strength: getStrengthLabel(best.score),
    rhythm: getRhythmLabel(row.avg_shots),

    insight: buildInsight(row, best, profile),

    best_pick_1: best.market,
    best_pick_2: alternatives[0]?.market || null,
    best_pick_3: alternatives[1]?.market || null,
    aggressive_pick: aggressivePick,

    alternatives_count: alternatives.filter(Boolean).length,
  }
}

// ======================================
// RADAR
// ======================================

function chooseRadar(analyses) {
  const today = []
  const future = []

  for (const item of analyses) {
    const dayOffset = getDayOffsetFromToday(item.kickoff)
    if (dayOffset <= 0) today.push(item)
    else future.push(item)
  }

  const todaySorted = [...today].sort(compareByScoreThenKickoff)
  const futureSorted = [...future].sort(compareByScoreThenKickoff)
  const radarPool = [...todaySorted, ...futureSorted]

  const radar = []
  const usedMatchIds = new Set()
  const usedExactMarkets = {}
  const usedFamilies = {}
  const usedLeagues = {}

  for (const item of radarPool) {
    if (usedMatchIds.has(item.match_id)) continue

    const exactMarketCount = usedExactMarkets[item.main_pick] || 0
    const familyCount = usedFamilies[item.main_family] || 0
    const leagueCount = usedLeagues[item.league] || 0

    if (exactMarketCount >= 2) continue
    if (leagueCount >= 3) continue

    if (item.main_family === "gols" && familyCount >= 4) continue
    if (item.main_family === "resultado" && familyCount >= 3) continue
    if (item.main_family === "escanteios" && familyCount >= 3) continue
    if (item.main_family === "cards" && familyCount >= 3) continue
    if (item.main_family === "btts" && familyCount >= 2) continue
    if (item.main_family === "shots" && familyCount >= 2) continue
    if (item.main_family === "sot" && familyCount >= 2) continue

    radar.push(item)
    usedMatchIds.add(item.match_id)
    usedExactMarkets[item.main_pick] = exactMarketCount + 1
    usedFamilies[item.main_family] = familyCount + 1
    usedLeagues[item.league] = leagueCount + 1

    if (radar.length === RADAR_SIZE) break
  }

  if (radar.length < RADAR_SIZE) {
    const backup = [...analyses].sort(compareByScoreThenKickoff)

    for (const item of backup) {
      if (usedMatchIds.has(item.match_id)) continue
      radar.push(item)
      usedMatchIds.add(item.match_id)
      if (radar.length === RADAR_SIZE) break
    }
  }

  return radar.sort(compareByRadarPriority)
}

// ======================================
// BILHETE
// ======================================

function buildTicketFromRadar(radar) {
  const ranked = [...radar].sort((a, b) => {
    const aDay = getDayOffsetFromToday(a.kickoff)
    const bDay = getDayOffsetFromToday(b.kickoff)

    if (aDay !== bDay) return aDay - bDay
    return compareByScoreThenKickoff(a, b)
  })

  const uniqueMatches = []
  const usedMatches = new Set()

  for (const item of ranked) {
    if (usedMatches.has(item.match_id)) continue
    uniqueMatches.push(item)
    usedMatches.add(item.match_id)
  }

  const desiredSize =
    uniqueMatches.length >= 3 ? TICKET_MAX_SIZE : TICKET_MIN_SIZE

  const ticket = []
  const usedTicketMatches = new Set()
  const usedFamilies = new Set()

  for (const item of ranked) {
    if (usedTicketMatches.has(item.match_id)) continue
    if (usedFamilies.has(item.main_family)) continue

    ticket.push(item)
    usedTicketMatches.add(item.match_id)
    usedFamilies.add(item.main_family)

    if (ticket.length === desiredSize) break
  }

  if (ticket.length < desiredSize) {
    for (const item of ranked) {
      if (usedTicketMatches.has(item.match_id)) continue

      ticket.push(item)
      usedTicketMatches.add(item.match_id)

      if (ticket.length === desiredSize) break
    }
  }

  return ticket.sort(compareByRadarPriority)
}

// ======================================
// UPDATE MATCH_ANALYSIS
// ======================================

async function updateMatchAnalysisFromBrain(analyses) {
  for (const item of analyses) {
    const { error } = await supabase
      .from("match_analysis")
      .update({
        best_pick_1: item.best_pick_1,
        best_pick_2: item.best_pick_2,
        best_pick_3: item.best_pick_3,
        aggressive_pick: item.aggressive_pick,
        analysis_text: item.insight,
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

// ======================================
// REBUILD DAILY PICKS
// ======================================

async function rebuildDailyPicks(radar, ticket) {
  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (deleteError) throw deleteError

  const orderedRadar = [...radar].sort(compareByRadarPriority)

  const rows = orderedRadar.map((item, index) => {
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

// ======================================
// RUNNER FINAL
// ======================================

async function runScoutlyBrain() {
  console.log("🧠 Scoutly Brain FINAL V2 iniciado...")

  const matches = await loadActiveMatches()
  console.log(`📦 Jogos ativos carregados para análise: ${matches.length}`)

  const analyses = matches
    .map(buildAnalysisFromRow)
    .filter(Boolean)

  console.log(`🧪 Análises válidas geradas: ${analyses.length}`)

  if (!analyses.length) {
    console.log("⚠️ Nenhuma análise válida encontrada para a janela ativa.")
    await supabase.from("daily_picks").delete().neq("id", 0)
    return
  }

  await updateMatchAnalysisFromBrain(analyses)

  const radar = chooseRadar(analyses)
  const ticket = buildTicketFromRadar(radar)

  console.log("DEBUG RADAR FINAL:")
  radar.forEach((item, index) => {
    console.log(
      index + 1,
      buildMatchLabel(item),
      "->",
      item.main_pick,
      "| family:",
      item.main_family,
      "| subfamily:",
      item.main_subfamily,
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

  console.log("🎟️ BILHETE FINAL:")
  ticket.forEach((item, index) => {
    console.log(
      index + 1,
      buildMatchLabel(item),
      "->",
      item.main_pick,
      "| family:",
      item.main_family,
      "| subfamily:",
      item.main_subfamily,
      "| strength:",
      item.strength,
      "| score:",
      item.main_score,
      "| kickoff:",
      item.kickoff
    )
  })

  await rebuildDailyPicks(radar, ticket)

  console.log("✅ Scoutly Brain FINAL V2 finalizado com sucesso.")
  console.log(`📡 Radar do dia gerado com ${radar.length} jogo(s).`)
  console.log(`🎫 Bilhete do dia definido com ${ticket.length} jogo(s).`)
}

runScoutlyBrain().catch((error) => {
  console.error("❌ Erro no Scoutly Brain FINAL V2:", error)
  process.exit(1)
})
