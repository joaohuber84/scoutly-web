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

function marketFamily(market) {
  const m = String(market || "").trim().toLowerCase()

  if (!m) return "outro"
  if (m.includes("escanteio")) return "escanteios"
  if (m.includes("finaliza")) return "shots"
  if (m.includes("no gol")) return "sot"
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
    avgCorners >= 9.1 &&
    avgShots >= 21 &&
    avgGoals >= 2.2
  ) {
    return "estatistico"
  }

  if (
    avgShots >= 22 &&
    avgSOT >= 7 &&
    avgCorners < 9.2
  ) {
    return "volume"
  }

  if (
    avgSOT >= 7 &&
    avgGoals >= 2.2 &&
    avgShots <= 23
  ) {
    return "precisao"
  }

  if (
    avgCards >= 4.2 &&
    avgGoals <= 2.8
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

function buildInsight(row, bestPick, profile) {
  if (row.analysis_text && String(row.analysis_text).trim()) {
    return String(row.analysis_text).trim()
  }

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
    return `A leitura Scoutly projeta cerca de ${avgCorners} escanteios, com ritmo ${rhythm} e volume ofensivo suficiente para transformar esse mercado em uma oportunidade estatística relevante.`
  }

  if (market.includes("finalizações")) {
    return `A leitura Scoutly projeta cerca de ${avgShots} finalizações totais, com ritmo ${rhythm} e sustentação estatística clara para transformar volume ofensivo em oportunidade real de mercado.`
  }

  if (market.includes("no gol")) {
    return `A leitura Scoutly projeta cerca de ${avgSOT} finalizações no gol, indicando um cenário de boa produção ofensiva com precisão suficiente para sustentar esse mercado.`
  }

  if (market.includes("cart")) {
    return `A leitura Scoutly projeta cerca de ${avgCards} cartões, sugerindo um confronto com nível de contato e tensão suficiente para transformar disciplina em oportunidade de mercado.`
  }

  if (market.includes("dupla chance")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate. O equilíbrio da partida ainda pede segurança, e por isso a dupla chance aparece como uma leitura sólida.`
  }

  return `A leitura Scoutly classifica este confronto como ${profile}, combinando projeção de ${avgGoals} gols, ${avgCorners} escanteios, ${avgShots} finalizações e ${avgSOT} no gol para destacar essa oportunidade.`
}

function pushCandidate(candidates, item) {
  candidates.push({
    ...item,
    probability: clamp(toNumber(item.probability), 0, 1),
    score: clamp(toNumber(item.score), 0, 1),
  })
}

function buildCornerCandidates(row, profile) {
  const candidates = []

  const avgCorners = toNumber(row.avg_corners)
  const avgShots = toNumber(row.avg_shots)
  const avgGoals = toNumber(row.avg_goals)

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

  if (avgCorners >= 6.5) {
    add(
      "Mais de 6.5 escanteios",
      clamp(0.60 + (avgCorners - 6.5) * 0.05 + (avgShots >= 18 ? 0.02 : 0), 0.60, 0.92),
      clamp(0.67 + (avgCorners - 6.5) * 0.035 + (profile === "estatistico" ? 0.02 : 0), 0.60, 0.88),
      "corners_over65"
    )
  }

  if (avgCorners >= 7.4) {
    add(
      "Mais de 7.5 escanteios",
      clamp(0.58 + (avgCorners - 7.4) * 0.05 + (avgShots >= 20 ? 0.02 : 0), 0.58, 0.89),
      clamp(0.66 + (avgCorners - 7.4) * 0.034 + (avgGoals >= 2.2 ? 0.01 : 0), 0.58, 0.86),
      "corners_over75"
    )
  }

  if (avgCorners >= 8.6) {
    add(
      "Mais de 8.5 escanteios",
      clamp(0.55 + (avgCorners - 8.6) * 0.05 + (avgShots >= 21 ? 0.02 : 0), 0.55, 0.85),
      clamp(0.64 + (avgCorners - 8.6) * 0.032 + (profile === "estatistico" ? 0.01 : 0), 0.55, 0.83),
      "corners_over85"
    )
  }

  if (avgCorners <= 9.0) {
    add(
      "Menos de 12.5 escanteios",
      clamp(0.63 + (9.0 - avgCorners) * 0.04, 0.63, 0.91),
      clamp(0.70 + (9.0 - avgCorners) * 0.028 + (profile === "controlado" ? 0.02 : 0), 0.63, 0.88),
      "corners_under125"
    )
  }

  if (avgCorners <= 8.2) {
    add(
      "Menos de 11.5 escanteios",
      clamp(0.60 + (8.2 - avgCorners) * 0.04, 0.60, 0.88),
      clamp(0.68 + (8.2 - avgCorners) * 0.028 + (avgShots <= 19 ? 0.01 : 0), 0.60, 0.85),
      "corners_under115"
    )
  }

  if (avgCorners <= 7.2) {
    add(
      "Menos de 10.5 escanteios",
      clamp(0.56 + (7.2 - avgCorners) * 0.04, 0.56, 0.84),
      clamp(0.65 + (7.2 - avgCorners) * 0.028 + (profile === "defensivo" ? 0.01 : 0), 0.56, 0.81),
      "corners_under105"
    )
  }

  candidates.forEach((c) => {
    if (c.subfamily.includes("under") && avgShots >= 23) {
      c.score = clamp(c.score - 0.05, 0, 1)
      c.probability = clamp(c.probability - 0.04, 0, 1)
    }

    if (c.subfamily.includes("over") && avgShots <= 17) {
      c.score = clamp(c.score - 0.05, 0, 1)
      c.probability = clamp(c.probability - 0.04, 0, 1)
    }

    if (profile === "volume" && c.family === "escanteios") {
      c.score = clamp(c.score - 0.02, 0, 1)
    }
  })

  return candidates
}

function buildShotsCandidates(row, profile) {
  const candidates = []

  const avgShots = toNumber(row.avg_shots)
  const avgGoals = toNumber(row.avg_goals)
  const probShots = toNumber(row.prob_shots)

  function add(market, probability, score, subfamily, macro = "volume") {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "shots",
      subfamily,
      macro,
    })
  }

  if (avgShots >= 18.5) {
    add(
      "Mais de 17.5 finalizações",
      clamp(Math.max(probShots, 0.61) + (avgShots - 18.5) * 0.02, 0.61, 0.91),
      clamp(0.68 + (avgShots - 18.5) * 0.022 + (profile === "volume" ? 0.03 : 0), 0.61, 0.89),
      "shots_over175"
    )
  }

  if (avgShots >= 20.0) {
    add(
      "Mais de 19.5 finalizações",
      clamp(Math.max(probShots - 0.01, 0.58) + (avgShots - 20.0) * 0.02, 0.58, 0.88),
      clamp(0.66 + (avgShots - 20.0) * 0.022 + (avgGoals >= 2.2 ? 0.01 : 0), 0.58, 0.86),
      "shots_over195"
    )
  }

  if (avgShots >= 22.0) {
    add(
      "Mais de 21.5 finalizações",
      clamp(Math.max(probShots - 0.03, 0.55) + (avgShots - 22.0) * 0.02, 0.55, 0.84),
      clamp(0.63 + (avgShots - 22.0) * 0.022 + (profile === "ofensivo" ? 0.01 : 0), 0.55, 0.82),
      "shots_over215"
    )
  }

  if (avgShots <= 28.0) {
    add(
      "Menos de 29.5 finalizações",
      clamp(0.62 + (28.0 - avgShots) * 0.018, 0.62, 0.90),
      clamp(0.69 + (28.0 - avgShots) * 0.016 + (profile === "controlado" ? 0.02 : 0), 0.62, 0.88),
      "shots_under295"
    )
  }

  if (avgShots <= 25.0) {
    add(
      "Menos de 26.5 finalizações",
      clamp(0.59 + (25.0 - avgShots) * 0.018, 0.59, 0.87),
      clamp(0.66 + (25.0 - avgShots) * 0.016 + (profile === "defensivo" ? 0.02 : 0), 0.59, 0.85),
      "shots_under265"
    )
  }

  candidates.forEach((c) => {
    if (c.subfamily.includes("under") && avgGoals >= 3.0) {
      c.score = clamp(c.score - 0.04, 0, 1)
      c.probability = clamp(c.probability - 0.03, 0, 1)
    }

    if (c.subfamily.includes("over") && avgGoals <= 1.9) {
      c.score = clamp(c.score - 0.04, 0, 1)
      c.probability = clamp(c.probability - 0.03, 0, 1)
    }
  })

  return candidates
}

function buildSOTCandidates(row, profile) {
  const candidates = []

  const avgSOT = toNumber(row.avg_shots_on_target)
  const avgGoals = toNumber(row.avg_goals)
  const probSot = toNumber(row.prob_sot)

  function add(market, probability, score, subfamily, macro = "precisao") {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "sot",
      subfamily,
      macro,
    })
  }

  if (avgSOT >= 5.8) {
    add(
      "Mais de 5.5 finalizações no gol",
      clamp(Math.max(probSot, 0.61) + (avgSOT - 5.8) * 0.03, 0.61, 0.92),
      clamp(0.69 + (avgSOT - 5.8) * 0.028 + (profile === "precisao" ? 0.03 : 0), 0.61, 0.90),
      "sot_over55"
    )
  }

  if (avgSOT >= 6.8) {
    add(
      "Mais de 6.5 finalizações no gol",
      clamp(Math.max(probSot - 0.01, 0.58) + (avgSOT - 6.8) * 0.03, 0.58, 0.89),
      clamp(0.67 + (avgSOT - 6.8) * 0.028 + (avgGoals >= 2.2 ? 0.01 : 0), 0.58, 0.87),
      "sot_over65"
    )
  }

  if (avgSOT >= 7.8) {
    add(
      "Mais de 7.5 finalizações no gol",
      clamp(Math.max(probSot - 0.03, 0.55) + (avgSOT - 7.8) * 0.03, 0.55, 0.85),
      clamp(0.64 + (avgSOT - 7.8) * 0.028 + (profile === "ofensivo" ? 0.01 : 0), 0.55, 0.83),
      "sot_over75"
    )
  }

  if (avgSOT <= 10.0) {
    add(
      "Menos de 10.5 finalizações no gol",
      clamp(0.61 + (10.0 - avgSOT) * 0.022, 0.61, 0.90),
      clamp(0.68 + (10.0 - avgSOT) * 0.018 + (profile === "controlado" ? 0.02 : 0), 0.61, 0.88),
      "sot_under105"
    )
  }

  if (avgSOT <= 8.8) {
    add(
      "Menos de 9.5 finalizações no gol",
      clamp(0.58 + (8.8 - avgSOT) * 0.022, 0.58, 0.87),
      clamp(0.65 + (8.8 - avgSOT) * 0.018 + (profile === "defensivo" ? 0.02 : 0), 0.58, 0.85),
      "sot_under95"
    )
  }

  candidates.forEach((c) => {
    if (c.subfamily.includes("under") && avgGoals >= 3.0) {
      c.score = clamp(c.score - 0.04, 0, 1)
      c.probability = clamp(c.probability - 0.03, 0, 1)
    }

    if (c.subfamily.includes("over") && avgGoals <= 1.9) {
      c.score = clamp(c.score - 0.04, 0, 1)
      c.probability = clamp(c.probability - 0.03, 0, 1)
    }
  })

  return candidates
}

function buildCardsCandidates(row, profile) {
  const candidates = []

  const avgCards = toNumber(row.avg_cards)
  const probCards = toNumber(row.prob_cards)
  const avgGoals = toNumber(row.avg_goals)

  function add(market, probability, score, subfamily, macro = "disciplina") {
    pushCandidate(candidates, {
      market,
      probability,
      score,
      family: "cards",
      subfamily,
      macro,
    })
  }

  if (avgCards >= 2.8) {
    add(
      "Mais de 2.5 cartões",
      clamp(Math.max(probCards, 0.60) + (avgCards - 2.8) * 0.03, 0.60, 0.91),
      clamp(0.68 + (avgCards - 2.8) * 0.026 + (profile === "disciplinar" ? 0.03 : 0), 0.60, 0.89),
      "cards_over25"
    )
  }

  if (avgCards >= 3.6) {
    add(
      "Mais de 3.5 cartões",
      clamp(Math.max(probCards - 0.01, 0.57) + (avgCards - 3.6) * 0.03, 0.57, 0.88),
      clamp(0.66 + (avgCards - 3.6) * 0.026 + (profile === "disciplinar" ? 0.02 : 0), 0.57, 0.86),
      "cards_over35"
    )
  }

  if (avgCards >= 4.6) {
    add(
      "Mais de 4.5 cartões",
      clamp(Math.max(probCards - 0.03, 0.54) + (avgCards - 4.6) * 0.03, 0.54, 0.84),
      clamp(0.62 + (avgCards - 4.6) * 0.024 + (profile === "disciplinar" ? 0.02 : 0), 0.54, 0.82),
      "cards_over45"
    )
  }

  if (avgCards <= 5.8) {
    add(
      "Menos de 6.5 cartões",
      clamp(0.62 + (5.8 - avgCards) * 0.02, 0.62, 0.89),
      clamp(0.69 + (5.8 - avgCards) * 0.017 + (profile === "controlado" ? 0.02 : 0), 0.62, 0.87),
      "cards_under65"
    )
  }

  if (avgCards <= 4.8) {
    add(
      "Menos de 5.5 cartões",
      clamp(0.58 + (4.8 - avgCards) * 0.02, 0.58, 0.86),
      clamp(0.65 + (4.8 - avgCards) * 0.017 + (profile === "defensivo" ? 0.01 : 0), 0.58, 0.84),
      "cards_under55"
    )
  }

  candidates.forEach((c) => {
    if (profile === "ofensivo" && c.subfamily.includes("under") && avgGoals >= 3.0) {
      c.score = clamp(c.score - 0.02, 0, 1)
    }
  })

  return candidates
}

function buildMarketCandidates(row, options = {}) {
  const relaxed = options.relaxed === true
  const candidates = []

  const avgGoals = toNumber(row.avg_goals)
  const avgShots = toNumber(row.avg_shots)

  const over15Prob = toNumber(row.over15_prob)
  const over25Prob = toNumber(row.over25_prob)
  const under25Prob = toNumber(row.under25_prob)
  const under35Prob = toNumber(row.under35_prob)
  const bttsProb = toNumber(row.btts_prob)
  const bttsNoProb = clamp(1 - bttsProb, 0, 1)

  const homeWin = toNumber(row.home_win_prob)
  const draw = toNumber(row.draw_prob)
  const awayWin = toNumber(row.away_win_prob)

  const homeOrDraw = clamp(homeWin + draw, 0, 1)
  const awayOrDraw = clamp(awayWin + draw, 0, 1)

  const profile = getGameProfile(row)

  function minProb(v) {
    return relaxed ? v - 0.05 : v
  }

  if (over25Prob >= minProb(0.65)) {
    let score = over25Prob + (avgGoals >= 2.7 ? 0.05 : 0) + (avgShots >= 22 ? 0.04 : 0)
    if (profile === "ofensivo") score += 0.05
    if (profile === "precisao") score += 0.02
    if (profile === "defensivo") score -= 0.08
    if (profile === "controlado") score -= 0.03

    pushCandidate(candidates, {
      market: "Mais de 2.5 gols",
      probability: over25Prob,
      score,
      family: "gols",
      subfamily: "over25",
      macro: "ofensivo",
    })
  }

  if (over15Prob >= minProb(0.77)) {
    let score = over15Prob + (avgGoals >= 2.3 ? 0.04 : 0) + (avgShots >= 20 ? 0.03 : 0)
    if (profile === "ofensivo") score += 0.03
    if (profile === "precisao") score += 0.02
    if (profile === "defensivo") score -= 0.04
    if (profile === "controlado") score -= 0.02

    pushCandidate(candidates, {
      market: "Mais de 1.5 gols",
      probability: over15Prob,
      score,
      family: "gols",
      subfamily: "over15",
      macro: "ofensivo",
    })
  }

  if (bttsProb >= minProb(0.64)) {
    let score = bttsProb + (avgGoals >= 2.6 ? 0.04 : 0) + (avgShots >= 21 ? 0.03 : 0)
    if (profile === "ofensivo") score += 0.04
    if (profile === "precisao") score += 0.02
    if (profile === "defensivo") score -= 0.08
    if (profile === "controlado") score -= 0.03

    pushCandidate(candidates, {
      market: "Ambas marcam",
      probability: bttsProb,
      score,
      family: "btts",
      subfamily: "btts_yes",
      macro: "ofensivo",
    })
  }

  if (under25Prob >= minProb(0.77)) {
    let score = under25Prob + (avgGoals <= 2.1 ? 0.02 : 0) + (avgShots <= 17 ? 0.01 : 0)
    if (profile === "defensivo") score += 0.02
    if (profile === "controlado") score += 0.01
    if (profile === "ofensivo") score -= 0.08

    pushCandidate(candidates, {
      market: "Menos de 2.5 gols",
      probability: under25Prob,
      score,
      family: "gols",
      subfamily: "under25",
      macro: "defensivo",
    })
  }

  if (under35Prob >= minProb(0.81)) {
    let score = under35Prob + (avgGoals <= 2.6 ? 0.01 : 0) + (avgShots <= 20 ? 0.01 : 0)
    if (profile === "controlado") score += 0.02
    if (profile === "defensivo") score += 0.01
    if (profile === "ofensivo") score -= 0.06

    pushCandidate(candidates, {
      market: "Menos de 3.5 gols",
      probability: under35Prob,
      score,
      family: "gols",
      subfamily: "under35",
      macro: "defensivo",
    })
  }

  if (bttsNoProb >= minProb(0.73)) {
    let score = bttsNoProb + (avgGoals <= 2.2 ? 0.01 : 0) + (avgShots <= 18 ? 0.01 : 0)
    if (profile === "defensivo") score += 0.02
    if (profile === "controlado") score += 0.01
    if (profile === "ofensivo") score -= 0.09

    pushCandidate(candidates, {
      market: "Ambas não marcam",
      probability: bttsNoProb,
      score,
      family: "btts",
      subfamily: "btts_no",
      macro: "defensivo",
    })
  }

  buildShotsCandidates(row, profile).forEach((item) => candidates.push(item))
  buildSOTCandidates(row, profile).forEach((item) => candidates.push(item))
  buildCardsCandidates(row, profile).forEach((item) => candidates.push(item))
  buildCornerCandidates(row, profile).forEach((item) => candidates.push(item))

  if (homeOrDraw >= minProb(0.73) && homeWin >= awayWin) {
    let score = homeOrDraw + (homeWin > awayWin ? 0.02 : 0)
    if (homeWin >= 0.50) score += 0.02

    pushCandidate(candidates, {
      market: `Dupla chance ${row.home_team} ou empate`,
      probability: homeOrDraw,
      score,
      family: "resultado",
      subfamily: "home_draw",
      macro: "protecao",
    })
  }

  if (awayOrDraw >= minProb(0.73) && awayWin > homeWin) {
    let score = awayOrDraw + (awayWin > homeWin ? 0.02 : 0)
    if (awayWin >= 0.50) score += 0.02

    pushCandidate(candidates, {
      market: `Dupla chance ${row.away_team} ou empate`,
      probability: awayOrDraw,
      score,
      family: "resultado",
      subfamily: "away_draw",
      macro: "protecao",
    })
  }

  candidates.forEach((c) => {
    if (c.market === "Mais de 1.5 gols") c.score = clamp(c.score + 0.02, 0, 1)
    if (c.market === "Mais de 2.5 gols") c.score = clamp(c.score + 0.01, 0, 1)
    if (c.market === "Menos de 3.5 gols") c.score = clamp(c.score + 0.01, 0, 1)
    if (c.market === "Ambas não marcam") c.score = clamp(c.score - 0.01, 0, 1)

    if (c.family === "escanteios" && (profile === "volume" || profile === "precisao")) {
      c.score = clamp(c.score - 0.02, 0, 1)
    }

    if (c.family === "shots" && profile === "volume") {
      c.score = clamp(c.score + 0.03, 0, 1)
    }

    if (c.family === "sot" && profile === "precisao") {
      c.score = clamp(c.score + 0.03, 0, 1)
    }

    if (c.family === "cards" && profile === "disciplinar") {
      c.score = clamp(c.score + 0.03, 0, 1)
    }
  })

  const minProbability = relaxed ? 0.54 : 0.60
  const minScore = relaxed ? 0.56 : 0.60

  return candidates
    .filter((c) => c.probability >= minProbability && c.score >= minScore)
    .sort((a, b) => b.score - a.score)
}

function chooseBestAndAlternatives(candidates, row) {
  if (!candidates.length) return { best: null, alternatives: [] }

  const sorted = [...candidates].sort((a, b) => b.score - a.score)
  const best = sorted[0]

  const alternatives = []
  const usedMarkets = new Set([best.market])
  const usedFamilies = new Set([best.family])

  for (const item of sorted.slice(1)) {
    if (!item || !item.market) continue
    if (usedMarkets.has(item.market)) continue
    if (usedFamilies.has(item.family)) continue

    alternatives.push(item)
    usedMarkets.add(item.market)
    usedFamilies.add(item.family)

    if (alternatives.length === 2) break
  }

  if (alternatives.length < 2) {
    for (const item of sorted.slice(1)) {
      if (!item || !item.market) continue
      if (usedMarkets.has(item.market)) continue

      alternatives.push(item)
      usedMarkets.add(item.market)

      if (alternatives.length === 2) break
    }
  }

  if (alternatives.length < 2 && row) {
    const relaxed = buildMarketCandidates(row, { relaxed: true })

    for (const item of relaxed) {
      if (!item || !item.market) continue
      if (usedMarkets.has(item.market)) continue

      alternatives.push(item)
      usedMarkets.add(item.market)

      if (alternatives.length === 2) break
    }
  }

  return { best, alternatives }
}

function buildAnalysisFromRow(row) {
  const candidates = buildMarketCandidates(row)
  if (!candidates.length) return null

  const profile = getGameProfile(row)
  const { best, alternatives } = chooseBestAndAlternatives(candidates, row)
  if (!best) return null

  const rhythm = getRhythmLabel(row.avg_shots)

  const aggressivePick =
    alternatives.find(
      (x) =>
        x.subfamily === "over25" ||
        x.subfamily === "btts_yes" ||
        x.subfamily === "over15" ||
        String(x.subfamily || "").includes("corners_over") ||
        String(x.subfamily || "").includes("shots_over") ||
        String(x.subfamily || "").includes("sot_over") ||
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
    game_profile: profile,
    main_pick: best.market,
    main_probability: best.probability,
    main_score: best.score,
    main_family: best.family,
    main_subfamily: best.subfamily,
    main_macro: best.macro,
    strength: getStrengthLabel(best.score),
    rhythm,
    insight: buildInsight(row, best, profile),
    best_pick_1: best.market,
    best_pick_2: alternatives[0]?.market || null,
    best_pick_3: alternatives[1]?.market || null,
    aggressive_pick: aggressivePick,
    alternatives_count: alternatives.filter(Boolean).length,
  }
}

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
  const usedSubfamilies = {}
  const usedMacros = {}
  const usedLeagues = {}

  for (const item of radarPool) {
    if (usedMatchIds.has(item.match_id)) continue

    const exactMarketCount = usedExactMarkets[item.main_pick] || 0
    const familyCount = usedFamilies[item.main_family] || 0
    const subfamilyCount = usedSubfamilies[item.main_subfamily] || 0
    const macroCount = usedMacros[item.main_macro] || 0
    const leagueCount = usedLeagues[item.league] || 0

    if (exactMarketCount >= 2) continue
    if (leagueCount >= 3) continue

    if (item.main_family === "escanteios" && familyCount >= 3) continue
    if (item.main_family === "gols" && familyCount >= 4) continue
    if (item.main_family === "btts" && familyCount >= 3) continue
    if (item.main_family === "resultado" && familyCount >= 3) continue
    if (item.main_family === "shots" && familyCount >= 3) continue
    if (item.main_family === "sot" && familyCount >= 3) continue
    if (item.main_family === "cards" && familyCount >= 3) continue

    if (
      ["corners_under105", "corners_under115", "corners_under125"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (
      ["corners_over65", "corners_over75", "corners_over85"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (
      ["shots_over175", "shots_over195", "shots_over215"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (
      ["shots_under295", "shots_under265"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (
      ["sot_over55", "sot_over65", "sot_over75"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (
      ["sot_under105", "sot_under95"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (
      ["cards_over25", "cards_over35", "cards_over45"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (
      ["cards_under65", "cards_under55"].includes(item.main_subfamily) &&
      subfamilyCount >= 2
    ) {
      continue
    }

    if (item.main_subfamily === "over15" && subfamilyCount >= 3) continue
    if (item.main_subfamily === "over25" && subfamilyCount >= 2) continue
    if (item.main_subfamily === "under35" && subfamilyCount >= 2) continue
    if (item.main_subfamily === "under25" && subfamilyCount >= 2) continue
    if (item.main_subfamily === "btts_yes" && subfamilyCount >= 2) continue
    if (item.main_subfamily === "btts_no" && subfamilyCount >= 2) continue

    if (item.main_macro === "estatistico" && macroCount >= 3) continue
    if (item.main_macro === "defensivo" && macroCount >= 4) continue
    if (item.main_macro === "ofensivo" && macroCount >= 4) continue
    if (item.main_macro === "protecao" && macroCount >= 3) continue
    if (item.main_macro === "volume" && macroCount >= 3) continue
    if (item.main_macro === "precisao" && macroCount >= 3) continue
    if (item.main_macro === "disciplina" && macroCount >= 3) continue

    radar.push(item)
    usedMatchIds.add(item.match_id)
    usedExactMarkets[item.main_pick] = exactMarketCount + 1
    usedFamilies[item.main_family] = familyCount + 1
    usedSubfamilies[item.main_subfamily] = subfamilyCount + 1
    usedMacros[item.main_macro] = macroCount + 1
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

function buildTicketFromRadar(radar) {
  const todayFirst = [...radar].sort((a, b) => {
    const aDay = getDayOffsetFromToday(a.kickoff)
    const bDay = getDayOffsetFromToday(b.kickoff)

    if (aDay !== bDay) return aDay - bDay
    return compareByScoreThenKickoff(a, b)
  })

  const ranked = [...todayFirst]

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
  const usedSubfamilies = new Set()

  for (const item of ranked) {
    if (usedTicketMatches.has(item.match_id)) continue
    if (usedFamilies.has(item.main_family)) continue
    if (usedSubfamilies.has(item.main_subfamily)) continue

    ticket.push(item)
    usedTicketMatches.add(item.match_id)
    usedFamilies.add(item.main_family)
    usedSubfamilies.add(item.main_subfamily)

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

async function runScoutlyBrain() {
  console.log("🧠 Scoutly Brain V16 iniciado...")

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

  console.log("✅ Scoutly Brain V16 finalizado com sucesso.")
  console.log(`📡 Radar do dia gerado com ${radar.length} jogo(s).`)
  console.log(`🎫 Bilhete do dia definido com ${ticket.length} jogo(s).`)
}

runScoutlyBrain().catch((error) => {
  console.error("❌ Erro no Scoutly Brain V16:", error)
  process.exit(1)
})
