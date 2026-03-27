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
const UPCOMING_WINDOW_HOURS = 30
const PAST_GRACE_HOURS = 3
const RADAR_SIZE = 10
const TICKET_SIZE = 3

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

function normalizeText(value) {
  return String(value || "").trim().toLowerCase()
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

function getTomorrowInTZ() {
  const now = new Date()
  const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000)
  return formatDateInTZ(tomorrow, TIMEZONE)
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

function safeLeague(row) {
  return row.league || "Liga"
}

function safeArrayFromPipe(value) {
  if (!value) return []
  if (Array.isArray(value)) return value.filter(Boolean).slice(0, 5)

  return String(value)
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean)
    .slice(0, 5)
}

function getStrengthLabel(score) {
  if (score >= 0.86) return "Muito forte"
  if (score >= 0.74) return "Boa"
  return "Moderada"
}

function getRhythmLabel(avgShots) {
  const shots = toNumber(avgShots)
  if (shots >= 24) return "Alto"
  if (shots >= 16) return "Moderado"
  return "Baixo"
}

function buildCornersOver85Prob(avgCorners, avgShots, avgGoals) {
  const base =
    avgCorners * 0.07 +
    avgShots * 0.01 +
    avgGoals * 0.04

  return clamp(base, 0.18, 0.88)
}

function hasRealMetrics(row) {
  const values = [
    row.avg_goals,
    row.avg_corners,
    row.avg_shots,
    row.avg_shots_on_target,
    row.avg_cards,
    row.over15_prob,
    row.over25_prob,
    row.btts_prob,
    row.home_win_prob,
    row.draw_prob,
    row.away_win_prob,
    row.expected_home_goals,
    row.expected_away_goals,
    row.expected_corners,
  ]

  return values.some((v) => Number.isFinite(Number(v)) && Number(v) > 0)
}

function chooseFirstNumber(...values) {
  for (const value of values) {
    const num = Number(value)
    if (Number.isFinite(num)) return num
  }
  return null
}

function chooseFirstText(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) return value.trim()
  }
  return null
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
      home_form,
      away_form,
      over25_prob,
      btts_prob,
      under25_prob,
      under35_prob,
      corners_over85_prob,
      draw_result_prob,
      away_result_prob,
      home_result_prob,
      analysis_text,
      value_pick,
      safe_pick,
      balanced_pick,
      aggressive_pick,
      fixture_id,
      country,
      markets,
      created_at
    `)

  if (analysisError) {
    throw analysisError
  }

  const { data: stats, error: statsError } = await supabase
    .from("match_stats")
    .select(`
      match_id,
      home_shots,
      home_shots_on_target,
      home_corners,
      home_yellow_cards,
      away_shots,
      away_shots_on_target,
      away_corners,
      away_yellow_cards,
      created_at
    `)

  if (statsError) {
    throw statsError
  }

  return {
    matches: matches || [],
    analysis: analysis || [],
    stats: stats || [],
  }
}

function mergeMatchRow(matchRow, analysisMap, statsMap) {
  const analysis = analysisMap.get(String(matchRow.id)) || {}
  const stats = statsMap.get(String(matchRow.id)) || {}

  const metrics = matchRow.metrics || {}
  const markets = matchRow.markets || {}
  const probabilities = matchRow.probabilities || {}
  const analysisMarkets = analysis.markets || {}

  const avgGoals = chooseFirstNumber(
    metrics.goals,
    chooseFirstNumber(analysis.expected_home_goals, 0) +
      chooseFirstNumber(analysis.expected_away_goals, 0)
  ) ?? 0

  const avgCorners = chooseFirstNumber(
    metrics.corners,
    markets.corners,
    analysis.expected_corners,
    chooseFirstNumber(stats.home_corners, 0) + chooseFirstNumber(stats.away_corners, 0)
  ) ?? 0

  const avgShots = chooseFirstNumber(
    metrics.shots,
    markets.shots,
    chooseFirstNumber(analysis.expected_home_shots, 0) +
      chooseFirstNumber(analysis.expected_away_shots, 0),
    chooseFirstNumber(stats.home_shots, 0) + chooseFirstNumber(stats.away_shots, 0)
  ) ?? 0

  const avgShotsOnTarget = chooseFirstNumber(
    metrics.shots_on_target,
    markets.shots_on_target,
    chooseFirstNumber(analysis.expected_home_sot, 0) +
      chooseFirstNumber(analysis.expected_away_sot, 0),
    chooseFirstNumber(stats.home_shots_on_target, 0) +
      chooseFirstNumber(stats.away_shots_on_target, 0)
  ) ?? 0

  const avgCards = chooseFirstNumber(
    metrics.cards,
    markets.cards,
    analysis.expected_cards,
    chooseFirstNumber(stats.home_yellow_cards, 0) +
      chooseFirstNumber(stats.away_yellow_cards, 0)
  ) ?? 0

  const over15Prob = clamp(
    chooseFirstNumber(
      markets.over15,
      analysisMarkets.over15,
      avgGoals >= 2.2 ? 0.78 : avgGoals >= 1.8 ? 0.67 : 0.52
    ) ?? 0,
    0,
    1
  )

  const over25Prob = clamp(
    chooseFirstNumber(
      markets.over25,
      analysis.over25_prob,
      analysis.prob_over25,
      analysisMarkets.over25,
      avgGoals >= 2.8 ? 0.72 : avgGoals >= 2.4 ? 0.60 : 0.44
    ) ?? 0,
    0,
    1
  )

  const bttsProb = clamp(
    chooseFirstNumber(
      markets.btts,
      analysis.btts_prob,
      analysis.prob_btts,
      analysisMarkets.btts,
      avgGoals >= 2.7 ? 0.64 : avgGoals >= 2.2 ? 0.55 : 0.42
    ) ?? 0,
    0,
    1
  )

  const homeWinProb = clamp(
    chooseFirstNumber(
      probabilities.home,
      analysis.home_result_prob,
      0
    ) ?? 0,
    0,
    1
  )

  const drawProb = clamp(
    chooseFirstNumber(
      probabilities.draw,
      analysis.draw_result_prob,
      0
    ) ?? 0,
    0,
    1
  )

  const awayWinProb = clamp(
    chooseFirstNumber(
      probabilities.away,
      analysis.away_result_prob,
      0
    ) ?? 0,
    0,
    1
  )

  const under25Prob = clamp(
    chooseFirstNumber(
      analysis.under25_prob,
      1 - over25Prob
    ) ?? 0,
    0,
    1
  )

  const under35Prob = clamp(
    chooseFirstNumber(
      analysis.under35_prob,
      1 - Math.max(over25Prob - 0.18, 0)
    ) ?? 0,
    0,
    1
  )

  const cornersOver85Prob = clamp(
    chooseFirstNumber(
      analysis.corners_over85_prob,
      buildCornersOver85Prob(avgCorners, avgShots, avgGoals)
    ) ?? 0,
    0,
    1
  )

  const confidenceScore = clamp(
    chooseFirstNumber(
      matchRow.confidence_score,
      matchRow.probability,
      analysis.prob_over25,
      over25Prob,
      over15Prob
    ) ?? 0,
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

    pick: chooseFirstText(
      matchRow.pick,
      analysis.best_pick_1,
      analysis.value_pick
    ),

    insight: chooseFirstText(
      matchRow.insight,
      analysis.analysis_text
    ),

    game_profile: chooseFirstText(matchRow.game_profile),

    confidence_score: confidenceScore,

    avg_goals: round1(avgGoals),
    avg_corners: round1(avgCorners),
    avg_shots: Math.round(toNumber(avgShots, 0)),
    avg_shots_on_target: Math.round(toNumber(avgShotsOnTarget, 0)),
    avg_cards: round1(avgCards),

    over15_prob: round2(over15Prob),
    over25_prob: round2(over25Prob),
    under25_prob: round2(under25Prob),
    under35_prob: round2(under35Prob),
    btts_prob: round2(bttsProb),
    corners_over85_prob: round2(cornersOver85Prob),

    home_win_prob: round2(homeWinProb),
    draw_prob: round2(drawProb),
    away_win_prob: round2(awayWinProb),

    home_result_prob: round2(homeWinProb),
    draw_result_prob: round2(drawProb),
    away_result_prob: round2(awayWinProb),

    expected_home_goals: round2(chooseFirstNumber(analysis.expected_home_goals, 0) ?? 0),
    expected_away_goals: round2(chooseFirstNumber(analysis.expected_away_goals, 0) ?? 0),
    expected_corners: round2(chooseFirstNumber(analysis.expected_corners, avgCorners) ?? 0),

    best_pick_1: chooseFirstText(analysis.best_pick_1, matchRow.pick),
    best_pick_2: chooseFirstText(analysis.best_pick_2),
    best_pick_3: chooseFirstText(analysis.best_pick_3),
    safe_pick: chooseFirstText(analysis.safe_pick),
    balanced_pick: chooseFirstText(analysis.balanced_pick),
    aggressive_pick: chooseFirstText(analysis.aggressive_pick),
    value_pick: chooseFirstText(analysis.value_pick),

    home_form: safeArrayFromPipe(analysis.home_form),
    away_form: safeArrayFromPipe(analysis.away_form),
  }
}

async function loadTodaysMatches() {
  const today = getTodayInTZ()
  const tomorrow = getTomorrowInTZ()
  const now = new Date()
  const minTime = new Date(now.getTime() - PAST_GRACE_HOURS * 60 * 60 * 1000)
  const maxTime = new Date(now.getTime() + UPCOMING_WINDOW_HOURS * 60 * 60 * 1000)

  const { matches, analysis, stats } = await loadBaseTables()

  console.log("DEBUG DATA HOJE:", today)
  console.log("DEBUG TOTAL RAW MATCHES:", matches.length)
  console.log("DEBUG TOTAL ANALYSIS:", analysis.length)
  console.log("DEBUG TOTAL STATS:", stats.length)

  const analysisMap = new Map(
    analysis.map((row) => [String(row.match_id), row])
  )

  const statsMap = new Map(
    stats.map((row) => [String(row.match_id), row])
  )

  const merged = matches.map((row) => mergeMatchRow(row, analysisMap, statsMap))

  const filtered = merged.filter((row) => {
    const kickoffDate = row.kickoff ? new Date(row.kickoff) : null
    const kickoffValid = kickoffDate && !Number.isNaN(kickoffDate.getTime())
    const kickoffDay = getKickoffDateOnly(row.kickoff)

    const insideWindow =
      kickoffValid &&
      kickoffDate.getTime() >= minTime.getTime() &&
      kickoffDate.getTime() <= maxTime.getTime()

    const byCalendar =
      kickoffDay === today || kickoffDay === tomorrow

    return insideWindow || byCalendar
  })

  const finalRows = filtered
    .filter((row) => row.home_team && row.away_team && row.league)
    .filter((row) => hasRealMetrics(row))

  console.log("DEBUG TOTAL FILTRADOS HOJE:", finalRows.length)

  return finalRows
}

function getGameProfile(row) {
  if (row.game_profile && String(row.game_profile).trim()) {
    return String(row.game_profile).trim()
  }

  const avgGoals = toNumber(row.avg_goals)
  const avgShots = toNumber(row.avg_shots)
  const avgCorners = toNumber(row.avg_corners)

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
    avgGoals <= 2.1 &&
    under25Prob >= 0.72 &&
    bttsNoProb >= 0.70 &&
    avgShots <= 18
  ) {
    return "defensivo"
  }

  if (avgCorners >= 9.2 && avgShots >= 21 && avgGoals >= 2.2) {
    return "corners"
  }

  if (under35Prob >= 0.79 && avgGoals <= 2.5 && avgShots <= 20) {
    return "controlado"
  }

  return "equilibrado"
}

function buildInsight(row, bestPick, profile) {
  if (row.insight && String(row.insight).trim()) {
    return String(row.insight).trim()
  }

  const avgGoals = round1(row.avg_goals)
  const avgCorners = round1(row.avg_corners)
  const avgShots = Math.round(toNumber(row.avg_shots))
  const rhythm = getRhythmLabel(avgShots).toLowerCase()

  const market = normalizeText(bestPick.market)

  if (market.includes("mais de 2.5 gols")) {
    return `A leitura Scoutly projeta um jogo mais aberto, com potencial real para 3 ou mais gols. A média esperada está em ${avgGoals} gols, com ritmo ofensivo ${rhythm}, reforçando essa linha como a melhor interpretação da partida.`
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

  if (market.includes("ambas não marcam")) {
    return `A leitura Scoutly vê um confronto com baixa tendência de gols dos dois lados. A projeção ofensiva é moderada, e o cenário sugere maior chance de uma das equipes passar em branco.`
  }

  if (market.includes("ambas marcam")) {
    return `A leitura Scoutly identifica espaço para gols dos dois lados. A expectativa ofensiva, o ritmo ${rhythm} e o equilíbrio do confronto criam um cenário interessante para ambas marcam.`
  }

  if (market.includes("escanteios")) {
    return `A leitura Scoutly projeta cerca de ${avgCorners} escanteios, com ritmo ${rhythm} e volume ofensivo suficiente para transformar esse mercado em uma oportunidade estatística relevante.`
  }

  if (market.includes("dupla chance")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate. O equilíbrio da partida ainda pede segurança, e por isso a dupla chance aparece como leitura mais sólida.`
  }

  return `A leitura Scoutly classifica este confronto como ${profile}, combinando projeção de ${avgGoals} gols, ${avgCorners} escanteios e ritmo ${rhythm} para destacar essa oportunidade.`
}

function pushCandidate(candidates, item) {
  candidates.push({
    ...item,
    probability: clamp(toNumber(item.probability), 0, 1),
    score: clamp(toNumber(item.score), 0, 1),
  })
}

function buildMarketCandidates(row) {
  const candidates = []

  const avgGoals = toNumber(row.avg_goals)
  const avgCorners = toNumber(row.avg_corners)
  const avgShots = toNumber(row.avg_shots)

  const over15Prob = toNumber(row.over15_prob)
  const over25Prob = toNumber(row.over25_prob)
  const under25Prob = toNumber(row.under25_prob)
  const under35Prob = toNumber(row.under35_prob)
  const bttsProb = toNumber(row.btts_prob)
  const bttsNoProb = clamp(1 - bttsProb, 0, 1)
  const cornersOver85Prob = toNumber(row.corners_over85_prob)

  const homeWin =
    row.home_result_prob != null
      ? toNumber(row.home_result_prob)
      : toNumber(row.home_win_prob)

  const draw =
    row.draw_result_prob != null
      ? toNumber(row.draw_result_prob)
      : toNumber(row.draw_prob)

  const awayWin =
    row.away_result_prob != null
      ? toNumber(row.away_result_prob)
      : toNumber(row.away_win_prob)

  const homeOrDraw = clamp(homeWin + draw, 0, 1)
  const awayOrDraw = clamp(awayWin + draw, 0, 1)

  const profile = getGameProfile(row)

  if (over25Prob >= 0.65) {
    let score =
      over25Prob +
      (avgGoals >= 2.7 ? 0.05 : 0) +
      (avgShots >= 22 ? 0.04 : 0)

    if (profile === "ofensivo") score += 0.05
    if (profile === "defensivo") score -= 0.08
    if (profile === "controlado") score -= 0.03

    pushCandidate(candidates, {
      market: "Mais de 2.5 gols",
      probability: over25Prob,
      score,
      family: "gols",
      subfamily: "over",
      macro: "ofensivo",
    })
  }

  if (over15Prob >= 0.77) {
    let score =
      over15Prob +
      (avgGoals >= 2.3 ? 0.04 : 0) +
      (avgShots >= 20 ? 0.03 : 0)

    if (profile === "ofensivo") score += 0.03
    if (profile === "defensivo") score -= 0.04
    if (profile === "controlado") score -= 0.02

    pushCandidate(candidates, {
      market: "Mais de 1.5 gols",
      probability: over15Prob,
      score,
      family: "gols",
      subfamily: "over",
      macro: "ofensivo",
    })
  }

  if (bttsProb >= 0.64) {
    let score =
      bttsProb +
      (avgGoals >= 2.6 ? 0.04 : 0) +
      (avgShots >= 21 ? 0.03 : 0)

    if (profile === "ofensivo") score += 0.04
    if (profile === "defensivo") score -= 0.08
    if (profile === "controlado") score -= 0.03

    pushCandidate(candidates, {
      market: "Ambas marcam",
      probability: bttsProb,
      score,
      family: "btts",
      subfamily: "yes",
      macro: "ofensivo",
    })
  }

  if (under25Prob >= 0.77) {
    let score =
      under25Prob +
      (avgGoals <= 2.1 ? 0.02 : 0) +
      (avgShots <= 17 ? 0.01 : 0)

    if (profile === "defensivo") score += 0.02
    if (profile === "ofensivo") score -= 0.08
    if (profile === "controlado") score += 0.01

    pushCandidate(candidates, {
      market: "Menos de 2.5 gols",
      probability: under25Prob,
      score,
      family: "gols",
      subfamily: "under",
      macro: "defensivo",
    })
  }

  if (under35Prob >= 0.81) {
    let score =
      under35Prob +
      (avgGoals <= 2.6 ? 0.01 : 0) +
      (avgShots <= 20 ? 0.01 : 0)

    if (profile === "controlado") score += 0.02
    if (profile === "defensivo") score += 0.01
    if (profile === "ofensivo") score -= 0.06

    pushCandidate(candidates, {
      market: "Menos de 3.5 gols",
      probability: under35Prob,
      score,
      family: "gols",
      subfamily: "under",
      macro: "defensivo",
    })
  }

  if (bttsNoProb >= 0.73) {
    let score =
      bttsNoProb +
      (avgGoals <= 2.2 ? 0.01 : 0) +
      (avgShots <= 18 ? 0.01 : 0)

    if (profile === "defensivo") score += 0.02
    if (profile === "ofensivo") score -= 0.09
    if (profile === "controlado") score += 0.01

    pushCandidate(candidates, {
      market: "Ambas não marcam",
      probability: bttsNoProb,
      score,
      family: "btts",
      subfamily: "no",
      macro: "defensivo",
    })
  }

  if (cornersOver85Prob >= 0.64) {
    let score =
      cornersOver85Prob +
      (avgCorners >= 8.8 ? 0.04 : 0) +
      (avgShots >= 20 ? 0.02 : 0)

    if (profile === "corners") score += 0.04
    if (profile === "ofensivo") score += 0.01

    pushCandidate(candidates, {
      market: "Mais de 8.5 escanteios",
      probability: cornersOver85Prob,
      score,
      family: "escanteios",
      subfamily: "over",
      macro: "estatistico",
    })
  }

  const cornersUnder105Prob = clamp(
    1 - Math.max(cornersOver85Prob - 0.18, 0),
    0,
    1
  )

  if (avgCorners <= 8.6 && cornersUnder105Prob >= 0.67) {
    let score = cornersUnder105Prob + (avgCorners <= 8.1 ? 0.02 : 0)

    if (profile === "corners") score -= 0.04
    if (profile === "ofensivo") score -= 0.02

    pushCandidate(candidates, {
      market: "Menos de 10.5 escanteios",
      probability: cornersUnder105Prob,
      score,
      family: "escanteios",
      subfamily: "under",
      macro: "estatistico",
    })
  }

  if (homeOrDraw >= 0.73 && homeWin >= awayWin) {
    let score = homeOrDraw + (homeWin > awayWin ? 0.02 : 0)
    if (homeWin >= 0.50) score += 0.02

    pushCandidate(candidates, {
      market: `Dupla chance ${row.home_team} ou empate`,
      probability: homeOrDraw,
      score,
      family: "dupla",
      subfamily: "home_draw",
      macro: "protecao",
    })
  }

  if (awayOrDraw >= 0.73 && awayWin > homeWin) {
    let score = awayOrDraw + (awayWin > homeWin ? 0.02 : 0)
    if (awayWin >= 0.50) score += 0.02

    pushCandidate(candidates, {
      market: `Dupla chance ${row.away_team} ou empate`,
      probability: awayOrDraw,
      score,
      family: "dupla",
      subfamily: "away_draw",
      macro: "protecao",
    })
  }

  candidates.forEach((c) => {
    if (c.macro === "ofensivo") {
      c.score = clamp(c.score + 0.03, 0, 1)
    }

    if (c.macro === "defensivo" && c.subfamily === "under") {
      c.score = clamp(c.score - 0.03, 0, 1)
    }

    if (c.market === "Ambas não marcam") {
      c.score = clamp(c.score - 0.01, 0, 1)
    }

    if (c.market === "Menos de 10.5 escanteios") {
      c.score = clamp(c.score - 0.04, 0, 1)
    }
  })

  return candidates.sort((a, b) => b.score - a.score)
}

function chooseBestAndAlternatives(candidates) {
  if (!candidates.length) return { best: null, alternatives: [] }

  const sorted = [...candidates].sort((a, b) => b.score - a.score)
  const best = sorted[0]

  const alternatives = sorted
    .slice(1)
    .filter((item, index, arr) => {
      return arr.findIndex((x) => x.market === item.market) === index
    })
    .slice(0, 2)

  return { best, alternatives }
}

function buildAnalysisFromRow(row) {
  const candidates = buildMarketCandidates(row)
  if (!candidates.length) return null

  const profile = getGameProfile(row)
  const { best, alternatives } = chooseBestAndAlternatives(candidates)

  if (!best) return null

  const rhythm = getRhythmLabel(row.avg_shots)

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
    alternatives,
    best_pick_1: best.market,
    best_pick_2: alternatives[0]?.market || null,
    best_pick_3: alternatives[1]?.market || null,
    aggressive_pick:
      alternatives.find(
        (x) =>
          x.subfamily === "over" ||
          x.market.includes("Ambas marcam") ||
          x.market.includes("Mais de 2.5 gols") ||
          x.market.includes("Mais de 8.5 escanteios")
      )?.market || null,
    home_form: row.home_form || [],
    away_form: row.away_form || [],
  }
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

function chooseRadar(analyses) {
  const sorted = [...analyses].sort(compareByScoreThenKickoff)

  const radar = []
  const usedMatchIds = new Set()
  const usedMarkets = {}
  const usedMacros = {}

  for (const item of sorted) {
    if (usedMatchIds.has(item.match_id)) continue

    const marketCount = usedMarkets[item.main_pick] || 0
    const macroCount = usedMacros[item.main_macro] || 0

    if (marketCount >= 2) continue
    if (item.main_macro === "defensivo" && macroCount >= 3) continue
    if (item.main_macro === "estatistico" && macroCount >= 2) continue
    if (item.main_macro === "protecao" && macroCount >= 2) continue
    if (item.main_macro === "ofensivo" && macroCount >= 3) continue

    radar.push(item)
    usedMatchIds.add(item.match_id)
    usedMarkets[item.main_pick] = marketCount + 1
    usedMacros[item.main_macro] = macroCount + 1

    if (radar.length === RADAR_SIZE) break
  }

  if (radar.length < RADAR_SIZE) {
    for (const item of sorted) {
      if (usedMatchIds.has(item.match_id)) continue
      radar.push(item)
      usedMatchIds.add(item.match_id)
      if (radar.length === RADAR_SIZE) break
    }
  }

  return radar
}

function buildTicketFromRadar(radar) {
  const ranked = [...radar].sort(compareByScoreThenKickoff)

  const ticket = []
  const usedMatches = new Set()
  const usedFamilies = new Set()

  for (const item of ranked) {
    if (usedMatches.has(item.match_id)) continue

    const familyKey = `${item.main_family}:${item.main_subfamily}`
    if (usedFamilies.has(familyKey) && ticket.length < 2) continue

    ticket.push(item)
    usedMatches.add(item.match_id)
    usedFamilies.add(familyKey)

    if (ticket.length === TICKET_SIZE) break
  }

  if (ticket.length < TICKET_SIZE) {
    for (const item of ranked) {
      if (usedMatches.has(item.match_id)) continue
      ticket.push(item)
      usedMatches.add(item.match_id)
      if (ticket.length === TICKET_SIZE) break
    }
  }

  return ticket.sort(compareByKickoff)
}

async function updateMatchesInsights(analyses) {
  for (const item of analyses) {
    const { error } = await supabase
      .from("matches")
      .update({
        pick: item.main_pick,
        insight: item.insight,
        probability: round2(item.main_probability),
        game_profile: item.game_profile,
        confidence_score: round2(item.main_score),
        updated_at: new Date().toISOString(),
      })
      .eq("id", item.match_id)

    if (error) {
      console.error(`Erro ao atualizar match ${item.match_id}:`, error.message)
    }
  }
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

  const rows = radar.map((item, index) => {
    const isInTicket = ticket.some((t) => String(t.match_id) === String(item.match_id))

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
  console.log("🧠 Scoutly Brain V6 iniciado...")

  const matches = await loadTodaysMatches()
  console.log(`📦 Jogos carregados para análise: ${matches.length}`)

  const analyses = matches
    .map(buildAnalysisFromRow)
    .filter(Boolean)

  console.log(`🧪 Análises válidas geradas: ${analyses.length}`)

  if (!analyses.length) {
    console.log("⚠️ Nenhuma análise válida encontrada para hoje.")
    await supabase.from("daily_picks").delete().neq("id", 0)
    return
  }

  await updateMatchesInsights(analyses)
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
      "| macro:",
      item.main_macro,
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
      "| score:",
      item.main_score,
      "| kickoff:",
      item.kickoff
    )
  })

  await rebuildDailyPicks(radar, ticket)

  console.log("✅ Scoutly Brain V6 finalizado com sucesso.")
  console.log(`📡 Radar do dia gerado com ${radar.length} jogo(s).`)
  console.log(`🎫 Bilhete do dia definido com ${ticket.length} jogo(s).`)
}

runScoutlyBrain().catch((error) => {
  console.error("❌ Erro no Scoutly Brain V6:", error)
  process.exit(1)
})
