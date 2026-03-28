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
    row.prob_over25,
    row.prob_btts,
    row.prob_corners,
    row.prob_shots,
    row.prob_sot,
    row.prob_cards,
  ]

  return values.some((v) => Number.isFinite(Number(v)) && Number(v) > 0)
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

  if (analysisError) {
    throw analysisError
  }

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
  const under25Prob = clamp(1 - over25Prob, 0, 1)
  const under35Prob = clamp(
    toNumber(markets.under35, 0) ||
      clamp(1 - Math.max(over25Prob - 0.18, 0), 0, 1),
    0,
    1
  )

  const cornersOver85Prob = clamp(
    avgCorners >= 10.6
      ? 0.86
      : avgCorners >= 9.6
        ? 0.78
        : avgCorners >= 8.8
          ? 0.70
          : avgCorners >= 8.1
            ? 0.63
            : toNumber(analysis.prob_corners, 0),
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
    corners_over85_prob: round2(cornersOver85Prob),

    home_win_prob: round2(homeWinProb),
    draw_prob: round2(drawProb),
    away_win_prob: round2(awayWinProb),

    expected_home_goals: round2(expectedHomeGoals),
    expected_away_goals: round2(expectedAwayGoals),
    expected_corners: round2(expectedCorners),

    confidence_score: round2(confidenceScore),

    best_pick_1: analysis.best_pick_1 || matchRow.pick || null,
    best_pick_2: analysis.best_pick_2 || null,
    best_pick_3: analysis.best_pick_3 || null,
    aggressive_pick: analysis.aggressive_pick || null,
    analysis_text: analysis.analysis_text || matchRow.insight || null,
    game_profile: matchRow.game_profile || null,
  }
}

async function loadTodaysMatches() {
  const today = getTodayInTZ()
  const now = new Date()
  const minTime = new Date(now.getTime() - PAST_GRACE_HOURS * 60 * 60 * 1000)

  const { matches, analysis } = await loadBaseTables()

  console.log("DEBUG DATA HOJE:", today)
  console.log("DEBUG TOTAL RAW MATCHES:", matches.length)
  console.log("DEBUG TOTAL ANALYSIS:", analysis.length)

  const analysisMap = new Map(
    analysis.map((row) => [String(row.match_id), row])
  )

  const merged = matches.map((row) => mergeMatchRow(row, analysisMap))

  const filtered = merged.filter((row) => {
    const kickoffDate = row.kickoff ? new Date(row.kickoff) : null
    const kickoffValid = kickoffDate && !Number.isNaN(kickoffDate.getTime())
    const kickoffDay = getKickoffDateOnly(row.kickoff)

    return (
      kickoffValid &&
      kickoffDay === today &&
      kickoffDate.getTime() >= minTime.getTime()
    )
  })

  const finalRows = filtered
    .filter((row) => row.home_team && row.away_team && row.league)
    .filter((row) => hasMinimumAnalysis(row))

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
    return "estatistico"
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
  const rhythm = getRhythmLabel(avgShots).toLowerCase()

  const market = String(bestPick.market || "").trim().toLowerCase()

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

function marketFamily(market) {
  const m = String(market || "").trim().toLowerCase()

  if (!m) return "outro"
  if (m.includes("escanteio")) return "escanteios"
  if (m.includes("ambas")) return "btts"
  if (m.includes("dupla chance") || m.includes("empate") || m.includes("vitória")) return "resultado"
  if (m.includes("gol")) return "gols"
  return "outro"
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

  const homeWin = toNumber(row.home_win_prob)
  const draw = toNumber(row.draw_prob)
  const awayWin = toNumber(row.away_win_prob)

  const homeOrDraw = clamp(homeWin + draw, 0, 1)
  const awayOrDraw = clamp(awayWin + draw, 0, 1)

  const profile = getGameProfile(row)

  if (over25Prob >= 0.65) {
    let score = over25Prob + (avgGoals >= 2.7 ? 0.05 : 0) + (avgShots >= 22 ? 0.04 : 0)
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
    let score = over15Prob + (avgGoals >= 2.3 ? 0.04 : 0) + (avgShots >= 20 ? 0.03 : 0)
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
    let score = bttsProb + (avgGoals >= 2.6 ? 0.04 : 0) + (avgShots >= 21 ? 0.03 : 0)
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
    let score = under25Prob + (avgGoals <= 2.1 ? 0.02 : 0) + (avgShots <= 17 ? 0.01 : 0)
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
    let score = under35Prob + (avgGoals <= 2.6 ? 0.01 : 0) + (avgShots <= 20 ? 0.01 : 0)
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
    let score = bttsNoProb + (avgGoals <= 2.2 ? 0.01 : 0) + (avgShots <= 18 ? 0.01 : 0)
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
    let score = cornersOver85Prob + (avgCorners >= 8.8 ? 0.04 : 0) + (avgShots >= 20 ? 0.02 : 0)
    if (profile === "estatistico") score += 0.04
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

  const cornersUnder105Prob = clamp(1 - Math.max(cornersOver85Prob - 0.18, 0), 0, 1)

  if (avgCorners <= 8.6 && cornersUnder105Prob >= 0.67) {
    let score = cornersUnder105Prob + (avgCorners <= 8.1 ? 0.02 : 0)
    if (profile === "estatistico") score -= 0.04
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
    if (c.macro === "ofensivo") c.score = clamp(c.score + 0.03, 0, 1)
    if (c.macro === "defensivo" && c.subfamily === "under") c.score = clamp(c.score - 0.03, 0, 1)
    if (c.market === "Ambas não marcam") c.score = clamp(c.score - 0.01, 0, 1)
    if (c.market === "Menos de 10.5 escanteios") c.score = clamp(c.score - 0.04, 0, 1)
  })

  return candidates.sort((a, b) => b.score - a.score)
}

function chooseBestAndAlternatives(candidates) {
  if (!candidates.length) return { best: null, alternatives: [] }

  const sorted = [...candidates].sort((a, b) => b.score - a.score)
  const best = sorted[0]

  const usedFamilies = new Set()
  usedFamilies.add(best.family || marketFamily(best.market))

  const alternatives = []

  for (const item of sorted.slice(1)) {
    if (!item || !item.market) continue
    if (item.market === best.market) continue

    const family = item.family || marketFamily(item.market)
    if (usedFamilies.has(family)) continue

    alternatives.push(item)
    usedFamilies.add(family)

    if (alternatives.length === 2) break
  }

  if (alternatives.length < 2) {
    for (const item of sorted.slice(1)) {
      if (!item || !item.market) continue
      if (item.market === best.market) continue
      if (alternatives.find((x) => x.market === item.market)) continue

      alternatives.push(item)

      if (alternatives.length === 2) break
    }
  }

  return { best, alternatives }
}

function buildAnalysisFromRow(row) {
  const candidates = buildMarketCandidates(row)
  if (!candidates.length) return null

  const profile = getGameProfile(row)
  const { best, alternatives } = chooseBestAndAlternatives(candidates)
  if (!best) return null

  const rhythm = getRhythmLabel(row.avg_shots)

  const aggressivePick =
    alternatives.find(
      (x) =>
        x.subfamily === "over" ||
        x.market.includes("Ambas marcam") ||
        x.market.includes("Mais de 2.5 gols") ||
        x.market.includes("Mais de 8.5 escanteios")
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
  }
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
    if (item.main_macro === "defensivo" && macroCount >= 4) continue
    if (item.main_macro === "estatistico" && macroCount >= 3) continue
    if (item.main_macro === "protecao" && macroCount >= 3) continue
    if (item.main_macro === "ofensivo" && macroCount >= 4) continue

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

  return radar.sort(compareByKickoff)
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
  console.log("🧠 Scoutly Brain V10 iniciado...")

  const matches = await loadTodaysMatches()
  console.log(`📦 Jogos de hoje carregados para análise: ${matches.length}`)

  const analyses = matches
    .map(buildAnalysisFromRow)
    .filter(Boolean)

  console.log(`🧪 Análises válidas geradas: ${analyses.length}`)

  if (!analyses.length) {
    console.log("⚠️ Nenhuma análise válida encontrada para hoje.")
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

  console.log("✅ Scoutly Brain V10 finalizado com sucesso.")
  console.log(`📡 Radar do dia gerado com ${radar.length} jogo(s).`)
  console.log(`🎫 Bilhete do dia definido com ${ticket.length} jogo(s).`)
}

runScoutlyBrain().catch((error) => {
  console.error("❌ Erro no Scoutly Brain V10:", error)
  process.exit(1)
})
