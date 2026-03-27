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
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
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

function buildMatchLabel(row) {
  return `${row.home_team} x ${row.away_team}`
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

function safeLeague(row) {
  return row.league || "Liga"
}

function safeProbability(value) {
  return clamp(toNumber(value, 0), 0, 1)
}

function hasForbiddenMarker(value = "") {
  const v = normalizeText(value)

  return (
    v.includes("u17") ||
    v.includes("u18") ||
    v.includes("u19") ||
    v.includes("u20") ||
    v.includes("u21") ||
    v.includes("u23") ||
    v.includes("under 17") ||
    v.includes("under 18") ||
    v.includes("under 19") ||
    v.includes("under 20") ||
    v.includes("under 21") ||
    v.includes("under 23") ||
    v.includes("sub 17") ||
    v.includes("sub 18") ||
    v.includes("sub 19") ||
    v.includes("sub 20") ||
    v.includes("sub 21") ||
    v.includes("sub 23") ||
    v.includes("reserve") ||
    v.includes("reserves") ||
    v.includes("open cup")
  )
}

function hasRealMetrics(row) {
  const metrics = row?.metrics || {}
  const markets = row?.markets || {}
  const probabilities = row?.probabilities || {}

  const values = [
    row?.expected_home_goals,
    row?.expected_away_goals,
    row?.expected_corners,
    row?.expected_home_shots,
    row?.expected_away_shots,
    row?.expected_home_sot,
    row?.expected_away_sot,
    metrics.goals,
    metrics.corners,
    metrics.shots,
    metrics.shots_on_target,
    metrics.cards,
    markets.over15,
    markets.over25,
    markets.btts,
    markets.corners,
    probabilities.home,
    probabilities.draw,
    probabilities.away,
    row?.prob_over25,
    row?.prob_btts,
    row?.prob_corners,
    row?.prob_shots,
    row?.prob_sot,
    row?.prob_cards,
  ]

  return values.some((v) => Number.isFinite(Number(v)) && Number(v) > 0)
}

function buildCornersOver85Prob(avgCorners, avgShots, avgGoals) {
  const base =
    avgCorners * 0.07 +
    avgShots * 0.01 +
    avgGoals * 0.04

  return clamp(base, 0.18, 0.88)
}

function normalizeMatchRow(row) {
  const metrics = row.metrics || {}
  const markets = row.markets || {}
  const probabilities = row.probabilities || {}

  const expectedHomeGoals =
    toNumber(row.expected_home_goals, null) ??
    toNumber(metrics.expected_home_goals, null)

  const expectedAwayGoals =
    toNumber(row.expected_away_goals, null) ??
    toNumber(metrics.expected_away_goals, null)

  const avgGoals =
    expectedHomeGoals !== null && expectedAwayGoals !== null
      ? expectedHomeGoals + expectedAwayGoals
      : toNumber(metrics.goals, 0)

  const avgCorners =
    toNumber(row.expected_corners, null) ??
    toNumber(metrics.corners ?? markets.corners, 0)

  const avgShots =
    toNumber(row.expected_home_shots, 0) +
      toNumber(row.expected_away_shots, 0) ||
    toNumber(metrics.shots, 0)

  const avgShotsOnTarget =
    toNumber(row.expected_home_sot, 0) +
      toNumber(row.expected_away_sot, 0) ||
    toNumber(metrics.shots_on_target, 0)

  const avgCards =
    toNumber(row.expected_cards, null) ??
    toNumber(metrics.cards, 0)

  const avgFouls =
    toNumber(metrics.fouls, 0)

  const over15Prob =
    safeProbability(markets.over15) ||
    clamp(safeProbability(row.prob_over25) + 0.18, 0, 1)

  const over25Prob =
    safeProbability(row.prob_over25) ||
    safeProbability(markets.over25)

  const bttsProb =
    safeProbability(row.prob_btts) ||
    safeProbability(markets.btts)

  const cornersProb =
    safeProbability(row.prob_corners) ||
    buildCornersOver85Prob(avgCorners, avgShots, avgGoals)

  const shotsProb =
    safeProbability(row.prob_shots)

  const sotProb =
    safeProbability(row.prob_sot)

  const cardsProb =
    safeProbability(row.prob_cards)

  const homeProb =
    safeProbability(row.home_result_prob) ||
    safeProbability(probabilities.home)

  const drawProb =
    safeProbability(row.draw_result_prob) ||
    safeProbability(probabilities.draw)

  const awayProb =
    safeProbability(row.away_result_prob) ||
    safeProbability(probabilities.away)

  const under25Prob =
    safeProbability(row.under25_prob) ||
    clamp(1 - over25Prob, 0, 1)

  const under35Prob =
    safeProbability(row.under35_prob) ||
    clamp(1 - Math.max(over25Prob - 0.18, 0), 0, 1)

  return {
    ...row,
    avg_goals: round1(avgGoals),
    avg_corners: round1(avgCorners),
    avg_shots: Math.round(avgShots),
    avg_shots_on_target: Math.round(avgShotsOnTarget),
    avg_cards: round1(avgCards),
    avg_fouls: round1(avgFouls),
    over15_prob: over15Prob,
    over25_prob: over25Prob,
    under25_prob: under25Prob,
    under35_prob: under35Prob,
    btts_prob: bttsProb,
    corners_over85_prob: cornersProb,
    shots_prob: shotsProb,
    sot_prob: sotProb,
    cards_prob: cardsProb,
    home_win_prob: homeProb,
    draw_prob: drawProb,
    away_win_prob: awayProb,
    home_result_prob: homeProb,
    draw_result_prob: drawProb,
    away_result_prob: awayProb,
    home_form: row.home_form || "",
    away_form: row.away_form || "",
  }
}

function getGameProfile(row) {
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
    return `A leitura Scoutly projeta cerca de ${avgCorners} escanteios, com ritmo ${rhythm}. Esse comportamento torna a linha de escanteios uma das melhores oportunidades estatísticas deste jogo.`
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

  const homeWin = toNumber(row.home_result_prob)
  const draw = toNumber(row.draw_result_prob)
  const awayWin = toNumber(row.away_result_prob)

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

  const cornersUnder95Prob = clamp(
    1 - Math.max(cornersOver85Prob - 0.10, 0),
    0,
    1
  )

  if (avgCorners <= 8.1 && cornersUnder95Prob >= 0.70) {
    let score = cornersUnder95Prob + (avgCorners <= 7.5 ? 0.02 : 0)

    if (profile === "corners") score -= 0.05
    if (profile === "ofensivo") score -= 0.03

    pushCandidate(candidates, {
      market: "Menos de 9.5 escanteios",
      probability: cornersUnder95Prob,
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
      c.score = clamp(c.score + 0.02, 0, 1)
    }

    if (c.market === "Menos de 9.5 escanteios") {
      c.score = clamp(c.score - 0.01, 0, 1)
    }

    if (c.market === "Ambas não marcam") {
      c.score = clamp(c.score - 0.01, 0, 1)
    }
  })

  return candidates.sort((a, b) => b.score - a.score)
}

function chooseBestAndAlternatives(candidates) {
  if (!candidates.length) {
    return { best: null, alternatives: [], aggressive: null }
  }

  const sorted = [...candidates].sort((a, b) => b.score - a.score)
  const best = sorted[0]

  const alternatives = sorted
    .slice(1)
    .filter((item, index, arr) => {
      return arr.findIndex((x) => x.market === item.market) === index
    })
    .slice(0, 2)

  const aggressive =
    sorted.find(
      (item) =>
        item.market !== best.market &&
        (
          item.market.includes("Mais de 2.5 gols") ||
          item.market.includes("Ambas marcam") ||
          item.market.includes("Mais de 8.5 escanteios")
        )
    ) || alternatives[1] || alternatives[0] || null

  return { best, alternatives, aggressive }
}

function buildAnalysisFromRow(row) {
  const candidates = buildMarketCandidates(row)
  if (!candidates.length) return null

  const profile = getGameProfile(row)
  const { best, alternatives, aggressive } = chooseBestAndAlternatives(candidates)
  if (!best) return null

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
    main_probability: round2(best.probability),
    main_score: round2(best.score),
    main_family: best.family,
    main_subfamily: best.subfamily,
    main_macro: best.macro,
    strength: getStrengthLabel(best.score),
    rhythm: getRhythmLabel(row.avg_shots),
    insight: buildInsight(row, best, profile),
    alternatives,
    aggressive_pick: aggressive?.market || null,
    home_form: row.home_form || "",
    away_form: row.away_form || "",
  }
}

function compareAnalyses(a, b) {
  if (b.main_score !== a.main_score) return b.main_score - a.main_score

  if (b.main_probability !== a.main_probability) {
    return b.main_probability - a.main_probability
  }

  const aTime = a.kickoff
    ? new Date(a.kickoff).getTime()
    : Number.MAX_SAFE_INTEGER

  const bTime = b.kickoff
    ? new Date(b.kickoff).getTime()
    : Number.MAX_SAFE_INTEGER

  return aTime - bTime
}

function chooseRadarAndTicket(analyses) {
  const sorted = [...analyses].sort(compareAnalyses)

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
    if (item.main_macro === "estatistico" && macroCount >= 3) continue
    if (item.main_macro === "protecao" && macroCount >= 2) continue
    if (item.main_macro === "ofensivo" && macroCount >= 4) continue

    radar.push(item)
    usedMatchIds.add(item.match_id)
    usedMarkets[item.main_pick] = marketCount + 1
    usedMacros[item.main_macro] = macroCount + 1

    if (radar.length >= RADAR_SIZE) break
  }

  if (radar.length < RADAR_SIZE) {
    for (const item of sorted) {
      if (usedMatchIds.has(item.match_id)) continue

      radar.push(item)
      usedMatchIds.add(item.match_id)

      if (radar.length >= RADAR_SIZE) break
    }
  }

  const ticket = radar.slice(0, TICKET_SIZE)

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

  return { radar, ticket }
}

async function loadTodaysMatches() {
  const today = getTodayInTZ()

  const { data, error } = await supabase
    .from("matches")
    .select(`
      id,
      home_team,
      away_team,
      league,
      kickoff,
      home_logo,
      away_logo,
      metrics,
      markets,
      probabilities,
      priority,
      match_analysis (
        match_id,
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
        home_result_prob,
        draw_result_prob,
        away_result_prob,
        under25_prob,
        under35_prob,
        game_profile,
        home_form,
        away_form,
        best_pick_1,
        best_pick_2,
        best_pick_3,
        aggressive_pick,
        analysis_text,
        confidence_score
      )
    `)
    .order("kickoff", { ascending: true, nullsFirst: false })

  if (error) throw error

  console.log("DEBUG DATA HOJE:", today)
  console.log("DEBUG TOTAL RAW MATCHES:", (data || []).length)

  const merged = (data || []).map((row) => {
    const analysis = Array.isArray(row.match_analysis)
      ? row.match_analysis[0] || {}
      : row.match_analysis || {}

    return {
      ...row,
      ...analysis,
    }
  })

  console.log("DEBUG TOTAL ANALYSIS:", merged.filter((x) => x.match_id).length)

  const filtered = merged
    .filter((row) => row.home_team && row.away_team && row.league && row.kickoff)
    .filter((row) => !hasForbiddenMarker(row.league))
    .filter((row) => getKickoffDateOnly(row.kickoff) === today)
    .filter((row) => hasRealMetrics(row))
    .map(normalizeMatchRow)
    .filter((row) => {
      return (
        row.avg_goals > 0 ||
        row.avg_corners > 0 ||
        row.avg_shots > 0 ||
        row.over15_prob > 0 ||
        row.over25_prob > 0 ||
        row.btts_prob > 0 ||
        row.home_win_prob > 0 ||
        row.draw_prob > 0 ||
        row.away_win_prob > 0
      )
    })

  console.log("DEBUG TOTAL FILTRADOS HOJE:", filtered.length)
  return filtered
}

async function updateMatchesInsights(analyses) {
  for (const item of analyses) {
    const { error } = await supabase
      .from("matches")
      .update({
        pick: item.main_pick,
        insight: item.insight,
        probability: round2(item.main_probability),
        updated_at: new Date().toISOString(),
      })
      .eq("id", item.match_id)

    if (error) {
      console.error(`Erro ao atualizar matches ${item.match_id}:`, error.message)
    }
  }
}

async function updateMatchAnalysis(analyses) {
  for (const item of analyses) {
    const payload = {
      match_id: item.match_id,
      best_pick_1: item.main_pick,
      best_pick_2: item.alternatives[0]?.market || null,
      best_pick_3: item.alternatives[1]?.market || null,
      aggressive_pick: item.aggressive_pick,
      analysis_text: item.insight,
      confidence_score: round2(item.main_score),
      game_profile: item.game_profile,
      home_form: item.home_form || null,
      away_form: item.away_form || null,
      updated_at: new Date().toISOString(),
    }

    const { error } = await supabase
      .from("match_analysis")
      .upsert(payload, { onConflict: "match_id" })

    if (error) {
      console.error(`Erro ao atualizar match_analysis ${item.match_id}:`, error.message)
    }
  }
}

async function rebuildDailyPicks(radar) {
  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (deleteError) throw deleteError

  const rows = radar.map((item, index) => ({
    rank: index + 1,
    match_id: item.match_id,
    home_team: item.home_team,
    away_team: item.away_team,
    league: item.league,
    market: item.main_pick,
    probability: round2(item.main_probability),
    is_opportunity: true,
    home_logo: item.home_logo || null,
    away_logo: item.away_logo || null,
    kickoff: item.kickoff,
    created_at: new Date().toISOString(),
  }))

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
  console.log("🧠 Scoutly Brain V5 iniciado...")

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

  await updateMatchesInsights(analyses)
  await updateMatchAnalysis(analyses)

  const { radar, ticket } = chooseRadarAndTicket(analyses)

  await rebuildDailyPicks(radar)

  console.log("✅ Scoutly Brain V5 finalizado com sucesso.")
  console.log(`📡 Radar do dia gerado com ${radar.length} jogo(s).`)
  console.log(`🎫 Bilhete do dia definido com ${ticket.length} jogo(s).`)

  if (ticket.length) {
    ticket.forEach((item, index) => {
      console.log(
        `🎯 Bilhete ${index + 1}: ${buildMatchLabel(item)} -> ${item.main_pick}`
      )
    })
  }
}

runScoutlyBrain().catch((error) => {
  console.error("❌ Erro no Scoutly Brain V5:", error)
  process.exit(1)
})
