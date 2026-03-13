const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const TIMEZONE = "America/Sao_Paulo"
const TOP_LIMIT = 5
const MIN_CONFIDENCE = 60

function toNum(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 2) {
  return Number(toNum(value).toFixed(decimals))
}

function pct(prob) {
  return round(toNum(prob) * 100, 1)
}

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
}

function todayDateInTimezone(timezone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(new Date())

  const year = parts.find(p => p.type === "year")?.value
  const month = parts.find(p => p.type === "month")?.value
  const day = parts.find(p => p.type === "day")?.value

  return `${year}-${month}-${day}`
}

function confidenceLevel(confidence) {
  if (confidence >= 80) return "Muito forte"
  if (confidence >= 72) return "Forte"
  if (confidence >= 60) return "Boa"
  return "Moderada"
}

function leagueWeight(league) {
  const key = normalizeText(league)

  const weights = {
    "uefa champions league": 20,
    "uefa europa league": 18,
    "uefa europa conference league": 17,
    "premier league": 18,
    "la liga": 17,
    "serie a": 17,
    "bundesliga": 16,
    "ligue 1": 15,
    "brasileirao serie a": 17,
    "campeonato brasileiro serie a": 17,
    "copa do brasil": 17,
    "brasileirao serie b": 15,
    "campeonato brasileiro serie b": 15,
    "conmebol libertadores": 18,
    "conmebol sudamericana": 15,
    "eredivisie": 14,
    "liga profesional argentina": 14,
    "coppa italia": 15,
    "copa del rey": 15,
    "fa cup": 15,
    "super lig": 11,
    "super league 1": 10
  }

  return weights[key] || 6
}

function buildDataQuality(match, stats) {
  let score = 0

  if (toNum(match.avg_goals) > 0) score += 10
  if (toNum(match.avg_corners) > 0) score += 10
  if (toNum(match.avg_shots) > 0) score += 10
  if (toNum(match.over15_prob) > 0) score += 10
  if (toNum(match.over25_prob) > 0) score += 10
  if (toNum(match.btts_prob) > 0) score += 10

  if (stats) {
    if (toNum(stats.home_shots) > 0 || toNum(stats.away_shots) > 0) score += 10
    if (toNum(stats.home_shots_on_target) > 0 || toNum(stats.away_shots_on_target) > 0) score += 10
    if (toNum(stats.home_corners) > 0 || toNum(stats.away_corners) > 0) score += 10
    if (toNum(stats.home_yellow_cards) > 0 || toNum(stats.away_yellow_cards) > 0) score += 10
  }

  return clamp(score, 0, 100)
}

function sanitizeStats(match, stats) {
  const avgShots = toNum(match.avg_shots)
  const avgCorners = toNum(match.avg_corners)
  const avgGoals = toNum(match.avg_goals)

  let homeShots = toNum(stats?.home_shots)
  let awayShots = toNum(stats?.away_shots)

  let homeSot = toNum(stats?.home_shots_on_target)
  let awaySot = toNum(stats?.away_shots_on_target)

  let homeCorners = toNum(stats?.home_corners)
  let awayCorners = toNum(stats?.away_corners)

  let homeCards = toNum(stats?.home_yellow_cards)
  let awayCards = toNum(stats?.away_yellow_cards)

  let inconsistency = 0

  // Fallback para chutes totais
  if (homeShots <= 0 && awayShots <= 0 && avgShots > 0) {
    homeShots = round(avgShots * 0.52)
    awayShots = round(avgShots * 0.48)
    inconsistency += 4
  }

  // Fallback para chutes no gol
  if (homeSot <= 0 && awaySot <= 0) {
    const totalShots = homeShots + awayShots
    const estimatedSot = totalShots > 0 ? Math.max(2, round(totalShots * 0.32)) : 0
    homeSot = round(estimatedSot * 0.52)
    awaySot = round(estimatedSot * 0.48)
    inconsistency += 5
  }

  // Nunca deixar SOT > shots
  if (homeSot > homeShots) {
    homeSot = homeShots
    inconsistency += 12
  }

  if (awaySot > awayShots) {
    awaySot = awayShots
    inconsistency += 12
  }

  // Se total SOT > total shots, corta
  let totalShots = homeShots + awayShots
  let totalSot = homeSot + awaySot

  if (totalSot > totalShots) {
    totalSot = totalShots
    homeSot = round(totalSot * 0.52)
    awaySot = totalShots - homeSot
    inconsistency += 15
  }

  // Fallback corners
  if (homeCorners <= 0 && awayCorners <= 0 && avgCorners > 0) {
    homeCorners = round(avgCorners * 0.52)
    awayCorners = round(avgCorners * 0.48)
    inconsistency += 4
  }

  // Cartões fallback
  if (homeCards <= 0 && awayCards <= 0) {
    homeCards = 2
    awayCards = 2
    inconsistency += 3
  }

  // Coerência: gols esperados baixos com muito SOT pode indicar dado torto
  if (avgGoals > 0 && totalSot > 0) {
    if (avgGoals < 1.2 && totalSot >= 7) inconsistency += 10
    if (avgGoals < 1.0 && totalSot >= 6) inconsistency += 8
  }

  // Coerência: muito pouco chute com muito escanteio ou vice-versa
  if (totalShots > 0 && (homeCorners + awayCorners) > 0) {
    if (totalShots <= 10 && (homeCorners + awayCorners) >= 11) inconsistency += 8
    if (totalShots >= 24 && (homeCorners + awayCorners) <= 4) inconsistency += 6
  }

  return {
    homeShots,
    awayShots,
    homeSot,
    awaySot,
    homeCorners,
    awayCorners,
    homeCards,
    awayCards,
    totalShots: homeShots + awayShots,
    totalSot: homeSot + awaySot,
    totalCorners: homeCorners + awayCorners,
    totalCards: homeCards + awayCards,
    inconsistency: clamp(inconsistency, 0, 100)
  }
}

function buildRiskDetector(match, cleanStats) {
  const avgGoals = toNum(match.avg_goals)
  const avgCorners = toNum(match.avg_corners)
  const avgShots = toNum(match.avg_shots)
  const over25 = toNum(match.over25_prob)
  const btts = toNum(match.btts_prob)
  const homeWin = toNum(match.home_win_prob || match.home_result_prob)
  const draw = toNum(match.draw_prob || match.draw_result_prob)
  const awayWin = toNum(match.away_win_prob || match.away_result_prob)

  const dataQuality = buildDataQuality(match, cleanStats)

  let risk = 0

  const spread = Math.max(homeWin, draw, awayWin) - Math.min(homeWin, draw, awayWin)
  if (spread < 0.15) risk += 18

  if (avgGoals > 0 && avgGoals < 1.4) risk += 10
  if (avgCorners > 0 && avgCorners < 6.5) risk += 8
  if (avgShots > 0 && avgShots < 12) risk += 8

  if (Math.abs(over25 - 0.5) < 0.07) risk += 10
  if (Math.abs(btts - 0.5) < 0.08) risk += 10

  if (dataQuality < 60) risk += 18
  if (dataQuality < 45) risk += 12

  risk += cleanStats.inconsistency * 0.55

  return {
    riskScore: clamp(risk, 0, 100),
    label:
      risk >= 55 ? "alto" :
      risk >= 30 ? "medio" :
      "baixo"
  }
}

function buildLeagueDetector(match) {
  const weight = leagueWeight(match.league)

  return {
    weight,
    penalty:
      weight >= 16 ? 0 :
      weight >= 12 ? 4 :
      weight >= 9 ? 8 :
      14
  }
}

function buildExpectedValues(match, cleanStats) {
  const avgGoals = toNum(match.avg_goals)
  const powerHome = Math.max(toNum(match.power_home, 1), 0.1)
  const powerAway = Math.max(toNum(match.power_away, 1), 0.1)
  const powerSum = powerHome + powerAway

  let expectedHomeGoals = avgGoals > 0 ? avgGoals * (powerHome / powerSum) : 0.8
  let expectedAwayGoals = avgGoals > 0 ? avgGoals * (powerAway / powerSum) : 0.8

  expectedHomeGoals = clamp(expectedHomeGoals, 0.2, 3.5)
  expectedAwayGoals = clamp(expectedAwayGoals, 0.2, 3.5)

  const totalShots = cleanStats.totalShots > 0 ? cleanStats.totalShots : Math.max(toNum(match.avg_shots), 12)
  const totalSot = cleanStats.totalSot > 0 ? cleanStats.totalSot : Math.max(round(totalShots * 0.32), 3)
  const totalCorners = cleanStats.totalCorners > 0 ? cleanStats.totalCorners : Math.max(toNum(match.avg_corners), 6)
  const totalCards = cleanStats.totalCards > 0 ? cleanStats.totalCards : 4

  const cappedSot = Math.min(totalSot, totalShots)

  return {
    expectedHomeGoals: round(expectedHomeGoals),
    expectedAwayGoals: round(expectedAwayGoals),
    expectedHomeShots: round(cleanStats.homeShots),
    expectedAwayShots: round(cleanStats.awayShots),
    expectedHomeSot: round(cleanStats.homeSot),
    expectedAwaySot: round(cleanStats.awaySot),
    expectedTotalShots: round(totalShots),
    expectedTotalSot: round(cappedSot),
    expectedCorners: round(totalCorners),
    expectedCards: round(totalCards)
  }
}

function lineInflationPenalty(expected, line) {
  const diff = line - expected

  if (diff <= 0.2) return 0
  if (diff <= 0.8) return 3
  if (diff <= 1.5) return 7
  return 12
}

function makePick({ market, confidence, category }) {
  const fixed = clamp(Math.round(confidence), 0, 97)
  return {
    market,
    confidence: fixed,
    probability: round(fixed / 100, 4),
    level: confidenceLevel(fixed),
    category
  }
}

function buildCandidates(match, cleanStats) {
  const avgGoals = toNum(match.avg_goals)
  const avgCorners = toNum(match.avg_corners)
  const avgShots = toNum(match.avg_shots)

  const over15 = toNum(match.over15_prob)
  const over25 = toNum(match.over25_prob)
  const under25 = toNum(match.under25_prob)
  const under35 = toNum(match.under35_prob)
  const btts = toNum(match.btts_prob)
  const corners85 = toNum(match.corners_over85_prob)

  const homeWin = toNum(match.home_win_prob || match.home_result_prob)
  const draw = toNum(match.draw_prob || match.draw_result_prob)
  const awayWin = toNum(match.away_win_prob || match.away_result_prob)

  const powerHome = toNum(match.power_home)
  const powerAway = toNum(match.power_away)
  const homeForm = toNum(match.home_form)
  const awayForm = toNum(match.away_form)

  const expected = buildExpectedValues(match, cleanStats)
  const risk = buildRiskDetector(match, cleanStats)
  const league = buildLeagueDetector(match)

  const inconsistencyPenalty = cleanStats.inconsistency * 0.35
  const riskPenalty = risk.riskScore * 0.18
  const leaguePenalty = league.penalty

  const candidates = []

  // GOLS
  if (over15 >= 0.68) {
    let conf = pct(over15) + 3
    if (avgGoals >= 2.0) conf += 3
    if (expected.expectedTotalShots >= 18) conf += 2
    conf -= lineInflationPenalty(avgGoals || 1.8, 1.5)
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Mais de 1.5 gols",
      confidence: conf,
      category: "gols"
    }))
  }

  if (over25 >= 0.60) {
    let conf = pct(over25) + 2
    if (avgGoals >= 2.6) conf += 4
    if (expected.expectedTotalShots >= 21) conf += 3
    conf -= lineInflationPenalty(avgGoals || 2.2, 2.5)
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Mais de 2.5 gols",
      confidence: conf,
      category: "gols"
    }))
  }

  if (under35 >= 0.68) {
    let conf = pct(under35) + 2
    if (avgGoals <= 2.8) conf += 3
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Menos de 3.5 gols",
      confidence: conf,
      category: "gols"
    }))
  }

  if (under25 >= 0.60) {
    let conf = pct(under25) + 2
    if (avgGoals <= 2.1) conf += 4
    if (btts <= 0.45) conf += 2
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Menos de 2.5 gols",
      confidence: conf,
      category: "gols"
    }))
  }

  if (btts >= 0.60) {
    let conf = pct(btts) + 2
    if (avgGoals >= 2.5) conf += 3
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Ambas marcam",
      confidence: conf,
      category: "gols"
    }))
  }

  if (btts > 0 && btts <= 0.42) {
    let conf = pct(1 - btts) + 2
    if (avgGoals <= 2.2) conf += 3
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Ambas não marcam",
      confidence: conf,
      category: "gols"
    }))
  }

  // ESCANTEIOS
  if (avgCorners >= 7.8) {
    let conf = 63 + (avgCorners - 7.8) * 8
    conf -= lineInflationPenalty(avgCorners, 7.5)
    conf -= riskPenalty * 0.8
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Mais de 7.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (corners85 >= 0.60) {
    let conf = pct(corners85) + 2
    if (avgCorners >= 9.0) conf += 3
    conf -= lineInflationPenalty(avgCorners, 8.5)
    conf -= riskPenalty * 0.8
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Mais de 8.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (avgCorners >= 9.8) {
    let conf = 61 + (avgCorners - 9.8) * 8
    conf -= lineInflationPenalty(avgCorners, 9.5)
    conf -= riskPenalty * 0.8
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Mais de 9.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (avgCorners > 0 && avgCorners <= 10.2) {
    let conf = 72 - Math.max(0, avgCorners - 8.8) * 4
    conf -= riskPenalty * 0.8
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Menos de 11.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (cleanStats.homeCorners >= 3.8) {
    let conf = 62 + (cleanStats.homeCorners - 3.8) * 10
    if (powerHome >= powerAway) conf += 3
    conf -= riskPenalty * 0.7
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Casa mais de 3.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (cleanStats.awayCorners >= 3.8) {
    let conf = 62 + (cleanStats.awayCorners - 3.8) * 10
    if (powerAway >= powerHome) conf += 3
    conf -= riskPenalty * 0.7
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Visitante mais de 3.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  // SEGURANÇA
  if (homeWin + draw >= 0.72) {
    let conf = pct(homeWin + draw) + 2
    if (powerHome >= powerAway) conf += 2
    if (homeForm >= awayForm) conf += 2
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Dupla chance casa ou empate",
      confidence: conf,
      category: "seguranca"
    }))
  }

  if (awayWin + draw >= 0.72) {
    let conf = pct(awayWin + draw) + 2
    if (powerAway >= powerHome) conf += 2
    if (awayForm >= homeForm) conf += 2
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: "Dupla chance visitante ou empate",
      confidence: conf,
      category: "seguranca"
    }))
  }

  if (homeWin >= 0.57 && (powerHome - powerAway) >= 0.20) {
    let conf = pct(homeWin) + 4
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: `Vitória ${match.home_team}`,
      confidence: conf,
      category: "seguranca"
    }))
  }

  if (awayWin >= 0.57 && (powerAway - powerHome) >= 0.20) {
    let conf = pct(awayWin) + 4
    conf -= riskPenalty
    conf -= leaguePenalty
    conf -= inconsistencyPenalty

    candidates.push(makePick({
      market: `Vitória ${match.away_team}`,
      confidence: conf,
      category: "seguranca"
    }))
  }

  return {
    candidates: candidates
      .filter(c => c.confidence >= MIN_CONFIDENCE)
      .sort((a, b) => b.confidence - a.confidence),
    risk,
    league,
    expected
  }
}

function duplicateGroup(market) {
  const m = normalizeText(market)

  if (m.includes("mais de 1.5 gols")) return "grupo_gols_over_15"
  if (m.includes("mais de 2.5 gols")) return "grupo_gols_over_25"
  if (m.includes("menos de 2.5 gols")) return "grupo_gols_under_25"
  if (m.includes("menos de 3.5 gols")) return "grupo_gols_under_35"

  if (m.includes("mais de 7.5 escanteios")) return "grupo_esc_over_75"
  if (m.includes("mais de 8.5 escanteios")) return "grupo_esc_over_85"
  if (m.includes("mais de 9.5 escanteios")) return "grupo_esc_over_95"
  if (m.includes("menos de 11.5 escanteios")) return "grupo_esc_under_115"

  if (m.includes("casa mais de 3.5 escanteios")) return "grupo_casa_esc"
  if (m.includes("visitante mais de 3.5 escanteios")) return "grupo_fora_esc"

  if (m.includes("dupla chance casa ou empate")) return "grupo_dc_casa"
  if (m.includes("dupla chance visitante ou empate")) return "grupo_dc_fora"

  if (m.includes("vitoria")) return "grupo_vitoria"

  if (m.includes("ambas marcam")) return "grupo_btts_yes"
  if (m.includes("ambas nao marcam")) return "grupo_btts_no"

  return m
}

function selectBestThree(candidates) {
  const chosen = []
  const groupSet = new Set()
  const categoryCount = {}

  for (const pick of candidates) {
    if (chosen.length >= 3) break

    const group = duplicateGroup(pick.market)
    if (groupSet.has(group)) continue

    const count = categoryCount[pick.category] || 0
    if (count >= 2) continue

    if (chosen.length === 1 && chosen[0].category === pick.category) {
      const hasOtherCategory = candidates.some(c =>
        c.category !== chosen[0].category &&
        !groupSet.has(duplicateGroup(c.market))
      )
      if (hasOtherCategory) continue
    }

    chosen.push(pick)
    groupSet.add(group)
    categoryCount[pick.category] = count + 1
  }

  return chosen
}

function buildInsight(match, risk, expected, cleanStats) {
  const avgGoals = toNum(match.avg_goals)
  const avgCorners = toNum(match.avg_corners)
  const diff = toNum(match.power_home) - toNum(match.power_away)

  if (risk.label === "alto") {
    return "A leitura Scoutly vê um confronto mais instável, então a prioridade é trabalhar com linhas mais protegidas."
  }

  if (cleanStats.inconsistency >= 20) {
    return "A leitura Scoutly detecta sinais mistos nos dados desta partida, então os mercados sugeridos priorizam proteção e consistência."
  }

  if (avgGoals >= 2.8 && expected.expectedTotalShots >= 21) {
    return "A leitura Scoutly aponta um jogo ofensivo, com bom volume de finalizações e cenário favorável para mercados de gols."
  }

  if (avgCorners >= 9.2) {
    return "A leitura Scoutly vê um confronto com tendência de pressão pelos lados e bom potencial para linhas de escanteios."
  }

  if (avgGoals <= 2.1 && expected.expectedTotalShots <= 17) {
    return "A leitura Scoutly projeta um jogo mais controlado, com menor explosão ofensiva e tendência de placar mais curto."
  }

  if (diff >= 0.22) {
    return `A leitura Scoutly vê vantagem para ${match.home_team}, com cenário favorável para mercados de proteção a favor do mandante.`
  }

  if (diff <= -0.22) {
    return `A leitura Scoutly vê vantagem para ${match.away_team}, com cenário favorável para mercados de proteção a favor do visitante.`
  }

  return `A leitura Scoutly indica um confronto equilibrado, com ${round(expected.expectedCorners, 1)} escanteios projetados e ${round(expected.expectedHomeGoals + expected.expectedAwayGoals, 1)} gols esperados.`
}

function buildMatchScore(match, picks, risk, league, cleanStats) {
  const main = picks[0]
  if (!main) return 0

  let score = main.confidence
  score += league.weight * 0.7
  score -= risk.riskScore * 0.25
  score -= cleanStats.inconsistency * 0.15
  score += picks.length >= 3 ? 4 : picks.length === 2 ? 2 : 0

  return round(score, 2)
}

async function fetchTodayMatches() {
  const today = todayDateInTimezone(TIMEZONE)

  const { data, error } = await supabase
    .from("matches")
    .select("*")
    .eq("match_date", today)
    .order("kickoff", { ascending: true })

  if (error) throw error
  return data || []
}

async function fetchStatsMap(matchIds) {
  if (!matchIds.length) return new Map()

  const { data, error } = await supabase
    .from("match_stats")
    .select("*")
    .in("match_id", matchIds)

  if (error) throw error

  const map = new Map()
  for (const row of data || []) {
    map.set(row.match_id, row)
  }

  return map
}

async function replaceDailyPicks(rows) {
  const { error: delError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (delError) throw delError

  if (!rows.length) return

  const { error: insError } = await supabase
    .from("daily_picks")
    .insert(rows)

  if (insError) throw insError
}

async function replaceMatchAnalysis(rows, matchIds) {
  try {
    const { error: delError } = await supabase
      .from("match_analysis")
      .delete()
      .in("match_id", matchIds)

    if (delError) {
      console.warn("Aviso ao limpar match_analysis:", delError.message)
      return
    }

    if (!rows.length) return

    const { error: insError } = await supabase
      .from("match_analysis")
      .insert(rows)

    if (insError) {
      console.warn("Aviso ao inserir match_analysis:", insError.message)
    }
  } catch (err) {
    console.warn("Aviso geral em match_analysis:", err.message)
  }
}

async function updateMatches(rows) {
  for (const row of rows) {
    const { error } = await supabase
      .from("matches")
      .update({
        pick: row.pick,
        insight: row.insight
      })
      .eq("id", row.id)

    if (error) {
      console.warn(`Aviso ao atualizar match ${row.id}:`, error.message)
    }
  }
}

async function runBrainV2() {
  console.log("Scoutly Brain V2 iniciado")

  const matches = await fetchTodayMatches()
  if (!matches.length) {
    console.log("Nenhum jogo encontrado hoje.")
    return
  }

  const matchIds = matches.map(m => m.id)
  const statsMap = await fetchStatsMap(matchIds)

  const analyses = []
  const matchUpdates = []

  for (const match of matches) {
    const rawStats = statsMap.get(match.id) || null
    const cleanStats = sanitizeStats(match, rawStats)

    const { candidates, risk, league, expected } = buildCandidates(match, cleanStats)
    const picks = selectBestThree(candidates)

    if (!picks.length) continue

    const insight = buildInsight(match, risk, expected, cleanStats)
    const score = buildMatchScore(match, picks, risk, league, cleanStats)

    analyses.push({
      match,
      rawStats,
      cleanStats,
      picks,
      insight,
      risk,
      league,
      expected,
      score
    })

    matchUpdates.push({
      id: match.id,
      pick: picks[0].market,
      insight
    })
  }

  await updateMatches(matchUpdates)

  const sorted = analyses.sort((a, b) => b.score - a.score)

  const top = []
  const categoryCount = { gols: 0, escanteios: 0, seguranca: 0 }

  for (const item of sorted) {
    if (top.length >= TOP_LIMIT) break

    const cat = item.picks[0].category
    const limit = cat === "seguranca" ? 1 : 2

    if ((categoryCount[cat] || 0) >= limit) continue

    const repeated = top.some(existing =>
      duplicateGroup(existing.picks[0].market) === duplicateGroup(item.picks[0].market)
    )

    if (repeated) continue

    top.push(item)
    categoryCount[cat] = (categoryCount[cat] || 0) + 1
  }

  if (top.length < TOP_LIMIT) {
    for (const item of sorted) {
      if (top.length >= TOP_LIMIT) break
      if (top.some(x => x.match.id === item.match.id)) continue
      top.push(item)
    }
  }

  const dailyRows = top.map((item, index) => ({
    created_at: new Date().toISOString(),
    rank: index + 1,
    match_id: item.match.id,
    home_team: item.match.home_team,
    away_team: item.match.away_team,
    league: item.match.league,
    market: item.picks[0].market,
    probability: round(item.picks[0].confidence / 100, 4),
    is_opportunity: true
  }))

  const analysisRows = analyses.map(item => ({
    match_id: item.match.id,
    created_at: new Date().toISOString(),

    home_strength: round(toNum(item.match.power_home), 4),
    away_strength: round(toNum(item.match.power_away), 4),

    expected_home_goals: item.expected.expectedHomeGoals,
    expected_away_goals: item.expected.expectedAwayGoals,
    expected_home_shots: item.expected.expectedHomeShots,
    expected_away_shots: item.expected.expectedAwayShots,
    expected_home_sot: item.expected.expectedHomeSot,
    expected_away_sot: item.expected.expectedAwaySot,
    expected_corners: item.expected.expectedCorners,
    expected_cards: item.expected.expectedCards,

    prob_over25: round(toNum(item.match.over25_prob), 4),
    prob_btts: round(toNum(item.match.btts_prob), 4),
    prob_corners: round(toNum(item.match.corners_over85_prob), 4),
    prob_shots: round(clamp((item.expected.expectedTotalShots - 14) / 12, 0, 1), 4),
    prob_sot: round(clamp((item.expected.expectedTotalSot - 4) / 6, 0, 1), 4),
    prob_cards: round(clamp((item.expected.expectedCards - 3) / 4, 0, 1), 4),

    best_pick_1: item.picks[0]?.market || null,
    best_pick_2: item.picks[1]?.market || null,
    best_pick_3: item.picks[2]?.market || null
  }))

  await replaceDailyPicks(dailyRows)
  await replaceMatchAnalysis(analysisRows, matchIds)

  console.log(`Jogos analisados: ${analyses.length}`)
  console.log(`Top salvo: ${dailyRows.length}`)
  console.log("Scoutly Brain V2 finalizado com sucesso")
}

runBrainV2().catch(error => {
  console.error("Erro fatal no Scoutly Brain V2:", error)
  process.exit(1)
