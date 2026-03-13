const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const TIMEZONE = "America/Sao_Paulo"
const TOP_LIMIT = 5
const MIN_CONFIDENCE = 62

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
  if (confidence >= 62) return "Boa"
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
    "liga portugal": 13,
    "fa cup": 15,
    "copa del rey": 15,
    "coppa italia": 15,
    "liga profesional argentina": 14,
    "mls": 12,
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
    if (toNum(stats.home_shots) > 0) score += 10
    if (toNum(stats.away_shots) > 0) score += 10
    if (toNum(stats.home_corners) > 0) score += 10
    if (toNum(stats.away_corners) > 0) score += 10
  }

  return clamp(score, 0, 100)
}

function buildRiskDetector(match, stats) {
  const avgGoals = toNum(match.avg_goals)
  const avgCorners = toNum(match.avg_corners)
  const avgShots = toNum(match.avg_shots)
  const over25 = toNum(match.over25_prob)
  const btts = toNum(match.btts_prob)
  const homeWin = toNum(match.home_win_prob || match.home_result_prob)
  const draw = toNum(match.draw_prob || match.draw_result_prob)
  const awayWin = toNum(match.away_win_prob || match.away_result_prob)

  const dataQuality = buildDataQuality(match, stats)

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

function buildExpectedValues(match, stats) {
  const homeGoals = toNum(match.avg_goals) * (toNum(match.power_home, 1) / Math.max(toNum(match.power_home, 1) + toNum(match.power_away, 1), 0.5))
  const awayGoals = Math.max(toNum(match.avg_goals) - homeGoals, 0.2)

  const homeShots = toNum(stats?.home_shots)
  const awayShots = toNum(stats?.away_shots)
  const totalShots = homeShots + awayShots > 0 ? homeShots + awayShots : toNum(match.avg_shots)

  const homeShotsOnTarget = toNum(stats?.home_shots_on_target)
  const awayShotsOnTarget = toNum(stats?.away_shots_on_target)

  const homeCorners = toNum(stats?.home_corners)
  const awayCorners = toNum(stats?.away_corners)
  const totalCorners = homeCorners + awayCorners > 0 ? homeCorners + awayCorners : toNum(match.avg_corners)

  const totalCards = toNum(stats?.home_yellow_cards) + toNum(stats?.away_yellow_cards)

  return {
    expectedHomeGoals: round(homeGoals),
    expectedAwayGoals: round(awayGoals),
    expectedHomeShots: round(homeShots || totalShots * 0.52),
    expectedAwayShots: round(awayShots || totalShots * 0.48),
    expectedCorners: round(totalCorners),
    expectedCards: round(totalCards || 4.2)
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

function buildCandidates(match, stats) {
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

  const homeCorners = toNum(stats?.home_corners)
  const awayCorners = toNum(stats?.away_corners)

  const expected = buildExpectedValues(match, stats)
  const risk = buildRiskDetector(match, stats)
  const league = buildLeagueDetector(match)

  const riskPenalty = risk.riskScore * 0.18
  const leaguePenalty = league.penalty

  const candidates = []

  // GOLS
  if (over15 >= 0.70) {
    let conf = pct(over15) + 4
    if (avgGoals >= 2.0) conf += 3
    if (avgShots >= 18) conf += 2
    conf -= lineInflationPenalty(avgGoals, 1.5)
    conf -= riskPenalty
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Mais de 1.5 gols",
      confidence: conf,
      category: "gols"
    }))
  }

  if (over25 >= 0.60) {
    let conf = pct(over25) + 2
    if (avgGoals >= 2.6) conf += 4
    if (avgShots >= 21) conf += 3
    conf -= lineInflationPenalty(avgGoals, 2.5)
    conf -= riskPenalty
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Mais de 2.5 gols",
      confidence: conf,
      category: "gols"
    }))
  }

  if (under35 >= 0.70) {
    let conf = pct(under35) + 2
    if (avgGoals <= 2.8) conf += 3
    conf -= riskPenalty
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Menos de 3.5 gols",
      confidence: conf,
      category: "gols"
    }))
  }

  if (under25 >= 0.62) {
    let conf = pct(under25) + 2
    if (avgGoals <= 2.1) conf += 4
    if (btts <= 0.45) conf += 2
    conf -= riskPenalty
    conf -= leaguePenalty

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

    candidates.push(makePick({
      market: "Ambas marcam",
      confidence: conf,
      category: "gols"
    }))
  }

  if (btts <= 0.42) {
    let conf = pct(1 - btts) + 2
    if (avgGoals <= 2.2) conf += 3
    conf -= riskPenalty
    conf -= leaguePenalty

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

    candidates.push(makePick({
      market: "Mais de 9.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (avgCorners <= 10.2) {
    let conf = 72 - Math.max(0, avgCorners - 8.8) * 4
    conf -= riskPenalty * 0.8
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Menos de 11.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (homeCorners >= 3.8) {
    let conf = 62 + (homeCorners - 3.8) * 10
    if (powerHome >= powerAway) conf += 3
    conf -= riskPenalty * 0.7
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Casa mais de 3.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (awayCorners >= 3.8) {
    let conf = 62 + (awayCorners - 3.8) * 10
    if (powerAway >= powerHome) conf += 3
    conf -= riskPenalty * 0.7
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Visitante mais de 3.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (homeCorners >= 4.8) {
    let conf = 60 + (homeCorners - 4.8) * 10
    if (powerHome > powerAway) conf += 3
    conf -= riskPenalty * 0.7
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Casa mais de 4.5 escanteios",
      confidence: conf,
      category: "escanteios"
    }))
  }

  if (awayCorners >= 4.8) {
    let conf = 60 + (awayCorners - 4.8) * 10
    if (powerAway > powerHome) conf += 3
    conf -= riskPenalty * 0.7
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Visitante mais de 4.5 escanteios",
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

    candidates.push(makePick({
      market: "Casa ou empate",
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

    candidates.push(makePick({
      market: "Visitante ou empate",
      confidence: conf,
      category: "seguranca"
    }))
  }

  if (homeWin >= 0.57 && (powerHome - powerAway) >= 0.20) {
    let conf = pct(homeWin) + 4
    conf -= riskPenalty
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Vitória da casa",
      confidence: conf,
      category: "seguranca"
    }))
  }

  if (awayWin >= 0.57 && (powerAway - powerHome) >= 0.20) {
    let conf = pct(awayWin) + 4
    conf -= riskPenalty
    conf -= leaguePenalty

    candidates.push(makePick({
      market: "Vitória do visitante",
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

  if (m.includes("mais de 1.5 gols")) return "grupo_gols_over"
  if (m.includes("mais de 2.5 gols")) return "grupo_gols_over"
  if (m.includes("menos de 2.5 gols")) return "grupo_gols_under"
  if (m.includes("menos de 3.5 gols")) return "grupo_gols_under"

  if (m.includes("mais de 7.5 escanteios")) return "grupo_esc_over"
  if (m.includes("mais de 8.5 escanteios")) return "grupo_esc_over"
  if (m.includes("mais de 9.5 escanteios")) return "grupo_esc_over"
  if (m.includes("menos de 11.5 escanteios")) return "grupo_esc_under"

  if (m.includes("casa mais de 3.5 escanteios")) return "grupo_casa_esc"
  if (m.includes("casa mais de 4.5 escanteios")) return "grupo_casa_esc"
  if (m.includes("visitante mais de 3.5 escanteios")) return "grupo_fora_esc"
  if (m.includes("visitante mais de 4.5 escanteios")) return "grupo_fora_esc"

  if (m.includes("casa ou empate")) return "grupo_casa_protecao"
  if (m.includes("vitoria da casa")) return "grupo_casa_protecao"
  if (m.includes("visitante ou empate")) return "grupo_fora_protecao"
  if (m.includes("vitoria do visitante")) return "grupo_fora_protecao"

  if (m.includes("ambas marcam")) return "grupo_btts_yes"
  if (m.includes("ambas nao marcam")) return "grupo_btts_no"

  return m
}

function selectBestThree(candidates) {
  const chosen = []
  const categoryCount = {}
  const groupSet = new Set()

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

function buildInsight(match, stats, risk, expected) {
  const avgGoals = toNum(match.avg_goals)
  const avgCorners = toNum(match.avg_corners)
  const avgShots = toNum(match.avg_shots)
  const diff = toNum(match.power_home) - toNum(match.power_away)

  if (risk.label === "alto") {
    return "A leitura Scoutly vê um confronto mais instável, então a prioridade é buscar linhas mais protegidas."
  }

  if (avgGoals >= 2.8 && avgShots >= 21) {
    return "A leitura Scoutly aponta um jogo ofensivo, com boa projeção de gols e volume de finalizações acima da média."
  }

  if (avgCorners >= 9.2) {
    return "A leitura Scoutly vê pressão pelos lados e cenário favorável para mercados de escanteios."
  }

  if (avgGoals <= 2.1 && avgShots <= 16) {
    return "A leitura Scoutly projeta um jogo mais controlado, com tendência de placar mais curto."
  }

  if (diff >= 0.22) {
    return `A leitura Scoutly vê vantagem para ${match.home_team}, com superioridade recente e boa tendência de controle.`
  }

  if (diff <= -0.22) {
    return `A leitura Scoutly vê vantagem para ${match.away_team}, com cenário favorável para mercados de proteção.`
  }

  return `A leitura Scoutly indica um confronto equilibrado, com ${round(expected.expectedCorners, 1)} escanteios projetados e ${round(expected.expectedHomeGoals + expected.expectedAwayGoals, 1)} gols esperados.`
}

function buildMatchScore(match, picks, risk, league) {
  const main = picks[0]
  if (!main) return 0

  let score = main.confidence
  score += league.weight * 0.7
  score -= risk.riskScore * 0.25
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
    const stats = statsMap.get(match.id) || null
    const { candidates, risk, league, expected } = buildCandidates(match, stats)
    const picks = selectBestThree(candidates)

    if (!picks.length) continue

    const insight = buildInsight(match, stats, risk, expected)
    const score = buildMatchScore(match, picks, risk, league)

    analyses.push({
      match,
      stats,
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
    expected_home_goals: item.expected.expectedHomeGoals,
    expected_away_goals: item.expected.expectedAwayGoals,
    expected_home_shots: item.expected.expectedHomeShots,
    expected_away_shots: item.expected.expectedAwayShots,
    expected_corners: item.expected.expectedCorners,
    expected_cards: item.expected.expectedCards,

    prob_over25: toNum(item.match.over25_prob),
    prob_btts: toNum(item.match.btts_prob),
    prob_corners: toNum(item.match.corners_over85_prob),
    prob_shots: round(clamp((toNum(item.match.avg_shots) - 16) / 10, 0, 1), 4),
    prob_sot: round(clamp(((toNum(item.stats?.home_shots_on_target) + toNum(item.stats?.away_shots_on_target)) - 5) / 5, 0, 1), 4),
    prob_cards: round(clamp((item.expected.expectedCards - 3.5) / 3, 0, 1), 4),

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
})
