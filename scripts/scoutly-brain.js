const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const TIMEZONE = "America/Sao_Paulo"
const MIN_CONFIDENCE = 65
const TOP_LIMIT = 5

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 2) {
  return Number(Number(value || 0).toFixed(decimals))
}

function toNum(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
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

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
}

function confidenceLevel(confidence) {
  if (confidence >= 80) return "Muito forte"
  if (confidence >= 70) return "Forte"
  return "Boa"
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
    "copa del rey": 15,
    "coppa italia": 15,
    "fa cup": 15,
    "liga profesional argentina": 14
  }

  return weights[key] || 8
}

function buildExpectedTeamCorners(match, stats) {
  const avgCorners = toNum(match.avg_corners)
  const homeCornersStats = toNum(stats?.home_corners)
  const awayCornersStats = toNum(stats?.away_corners)

  const powerHome = Math.max(toNum(match.power_home, 0.4), 0.1)
  const powerAway = Math.max(toNum(match.power_away, 0.4), 0.1)
  const totalPower = powerHome + powerAway

  const homeShare = totalPower > 0 ? powerHome / totalPower : 0.5
  const awayShare = totalPower > 0 ? powerAway / totalPower : 0.5

  const homeEstimated = homeCornersStats > 0 ? homeCornersStats : avgCorners * homeShare
  const awayEstimated = awayCornersStats > 0 ? awayCornersStats : avgCorners * awayShare

  return {
    home: round(homeEstimated),
    away: round(awayEstimated)
  }
}

function buildExpectedShotsOnTarget(match, stats) {
  const home = toNum(stats?.home_shots_on_target)
  const away = toNum(stats?.away_shots_on_target)

  if (home > 0 || away > 0) {
    return { home, away, total: home + away }
  }

  const avgShots = toNum(match.avg_shots)
  const total = round(avgShots * 0.32)
  return {
    home: round(total * 0.52),
    away: round(total * 0.48),
    total
  }
}

function buildDataQuality(match, stats) {
  let points = 0

  if (toNum(match.avg_goals) > 0) points += 1
  if (toNum(match.avg_corners) > 0) points += 1
  if (toNum(match.avg_shots) > 0) points += 1
  if (toNum(match.over15_prob) > 0) points += 1
  if (toNum(match.over25_prob) > 0) points += 1
  if (toNum(match.btts_prob) > 0) points += 1

  if (stats) {
    if (toNum(stats.home_shots) > 0) points += 1
    if (toNum(stats.away_shots) > 0) points += 1
    if (toNum(stats.home_corners) > 0) points += 1
    if (toNum(stats.away_corners) > 0) points += 1
  }

  return clamp(points * 10, 35, 100)
}

function makePick({ market, confidence, category, reason }) {
  const fixed = clamp(Math.round(confidence), 0, 99)

  return {
    market,
    confidence: fixed,
    probability: round(fixed / 100, 4),
    level: confidenceLevel(fixed),
    category,
    reason
  }
}

function buildInsight(match, stats) {
  const avgGoals = toNum(match.avg_goals)
  const avgCorners = toNum(match.avg_corners)
  const avgShots = toNum(match.avg_shots)
  const powerHome = toNum(match.power_home)
  const powerAway = toNum(match.power_away)
  const bttsProb = toNum(match.btts_prob)
  const over25Prob = toNum(match.over25_prob)
  const expectedCorners = buildExpectedTeamCorners(match, stats)

  const diff = powerHome - powerAway

  if (avgGoals >= 2.8 && avgShots >= 21) {
    return "Scoutly vê um jogo com tendência ofensiva forte, bom volume de finalizações e cenário favorável para mercados de gols."
  }

  if (avgCorners >= 9.2 && expectedCorners.home >= 4.2 && expectedCorners.away >= 4.2) {
    return "Scoutly enxerga uma partida com boa pressão pelos lados e potencial interessante para mercados de escanteios."
  }

  if (bttsProb <= 0.42 && avgGoals <= 2.2) {
    return "Scoutly projeta um confronto mais controlado, com tendência de poucos gols e menor abertura ofensiva."
  }

  if (over25Prob >= 0.66 && bttsProb >= 0.58) {
    return "Scoutly identifica equilíbrio ofensivo entre os times e boa chance de um jogo com gols dos dois lados."
  }

  if (diff >= 0.22) {
    return "Scoutly vê superioridade do mandante, com melhor força recente e tendência de controle maior da partida."
  }

  if (diff <= -0.22) {
    return "Scoutly vê superioridade do visitante, com leitura favorável para proteção a favor da equipe de fora."
  }

  return "Scoutly projeta um jogo equilibrado, e a melhor leitura vem da combinação entre produção ofensiva, força recente e tendência estatística."
}

function buildCandidates(match, stats) {
  const avgGoals = toNum(match.avg_goals)
  const avgCorners = toNum(match.avg_corners)
  const avgShots = toNum(match.avg_shots)
  const homeWinProb = toNum(match.home_win_prob)
  const drawProb = toNum(match.draw_prob)
  const awayWinProb = toNum(match.away_win_prob)
  const over15Prob = toNum(match.over15_prob)
  const over25Prob = toNum(match.over25_prob)
  const under25Prob = toNum(match.under25_prob)
  const under35Prob = toNum(match.under35_prob)
  const bttsProb = toNum(match.btts_prob)
  const cornersOver85Prob = toNum(match.corners_over85_prob)
  const powerHome = toNum(match.power_home)
  const powerAway = toNum(match.power_away)
  const homeForm = toNum(match.home_form)
  const awayForm = toNum(match.away_form)

  const dataQuality = buildDataQuality(match, stats)
  const qualityPenalty = dataQuality < 55 ? 8 : dataQuality < 70 ? 4 : 0

  const shotsOnTarget = buildExpectedShotsOnTarget(match, stats)
  const corners = buildExpectedTeamCorners(match, stats)

  const diff = powerHome - powerAway
  const candidates = []

  // Gols
  if (over15Prob >= 0.72) {
    let confidence = over15Prob * 100
    if (avgGoals >= 2.1) confidence += 4
    if (avgShots >= 18) confidence += 3
    if (bttsProb >= 0.52) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Mais de 1.5 gols",
      confidence,
      category: "gols",
      reason: "boa projeção de gols e volume ofensivo"
    }))
  }

  if (over25Prob >= 0.62) {
    let confidence = over25Prob * 100
    if (avgGoals >= 2.6) confidence += 5
    if (avgShots >= 21) confidence += 3
    if (shotsOnTarget.total >= 7) confidence += 3
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Mais de 2.5 gols",
      confidence,
      category: "gols",
      reason: "projeção ofensiva acima da média"
    }))
  }

  if (under35Prob >= 0.72) {
    let confidence = under35Prob * 100
    if (avgGoals <= 2.9) confidence += 4
    if (avgShots <= 24) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Menos de 3.5 gols",
      confidence,
      category: "gols",
      reason: "cenário mais controlado para linha alta de gols"
    }))
  }

  if (under25Prob >= 0.64) {
    let confidence = under25Prob * 100
    if (avgGoals <= 2.1) confidence += 4
    if (bttsProb <= 0.45) confidence += 3
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Menos de 2.5 gols",
      confidence,
      category: "gols",
      reason: "baixa projeção de gols totais"
    }))
  }

  if (bttsProb >= 0.60) {
    let confidence = bttsProb * 100
    if (avgGoals >= 2.5) confidence += 4
    if (shotsOnTarget.home >= 3) confidence += 2
    if (shotsOnTarget.away >= 3) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Ambas marcam",
      confidence,
      category: "gols",
      reason: "boa produção ofensiva dos dois lados"
    }))
  }

  if (bttsProb <= 0.42) {
    let confidence = (1 - bttsProb) * 100
    if (avgGoals <= 2.2) confidence += 4
    if (Math.abs(diff) >= 0.25) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Ambas não marcam",
      confidence,
      category: "gols",
      reason: "jogo com tendência menor de gol dos dois lados"
    }))
  }

  // Escanteios
  if (avgCorners >= 8.2) {
    let confidence = 60 + (avgCorners - 8.2) * 8
    if (corners.home + corners.away >= 8.5) confidence += 4
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Mais de 7.5 escanteios",
      confidence,
      category: "escanteios",
      reason: "média de escanteios favorável"
    }))
  }

  if (cornersOver85Prob >= 0.62) {
    let confidence = cornersOver85Prob * 100
    if (avgCorners >= 9) confidence += 4
    if (corners.home + corners.away >= 9) confidence += 3
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Mais de 8.5 escanteios",
      confidence,
      category: "escanteios",
      reason: "boa projeção total de escanteios"
    }))
  }

  if (avgCorners <= 10.2) {
    let confidence = 72 - Math.max(0, avgCorners - 8.8) * 4
    if (cornersOver85Prob <= 0.58) confidence += 3
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Menos de 11.5 escanteios",
      confidence,
      category: "escanteios",
      reason: "linha alta com boa margem de proteção"
    }))
  }

  if (corners.home >= 4.0) {
    let confidence = 60 + (corners.home - 4) * 10
    if (powerHome > powerAway) confidence += 4
    if (toNum(stats?.home_shots) >= toNum(stats?.away_shots)) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Casa mais de 3.5 escanteios",
      confidence,
      category: "escanteios",
      reason: "mandante com tendência de pressionar mais"
    }))
  }

  if (corners.away >= 4.0) {
    let confidence = 60 + (corners.away - 4) * 10
    if (powerAway > powerHome) confidence += 4
    if (toNum(stats?.away_shots) >= 9) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Visitante mais de 3.5 escanteios",
      confidence,
      category: "escanteios",
      reason: "visitante com produção ofensiva interessante"
    }))
  }

  if (corners.home >= 5.0) {
    let confidence = 62 + (corners.home - 5) * 10
    if (powerHome > powerAway) confidence += 3
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Casa mais de 4.5 escanteios",
      confidence,
      category: "escanteios",
      reason: "mandante com volume para linha mais agressiva"
    }))
  }

  if (corners.away >= 5.0) {
    let confidence = 62 + (corners.away - 5) * 10
    if (powerAway > powerHome) confidence += 3
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Visitante mais de 4.5 escanteios",
      confidence,
      category: "escanteios",
      reason: "visitante com pressão ofensiva suficiente"
    }))
  }

  // Segurança
  if (homeWinProb + drawProb >= 0.72) {
    let confidence = (homeWinProb + drawProb) * 100
    if (powerHome >= powerAway) confidence += 3
    if (homeForm >= awayForm) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Casa ou empate",
      confidence,
      category: "seguranca",
      reason: "boa proteção a favor do mandante"
    }))
  }

  if (awayWinProb + drawProb >= 0.72) {
    let confidence = (awayWinProb + drawProb) * 100
    if (powerAway >= powerHome) confidence += 3
    if (awayForm >= homeForm) confidence += 2
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Visitante ou empate",
      confidence,
      category: "seguranca",
      reason: "boa proteção a favor do visitante"
    }))
  }

  if (homeWinProb >= 0.57 && diff >= 0.20) {
    let confidence = homeWinProb * 100 + 4
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Vitória da casa",
      confidence,
      category: "seguranca",
      reason: "superioridade clara do mandante"
    }))
  }

  if (awayWinProb >= 0.57 && diff <= -0.20) {
    let confidence = awayWinProb * 100 + 4
    confidence -= qualityPenalty

    candidates.push(makePick({
      market: "Vitória do visitante",
      confidence,
      category: "seguranca",
      reason: "superioridade clara do visitante"
    }))
  }

  return candidates
    .filter(pick => pick.confidence >= MIN_CONFIDENCE)
    .sort((a, b) => b.confidence - a.confidence)
}

function isDuplicateMarket(a, b) {
  const x = normalizeText(a.market)
  const y = normalizeText(b.market)

  const groups = [
    ["mais de 7.5 escanteios", "mais de 8.5 escanteios"],
    ["casa mais de 3.5 escanteios", "casa mais de 4.5 escanteios"],
    ["visitante mais de 3.5 escanteios", "visitante mais de 4.5 escanteios"],
    ["mais de 1.5 gols", "mais de 2.5 gols"],
    ["menos de 2.5 gols", "menos de 3.5 gols"],
    ["casa ou empate", "vitoria da casa"],
    ["visitante ou empate", "vitoria do visitante"]
  ]

  return groups.some(group => group.includes(x) && group.includes(y))
}

function selectBestThree(candidates) {
  const selected = []
  const categoryCount = {}

  for (const pick of candidates) {
    if (selected.length >= 3) break

    const duplicate = selected.some(existing => isDuplicateMarket(existing, pick))
    if (duplicate) continue

    const count = categoryCount[pick.category] || 0
    if (count >= 2) continue

    if (selected.length === 1 && pick.category === selected[0].category) {
      const otherCategoryExists = candidates.some(
        item =>
          item.category !== selected[0].category &&
          !selected.some(s => isDuplicateMarket(s, item))
      )

      if (otherCategoryExists) continue
    }

    selected.push(pick)
    categoryCount[pick.category] = count + 1
  }

  return selected
}

function buildAnalysisSummary(homeTeam, awayTeam, insight, picks) {
  const lines = [
    `${homeTeam} x ${awayTeam}`,
    insight
  ]

  if (picks[0]) lines.push(`Principal: ${picks[0].market} (${picks[0].confidence}%)`)
  if (picks[1]) lines.push(`Extra 1: ${picks[1].market} (${picks[1].confidence}%)`)
  if (picks[2]) lines.push(`Extra 2: ${picks[2].market} (${picks[2].confidence}%)`)

  return lines.join(" | ")
}

function buildGameScore(match, mainPick, picks, stats) {
  const weight = leagueWeight(match.league)
  const dataQuality = buildDataQuality(match, stats)
  const varietyBonus = picks.length >= 3 ? 6 : picks.length === 2 ? 3 : 0
  return round(mainPick.confidence + weight + dataQuality * 0.08 + varietyBonus, 2)
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

async function fetchMatchStats(matchIds) {
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

async function saveMatchUpdates(rows) {
  for (const row of rows) {
    const { error } = await supabase
      .from("matches")
      .update({
        pick: row.pick,
        insight: row.insight
      })
      .eq("id", row.id)

    if (error) throw error
  }
}

async function replaceDailyPicks(rows) {
  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (deleteError) throw deleteError

  if (!rows.length) return

  const { error: insertError } = await supabase
    .from("daily_picks")
    .insert(rows)

  if (insertError) throw insertError
}

async function tryReplaceMatchAnalysis(rows, matchIds) {
  try {
    const { error: deleteError } = await supabase
      .from("match_analysis")
      .delete()
      .in("match_id", matchIds)

    if (deleteError) {
      console.warn("Aviso ao limpar match_analysis:", deleteError.message)
      return
    }

    if (!rows.length) return

    const { error: insertError } = await supabase
      .from("match_analysis")
      .insert(rows)

    if (insertError) {
      console.warn("Aviso ao inserir match_analysis:", insertError.message)
    }
  } catch (err) {
    console.warn("Aviso geral em match_analysis:", err.message)
  }
}

async function runBrainV2() {
  console.log("Scoutly Brain V2 iniciado")

  const matches = await fetchTodayMatches()

  if (!matches.length) {
    console.log("Nenhum jogo encontrado para hoje.")
    return
  }

  const matchIds = matches.map(match => match.id)
  const statsMap = await fetchMatchStats(matchIds)

  const analysisRows = []
  const dailyCandidates = []
  const matchUpdates = []

  for (const match of matches) {
    const stats = statsMap.get(match.id) || null
    const candidates = buildCandidates(match, stats)
    const picks = selectBestThree(candidates)

    if (!picks.length) continue

    const insight = buildInsight(match, stats)
    const mainPick = picks[0]
    const summary = buildAnalysisSummary(match.home_team, match.away_team, insight, picks)
    const score = buildGameScore(match, mainPick, picks, stats)

    matchUpdates.push({
      id: match.id,
      pick: mainPick.market,
      insight
    })

    analysisRows.push({
      match_id: match.id,
      created_at: new Date().toISOString(),
      insight,
      main_pick: mainPick.market,
      main_pick_confidence: mainPick.confidence,
      main_pick_level: mainPick.level,
      pick1: picks[0]?.market || null,
      pick1_confidence: picks[0]?.confidence || null,
      pick1_level: picks[0]?.level || null,
      pick2: picks[1]?.market || null,
      pick2_confidence: picks[1]?.confidence || null,
      pick2_level: picks[1]?.level || null,
      pick3: picks[2]?.market || null,
      pick3_confidence: picks[2]?.confidence || null,
      pick3_level: picks[2]?.level || null,
      analysis_summary: summary
    })

    dailyCandidates.push({
      score,
      match_id: match.id,
      home_team: match.home_team,
      away_team: match.away_team,
      league: match.league,
      kickoff: match.kickoff,
      main_pick: mainPick.market,
      confidence: mainPick.confidence,
      level: mainPick.level,
      category: mainPick.category,
      insight
    })
  }

  await saveMatchUpdates(matchUpdates)

  const sorted = dailyCandidates.sort((a, b) => b.score - a.score)

  const categoryCount = {
    gols: 0,
    escanteios: 0,
    seguranca: 0
  }

  const topPicks = []

  for (const item of sorted) {
    if (topPicks.length >= TOP_LIMIT) break

    const limit =
      item.category === "seguranca" ? 1 : 2

    if ((categoryCount[item.category] || 0) >= limit) continue

    const repeatedMarket = topPicks.some(existing =>
      normalizeText(existing.main_pick) === normalizeText(item.main_pick)
    )

    if (repeatedMarket) continue

    topPicks.push(item)
    categoryCount[item.category] = (categoryCount[item.category] || 0) + 1
  }

  const dailyRows = topPicks.map((item, index) => ({
    created_at: new Date().toISOString(),
    rank: index + 1,
    match_id: item.match_id,
    home_team: item.home_team,
    away_team: item.away_team,
    league: item.league,
    market: item.main_pick,
    probability: round(item.confidence / 100, 4),
    is_opportunity: true
  }))

  await replaceDailyPicks(dailyRows)
  await tryReplaceMatchAnalysis(analysisRows, matchIds)

  console.log(`Scoutly Brain V2 finalizado. Jogos analisados: ${analysisRows.length}`)
  console.log(`Top do dia salvo com ${dailyRows.length} picks`)
}

runBrainV2().catch(error => {
  console.error("Erro fatal no Scoutly Brain V2:", error)
  process.exit(1)
})
