const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const TIMEZONE = "America/Sao_Paulo"

function zonedNow() {
  return new Date(
    new Date().toLocaleString("en-US", { timeZone: TIMEZONE })
  )
}

function formatDateKey(dateLike) {
  if (!dateLike) return null
  const d = new Date(dateLike)
  if (Number.isNaN(d.getTime())) return null
  const local = new Date(d.toLocaleString("en-US", { timeZone: TIMEZONE }))
  const y = local.getFullYear()
  const m = String(local.getMonth() + 1).padStart(2, "0")
  const day = String(local.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function safeNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function clamp(n, min = 0, max = 1) {
  return Math.max(min, Math.min(max, n))
}

function round1(n) {
  return Math.round(safeNumber(n) * 10) / 10
}

function round2(n) {
  return Math.round(safeNumber(n) * 100) / 100
}

function pct(n) {
  return round1(safeNumber(n) * 100)
}

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
}

function getKickoff(row) {
  return row.kickoff || row.Kickoff || row.match_date || null
}

function kickoffDate(row) {
  const k = getKickoff(row)
  return k ? new Date(k) : null
}

function kickoffLabel(row) {
  const k = getKickoff(row)
  if (!k) return "Horário não disponível"

  const d = new Date(k)
  if (Number.isNaN(d.getTime())) return "Horário não disponível"

  return d.toLocaleString("pt-BR", {
    timeZone: TIMEZONE,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  })
}

function getLeagueCode(league) {
  const text = normalizeText(league)

  if (text.includes("premier league")) return "EN"
  if (text.includes("la liga")) return "ES"
  if (text.includes("bundesliga")) return "DE"
  if (text === "serie a" || text.includes("serie a")) return "IT"
  if (text.includes("ligue 1")) return "FR"
  if (text.includes("eredi")) return "NL"
  if (text.includes("super lig")) return "TR"
  if (text.includes("liga portugal")) return "PT"
  if (text.includes("brasileirao")) return "BR"
  if (text.includes("argentina") || text.includes("liga profesional")) return "AR"
  return "GL"
}

function leagueTier(league) {
  const text = normalizeText(league)

  const top = [
    "premier league",
    "la liga",
    "bundesliga",
    "serie a",
    "ligue 1",
    "eredivisie",
    "liga portugal",
    "super lig",
    "brasileirao serie a",
    "liga profesional argentina",
  ]

  if (top.some((item) => text.includes(item))) return 1
  return 0
}

function strengthLabel(prob) {
  if (prob >= 0.84) return "Muito forte"
  if (prob >= 0.76) return "Forte"
  if (prob >= 0.67) return "Boa"
  return "Moderada"
}

function marketTone(prob) {
  if (prob >= 0.84) return "Muito forte"
  if (prob >= 0.76) return "Boa"
  if (prob >= 0.67) return "Moderada"
  return "Oportunidade extra"
}

function buildTeamOrDraw(teamName) {
  return `Dupla chance ${teamName} ou empate`
}

function inferPace(totalGoals, totalShots) {
  if (totalGoals >= 2.8 || totalShots >= 24) return "Alto"
  if (totalGoals >= 2.1 || totalShots >= 18) return "Médio"
  return "Baixo"
}

function buildInsight(bestMarket, row, totalGoals, totalCorners, totalShots, favoriteSide) {
  if (bestMarket.includes("Mais de 1.5 gols")) {
    return `A leitura Scoutly vê boa chance de pelo menos 2 gols, com projeção ofensiva de ${round1(totalGoals)} gols no confronto.`
  }

  if (bestMarket.includes("Mais de 2.5 gols")) {
    return `A leitura Scoutly aponta jogo aberto, com volume para ultrapassar a linha de 2.5 gols. A projeção atual é de ${round1(totalGoals)} gols.`
  }

  if (bestMarket.includes("Menos de 2.5 gols")) {
    return `A leitura Scoutly indica confronto mais travado, com tendência de placar controlado e projeção de ${round1(totalGoals)} gols.`
  }

  if (bestMarket.includes("Menos de 3.5 gols")) {
    return `A leitura Scoutly aponta cenário de ritmo controlado, sem necessidade de explosão ofensiva para este jogo. A projeção atual é de ${round1(totalGoals)} gols.`
  }

  if (bestMarket.includes("Ambas marcam")) {
    return `A leitura Scoutly identifica potencial ofensivo dos dois lados, com espaço para os dois times deixarem o seu gol.`
  }

  if (bestMarket.includes("Ambas não marcam")) {
    return `A leitura Scoutly sugere dificuldade para pelo menos um dos lados converter em gol, com cenário mais fechado no placar.`
  }

  if (bestMarket.includes("Mais de 8.5 escanteios")) {
    return `A leitura Scoutly projeta jogo com pressão ofensiva e volume para cantos, com expectativa de ${round1(totalCorners)} escanteios no total.`
  }

  if (bestMarket.includes("Menos de 8.5 escanteios")) {
    return `A leitura Scoutly aponta partida menos acelerada em cantos, com projeção de ${round1(totalCorners)} escanteios no total.`
  }

  if (bestMarket.includes("Dupla chance")) {
    return `A leitura Scoutly vê proteção interessante para o lado ${favoriteSide}, que chega com melhor equilíbrio estatístico para não sair derrotado.`
  }

  return `A leitura Scoutly aponta este mercado como uma das melhores oportunidades estatísticas do confronto.`
}

function extractRecentForm(row) {
  const home = safeNumber(row.home_form, null)
  const away = safeNumber(row.away_form, null)

  if (home === null && away === null) {
    return null
  }

  return {
    home: home === null ? null : round1(home),
    away: away === null ? null : round1(away),
  }
}

function getAnalysisMetrics(match, analysis, stats) {
  const expectedHomeGoals = safeNumber(
    analysis.expected_home_goals,
    safeNumber(match.expected_home_goals, 0)
  )

  const expectedAwayGoals = safeNumber(
    analysis.expected_away_goals,
    safeNumber(match.expected_away_goals, 0)
  )

  const expectedHomeShots = safeNumber(
    analysis.expected_home_shots,
    safeNumber(match.expected_home_shots, 0)
  )

  const expectedAwayShots = safeNumber(
    analysis.expected_away_shots,
    safeNumber(match.expected_away_shots, 0)
  )

  const expectedHomeSot = safeNumber(
    analysis.expected_home_sot,
    safeNumber(match.expected_home_sot, 0)
  )

  const expectedAwaySot = safeNumber(
    analysis.expected_away_sot,
    safeNumber(match.expected_away_sot, 0)
  )

  const expectedCorners = safeNumber(
    analysis.expected_corners,
    safeNumber(match.expected_corners, safeNumber(match.avg_corners, 0))
  )

  const totalGoals = round1(expectedHomeGoals + expectedAwayGoals)
  let totalShots = round1(expectedHomeShots + expectedAwayShots)
  let totalSot = round1(expectedHomeSot + expectedAwaySot)

  if (totalShots <= 0 && stats) {
    totalShots = round1(
      safeNumber(stats.home_shots, 0) + safeNumber(stats.away_shots, 0)
    )
  }

  if (totalSot <= 0 && stats) {
    totalSot = round1(
      safeNumber(stats.home_shots_on_target, 0) +
        safeNumber(stats.away_shots_on_target, 0)
    )
  }

  if (totalShots > 0 && totalSot > totalShots) {
    totalSot = totalShots
  }

  return {
    expectedHomeGoals,
    expectedAwayGoals,
    totalGoals,
    totalShots: totalShots > 0 ? totalShots : round1(safeNumber(match.avg_shots, 0)),
    totalSot,
    expectedCorners: expectedCorners > 0 ? round1(expectedCorners) : round1(safeNumber(match.avg_corners, 0)),
  }
}

function buildCandidates(match, analysis, metrics) {
  const homeTeam = match.home_team
  const awayTeam = match.away_team

  const homeResultProb = safeNumber(
    analysis.home_result_prob ?? analysis.home_win_prob,
    0
  )
  const drawResultProb = safeNumber(
    analysis.draw_result_prob ?? analysis.draw_prob,
    0
  )
  const awayResultProb = safeNumber(
    analysis.away_result_prob ?? analysis.away_win_prob,
    0
  )

  const over15Prob = clamp(safeNumber(analysis.over15_prob, 0))
  const over25Prob = clamp(
    safeNumber(analysis.over25_prob ?? analysis.prob_over25, 0)
  )
  const under25Prob = clamp(safeNumber(analysis.under25_prob, 0))
  const under35Prob = clamp(safeNumber(analysis.under35_prob, 0))
  const bttsProb = clamp(safeNumber(analysis.btts_prob ?? analysis.prob_btts, 0))

  const cornersBase = analysis.corners_over85_prob ?? analysis.prob_corners
  const cornersOver85Prob = clamp(safeNumber(cornersBase, 0))

  const homeDoubleChance = clamp(homeResultProb + drawResultProb)
  const awayDoubleChance = clamp(awayResultProb + drawResultProb)

  const favoriteSide =
    homeDoubleChance >= awayDoubleChance ? homeTeam : awayTeam

  const candidates = [
    {
      market: "Mais de 1.5 gols",
      prob: over15Prob,
      family: "goals-over",
    },
    {
      market: "Mais de 2.5 gols",
      prob: over25Prob,
      family: "goals-over",
    },
    {
      market: "Menos de 2.5 gols",
      prob: under25Prob,
      family: "goals-under",
    },
    {
      market: "Menos de 3.5 gols",
      prob: under35Prob,
      family: "goals-under",
    },
    {
      market: "Ambas marcam",
      prob: bttsProb,
      family: "btts",
    },
    {
      market: "Ambas não marcam",
      prob: clamp(1 - bttsProb),
      family: "btts-no",
    },
    {
      market: "Mais de 8.5 escanteios",
      prob: cornersOver85Prob,
      family: "corners-over",
    },
    {
      market: "Menos de 8.5 escanteios",
      prob: clamp(1 - cornersOver85Prob),
      family: "corners-under",
    },
    {
      market: buildTeamOrDraw(homeTeam),
      prob: homeDoubleChance,
      family: "double-chance",
    },
    {
      market: buildTeamOrDraw(awayTeam),
      prob: awayDoubleChance,
      family: "double-chance",
    },
  ]
    .map((item) => ({
      ...item,
      prob: clamp(item.prob),
      favoriteSide,
      totalGoals: metrics.totalGoals,
      totalCorners: metrics.expectedCorners,
      totalShots: metrics.totalShots,
    }))
    .filter((item) => item.prob > 0.57)

  const unique = []
  const seen = new Set()

  for (const candidate of candidates.sort((a, b) => b.prob - a.prob)) {
    if (seen.has(candidate.market)) continue
    seen.add(candidate.market)
    unique.push(candidate)
  }

  return unique
}

function buildQualityScore(match, analysis, metrics, stats) {
  const leagueBoost = leagueTier(match.league) ? 0.07 : 0.02
  const hasKickoff = getKickoff(match) || getKickoff(analysis) ? 0.03 : 0
  const dataDensity =
    (metrics.totalGoals > 0 ? 0.04 : 0) +
    (metrics.expectedCorners > 0 ? 0.03 : 0) +
    (metrics.totalShots > 0 ? 0.03 : 0) +
    (safeNumber(stats?.home_shots, 0) + safeNumber(stats?.away_shots, 0) > 0 ? 0.03 : 0)

  return leagueBoost + hasKickoff + dataDensity
}

function sortByKickoffAsc(a, b) {
  const da = kickoffDate(a) ? kickoffDate(a).getTime() : Number.MAX_SAFE_INTEGER
  const db = kickoffDate(b) ? kickoffDate(b).getTime() : Number.MAX_SAFE_INTEGER
  return da - db
}

function buildAnalysisUpdate(match, analysis, stats) {
  const metrics = getAnalysisMetrics(match, analysis, stats)
  const candidates = buildCandidates(match, analysis, metrics)

  if (!candidates.length) return null

  const best = candidates[0]
  const topThree = candidates.slice(0, 3)

  const favoriteSide =
    best.market.includes(match.home_team) ? match.home_team :
    best.market.includes(match.away_team) ? match.away_team :
    safeNumber(analysis.home_result_prob ?? analysis.home_win_prob, 0) >=
    safeNumber(analysis.away_result_prob ?? analysis.away_win_prob, 0)
      ? match.home_team
      : match.away_team

  const insight = buildInsight(
    best.market,
    analysis,
    metrics.totalGoals,
    metrics.expectedCorners,
    metrics.totalShots,
    favoriteSide
  )

  const form = extractRecentForm(analysis)

  return {
    matchId: match.id,
    league: match.league,
    kickoff: getKickoff(analysis) || getKickoff(match),
    homeTeam: match.home_team,
    awayTeam: match.away_team,
    homeLogo: match.home_logo || match.home_jogo || null,
    awayLogo: match.away_logo || null,
    bestPick: best.market,
    bestProb: best.prob,
    strength: strengthLabel(best.prob),
    metrics,
    insight,
    form,
    topThree,
    qualityScore: buildQualityScore(match, analysis, metrics, stats),
  }
}

function diversifyTopPicks(analyses, limit = 5) {
  const marketFamilyCount = new Map()
  const chosen = []

  const pool = [...analyses].sort((a, b) => {
    const scoreA = a.bestProb + a.qualityScore
    const scoreB = b.bestProb + b.qualityScore
    return scoreB - scoreA
  })

  for (const item of pool) {
    const family = item.topThree[0]?.family || "other"
    const repeated = marketFamilyCount.get(family) || 0
    const adjustedScore = item.bestProb + item.qualityScore - repeated * 0.08

    item._adjustedScore = adjustedScore
  }

  pool.sort((a, b) => b._adjustedScore - a._adjustedScore)

  for (const item of pool) {
    if (chosen.length >= limit) break
    const family = item.topThree[0]?.family || "other"
    const repeated = marketFamilyCount.get(family) || 0

    if (repeated >= 2 && pool.length > limit) continue

    chosen.push(item)
    marketFamilyCount.set(family, repeated + 1)
  }

  return chosen.slice(0, limit).sort(sortByKickoffAsc)
}

async function fetchMatchesToday() {
  const { data, error } = await supabase
    .from("matches")
    .select("*")
    .order("kickoff", { ascending: true, nullsFirst: false })

  if (error) throw error

  const now = zonedNow()
  const todayKey = formatDateKey(now)

  return (data || []).filter((row) => {
    const rowKey =
      formatDateKey(row.kickoff) ||
      formatDateKey(row.match_date) ||
      formatDateKey(row.created_at)

    if (!rowKey) return false
    if (rowKey !== todayKey) return false

    const k = kickoffDate(row)
    if (!k) return true

    const localKickoff = new Date(k.toLocaleString("en-US", { timeZone: TIMEZONE }))
    return localKickoff.getTime() >= now.getTime() - 3 * 60 * 60 * 1000
  })
}

async function fetchMatchAnalysisByIds(ids) {
  if (!ids.length) return []

  const { data, error } = await supabase
    .from("match_analysis")
    .select("*")
    .in("match_id", ids)

  if (error) throw error
  return data || []
}

async function fetchMatchStatsByIds(ids) {
  if (!ids.length) return []

  const { data, error } = await supabase
    .from("match_stats")
    .select("*")
    .in("match_id", ids)

  if (error) throw error
  return data || []
}

function filterUpdateByExistingColumns(baseUpdate, sampleRow) {
  if (!sampleRow) return baseUpdate

  const allowed = new Set(Object.keys(sampleRow))
  const output = {}

  for (const [key, value] of Object.entries(baseUpdate)) {
    if (allowed.has(key)) {
      output[key] = value
    }
  }

  return output
}

async function updateMatchAnalysisRows(analyses, existingAnalysisRows) {
  const analysisById = new Map(
    existingAnalysisRows.map((row) => [row.match_id, row])
  )

  for (const item of analyses) {
    const original = analysisById.get(item.matchId)
    if (!original) continue

    const baseUpdate = {
      insight: item.insight,
      pick: item.bestPick,
      best_pick_1: item.topThree[0]?.market || null,
      best_pick_2: item.topThree[1]?.market || null,
      best_pick_3: item.topThree[2]?.market || null,
      kickoff: item.kickoff || original.kickoff || null,
    }

    const update = filterUpdateByExistingColumns(baseUpdate, original)

    if (!Object.keys(update).length) continue

    const { error } = await supabase
      .from("match_analysis")
      .update(update)
      .eq("match_id", item.matchId)

    if (error) throw error
  }
}

async function replaceDailyPicks(mainPick, topFive) {
  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .gte("id", 0)

  if (deleteError) throw deleteError

  const rows = []

  if (mainPick) {
    rows.push({
      rank: 0,
      match_id: mainPick.matchId,
      home_team: mainPick.homeTeam,
      away_team: mainPick.awayTeam,
      league: mainPick.league,
      market: mainPick.bestPick,
      probability: round2(mainPick.bestProb),
      is_opportunity: true,
    })
  }

  topFive.forEach((item, index) => {
    rows.push({
      rank: index + 1,
      match_id: item.matchId,
      home_team: item.homeTeam,
      away_team: item.awayTeam,
      league: item.league,
      market: item.bestPick,
      probability: round2(item.bestProb),
      is_opportunity: true,
    })
  })

  if (!rows.length) return

  const { error: insertError } = await supabase
    .from("daily_picks")
    .insert(rows)

  if (insertError) throw insertError
}

async function main() {
  console.log("🧠 Scoutly Brain V2.2 iniciado...")

  const matches = await fetchMatchesToday()
  console.log(`📦 Jogos do dia encontrados: ${matches.length}`)

  if (!matches.length) {
    await replaceDailyPicks(null, [])
    console.log("⚠️ Nenhum jogo válido hoje. Daily picks limpo.")
    return
  }

  const matchIds = matches.map((m) => m.id)
  const [analysisRows, statsRows] = await Promise.all([
    fetchMatchAnalysisByIds(matchIds),
    fetchMatchStatsByIds(matchIds),
  ])

  console.log(`📊 Match analysis carregado: ${analysisRows.length}`)
  console.log(`📈 Match stats carregado: ${statsRows.length}`)

  const analysisByMatchId = new Map(
    analysisRows.map((row) => [row.match_id, row])
  )

  const statsByMatchId = new Map(
    statsRows.map((row) => [row.match_id, row])
  )

  const built = []

  for (const match of matches) {
    const analysis = analysisByMatchId.get(match.id)
    if (!analysis) continue

    const stats = statsByMatchId.get(match.id)
    const builtItem = buildAnalysisUpdate(match, analysis, stats)

    if (!builtItem) continue

    built.push(builtItem)
  }

  console.log(`✅ Análises válidas montadas: ${built.length}`)

  if (!built.length) {
    await replaceDailyPicks(null, [])
    console.log("⚠️ Nenhuma análise forte o suficiente para salvar.")
    return
  }

  built.sort((a, b) => {
    const scoreA = a.bestProb + a.qualityScore
    const scoreB = b.bestProb + b.qualityScore
    return scoreB - scoreA
  })

  const mainPick = built[0]
  const topFive = diversifyTopPicks(
    built.filter((item) => item.matchId !== mainPick.matchId),
    5
  )

  await updateMatchAnalysisRows(built, analysisRows)
  await replaceDailyPicks(mainPick, topFive)

  console.log("🏁 Scoutly Brain V2.2 finalizado com sucesso.")
  console.log("⭐ Dica do dia:", mainPick.homeTeam, "x", mainPick.awayTeam, "-", mainPick.bestPick)
  console.log(
    "🔥 Top 5:",
    topFive.map((x) => `${x.homeTeam} x ${x.awayTeam} (${x.bestPick})`).join(" | ")
  )
}

main().catch((error) => {
  console.error("❌ Erro no Scoutly Brain V2.2:", {
    code: error?.code || null,
    details: error?.details || null,
    hint: error?.hint || null,
    message: error?.message || String(error),
  })
  process.exit(1)
})

