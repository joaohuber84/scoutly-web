const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const TODAY = new Date().toISOString().slice(0, 10)
const MAX_TOP_PICKS = 5
const MIN_CONFIDENCE_FOR_MAIN = 0.58

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round1(value) {
  if (value == null || Number.isNaN(Number(value))) return 0
  return Math.round(Number(value) * 10) / 10
}

function round2(value) {
  if (value == null || Number.isNaN(Number(value))) return 0
  return Math.round(Number(value) * 100) / 100
}

function normalizeLeagueLabel(league) {
  if (!league) return "Competição"
  return String(league).trim()
}

function getCountryCodeFromLeague(league) {
  const l = String(league || "").toLowerCase()

  if (l.includes("premier league")) return "EN"
  if (l.includes("la liga")) return "ES"
  if (l.includes("brasileirão")) return "BR"
  if (l.includes("liga profesional argentina")) return "AR"
  if (l.includes("bundesliga")) return "DE"
  if (l.includes("ligue 1")) return "FR"
  if (l.includes("eredivisie")) return "NL"
  if (l.includes("super lig")) return "TR"
  if (l.includes("serie a")) return "IT"
  if (l.includes("champions")) return "EU"
  if (l.includes("europa league")) return "EU"
  if (l.includes("conference league")) return "EU"

  return "INT"
}

function confidenceLabel(score) {
  if (score >= 0.78) return "Muito forte"
  if (score >= 0.66) return "Boa"
  if (score >= 0.56) return "Moderada"
  return "Oportunidade extra"
}

function offensiveRhythm(totalShots, totalSot, expectedGoals) {
  const score =
    Number(totalShots || 0) * 0.45 +
    Number(totalSot || 0) * 1.1 +
    Number(expectedGoals || 0) * 3.2

  if (score >= 20) return "Muito alto"
  if (score >= 15) return "Alto"
  if (score >= 10) return "Médio"
  return "Baixo"
}

function buildMainInsight(pickName, homeTeam, awayTeam, metrics) {
  const totalGoals = round1(metrics.totalExpectedGoals)
  const totalCorners = round1(metrics.expectedCorners)
  const rhythm = offensiveRhythm(
    metrics.expectedShots,
    metrics.expectedSot,
    metrics.totalExpectedGoals
  )

  if (pickName.includes("MAIS DE 1.5 GOLS")) {
    return `Scoutly vê um confronto com boa tendência ofensiva, projeção de ${totalGoals} gols esperados e cenário favorável para pelo menos 2 gols no jogo.`
  }

  if (pickName.includes("MAIS DE 2.5 GOLS")) {
    return `Scoutly identifica um jogo com ritmo ofensivo ${rhythm.toLowerCase()} e projeção de ${totalGoals} gols esperados, indicando valor na linha de mais de 2.5 gols.`
  }

  if (pickName.includes("MENOS DE 3.5 GOLS")) {
    return `Scoutly enxerga um confronto mais controlado, sem explosão ofensiva, com projeção de ${totalGoals} gols esperados e boa sustentação para menos de 3.5 gols.`
  }

  if (pickName.includes("AMBAS MARCAM")) {
    return `Scoutly detecta equilíbrio ofensivo entre ${homeTeam} e ${awayTeam}, com cenário consistente para gol dos dois lados.`
  }

  if (pickName.includes("AMBAS NÃO MARCAM")) {
    return `Scoutly aponta um cenário de desequilíbrio ofensivo ou baixa produção de um dos lados, deixando ambas não marcam como leitura consistente.`
  }

  if (pickName.includes("ESCANTEIOS")) {
    return `Scoutly projeta ${totalCorners} escanteios para a partida, com volume suficiente para sustentar essa linha de cantos.`
  }

  if (pickName.includes("DUPLA CHANCE")) {
    return `Scoutly enxerga maior proteção de resultado para esse lado, com leitura de equilíbrio controlado e margem estatística favorável à dupla chance.`
  }

  return `Scoutly cruzou gols, escanteios, finalizações e forças relativas do confronto e encontrou valor estatístico nessa leitura.`
}

function buildRecentFormPlaceholder() {
  return {
    home_form: null,
    away_form: null,
    recent_form_note: "Sem histórico recente disponível para este jogo."
  }
}

function chooseMainPick(candidatePicks) {
  const sorted = [...candidatePicks].sort((a, b) => b.score - a.score)
  return sorted[0] || null
}

function dedupePickNames(picks) {
  const seen = new Set()
  const result = []

  for (const pick of picks) {
    const key = pick.name.trim().toUpperCase()
    if (seen.has(key)) continue
    seen.add(key)
    result.push(pick)
  }

  return result
}

function diversifyPicks(allMatches) {
  const usedMarkets = new Map()
  const adjusted = allMatches.map((match) => ({ ...match }))

  for (const match of adjusted) {
    const marketKey = match.main_pick_market
    const alreadyUsed = usedMarkets.get(marketKey) || 0
    const diversityPenalty = alreadyUsed * 0.035
    match.final_score = round2(match.final_score - diversityPenalty)
    usedMarkets.set(marketKey, alreadyUsed + 1)
  }

  const finalList = []
  const marketCount = new Map()

  const sorted = [...adjusted].sort((a, b) => {
    if (b.final_score !== a.final_score) return b.final_score - a.final_score
    return new Date(a.kickoff || 0).getTime() - new Date(b.kickoff || 0).getTime()
  })

  for (const item of sorted) {
    const marketKey = item.main_pick_market
    const count = marketCount.get(marketKey) || 0

    if (count >= 3 && item.final_score < 0.74) continue

    marketCount.set(marketKey, count + 1)
    finalList.push(item)
  }

  return finalList
}

function buildCandidatePicks(row) {
  const homeTeam = row.home_team
  const awayTeam = row.away_team

  const homeStrength = Number(row.home_strength || 0)
  const awayStrength = Number(row.away_strength || 0)

  const homeWinProb = Number(row.home_win_prob || row.home_result_prob || 0)
  const drawProb = Number(row.draw_prob || row.draw_result_prob || 0)
  const awayWinProb = Number(row.away_win_prob || row.away_result_prob || 0)

  const probOver25 = Number(row.prob_over25 || row.over25_prob || 0)
  const probBtts = Number(row.prob_btts || row.btts_prob || 0)
  const probCorners = Number(row.prob_corners || row.corners_over85_prob || 0)

  const expectedHomeGoals = Number(row.expected_home_goals || 0)
  const expectedAwayGoals = Number(row.expected_away_goals || 0)
  const expectedGoals = expectedHomeGoals + expectedAwayGoals
  const expectedCorners = Number(row.expected_corners || 0)

  const goalGap = Math.abs(expectedHomeGoals - expectedAwayGoals)
  const strengthGap = Math.abs(homeStrength - awayStrength)

  const homeDoubleChance = clamp(homeWinProb + drawProb, 0, 0.99)
  const awayDoubleChance = clamp(awayWinProb + drawProb, 0, 0.99)

  const candidates = []

  if (expectedGoals >= 1.55) {
    candidates.push({
      name: "MAIS DE 1.5 GOLS",
      market: "gols",
      score: clamp(0.45 + expectedGoals * 0.18, 0, 0.95)
    })
  }

  if (probOver25 >= 0.54 || expectedGoals >= 2.55) {
    candidates.push({
      name: "MAIS DE 2.5 GOLS",
      market: "gols",
      score: clamp(Math.max(probOver25, 0.35 + expectedGoals * 0.2), 0, 0.95)
    })
  }

  if (expectedGoals <= 3.1) {
    candidates.push({
      name: "MENOS DE 3.5 GOLS",
      market: "gols",
      score: clamp(0.92 - expectedGoals * 0.16, 0, 0.95)
    })
  }

  if (probBtts >= 0.57 && expectedHomeGoals >= 0.8 && expectedAwayGoals >= 0.8) {
    candidates.push({
      name: "AMBAS MARCAM",
      market: "btts",
      score: clamp(probBtts, 0, 0.95)
    })
  }

  if (
    (probBtts <= 0.46 || expectedAwayGoals <= 0.72 || expectedHomeGoals <= 0.72) &&
    expectedGoals <= 3.05
  ) {
    candidates.push({
      name: "AMBAS NÃO MARCAM",
      market: "btts",
      score: clamp(1 - probBtts, 0, 0.95)
    })
  }

  if (expectedCorners >= 6.6) {
    candidates.push({
      name: "MAIS DE 6.5 ESCANTEIOS",
      market: "escanteios",
      score: clamp(Math.max(probCorners, 0.42 + expectedCorners * 0.05), 0, 0.95)
    })
  }

  if (expectedCorners >= 8.6) {
    candidates.push({
      name: "MAIS DE 8.5 ESCANTEIOS",
      market: "escanteios",
      score: clamp(Math.max(probCorners, 0.28 + expectedCorners * 0.06), 0, 0.95)
    })
  }

  if (expectedCorners <= 9.2) {
    candidates.push({
      name: "MENOS DE 10.5 ESCANTEIOS",
      market: "escanteios",
      score: clamp(0.88 - expectedCorners * 0.04, 0, 0.95)
    })
  }

  if (homeDoubleChance >= 0.7 && homeStrength >= awayStrength * 0.9) {
    candidates.push({
      name: `DUPLA CHANCE ${homeTeam.toUpperCase()} OU EMPATE`,
      market: "dupla_chance",
      score: clamp(homeDoubleChance - 0.04 + strengthGap * 0.03, 0, 0.95)
    })
  }

  if (awayDoubleChance >= 0.7 && awayStrength >= homeStrength * 0.9) {
    candidates.push({
      name: `DUPLA CHANCE ${awayTeam.toUpperCase()} OU EMPATE`,
      market: "dupla_chance",
      score: clamp(awayDoubleChance - 0.04 + strengthGap * 0.03, 0, 0.95)
    })
  }

  return dedupePickNames(candidates)
}

function buildMetrics(row) {
  const expectedHomeGoals = Number(row.expected_home_goals || 0)
  const expectedAwayGoals = Number(row.expected_away_goals || 0)
  const expectedHomeShots = Number(row.expected_home_shots || 0)
  const expectedAwayShots = Number(row.expected_away_shots || 0)
  const expectedHomeSot = Number(row.expected_home_sot || 0)
  const expectedAwaySot = Number(row.expected_away_sot || 0)
  const expectedCorners = Number(row.expected_corners || 0)

  return {
    totalExpectedGoals: expectedHomeGoals + expectedAwayGoals,
    expectedCorners,
    expectedShots: expectedHomeShots + expectedAwayShots,
    expectedSot: expectedHomeSot + expectedAwaySot,
    homeExpectedGoals: expectedHomeGoals,
    awayExpectedGoals: expectedAwayGoals,
    homeExpectedShots: expectedHomeShots,
    awayExpectedShots: expectedAwayShots,
    homeExpectedSot: expectedHomeSot,
    awayExpectedSot: expectedAwaySot
  }
}

function buildPickRecord(matchRow) {
  const metrics = buildMetrics(matchRow)
  const candidates = buildCandidatePicks(matchRow)
  const mainPick = chooseMainPick(candidates)

  if (!mainPick || mainPick.score < MIN_CONFIDENCE_FOR_MAIN) return null

  const extras = candidates
    .filter((p) => p.name !== mainPick.name)
    .sort((a, b) => b.score - a.score)
    .slice(0, 2)

  const insight = buildMainInsight(
    mainPick.name,
    matchRow.home_team,
    matchRow.away_team,
    metrics
  )

  const formData = buildRecentFormPlaceholder()

  const mainStrength = round2(mainPick.score)
  const extra1 = extras[0] || null
  const extra2 = extras[1] || null

  const countryCode = getCountryCodeFromLeague(matchRow.league)
  const leagueLabel = normalizeLeagueLabel(matchRow.league)

  const finalScore =
    mainStrength +
    (metrics.totalExpectedGoals >= 2 ? 0.03 : 0) +
    (metrics.expectedCorners >= 8 ? 0.02 : 0) +
    (matchRow.kickoff ? 0.01 : 0)

  return {
    rank: null,
    match_id: matchRow.match_id,
    home_team: matchRow.home_team,
    away_team: matchRow.away_team,
    league: leagueLabel,
    league_code: countryCode,
    kickoff: matchRow.kickoff || null,
    home_logo: matchRow.home_logo || null,
    away_logo: matchRow.away_logo || null,

    main_pick: mainPick.name,
    main_pick_market: mainPick.market,
    main_pick_score: mainStrength,
    main_pick_label: confidenceLabel(mainPick.score),

    pick_2: extra1 ? extra1.name : null,
    pick_2_score: extra1 ? round2(extra1.score) : null,
    pick_2_label: extra1 ? confidenceLabel(extra1.score) : null,

    pick_3: extra2 ? extra2.name : null,
    pick_3_score: extra2 ? round2(extra2.score) : null,
    pick_3_label: extra2 ? confidenceLabel(extra2.score) : null,

    insight,

    expected_goals: round1(metrics.totalExpectedGoals),
    expected_corners: round1(metrics.expectedCorners),
    expected_shots: Math.round(metrics.expectedShots),
    expected_sot: Math.round(metrics.expectedSot),
    offensive_rhythm: offensiveRhythm(
      metrics.expectedShots,
      metrics.expectedSot,
      metrics.totalExpectedGoals
    ),

    home_form: formData.home_form,
    away_form: formData.away_form,
    recent_form_note: formData.recent_form_note,

    final_score: round2(finalScore)
  }
}

async function loadUpcomingMatches() {
  const { data, error } = await supabase
    .from("matches")
    .select(`
      match_id,
      home_team,
      away_team,
      league,
      kickoff,
      home_logo,
      away_logo,
      match_date
    `)
    .gte("match_date", TODAY)
    .order("kickoff", { ascending: true })

  if (error) throw error
  return data || []
}

async function loadMatchAnalysis(matchIds) {
  if (!matchIds.length) return []

  const { data, error } = await supabase
    .from("match_analysis")
    .select(`
      match_id,
      home_strength,
      away_strength,
      home_win_prob,
      draw_prob,
      away_win_prob,
      home_result_prob,
      draw_result_prob,
      away_result_prob,
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
      over25_prob,
      btts_prob,
      corners_over85_prob
    `)
    .in("match_id", matchIds)

  if (error) throw error
  return data || []
}

async function loadUpcomingMatchesWithAnalysis() {
  const matches = await loadUpcomingMatches()
  if (!matches.length) return []

  const matchIds = matches.map((m) => m.match_id)
  const analyses = await loadMatchAnalysis(matchIds)

  const analysisMap = new Map()
  for (const row of analyses) {
    analysisMap.set(row.match_id, row)
  }

  const rows = []
  for (const match of matches) {
    const analysis = analysisMap.get(match.match_id)
    if (!analysis) continue

    rows.push({
      ...match,
      ...analysis
    })
  }

  return rows
}

async function clearTodayDailyPicks() {
  const start = `${TODAY}T00:00:00`
  const end = `${TODAY}T23:59:59`

  const { error } = await supabase
    .from("daily_picks")
    .delete()
    .gte("created_at", start)
    .lte("created_at", end)

  if (error) throw error
}

async function saveDailyPicks(picks) {
  if (!picks.length) return

  const rows = picks.map((pick, index) => ({
    rank: pick.rank,
    match_id: pick.match_id,
    home_team: pick.home_team,
    away_team: pick.away_team,
    league: pick.league,
    league_code: pick.league_code,
    kickoff: pick.kickoff,
    home_logo: pick.home_logo,
    away_logo: pick.away_logo,

    main_pick: pick.main_pick,
    main_pick_market: pick.main_pick_market,
    main_pick_score: pick.main_pick_score,
    main_pick_label: pick.main_pick_label,

    pick_2: pick.pick_2,
    pick_2_score: pick.pick_2_score,
    pick_2_label: pick.pick_2_label,

    pick_3: pick.pick_3,
    pick_3_score: pick.pick_3_score,
    pick_3_label: pick.pick_3_label,

    insight: pick.insight,

    expected_goals: pick.expected_goals,
    expected_corners: pick.expected_corners,
    expected_shots: pick.expected_shots,
    expected_sot: pick.expected_sot,
    offensive_rhythm: pick.offensive_rhythm,

    home_form: pick.home_form,
    away_form: pick.away_form,
    recent_form_note: pick.recent_form_note,

    score: pick.final_score,
    is_featured: index === 0
  }))

  const { error } = await supabase.from("daily_picks").insert(rows)
  if (error) throw error
}

async function runScoutlyBrain() {
  try {
    console.log("🧠 Scoutly Brain V2 iniciado...")

    const matches = await loadUpcomingMatchesWithAnalysis()

    if (!matches.length) {
      console.log("Nenhum jogo com análise disponível para hoje.")
      return
    }

    const built = matches
      .map(buildPickRecord)
      .filter(Boolean)

    if (!built.length) {
      console.log("Nenhuma oportunidade forte encontrada para hoje.")
      return
    }

    const rankedBase = diversifyPicks(built)
      .sort((a, b) => {
        if (b.final_score !== a.final_score) return b.final_score - a.final_score
        return new Date(a.kickoff || 0).getTime() - new Date(b.kickoff || 0).getTime()
      })

    const featured = rankedBase[0]

    const topFive = rankedBase
      .filter((item) => item.match_id !== featured.match_id)
      .sort((a, b) => new Date(a.kickoff || 0).getTime() - new Date(b.kickoff || 0).getTime())
      .slice(0, MAX_TOP_PICKS)

    const finalRows = [
      { ...featured, rank: 0 },
      ...topFive.map((item, idx) => ({ ...item, rank: idx + 1 }))
    ]

    await clearTodayDailyPicks()
    await saveDailyPicks(finalRows)

    console.log(`✅ Scoutly Brain V2 finalizado com sucesso.`)
    console.log(`⭐ Dica do dia: ${featured.home_team} x ${featured.away_team} -> ${featured.main_pick}`)
    console.log(`🔥 Top radar salvo: ${topFive.length} jogos`)
  } catch (error) {
    console.error("❌ Erro no Scoutly Brain V2:", error)
    process.exit(1)
  }
}

runScoutlyBrain()

