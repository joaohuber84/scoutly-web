const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const ALLOWED_LEAGUES = [
  'Premier League',
  'La Liga',
  'Serie A',
  'Bundesliga',
  'Ligue 1',
  'Eredivisie',
  'MLS',

  'UEFA Champions League',
  'UEFA Europa League',
  'UEFA Europa Conference League',
  'CONMEBOL Libertadores',
  'CONMEBOL Sudamericana',
  'CONCACAF Champions Cup',
  'Copa do Brasil',

  'Serie B',
  'Liga Profesional Argentina',

  'FIFA Club World Cup',
  'FIFA World Cup',
  'UEFA European Championship',
  'Copa America',
  'UEFA Nations League'
]

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 6) {
  return Number(Number(value).toFixed(decimals))
}

function logistic(x) {
  return 1 / (1 + Math.exp(-x))
}

function poisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0
  let factorial = 1
  for (let i = 2; i <= k; i++) factorial *= i
  return (Math.exp(-lambda) * Math.pow(lambda, k)) / factorial
}

function cumulativePoisson(lambda, maxK = 10) {
  const arr = []
  let sum = 0

  for (let k = 0; k <= maxK; k++) {
    const p = poisson(lambda, k)
    sum += p
    arr.push(p)
  }

  if (sum < 0.999) arr.push(1 - sum)
  return arr
}

function firstNumeric(obj, keys, fallback = null) {
  for (const key of keys) {
    const value = obj?.[key]
    const num = Number(value)
    if (!Number.isNaN(num) && value !== null && value !== undefined && value !== '') {
      return num
    }
  }
  return fallback
}

function weightedMetric(recent, season, recentWeight = 0.65) {
  const hasRecent = recent !== null && recent !== undefined && !Number.isNaN(Number(recent))
  const hasSeason = season !== null && season !== undefined && !Number.isNaN(Number(season))

  if (hasRecent && hasSeason) {
    return Number(recent) * recentWeight + Number(season) * (1 - recentWeight)
  }
  if (hasRecent) return Number(recent)
  if (hasSeason) return Number(season)
  return null
}

function probabilityToTier(prob) {
  const p = Number(prob || 0)
  if (p >= 0.80) return 'Muito forte'
  if (p >= 0.70) return 'Boa'
  if (p >= 0.60) return 'Moderada'
  return 'Oportunidade extra'
}

function strengthLabel(prob) {
  const p = Number(prob || 0)
  if (p >= 0.80) return 'Leitura muito forte'
  if (p >= 0.70) return 'Leitura boa'
  if (p >= 0.60) return 'Leitura moderada'
  return 'Leitura extra'
}

function familyFromMarket(market) {
  if (!market) return 'outros'
  if (market.includes('escanteios')) return 'escanteios'
  if (market.includes('gols')) return 'gols'
  if (market.includes('Ambas')) return 'ambas'
  if (market.includes('Dupla chance')) return 'dupla_chance'
  if (market.includes('Vitória')) return 'vitoria'
  if (market.includes('Empate')) return 'empate'
  return 'outros'
}

function marketKey(market) {
  if (!market) return 'desconhecido'
  if (market.includes('Dupla chance')) return market
  if (market.includes('Vitória')) return market
  if (market.includes('Ambas')) return market
  if (market.includes('escanteios')) return market
  if (market.includes('gols')) return market
  if (market.includes('Empate')) return 'Empate'
  return market
}

function isUpcomingMatch(match) {
  const raw = match.kickoff || match.match_date
  if (!raw) return false

  const kickoff = new Date(raw)
  if (Number.isNaN(kickoff.getTime())) return false

  const now = new Date()
  return kickoff.getTime() >= now.getTime() - 2 * 60 * 60 * 1000
}

function buildCoreMetrics(match) {
  const recentGoals = firstNumeric(match, [
    'avg_goals_recent',
    'recent_avg_goals',
    'last5_avg_goals'
  ])

  const seasonGoals = firstNumeric(match, [
    'avg_goals',
    'goals_avg',
    'season_avg_goals'
  ], 2.4)

  const recentCorners = firstNumeric(match, [
    'avg_corners_recent',
    'recent_avg_corners',
    'last5_avg_corners'
  ])

  const seasonCorners = firstNumeric(match, [
    'avg_corners',
    'corners_avg',
    'season_avg_corners'
  ], 8.8)

  const recentShots = firstNumeric(match, [
    'avg_shots_recent',
    'recent_avg_shots',
    'last5_avg_shots'
  ])

  const seasonShots = firstNumeric(match, [
    'avg_shots',
    'shots_avg',
    'season_avg_shots'
  ], 21)

  const recentShotsOnTarget = firstNumeric(match, [
    'avg_shots_on_target_recent',
    'recent_avg_shots_on_target',
    'last5_avg_shots_on_target',
    'avg_sot_recent',
    'recent_avg_sot'
  ])

  const seasonShotsOnTarget = firstNumeric(match, [
    'avg_shots_on_target',
    'shots_on_target_avg',
    'season_avg_shots_on_target',
    'avg_sot'
  ], 7)

  const avgGoals = clamp(weightedMetric(recentGoals, seasonGoals, 0.68) ?? 2.4, 0.8, 5.4)
  const avgCorners = clamp(weightedMetric(recentCorners, seasonCorners, 0.65) ?? 8.8, 5.5, 15.5)
  const avgShots = clamp(weightedMetric(recentShots, seasonShots, 0.65) ?? 21, 8, 36)
  const avgShotsOnTarget = clamp(weightedMetric(recentShotsOnTarget, seasonShotsOnTarget, 0.65) ?? 7, 2, 14)

  const powerHome = clamp(firstNumeric(match, ['power_home'], 1), 0.45, 2.25)
  const powerAway = clamp(firstNumeric(match, ['power_away'], 1), 0.45, 2.25)

  return {
    avgGoals,
    avgCorners,
    avgShots,
    avgShotsOnTarget,
    powerHome,
    powerAway
  }
}

function buildExpectedGoals(match) {
  const { avgGoals, powerHome, powerAway } = buildCoreMetrics(match)

  const totalPower = powerHome + powerAway || 2
  let homeXg = avgGoals * (powerHome / totalPower)
  let awayXg = avgGoals * (powerAway / totalPower)

  homeXg *= 1.08
  awayXg *= 0.92

  const totalXg = homeXg + awayXg
  if (totalXg > 0) {
    const factor = avgGoals / totalXg
    homeXg *= factor
    awayXg *= factor
  }

  return {
    homeXg: clamp(homeXg, 0.25, 3.8),
    awayXg: clamp(awayXg, 0.25, 3.8)
  }
}

function buildExpectedCorners(match) {
  const core = buildCoreMetrics(match)
  const blended = core.avgCorners * 0.72 + (6.8 + core.avgShots * 0.10 + core.avgGoals * 0.34) * 0.28
  return clamp(blended, 6.2, 15.5)
}

function buildExpectedShots(match) {
  const core = buildCoreMetrics(match)
  return {
    shots: clamp(core.avgShots, 8, 36),
    shotsOnTarget: clamp(core.avgShotsOnTarget, 2, 14)
  }
}

function offensiveTempo(match) {
  const corners = buildExpectedCorners(match)
  const { shots, shotsOnTarget } = buildExpectedShots(match)

  const raw = shots * 0.5 + shotsOnTarget * 1.1 + corners * 0.45
  if (raw >= 22) return 'Alto'
  if (raw >= 16) return 'Moderado'
  return 'Baixo'
}

function resultProbabilities(homeXg, awayXg, maxGoals = 8) {
  const homeDist = cumulativePoisson(homeXg, maxGoals)
  const awayDist = cumulativePoisson(awayXg, maxGoals)

  let homeWin = 0
  let draw = 0
  let awayWin = 0

  for (let h = 0; h < homeDist.length; h++) {
    for (let a = 0; a < awayDist.length; a++) {
      const p = homeDist[h] * awayDist[a]
      if (h > a) homeWin += p
      else if (h === a) draw += p
      else awayWin += p
    }
  }

  return {
    homeWin: clamp(homeWin, 0, 0.92),
    draw: clamp(draw, 0, 0.90),
    awayWin: clamp(awayWin, 0, 0.92)
  }
}

function totalGoalsProbabilities(homeXg, awayXg, maxGoals = 10) {
  const homeDist = cumulativePoisson(homeXg, maxGoals)
  const awayDist = cumulativePoisson(awayXg, maxGoals)

  let over15 = 0
  let over25 = 0
  let under25 = 0
  let under35 = 0
  let bttsYes = 0
  let bttsNo = 0

  for (let h = 0; h < homeDist.length; h++) {
    for (let a = 0; a < awayDist.length; a++) {
      const p = homeDist[h] * awayDist[a]
      const total = h + a

      if (total >= 2) over15 += p
      if (total >= 3) over25 += p
      if (total <= 2) under25 += p
      if (total <= 3) under35 += p
      if (h >= 1 && a >= 1) bttsYes += p
      if (!(h >= 1 && a >= 1)) bttsNo += p
    }
  }

  return {
    over15: clamp(over15, 0, 0.97),
    over25: clamp(over25, 0, 0.95),
    under25: clamp(under25, 0, 0.95),
    under35: clamp(under35, 0, 0.97),
    bttsYes: clamp(bttsYes, 0, 0.94),
    bttsNo: clamp(bttsNo, 0, 0.94)
  }
}

function lineScore(prob, family, market) {
  const p = Number(prob || 0)
  let score = p

  if (market === 'Mais de 1.5 gols') score += 0.06
  if (market === 'Menos de 3.5 gols') score += 0.05
  if (family === 'dupla_chance') score += 0.04
  if (family === 'escanteios') score += 0.02
  if (market === 'Empate') score -= 0.12
  if (market === 'Ambas marcam') score -= 0.02
  if (market === 'Ambas não marcam') score += 0.01

  if (p < 0.58) score -= 0.12
  if (p > 0.90) score -= 0.06

  return score
}

function chooseGoalMarkets(goalProbs) {
  return [
    { market: 'Mais de 1.5 gols', probability: goalProbs.over15 },
    { market: 'Mais de 2.5 gols', probability: goalProbs.over25 },
    { market: 'Menos de 2.5 gols', probability: goalProbs.under25 },
    { market: 'Menos de 3.5 gols', probability: goalProbs.under35 }
  ].map(item => ({
    ...item,
    family: 'gols',
    score: lineScore(item.probability, 'gols', item.market)
  }))
}

function chooseCornerMarkets(expectedCorners) {
  const lines = [6.5, 7.5, 8.5, 9.5, 10.5, 11.5]
  const candidates = []

  for (const line of lines) {
    const overProb = clamp(logistic((expectedCorners - line) / 1.12), 0.08, 0.92)
    const underProb = clamp(1 - overProb, 0.08, 0.92)

    candidates.push({
      market: `Mais de ${line} escanteios`,
      probability: overProb,
      family: 'escanteios',
      score: lineScore(overProb, 'escanteios', `Mais de ${line} escanteios`)
    })

    candidates.push({
      market: `Menos de ${line} escanteios`,
      probability: underProb,
      family: 'escanteios',
      score: lineScore(underProb, 'escanteios', `Menos de ${line} escanteios`)
    })
  }

  candidates.sort((a, b) => b.score - a.score)
  return candidates[0]
}

function buildMarketBoard(match) {
  const { homeXg, awayXg } = buildExpectedGoals(match)
  const results = resultProbabilities(homeXg, awayXg)
  const goals = totalGoalsProbabilities(homeXg, awayXg)
  const expectedCorners = buildExpectedCorners(match)

  const homeTeam = match.home_team || 'Mandante'
  const awayTeam = match.away_team || 'Visitante'

  const candidates = [
    {
      market: `Vitória do ${homeTeam}`,
      probability: results.homeWin,
      family: 'vitoria'
    },
    {
      market: `Vitória do ${awayTeam}`,
      probability: results.awayWin,
      family: 'vitoria'
    },
    {
      market: 'Empate',
      probability: results.draw,
      family: 'empate'
    },
    {
      market: `Dupla chance ${homeTeam} ou empate`,
      probability: clamp(results.homeWin + results.draw, 0, 0.97),
      family: 'dupla_chance'
    },
    {
      market: `Dupla chance ${awayTeam} ou empate`,
      probability: clamp(results.awayWin + results.draw, 0, 0.97),
      family: 'dupla_chance'
    },
    {
      market: 'Ambas marcam',
      probability: goals.bttsYes,
      family: 'ambas'
    },
    {
      market: 'Ambas não marcam',
      probability: goals.bttsNo,
      family: 'ambas'
    },
    ...chooseGoalMarkets(goals),
    chooseCornerMarkets(expectedCorners)
  ]
    .filter(Boolean)
    .map(item => ({
      ...item,
      probability: clamp(item.probability, 0.05, 0.97),
      score: item.score ?? lineScore(item.probability, item.family, item.market),
      tier: probabilityToTier(item.probability),
      strength_label: strengthLabel(item.probability)
    }))
    .sort((a, b) => b.score - a.score)

  const usedFamilies = new Set()
  const topDistinct = []

  for (const item of candidates) {
    if (topDistinct.length >= 4) break
    if (usedFamilies.has(item.family)) continue
    usedFamilies.add(item.family)
    topDistinct.push(item)
  }

  const tip = topDistinct[0] || {
    market: 'Mercado em revisão',
    probability: 0.60,
    family: 'outros',
    score: 0.60,
    tier: 'Moderada',
    strength_label: 'Leitura moderada'
  }

  return {
    results,
    goals,
    expectedCorners,
    board: candidates,
    topDistinct,
    tip
  }
}

function buildInsight(match, board) {
  const expectedCorners = round(board.expectedCorners, 1)
  const { shots, shotsOnTarget } = buildExpectedShots(match)
  const { avgGoals } = buildCoreMetrics(match)
  const tempo = offensiveTempo(match)

  if (board.tip.family === 'escanteios') {
    return `Confronto com ritmo ${tempo.toLowerCase()} e projeção de ${expectedCorners} escanteios, favorecendo essa linha como melhor oportunidade do jogo.`
  }

  if (board.tip.market === 'Mais de 1.5 gols') {
    return `Jogo com produção ofensiva suficiente para pelo menos 2 gols. A combinação de ${round(avgGoals, 1)} gols esperados, ${round(shots, 0)} finalizações e ${round(shotsOnTarget, 0)} no alvo sustenta essa leitura.`
  }

  if (board.tip.market === 'Menos de 3.5 gols' || board.tip.market === 'Menos de 2.5 gols') {
    return `Leitura de confronto mais controlado, com tendência de placar sem explosão ofensiva. O modelo vê esse mercado como o cenário mais consistente.`
  }

  if (board.tip.family === 'dupla_chance') {
    return `Mercado mais protegido para este confronto. O modelo identifica vantagem estatística suficiente para a equipe recomendada sair ao menos com o empate.`
  }

  if (board.tip.family === 'vitoria') {
    return `O modelo identifica superioridade estatística da equipe recomendada, transformando a vitória seca no melhor mercado do jogo.`
  }

  if (board.tip.family === 'ambas') {
    return `A leitura do modelo sugere cenário claro para o mercado de ambas, com base no equilíbrio ofensivo e no padrão recente das equipes.`
  }

  return `O modelo combinou produção ofensiva, forma recente e equilíbrio do confronto para destacar este mercado como a melhor leitura da partida.`
}

async function loadAllMatches() {
  let page = 0
  const pageSize = 500
  const all = []

  while (true) {
    const from = page * pageSize
    const to = from + pageSize - 1

    const { data, error } = await supabase
      .from('matches')
      .select('*')
      .range(from, to)

    if (error) throw error
    if (!data || !data.length) break

    all.push(...data)
    if (data.length < pageSize) break
    page += 1
  }

  return all
}

async function updateMatchesBrain() {
  const matches = await loadAllMatches()
  const filtered = matches.filter(
    match => ALLOWED_LEAGUES.includes(match.league) && isUpcomingMatch(match)
  )

  console.log(`Jogos futuros nas ligas aprovadas: ${filtered.length}`)

  for (const match of filtered) {
    const board = buildMarketBoard(match)
    const payload = {
      home_result_prob: round(board.results.homeWin),
      draw_result_prob: round(board.results.draw),
      away_result_prob: round(board.results.awayWin),

      home_win_prob: round(board.results.homeWin),
      draw_prob: round(board.results.draw),
      away_win_prob: round(board.results.awayWin),

      over15_prob: round(board.goals.over15),
      over25_prob: round(board.goals.over25),
      under25_prob: round(board.goals.under25),
      under35_prob: round(board.goals.under35),
      btts_prob: round(board.goals.bttsYes),

      avg_corners: round(board.expectedCorners, 2),

      pick: board.tip.market,
      insight: buildInsight(match, board),
      strength_label: board.tip.strength_label
    }

    const { error } = await supabase
      .from('matches')
      .update(payload)
      .eq('id', match.id)

    if (error) {
      console.error(`Erro ao atualizar match ${match.id}: ${error.message}`)
    }
  }
}

async function rebuildDailyPicks() {
  const { error: deleteError } = await supabase
    .from('daily_picks')
    .delete()
    .gte('rank', 1)

  if (deleteError) throw deleteError

  const { data: matches, error } = await supabase
    .from('matches')
    .select('*')

  if (error) throw error

  const filtered = (matches || []).filter(
    match => ALLOWED_LEAGUES.includes(match.league) && isUpcomingMatch(match)
  )

  const candidates = []

  for (const match of filtered) {
    const board = buildMarketBoard(match)

    board.topDistinct.forEach((item, index) => {
      candidates.push({
        match_id: match.id,
        home_team: match.home_team,
        away_team: match.away_team,
        league: match.league,
        market: item.market,
        probability: round(clamp(item.probability, 0.55, 0.92)),
        score: item.score - index * 0.04,
        family: item.family,
        market_key: marketKey(item.market)
      })
    })
  }

  candidates.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score
    if (b.probability !== a.probability) return b.probability - a.probability
    return String(a.league).localeCompare(String(b.league))
  })

  const selected = []
  const usedMatches = new Set()
  const leagueCount = {}
  const marketCount = {}

  function tryPick(candidate, strict = true) {
    if (usedMatches.has(candidate.match_id)) return false

    const league = candidate.league || 'Outras'
    const key = candidate.market_key || candidate.market || 'Mercado'

    if (strict) {
      if ((leagueCount[league] || 0) >= 2) return false
      if ((marketCount[key] || 0) >= 2) return false
    } else {
      if ((leagueCount[league] || 0) >= 3) return false
      if ((marketCount[key] || 0) >= 3) return false
    }

    selected.push(candidate)
    usedMatches.add(candidate.match_id)
    leagueCount[league] = (leagueCount[league] || 0) + 1
    marketCount[key] = (marketCount[key] || 0) + 1
    return true
  }

  for (const candidate of candidates) {
    if (selected.length >= 6) break
    tryPick(candidate, true)
  }

  for (const candidate of candidates) {
    if (selected.length >= 6) break
    tryPick(candidate, false)
  }

  const rows = selected
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score
      return b.probability - a.probability
    })
    .slice(0, 6)
    .map((item, index) => ({
      rank: index + 1,
      match_id: item.match_id,
      home_team: item.home_team,
      away_team: item.away_team,
      league: item.league,
      market: item.market,
      probability: item.probability,
      is_opportunity: index === 0
    }))

  if (!rows.length) {
    console.log('Nenhuma daily pick encontrada.')
    return
  }

  const { error: insertError } = await supabase
    .from('daily_picks')
    .insert(rows)

  if (insertError) throw insertError

  console.log('Daily picks recriadas com sucesso.')
}

async function run() {
  try {
    console.log('Iniciando Scoutly Brain V6...')
    await updateMatchesBrain()
    await rebuildDailyPicks()
    console.log('Scoutly Brain V6 finalizado com sucesso.')
  } catch (error) {
    console.error('Erro no Scoutly Brain V6:', error)
    process.exit(1)
  }
}

run()
