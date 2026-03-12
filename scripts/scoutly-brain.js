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
  'UEFA Champions League',
  'UEFA Europa League',
  'UEFA Europa Conference League',
  'CONMEBOL Libertadores',
  'CONMEBOL Sudamericana',
  'Copa do Brasil',
  'Serie B',
  'Liga Profesional Argentina',
  'Eredivisie',
  'Super Lig',
  'Süper Lig',
  'Pro League',
  'Saudi Pro League',
  'Primera Division',
  'Primera División',
  'Coppa Italia',
  'Copa del Rey',
  'CONCACAF Champions Cup',
  'AFC Champions League',
  'AFC Champions League Elite'
]

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 6) {
  return Number(Number(value).toFixed(decimals))
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

function logistic(x) {
  return 1 / (1 + Math.exp(-x))
}

function probabilityToStrength(prob) {
  const p = Number(prob || 0)
  if (p >= 0.80) return 'Leitura forte'
  if (p >= 0.68) return 'Leitura boa'
  if (p >= 0.58) return 'Leitura moderada'
  return 'Leitura fraca'
}

function isUpcomingMatch(match) {
  const raw = match.kickoff || match.match_date
  if (!raw) return false

  const kickoff = new Date(raw)
  if (Number.isNaN(kickoff.getTime())) return false

  const now = new Date()
  return kickoff.getTime() >= now.getTime() - 2 * 60 * 60 * 1000
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
  let btts = 0

  for (let h = 0; h < homeDist.length; h++) {
    for (let a = 0; a < awayDist.length; a++) {
      const p = homeDist[h] * awayDist[a]
      const total = h + a

      if (total >= 2) over15 += p
      if (total >= 3) over25 += p
      if (total <= 2) under25 += p
      if (total <= 3) under35 += p
      if (h >= 1 && a >= 1) btts += p
    }
  }

  return {
    over15: clamp(over15, 0, 0.96),
    over25: clamp(over25, 0, 0.94),
    under25: clamp(under25, 0, 0.94),
    under35: clamp(under35, 0, 0.96),
    btts: clamp(btts, 0, 0.93)
  }
}

function buildExpectedGoals(match) {
  const avgGoals = clamp(Number(match.avg_goals) || 2.4, 0.8, 5.2)
  const powerHome = clamp(Number(match.power_home) || 1, 0.45, 2.2)
  const powerAway = clamp(Number(match.power_away) || 1, 0.45, 2.2)

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

function projectedCorners(match) {
  const existingCorners = Number(match.avg_corners)
  const avgShots = clamp(Number(match.avg_shots) || 19, 8, 34)
  const avgGoals = clamp(Number(match.avg_goals) || 2.4, 0.8, 5.2)

  if (!Number.isNaN(existingCorners) && existingCorners >= 7 && existingCorners <= 14.5) {
    const blend = existingCorners * 0.7 + (7.2 + avgShots * 0.11 + avgGoals * 0.35) * 0.3
    return clamp(blend, 6.8, 14.8)
  }

  const projection = 7.2 + avgShots * 0.11 + avgGoals * 0.35
  return clamp(projection, 6.8, 14.8)
}

function cornersCandidateProb(projection, line, type) {
  const spread = 1.15
  const diff = projection - line
  const over = clamp(logistic(diff / spread), 0.08, 0.92)
  const under = clamp(1 - over, 0.08, 0.92)
  return type === 'over' ? over : under
}

function marketWeight(market, family) {
  if (market === 'Mais de 1.5 gols') return 1.00
  if (market === 'Menos de 3.5 gols') return 0.99
  if (family === 'dupla_chance') return 0.97
  if (family === 'escanteios') return 0.95
  if (market === 'Ambas marcam') return 0.91
  if (family === 'resultado') {
    if (market === 'Empate') return 0.72
    return 0.87
  }
  if (market === 'Mais de 2.5 gols') return 0.90
  if (market === 'Menos de 2.5 gols') return 0.92
  return 0.88
}

function marketClarityBonus(market, probability) {
  let bonus = 0

  if (market === 'Mais de 1.5 gols' && probability >= 0.70) bonus += 0.06
  if (market === 'Menos de 3.5 gols' && probability >= 0.68) bonus += 0.05
  if (market.includes(' ou empate') && probability >= 0.70) bonus += 0.04
  if (market.includes('escanteios') && probability >= 0.67) bonus += 0.03
  if (market === 'Empate') bonus -= 0.10

  return bonus
}

function marketRiskPenalty(market, probability) {
  let penalty = 0

  if (probability < 0.57) penalty += 0.20
  if (probability > 0.89) penalty += 0.10

  if (market === 'Mais de 2.5 gols' && probability < 0.66) penalty += 0.05
  if (market === 'Empate') penalty += 0.08

  return penalty
}

function marketScore(item) {
  const base = Number(item.probability || 0)
  const weight = marketWeight(item.market, item.family)
  const clarity = marketClarityBonus(item.market, base)
  const risk = marketRiskPenalty(item.market, base)

  return base * weight + clarity - risk
}

function buildMarketBoard(match) {
  const { homeXg, awayXg } = buildExpectedGoals(match)
  const result = resultProbabilities(homeXg, awayXg)
  const totals = totalGoalsProbabilities(homeXg, awayXg)
  const cornersProjection = projectedCorners(match)

  const homeOrDraw = clamp(result.homeWin + result.draw, 0, 0.97)
  const awayOrDraw = clamp(result.awayWin + result.draw, 0, 0.97)

  const cornerLines = [7.5, 8.5, 9.5, 10.5]
  const board = [
    { market: 'Vitória mandante', probability: result.homeWin, family: 'resultado' },
    { market: 'Empate', probability: result.draw, family: 'resultado' },
    { market: 'Vitória visitante', probability: result.awayWin, family: 'resultado' },

    { market: `${match.home_team} ou empate`, probability: homeOrDraw, family: 'dupla_chance' },
    { market: `${match.away_team} ou empate`, probability: awayOrDraw, family: 'dupla_chance' },

    { market: 'Mais de 1.5 gols', probability: totals.over15, family: 'gols' },
    { market: 'Mais de 2.5 gols', probability: totals.over25, family: 'gols' },
    { market: 'Menos de 2.5 gols', probability: totals.under25, family: 'gols' },
    { market: 'Menos de 3.5 gols', probability: totals.under35, family: 'gols' },

    { market: 'Ambas marcam', probability: totals.btts, family: 'ambas' }
  ]

  for (const line of cornerLines) {
    board.push({
      market: `Mais de ${line} escanteios`,
      probability: cornersCandidateProb(cornersProjection, line, 'over'),
      family: 'escanteios'
    })
    board.push({
      market: `Menos de ${line} escanteios`,
      probability: cornersCandidateProb(cornersProjection, line, 'under'),
      family: 'escanteios'
    })
  }

  const enriched = board
    .filter(item => item.market && item.probability !== null && item.probability !== undefined)
    .map(item => ({
      ...item,
      probability: clamp(item.probability, 0.05, 0.97),
      strength_label: probabilityToStrength(item.probability),
      score: marketScore(item)
    }))
    .sort((a, b) => b.score - a.score)

  const bestPick = enriched[0] || {
    market: 'Mercado em revisão',
    probability: 0.60,
    family: 'resultado',
    strength_label: 'Leitura moderada',
    score: 0.50
  }

  const topThree = enriched.slice(0, 3)

  return {
    result,
    totals,
    cornersProjection,
    board: enriched,
    bestPick,
    topThree
  }
}

function buildInsight(bestPick, computed, match) {
  const avgGoals = round(Number(match.avg_goals) || 0, 2)
  const projected = round(computed.cornersProjection || 0, 2)

  if (bestPick.market === 'Mais de 1.5 gols') {
    return `Leitura favorável para pelo menos 2 gols. O modelo vê essa linha como mais sólida do que cortes mais agressivos.`
  }

  if (bestPick.market === 'Mais de 2.5 gols') {
    return `Leitura ofensiva positiva, mas exigindo um jogo mais aberto para atingir 3 gols.`
  }

  if (bestPick.market === 'Menos de 2.5 gols') {
    return `Leitura de confronto mais travado, com menos espaço para um placar alto.`
  }

  if (bestPick.market === 'Menos de 3.5 gols') {
    return `Leitura de placar controlado, sem necessidade de um jogo explosivo para bater.`
  }

  if (bestPick.market === 'Ambas marcam') {
    return `Leitura de gol para os dois lados, sustentada por ataque suficiente das duas equipes.`
  }

  if (bestPick.family === 'dupla_chance') {
    return `Leitura mais protegida para resultado, priorizando consistência em vez de depender de vitória seca.`
  }

  if (bestPick.family === 'escanteios') {
    return `Projeção de aproximadamente ${projected} escanteios no jogo, usada para definir a linha mais coerente.`
  }

  if (bestPick.market === 'Vitória mandante') {
    return `Leitura de força superior do mandante no confronto.`
  }

  if (bestPick.market === 'Vitória visitante') {
    return `Leitura de força superior do visitante no confronto.`
  }

  if (bestPick.market === 'Empate') {
    return `Leitura de confronto equilibrado, com chance real de divisão de pontos.`
  }

  if (avgGoals > 0) {
    return `Leitura baseada na média de ${avgGoals} gols do confronto e nos pesos ofensivos do modelo.`
  }

  return `Leitura do modelo baseada nos dados ofensivos e no equilíbrio do confronto.`
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
  const allMatches = await loadAllMatches()

  const filteredMatches = allMatches.filter(
    match => ALLOWED_LEAGUES.includes(match.league) && isUpcomingMatch(match)
  )

  console.log(`Jogos futuros nas ligas aprovadas: ${filteredMatches.length}`)

  for (const match of filteredMatches) {
    const computed = buildMarketBoard(match)
    const bestPick = computed.bestPick

    const payload = {
      home_result_prob: round(computed.result.homeWin),
      draw_result_prob: round(computed.result.draw),
      away_result_prob: round(computed.result.awayWin),

      home_win_prob: round(computed.result.homeWin),
      draw_prob: round(computed.result.draw),
      away_win_prob: round(computed.result.awayWin),

      over15_prob: round(computed.totals.over15),
      over25_prob: round(computed.totals.over25),
      under25_prob: round(computed.totals.under25),
      under35_prob: round(computed.totals.under35),
      btts_prob: round(computed.totals.btts),

      avg_corners: round(computed.cornersProjection, 2),

      pick: bestPick.market || 'Mercado em revisão',
      insight: buildInsight(bestPick, computed, match),
      strength_label: bestPick.strength_label || 'Leitura moderada'
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

function marketExactKey(market) {
  if (!market) return 'desconhecido'

  if (market.includes(' ou empate')) return 'dupla_chance'
  if (market.includes('escanteios')) return market
  if (market.includes('Mais de 1.5 gols')) return 'mais_1_5_gols'
  if (market.includes('Mais de 2.5 gols')) return 'mais_2_5_gols'
  if (market.includes('Menos de 2.5 gols')) return 'menos_2_5_gols'
  if (market.includes('Menos de 3.5 gols')) return 'menos_3_5_gols'
  if (market.includes('Ambas marcam')) return 'ambas_marcam'
  if (market.includes('Vitória mandante')) return 'vitoria_mandante'
  if (market.includes('Vitória visitante')) return 'vitoria_visitante'
  if (market.includes('Empate')) return 'empate'

  return market
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

  const upcoming = (matches || []).filter(
    match => ALLOWED_LEAGUES.includes(match.league) && isUpcomingMatch(match)
  )

  const candidates = []

  for (const match of upcoming) {
    const computed = buildMarketBoard(match)

    computed.topThree.forEach((market, idx) => {
      candidates.push({
        match_id: match.id,
        home_team: match.home_team,
        away_team: match.away_team,
        league: match.league,
        market: market.market,
        family: market.family,
        probability: round(clamp(market.probability, 0.55, 0.88)),
        base_score: market.score,
        adjusted_score: market.score - idx * 0.035,
        market_key: marketExactKey(market.market)
      })
    })
  }

  candidates.sort((a, b) => {
    if (b.adjusted_score !== a.adjusted_score) return b.adjusted_score - a.adjusted_score
    if (b.probability !== a.probability) return b.probability - a.probability
    return String(a.league).localeCompare(String(b.league))
  })

  const selected = []
  const usedMatches = new Set()
  const familyCount = {}
  const exactMarketCount = {}

  function trySelect(candidate, strict = true) {
    if (usedMatches.has(candidate.match_id)) return false

    const family = candidate.family || 'outros'
    const exact = candidate.market_key || 'outros'

    if (strict) {
      if ((familyCount[family] || 0) >= 2) return false
      if ((exactMarketCount[exact] || 0) >= 2) return false
    } else {
      if ((exactMarketCount[exact] || 0) >= 3) return false
    }

    selected.push(candidate)
    usedMatches.add(candidate.match_id)
    familyCount[family] = (familyCount[family] || 0) + 1
    exactMarketCount[exact] = (exactMarketCount[exact] || 0) + 1
    return true
  }

  for (const candidate of candidates) {
    if (selected.length >= 6) break
    trySelect(candidate, true)
  }

  for (const candidate of candidates) {
    if (selected.length >= 6) break
    trySelect(candidate, false)
  }

  const rows = selected
    .sort((a, b) => {
      if (b.adjusted_score !== a.adjusted_score) return b.adjusted_score - a.adjusted_score
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
    console.log('Iniciando Scoutly Brain V5.1...')
    await updateMatchesBrain()
    await rebuildDailyPicks()
    console.log('Scoutly Brain V5.1 finalizado com sucesso.')
  } catch (error) {
    console.error('Erro no Scoutly Brain V5.1:', error)
    process.exit(1)
  }
}

run()


