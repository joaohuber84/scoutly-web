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
    draw: clamp(draw, 0, 0.92),
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

function normalizeFormLabel(originalText, numericValue) {
  if (typeof originalText === 'string' && originalText.trim()) {
    const t = originalText.trim().toLowerCase()
    if (t.includes('very strong')) return 'Muito forte'
    if (t.includes('strong')) return 'Forte'
    if (t.includes('average')) return 'Equilibrado'
    if (t.includes('weak')) return 'Fraco'
    return originalText
  }

  const n = Number(numericValue) || 1
  if (n >= 1.22) return 'Muito forte'
  if (n >= 1.05) return 'Forte'
  if (n >= 0.90) return 'Equilibrado'
  return 'Fraco'
}

function buildExpectedGoals(match) {
  const avgGoals = clamp(Number(match.avg_goals) || 2.4, 0.8, 5.5)
  const powerHome = clamp(Number(match.power_home) || 1, 0.45, 2.2)
  const powerAway = clamp(Number(match.power_away) || 1, 0.45, 2.2)

  const sumPower = powerHome + powerAway || 2
  let homeXg = avgGoals * (powerHome / sumPower)
  let awayXg = avgGoals * (powerAway / sumPower)

  homeXg *= 1.08
  awayXg *= 0.92

  const total = homeXg + awayXg
  if (total > 0) {
    const factor = avgGoals / total
    homeXg *= factor
    awayXg *= factor
  }

  return {
    homeXg: clamp(homeXg, 0.25, 3.8),
    awayXg: clamp(awayXg, 0.25, 3.8)
  }
}

function projectedCorners(match) {
  const existing = Number(match.avg_corners)
  if (!Number.isNaN(existing) && existing > 7) {
    return clamp(existing, 6, 16)
  }

  const shots = clamp(Number(match.avg_shots) || 18, 8, 36)
  const goals = clamp(Number(match.avg_goals) || 2.4, 0.8, 5.5)

  // projeção simples melhorada, mais próxima da realidade de jogo inteiro
  const projection = 4.5 + shots * 0.18 + goals * 0.55
  return clamp(projection, 6.5, 15.5)
}

function cornersLines(projected) {
  return [
    { line: 8.5, overProb: null, underProb: null },
    { line: 9.5, overProb: null, underProb: null },
    { line: 10.5, overProb: null, underProb: null }
  ].map(item => {
    let over = 0
    const lambda = clamp(projected, 3, 18)
    const start = Math.floor(item.line) + 1

    for (let k = start; k <= 25; k++) {
      over += poisson(lambda, k)
    }

    over = clamp(over, 0.05, 0.93)
    const under = clamp(1 - over, 0.05, 0.93)

    return {
      line: item.line,
      overProb: over,
      underProb: under
    }
  })
}

function probabilityToStrength(prob) {
  const p = Number(prob) || 0
  if (p >= 0.80) return 'Leitura forte'
  if (p >= 0.68) return 'Leitura boa'
  if (p >= 0.58) return 'Leitura moderada'
  return 'Leitura fraca'
}

function buildMarkets(match) {
  const { homeXg, awayXg } = buildExpectedGoals(match)
  const result = resultProbabilities(homeXg, awayXg)
  const totals = totalGoalsProbabilities(homeXg, awayXg)
  const cornersProjection = projectedCorners(match)
  const cornerOptions = cornersLines(cornersProjection)

  const markets = [
    { key: 'Vitória mandante', prob: result.homeWin, family: 'resultado' },
    { key: 'Empate', prob: result.draw, family: 'resultado' },
    { key: 'Vitória visitante', prob: result.awayWin, family: 'resultado' },

    { key: 'Mais de 1.5 gols', prob: totals.over15, family: 'gols' },
    { key: 'Mais de 2.5 gols', prob: totals.over25, family: 'gols' },
    { key: 'Menos de 2.5 gols', prob: totals.under25, family: 'gols' },
    { key: 'Menos de 3.5 gols', prob: totals.under35, family: 'gols' },

    { key: 'Ambas marcam', prob: totals.btts, family: 'ambas' }
  ]

  for (const c of cornerOptions) {
    markets.push({
      key: `Mais de ${c.line} escanteios`,
      prob: c.overProb,
      family: 'escanteios'
    })
    markets.push({
      key: `Menos de ${c.line} escanteios`,
      prob: c.underProb,
      family: 'escanteios'
    })
  }

  const best = [...markets].sort((a, b) => b.prob - a.prob)[0]

  return {
    homeXg,
    awayXg,
    result,
    totals,
    cornersProjection,
    cornerOptions,
    markets,
    best
  }
}

function buildInsight(bestMarket, match, computed) {
  const avgGoals = round(Number(match.avg_goals) || 0, 2)
  const avgCorners = round(computed.cornersProjection || match.avg_corners || 0, 2)
  const avgShots = round(Number(match.avg_shots) || 0, 2)

  if (bestMarket === 'Mais de 1.5 gols') {
    return `Leitura favorável para ao menos 2 gols, sustentada por média de ${avgGoals} gols e ${avgShots} finalizações.`
  }

  if (bestMarket === 'Mais de 2.5 gols') {
    return `Leitura ofensiva boa, mas exigindo um jogo mais aberto para chegar a 3 gols.`
  }

  if (bestMarket === 'Menos de 2.5 gols') {
    return `Leitura de jogo mais controlado, com menor espaço para placar alto.`
  }

  if (bestMarket === 'Menos de 3.5 gols') {
    return `Leitura de placar contido, com baixa necessidade de explosão ofensiva.`
  }

  if (bestMarket === 'Ambas marcam') {
    return `Leitura de equilíbrio ofensivo entre os lados, com espaço para gol dos dois times.`
  }

  if (bestMarket.includes('escanteios')) {
    return `Projeção de ${avgCorners} escanteios no jogo, usada para definir a melhor linha de escanteios.`
  }

  if (bestMarket === 'Vitória mandante') {
    return `Leitura favorável ao mandante pela força relativa e vantagem de mando.`
  }

  if (bestMarket === 'Vitória visitante') {
    return `Leitura favorável ao visitante pela força relativa superior no confronto.`
  }

  if (bestMarket === 'Empate') {
    return `Leitura de confronto equilibrado, com forças próximas e tendência a jogo parelho.`
  }

  return 'Leitura favorável de acordo com o modelo estatístico do confronto.'
}

function radarProbability(prob) {
  return clamp(prob, 0.55, 0.88)
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
    page++
  }

  return all
}

function isUpcomingMatch(match) {
  const raw = match.kickoff || match.match_date
  if (!raw) return false

  const kickoff = new Date(raw)
  if (Number.isNaN(kickoff.getTime())) return false

  const now = new Date()
  return kickoff.getTime() >= now.getTime() - 2 * 60 * 60 * 1000
}

function topStrongMarkets(markets, limit = 3) {
  const sorted = [...markets].sort((a, b) => b.prob - a.prob)
  return sorted.slice(0, limit)
}

async function updateMatchesBrain() {
  const allMatches = await loadAllMatches()

  const filteredMatches = allMatches.filter(
    match => ALLOWED_LEAGUES.includes(match.league) && isUpcomingMatch(match)
  )

  console.log(`Total de jogos lidos: ${allMatches.length}`)
  console.log(`Jogos futuros nas ligas aprovadas: ${filteredMatches.length}`)

  for (const match of filteredMatches) {
    const computed = buildMarkets(match)
    const bestMarket = computed.best.key
    const bestProb = clamp(Number(computed.best.prob) || 0.55, 0.45, 0.92)

    const strongest = topStrongMarkets(computed.markets, 3)

    const payload = {
      home_win_prob: round(computed.result.homeWin),
      draw_prob: round(computed.result.draw),
      away_win_prob: round(computed.result.awayWin),

      home_result_prob: round(computed.result.homeWin),
      draw_result_prob: round(computed.result.draw),
      away_result_prob: round(computed.result.awayWin),

      over15_prob: round(computed.totals.over15),
      over25_prob: round(computed.totals.over25),
      under25_prob: round(computed.totals.under25),
      under35_prob: round(computed.totals.under35),
      btts_prob: round(computed.totals.btts),

      avg_corners: round(computed.cornersProjection, 2),

      home_form: normalizeFormLabel(match.home_form, match.power_home),
      away_form: normalizeFormLabel(match.away_form, match.power_away),

      pick: bestMarket,
      insight: buildInsight(bestMarket, match, computed),
      strength_label: probabilityToStrength(bestProb),

      top_market_1: strongest[0]?.key || null,
      top_market_1_prob: strongest[0] ? round(strongest[0].prob) : null,
      top_market_2: strongest[1]?.key || null,
      top_market_2_prob: strongest[1] ? round(strongest[1].prob) : null,
      top_market_3: strongest[2]?.key || null,
      top_market_3_prob: strongest[2] ? round(strongest[2].prob) : null
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

  const upcoming = (matches || []).filter(
    match => ALLOWED_LEAGUES.includes(match.league) && isUpcomingMatch(match)
  )

  const bestPerGame = upcoming.map(match => {
    const options = [
      { market: 'Vitória mandante', prob: Number(match.home_result_prob || match.home_win_prob || 0) },
      { market: 'Empate', prob: Number(match.draw_result_prob || match.draw_prob || 0) },
      { market: 'Vitória visitante', prob: Number(match.away_result_prob || match.away_win_prob || 0) },
      { market: 'Mais de 1.5 gols', prob: Number(match.over15_prob || 0) },
      { market: 'Mais de 2.5 gols', prob: Number(match.over25_prob || 0) },
      { market: 'Menos de 2.5 gols', prob: Number(match.under25_prob || 0) },
      { market: 'Menos de 3.5 gols', prob: Number(match.under35_prob || 0) },
      { market: 'Ambas marcam', prob: Number(match.btts_prob || 0) },

      { market: match.top_market_1, prob: Number(match.top_market_1_prob || 0) },
      { market: match.top_market_2, prob: Number(match.top_market_2_prob || 0) },
      { market: match.top_market_3, prob: Number(match.top_market_3_prob || 0) }
    ]
      .filter(item => item.market)
      .map(item => ({
        market: item.market,
        prob: round(radarProbability(item.prob))
      }))

    const best = options.sort((a, b) => b.prob - a.prob)[0]

    return {
      match_id: match.id,
      home_team: match.home_team,
      away_team: match.away_team,
      league: match.league,
      market: best.market,
      probability: best.prob
    }
  })

  const rows = bestPerGame
    .sort((a, b) => {
      if (b.probability !== a.probability) return b.probability - a.probability
      return String(a.league).localeCompare(String(b.league))
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
    console.log('Iniciando Scoutly Brain V3...')
    await updateMatchesBrain()
    await rebuildDailyPicks()
    console.log('Scoutly Brain V3 finalizado com sucesso.')
  } catch (error) {
    console.error('Erro no Scoutly Brain V3:', error)
    process.exit(1)
  }
}

run()
