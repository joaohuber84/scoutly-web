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
  if (sum < 0.999) {
    arr.push(1 - sum)
  }
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
    homeWin: clamp(homeWin, 0, 0.95),
    draw: clamp(draw, 0, 0.95),
    awayWin: clamp(awayWin, 0, 0.95)
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
    over15: clamp(over15, 0, 0.95),
    over25: clamp(over25, 0, 0.95),
    under25: clamp(under25, 0, 0.95),
    under35: clamp(under35, 0, 0.95),
    btts: clamp(btts, 0, 0.95)
  }
}

function cornersProbability(avgCorners) {
  const lambda = clamp(Number(avgCorners) || 8.5, 2, 16)
  let over85 = 0
  for (let k = 9; k <= 20; k++) {
    over85 += poisson(lambda, k)
  }
  return clamp(over85, 0.05, 0.92)
}

function inferFormLabel(value) {
  const v = Number(value) || 0
  if (v >= 1.25) return 'Muito forte'
  if (v >= 1.05) return 'Forte'
  if (v >= 0.9) return 'Equilibrado'
  return 'Fraco'
}

function normalizeTextForm(originalText, numericValue) {
  if (originalText && typeof originalText === 'string' && originalText.trim()) {
    const t = originalText.trim().toLowerCase()
    if (t.includes('strong')) return 'Forte'
    if (t.includes('average')) return 'Equilibrado'
    if (t.includes('weak')) return 'Fraco'
    return originalText
  }
  return inferFormLabel(numericValue)
}

function buildExpectedGoals(match) {
  const avgGoals = clamp(Number(match.avg_goals) || 2.4, 0.8, 5.5)
  const powerHome = clamp(Number(match.power_home) || 1, 0.45, 2.2)
  const powerAway = clamp(Number(match.power_away) || 1, 0.45, 2.2)

  const sumPower = powerHome + powerAway || 2
  let homeXg = avgGoals * (powerHome / sumPower)
  let awayXg = avgGoals * (powerAway / sumPower)

  // leve bônus de mando
  homeXg *= 1.08
  awayXg *= 0.92

  // rebalancear para manter o total perto do avgGoals
  const total = homeXg + awayXg
  if (total > 0) {
    const factor = avgGoals / total
    homeXg *= factor
    awayXg *= factor
  }

  return {
    homeXg: clamp(homeXg, 0.2, 3.8),
    awayXg: clamp(awayXg, 0.2, 3.8)
  }
}

function buildMarkets(match) {
  const { homeXg, awayXg } = buildExpectedGoals(match)
  const result = resultProbabilities(homeXg, awayXg)
  const totals = totalGoalsProbabilities(homeXg, awayXg)
  const over85Corners = cornersProbability(match.avg_corners)

  const markets = [
    { key: 'Vitória mandante', prob: result.homeWin },
    { key: 'Empate', prob: result.draw },
    { key: 'Vitória visitante', prob: result.awayWin },
    { key: 'Mais de 1.5 gols', prob: totals.over15 },
    { key: 'Mais de 2.5 gols', prob: totals.over25 },
    { key: 'Menos de 2.5 gols', prob: totals.under25 },
    { key: 'Menos de 3.5 gols', prob: totals.under35 },
    { key: 'Ambas marcam', prob: totals.btts },
    { key: 'Mais de 8.5 escanteios', prob: over85Corners },
    { key: 'Menos de 8.5 escanteios', prob: 1 - over85Corners }
  ]

  const best = [...markets].sort((a, b) => b.prob - a.prob)[0]

  return {
    homeXg,
    awayXg,
    result,
    totals,
    over85Corners,
    markets,
    best
  }
}

function buildInsight(match, bestMarket) {
  const avgGoals = Number(match.avg_goals) || 0
  const avgCorners = Number(match.avg_corners) || 0
  const avgShots = Number(match.avg_shots) || 0

  if (bestMarket === 'Mais de 2.5 gols') {
    return `Modelo aponta valor em Mais de 2.5 gols com base em média ofensiva de ${round(avgGoals, 2)} gols e ${round(avgShots, 2)} finalizações.`
  }

  if (bestMarket === 'Menos de 2.5 gols') {
    return `Modelo aponta valor em Menos de 2.5 gols por volume ofensivo mais controlado e tendência de jogo travado.`
  }

  if (bestMarket === 'Ambas marcam') {
    return `Modelo vê valor em Ambas marcam pela distribuição ofensiva dos dois lados e equilíbrio de forças.`
  }

  if (bestMarket === 'Mais de 8.5 escanteios') {
    return `Modelo aponta valor em Mais de 8.5 escanteios com média total de ${round(avgCorners, 2)} escanteios.`
  }

  if (bestMarket === 'Menos de 8.5 escanteios') {
    return `Modelo aponta valor em Menos de 8.5 escanteios por tendência de produção lateral mais baixa.`
  }

  if (bestMarket === 'Vitória mandante') {
    return `Modelo enxerga vantagem do mandante pela força relativa da equipe e leve bônus de mando.`
  }

  if (bestMarket === 'Vitória visitante') {
    return `Modelo enxerga vantagem do visitante pela força relativa superior e melhor equilíbrio ofensivo.`
  }

  if (bestMarket === 'Empate') {
    return `Modelo detecta confronto equilibrado, com forças próximas e maior propensão a jogo amarrado.`
  }

  return `Modelo aponta valor neste mercado com base nos dados ofensivos, forma e equilíbrio do confronto.`
}

function mapProbabilityForRadar(prob) {
  // mantemos entre 0.55 e 0.88 para não virar “100%”
  return clamp(prob, 0.55, 0.88)
}

async function updateMatchesBrain() {
  let page = 0
  const pageSize = 500
  let allMatches = []

  while (true) {
    const from = page * pageSize
    const to = from + pageSize - 1

    const { data, error } = await supabase
      .from('matches')
      .select('*')
      .range(from, to)

    if (error) throw error
    if (!data || !data.length) break

    allMatches = allMatches.concat(data)
    if (data.length < pageSize) break
    page++
  }

  const filteredMatches = allMatches.filter(match =>
    ALLOWED_LEAGUES.includes(match.league)
  )

  console.log(`Total de jogos lidos: ${allMatches.length}`)
  console.log(`Jogos filtrados para ligas principais: ${filteredMatches.length}`)

  for (const match of filteredMatches) {
    const computed = buildMarkets(match)

    const homeForm = normalizeTextForm(match.home_form, match.power_home)
    const awayForm = normalizeTextForm(match.away_form, match.power_away)

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
      corners_over85_prob: round(computed.over85Corners),

      home_form: homeForm,
      away_form: awayForm,

      pick: computed.best.key,
      insight: buildInsight(match, computed.best.key)
    }

    const { error } = await supabase
      .from('matches')
      .update(payload)
      .eq('id', match.id)

    if (error) {
      console.error(`Erro ao atualizar match ${match.id}:`, error.message)
    }
  }
}

async function rebuildDailyPicks() {
  const { error: deleteError } = await supabase
    .from('daily_picks')
    .delete()
    .gte('rank', 1)

  if (deleteError) {
    throw deleteError
  }

  const { data: matches, error } = await supabase
    .from('matches')
    .select('*')

  if (error) throw error

  const filteredMatches = (matches || []).filter(match =>
    ALLOWED_LEAGUES.includes(match.league)
  )

  const bestPerGame = filteredMatches.map(match => {
    const options = [
      { market: 'Vitória mandante', prob: Number(match.home_result_prob || match.home_win_prob || 0) },
      { market: 'Empate', prob: Number(match.draw_result_prob || match.draw_prob || 0) },
      { market: 'Vitória visitante', prob: Number(match.away_result_prob || match.away_win_prob || 0) },
      { market: 'Mais de 1.5 gols', prob: Number(match.over15_prob || 0) },
      { market: 'Mais de 2.5 gols', prob: Number(match.over25_prob || 0) },
      { market: 'Menos de 2.5 gols', prob: Number(match.under25_prob || 0) },
      { market: 'Menos de 3.5 gols', prob: Number(match.under35_prob || 0) },
      { market: 'Ambas marcam', prob: Number(match.btts_prob || 0) },
      { market: 'Mais de 8.5 escanteios', prob: Number(match.corners_over85_prob || 0) },
      { market: 'Menos de 8.5 escanteios', prob: 1 - Number(match.corners_over85_prob || 0.5) }
    ].map(item => ({
      market: item.market,
      prob: mapProbabilityForRadar(item.prob)
    }))

    const best = options.sort((a, b) => b.prob - a.prob)[0]

    return {
      match_id: match.id,
      home_team: match.home_team,
      away_team: match.away_team,
      league: match.league,
      market: best.market,
      probability: round(best.prob)
    }
  })

  const uniqueSorted = bestPerGame
    .sort((a, b) => {
      if (b.probability !== a.probability) return b.probability - a.probability
      return String(a.league).localeCompare(String(b.league))
    })
    .slice(0, 6)

  const rows = uniqueSorted.map((item, index) => ({
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
    console.log('Iniciando Scoutly Brain...')
    await updateMatchesBrain()
    await rebuildDailyPicks()
    console.log('Scoutly Brain finalizado com sucesso.')
  } catch (error) {
    console.error('Erro no Scoutly Brain:', error)
    process.exit(1)
  }
}

run()
