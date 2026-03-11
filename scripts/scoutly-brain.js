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

const LEAGUE_PRIORITY = {
  'UEFA Champions League': 100,
  'CONMEBOL Libertadores': 96,
  'Premier League': 95,
  'La Liga': 93,
  'Serie A': 92,
  'Bundesliga': 91,
  'Ligue 1': 90,
  'Copa do Brasil': 89,
  'UEFA Europa League': 88,
  'UEFA Europa Conference League': 86,
  'Serie B': 84,
  'Liga Profesional Argentina': 82,
  'CONCACAF Champions Cup': 80,
  'AFC Champions League': 78,
  'AFC Champions League Elite': 78,
  'CONMEBOL Sudamericana': 76,
  'Eredivisie': 74,
  'Coppa Italia': 73,
  'Copa del Rey': 73,
  'Saudi Pro League': 70,
  'Pro League': 69,
  'Super Lig': 68,
  'Süper Lig': 68,
  'Primera Division': 66,
  'Primera División': 66
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 6) {
  return Number(Number(value).toFixed(decimals))
}

function factorial(n) {
  if (n <= 1) return 1
  let result = 1
  for (let i = 2; i <= n; i++) result *= i
  return result
}

function poisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0
  return (Math.exp(-lambda) * Math.pow(lambda, k)) / factorial(k)
}

function distribution(lambda, maxK = 10) {
  const probs = []
  let sum = 0

  for (let k = 0; k <= maxK; k++) {
    const p = poisson(lambda, k)
    probs.push(p)
    sum += p
  }

  if (sum < 0.999) probs.push(1 - sum)
  return probs
}

function resultProbabilities(homeXg, awayXg, maxGoals = 8) {
  const homeDist = distribution(homeXg, maxGoals)
  const awayDist = distribution(awayXg, maxGoals)

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

function totalsProbabilities(homeXg, awayXg, maxGoals = 10) {
  const homeDist = distribution(homeXg, maxGoals)
  const awayDist = distribution(awayXg, maxGoals)

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
  const lambda = clamp(Number(avgCorners) || 8.8, 2.5, 16)
  let over85 = 0

  for (let k = 9; k <= 20; k++) {
    over85 += poisson(lambda, k)
  }

  return clamp(over85, 0.08, 0.92)
}

function normalizeFormLabel(text, numericValue) {
  if (text && typeof text === 'string') {
    const t = text.trim().toLowerCase()
    if (t.includes('strong')) return 'Forte'
    if (t.includes('average')) return 'Equilibrado'
    if (t.includes('weak')) return 'Fraco'
    if (t.includes('forte')) return 'Forte'
    if (t.includes('equilibrado')) return 'Equilibrado'
    if (t.includes('fraco')) return 'Fraco'
  }

  const v = Number(numericValue) || 1
  if (v >= 1.2) return 'Muito forte'
  if (v >= 1.05) return 'Forte'
  if (v >= 0.9) return 'Equilibrado'
  return 'Fraco'
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
  const results = resultProbabilities(homeXg, awayXg)
  const totals = totalsProbabilities(homeXg, awayXg)
  const over85Corners = cornersProbability(match.avg_corners)

  const markets = [
    { market: 'Vitória mandante', prob: results.homeWin },
    { market: 'Empate', prob: results.draw },
    { market: 'Vitória visitante', prob: results.awayWin },
    { market: 'Mais de 1.5 gols', prob: totals.over15 },
    { market: 'Mais de 2.5 gols', prob: totals.over25 },
    { market: 'Menos de 2.5 gols', prob: totals.under25 },
    { market: 'Menos de 3.5 gols', prob: totals.under35 },
    { market: 'Ambas marcam', prob: totals.btts },
    { market: 'Mais de 8.5 escanteios', prob: over85Corners },
    { market: 'Menos de 8.5 escanteios', prob: 1 - over85Corners }
  ]
    .map(item => ({
      market: item.market,
      prob: clamp(item.prob, 0.08, 0.92)
    }))
    .sort((a, b) => b.prob - a.prob)

  return {
    homeXg,
    awayXg,
    results,
    totals,
    over85Corners,
    markets,
    best: markets[0]
  }
}

function buildInsight(match, bestMarket) {
  const avgGoals = round(Number(match.avg_goals) || 0, 2)
  const avgCorners = round(Number(match.avg_corners) || 0, 2)
  const avgShots = round(Number(match.avg_shots) || 0, 2)

  const insights = {
    'Mais de 2.5 gols': [
      `média ofensiva de ${avgGoals} gols`,
      `volume de ${avgShots} finalizações`,
      'tendência de jogo mais aberto'
    ],
    'Menos de 2.5 gols': [
      'tendência de jogo mais controlado',
      `produção média de ${avgGoals} gols`,
      'ritmo ofensivo mais moderado'
    ],
    'Menos de 3.5 gols': [
      'faixa de placar mais contida',
      'equilíbrio de forças',
      'cenário menos explosivo'
    ],
    'Ambas marcam': [
      'capacidade ofensiva dos dois lados',
      'equilíbrio de confronto',
      `média de ${avgShots} finalizações`
    ],
    'Mais de 8.5 escanteios': [
      `média de ${avgCorners} escanteios`,
      'tendência de pressão ofensiva',
      'produção lateral relevante'
    ],
    'Menos de 8.5 escanteios': [
      `média de ${avgCorners} escanteios`,
      'produção lateral mais baixa',
      'jogo com menor volume de cantos'
    ],
    'Vitória mandante': [
      'força relativa do mandante',
      'vantagem de mando',
      'equilíbrio ofensivo favorável'
    ],
    'Vitória visitante': [
      'força relativa superior do visitante',
      'melhor distribuição ofensiva',
      'cenário favorável fora'
    ],
    'Empate': [
      'confronto equilibrado',
      'probabilidade de jogo travado',
      'forças próximas entre os lados'
    ],
    'Mais de 1.5 gols': [
      'linha mais segura de gols',
      'produção ofensiva aceitável',
      'bom cenário para pelo menos 2 gols'
    ]
  }

  const bullets = insights[bestMarket] || [
    'dados ofensivos do confronto',
    'forma das equipes',
    'equilíbrio geral do jogo'
  ]

  return `Modelo aponta valor em ${bestMarket} por ${bullets.join(', ')}.`
}

function radarProbability(prob) {
  return clamp(prob, 0.56, 0.88)
}

function scoreForDailyPick(match, bestProb) {
  const leaguePriority = LEAGUE_PRIORITY[match.league] || 50
  return bestProb * 1000 + leaguePriority
}

function parseKickoff(match) {
  const raw = match.kickoff || match.Kickoff || null
  if (!raw) return null

  const d = new Date(raw)
  if (Number.isNaN(d.getTime())) return null
  return d
}

function isRelevantUpcomingMatch(match) {
  if (!ALLOWED_LEAGUES.includes(match.league)) return false

  const kickoff = parseKickoff(match)
  const now = new Date()

  if (!kickoff) {
    // se não tiver kickoff, mantém só se match_date for hoje
    if (!match.match_date) return false
    const today = new Date()
    const localDate = today.toISOString().slice(0, 10)
    return String(match.match_date) === localDate
  }

  // mantém jogos a partir de 30 min atrás até 36h à frente
  const diffMs = kickoff.getTime() - now.getTime()
  const minPast = -30 * 60 * 1000
  const maxFuture = 36 * 60 * 60 * 1000

  return diffMs >= minPast && diffMs <= maxFuture
}

async function fetchAllMatches() {
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
    page += 1
  }

  return allMatches
}

async function updateMatchesBrain() {
  const allMatches = await fetchAllMatches()
  const filteredMatches = allMatches.filter(match => ALLOWED_LEAGUES.includes(match.league))

  console.log(`Total de jogos lidos: ${allMatches.length}`)
  console.log(`Jogos nas ligas aprovadas: ${filteredMatches.length}`)

  for (const match of filteredMatches) {
    const computed = buildMarkets(match)

    const payload = {
      home_win_prob: round(computed.results.homeWin),
      draw_prob: round(computed.results.draw),
      away_win_prob: round(computed.results.awayWin),

      home_result_prob: round(computed.results.homeWin),
      draw_result_prob: round(computed.results.draw),
      away_result_prob: round(computed.results.awayWin),

      over15_prob: round(computed.totals.over15),
      over25_prob: round(computed.totals.over25),
      under25_prob: round(computed.totals.under25),
      under35_prob: round(computed.totals.under35),
      btts_prob: round(computed.totals.btts),
      corners_over85_prob: round(computed.over85Corners),

      home_form: normalizeFormLabel(match.home_form, match.power_home),
      away_form: normalizeFormLabel(match.away_form, match.power_away),

      pick: computed.best.market,
      insight: buildInsight(match, computed.best.market)
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

  if (deleteError) throw deleteError

  const allMatches = await fetchAllMatches()
  const candidates = allMatches
    .filter(isRelevantUpcomingMatch)
    .map(match => {
      const bestOptions = [
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
      ]
        .map(item => ({
          market: item.market,
          prob: radarProbability(item.prob)
        }))
        .sort((a, b) => b.prob - a.prob)

      const best = bestOptions[0]

      return {
        match_id: match.id,
        home_team: match.home_team,
        away_team: match.away_team,
        league: match.league,
        market: best.market,
        probability: round(best.prob),
        score: scoreForDailyPick(match, best.prob)
      }
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, 6)

  if (!candidates.length) {
    console.log('Nenhuma daily pick encontrada.')
    return
  }

  const rows = candidates.map((item, index) => ({
    rank: index + 1,
    match_id: item.match_id,
    home_team: item.home_team,
    away_team: item.away_team,
    league: item.league,
    market: item.market,
    probability: item.probability,
    is_opportunity: index === 0
  }))

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
