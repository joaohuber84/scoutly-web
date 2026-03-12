const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const ALLOWED_LEAGUES = [
  // Europa
  'Premier League',
  'La Liga',
  'Bundesliga',
  'Serie A',
  'Ligue 1',
  'Eredivisie',
  'Primeira Liga',
  'Belgian Pro League',
  'Super Lig',
  'Super League',

  // América
  'Brasileirão Série A',
  'Brasileirão Série B',
  'Liga Profesional Argentina',
  'MLS',
  'Liga MX',

  // Oriente Médio
  'Saudi Pro League',

  // Copas
  'UEFA Champions League',
  'UEFA Europa League',
  'UEFA Europa Conference League',
  'CONMEBOL Libertadores',
  'CONMEBOL Sudamericana',
  'Copa do Brasil',
  'CONCACAF Champions Cup'
]

const LEAGUE_ALIASES = {
  'Premier League': 'Premier League',
  'English Premier League': 'Premier League',
  'EPL': 'Premier League',

  'La Liga': 'La Liga',
  'Primera Division': 'La Liga',

  'Bundesliga': 'Bundesliga',
  'Serie A': 'Serie A',
  'Ligue 1': 'Ligue 1',
  'Eredivisie': 'Eredivisie',

  'Primeira Liga': 'Primeira Liga',
  'Liga Portugal': 'Primeira Liga',

  'Belgian Pro League': 'Belgian Pro League',
  'Jupiler Pro League': 'Belgian Pro League',

  'Super Lig': 'Super Lig',
  'Turkish Super League': 'Super Lig',

  'Super League': 'Super League',
  'Greek Super League': 'Super League',

  'Brasileirão Série A': 'Brasileirão Série A',
  'Brazil Serie A': 'Brasileirão Série A',
  'Serie A Brazil': 'Brasileirão Série A',

  'Brasileirão Série B': 'Brasileirão Série B',
  'Brazil Serie B': 'Brasileirão Série B',

  'Liga Profesional Argentina': 'Liga Profesional Argentina',
  'Argentina Primera Division': 'Liga Profesional Argentina',

  'MLS': 'MLS',
  'Major League Soccer': 'MLS',

  'Liga MX': 'Liga MX',
  'Mexican Primera League': 'Liga MX',

  'Saudi Pro League': 'Saudi Pro League',
  'Saudi Professional League': 'Saudi Pro League',

  'UEFA Champions League': 'UEFA Champions League',
  'Champions League': 'UEFA Champions League',

  'UEFA Europa League': 'UEFA Europa League',
  'Europa League': 'UEFA Europa League',

  'UEFA Europa Conference League': 'UEFA Europa Conference League',
  'Conference League': 'UEFA Europa Conference League',

  'CONMEBOL Libertadores': 'CONMEBOL Libertadores',
  'Copa Libertadores': 'CONMEBOL Libertadores',

  'CONMEBOL Sudamericana': 'CONMEBOL Sudamericana',
  'Copa Sudamericana': 'CONMEBOL Sudamericana',

  'Copa do Brasil': 'Copa do Brasil',

  'CONCACAF Champions Cup': 'CONCACAF Champions Cup',
  'CONCACAF Champions League': 'CONCACAF Champions Cup'
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 2) {
  return Number(Number(value).toFixed(decimals))
}

function logistic(x) {
  return 1 / (1 + Math.exp(-x))
}

function normalizeLeague(rawLeague) {
  if (!rawLeague) return null
  const normalized = LEAGUE_ALIASES[rawLeague] || rawLeague
  return ALLOWED_LEAGUES.includes(normalized) ? normalized : null
}

function firstNumber(obj, keys, fallback = null) {
  for (const key of keys) {
    const value = obj?.[key]
    const num = Number(value)
    if (!Number.isNaN(num) && value !== null && value !== undefined && value !== '') {
      return num
    }
  }
  return fallback
}

function firstString(obj, keys, fallback = '') {
  for (const key of keys) {
    const value = obj?.[key]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return fallback
}

function weightedValue(recent, season, recentWeight = 0.65) {
  const hasRecent = recent !== null && recent !== undefined && !Number.isNaN(Number(recent))
  const hasSeason = season !== null && season !== undefined && !Number.isNaN(Number(season))

  if (hasRecent && hasSeason) {
    return Number(recent) * recentWeight + Number(season) * (1 - recentWeight)
  }
  if (hasRecent) return Number(recent)
  if (hasSeason) return Number(season)
  return null
}

function isUpcomingMatch(match) {
  const raw = match.kickoff || match.match_date || match.start_time
  if (!raw) return false

  const kickoff = new Date(raw)
  if (Number.isNaN(kickoff.getTime())) return false

  const now = new Date()
  return kickoff.getTime() >= now.getTime() - 2 * 60 * 60 * 1000
}

function getMetrics(match) {
  const homeGoalsRecent = firstNumber(match, [
    'home_goals_avg_recent',
    'home_recent_goals_avg',
    'home_avg_goals_recent'
  ])
  const awayGoalsRecent = firstNumber(match, [
    'away_goals_avg_recent',
    'away_recent_goals_avg',
    'away_avg_goals_recent'
  ])
  const homeGoalsSeason = firstNumber(match, [
    'home_goals_avg',
    'home_avg_goals',
    'home_goals_for_avg'
  ], 1.2)
  const awayGoalsSeason = firstNumber(match, [
    'away_goals_avg',
    'away_avg_goals',
    'away_goals_for_avg'
  ], 1.1)

  const homeGoalsAgainstRecent = firstNumber(match, [
    'home_goals_against_avg_recent',
    'home_recent_goals_against_avg'
  ])
  const awayGoalsAgainstRecent = firstNumber(match, [
    'away_goals_against_avg_recent',
    'away_recent_goals_against_avg'
  ])
  const homeGoalsAgainstSeason = firstNumber(match, [
    'home_goals_against_avg',
    'home_avg_goals_against'
  ], 1.1)
  const awayGoalsAgainstSeason = firstNumber(match, [
    'away_goals_against_avg',
    'away_avg_goals_against'
  ], 1.2)

  const homeCornersRecent = firstNumber(match, [
    'home_corners_avg_recent',
    'home_recent_corners_avg'
  ])
  const awayCornersRecent = firstNumber(match, [
    'away_corners_avg_recent',
    'away_recent_corners_avg'
  ])
  const homeCornersSeason = firstNumber(match, [
    'home_corners_avg',
    'home_avg_corners'
  ], 4.8)
  const awayCornersSeason = firstNumber(match, [
    'away_corners_avg',
    'away_avg_corners'
  ], 4.3)

  const homeShotsRecent = firstNumber(match, [
    'home_shots_avg_recent',
    'home_recent_shots_avg'
  ])
  const awayShotsRecent = firstNumber(match, [
    'away_shots_avg_recent',
    'away_recent_shots_avg'
  ])
  const homeShotsSeason = firstNumber(match, [
    'home_shots_avg',
    'home_avg_shots'
  ], 11.5)
  const awayShotsSeason = firstNumber(match, [
    'away_shots_avg',
    'away_avg_shots'
  ], 10.3)

  const homeShotsOnTargetRecent = firstNumber(match, [
    'home_shots_on_target_avg_recent',
    'home_recent_shots_on_target_avg',
    'home_sot_avg_recent'
  ])
  const awayShotsOnTargetRecent = firstNumber(match, [
    'away_shots_on_target_avg_recent',
    'away_recent_shots_on_target_avg',
    'away_sot_avg_recent'
  ])
  const homeShotsOnTargetSeason = firstNumber(match, [
    'home_shots_on_target_avg',
    'home_avg_shots_on_target',
    'home_sot_avg'
  ], 4)
  const awayShotsOnTargetSeason = firstNumber(match, [
    'away_shots_on_target_avg',
    'away_avg_shots_on_target',
    'away_sot_avg'
  ], 3.6)

  const homeForm = firstNumber(match, [
    'home_form_score',
    'home_form',
    'form_home'
  ], 50)
  const awayForm = firstNumber(match, [
    'away_form_score',
    'away_form',
    'form_away'
  ], 50)

  const homeStrength = firstNumber(match, [
    'home_strength',
    'power_home',
    'team_home_power'
  ], 1)
  const awayStrength = firstNumber(match, [
    'away_strength',
    'power_away',
    'team_away_power'
  ], 1)

  const hg = weightedValue(homeGoalsRecent, homeGoalsSeason, 0.68) ?? 1.2
  const ag = weightedValue(awayGoalsRecent, awayGoalsSeason, 0.68) ?? 1.1
  const hga = weightedValue(homeGoalsAgainstRecent, homeGoalsAgainstSeason, 0.66) ?? 1.1
  const aga = weightedValue(awayGoalsAgainstRecent, awayGoalsAgainstSeason, 0.66) ?? 1.2

  const hc = weightedValue(homeCornersRecent, homeCornersSeason, 0.64) ?? 4.8
  const ac = weightedValue(awayCornersRecent, awayCornersSeason, 0.64) ?? 4.3

  const hs = weightedValue(homeShotsRecent, homeShotsSeason, 0.65) ?? 11.5
  const as = weightedValue(awayShotsRecent, awayShotsSeason, 0.65) ?? 10.3

  const hsot = weightedValue(homeShotsOnTargetRecent, homeShotsOnTargetSeason, 0.65) ?? 4
  const asot = weightedValue(awayShotsOnTargetRecent, awayShotsOnTargetSeason, 0.65) ?? 3.6

  const goalsExpected = clamp(
    (hg + ag + hga + aga) / 2,
    1.2,
    4.5
  )

  const cornersExpected = clamp(
    hc + ac + (hs + as) * 0.04,
    6.0,
    15.5
  )

  const shotsExpected = clamp(hs + as, 10, 32)
  const shotsOnTargetExpected = clamp(hsot + asot, 2, 12)

  const strengthDiff = homeStrength - awayStrength
  const formDiff = homeForm - awayForm
  const balanceIndex = Math.abs(strengthDiff) + Math.abs(formDiff / 20)

  const bttsBase = clamp(
    ((hg + ag) / 2 + (hga + aga) / 2) / 2.2,
    0.15,
    0.85
  )

  let profile = 'equilibrado'
  if (goalsExpected >= 2.9 && shotsExpected >= 22) profile = 'ofensivo'
  else if (goalsExpected <= 2.2 && shotsExpected <= 19) profile = 'travado'
  else if (balanceIndex >= 1.1) profile = 'desequilibrado'

  return {
    goalsExpected: round(goalsExpected, 2),
    cornersExpected: round(cornersExpected, 2),
    shotsExpected: round(shotsExpected, 0),
    shotsOnTargetExpected: round(shotsOnTargetExpected, 0),
    homeForm,
    awayForm,
    homeStrength,
    awayStrength,
    strengthDiff,
    formDiff,
    bttsBase,
    profile
  }
}

function tierFromScore(score) {
  if (score >= 86) return 'Muito forte'
  if (score >= 74) return 'Boa'
  if (score >= 64) return 'Moderada'
  return 'Oportunidade extra'
}

function labelFromScore(score) {
  if (score >= 86) return 'Leitura muito forte'
  if (score >= 74) return 'Leitura boa'
  if (score >= 64) return 'Leitura moderada'
  return 'Leitura extra'
}

function buildScores(match, metrics) {
  const home = firstString(match, ['home_team', 'team_home'], 'Mandante')
  const away = firstString(match, ['away_team', 'team_away'], 'Visitante')

  const goalAttackScore = clamp(
    metrics.goalsExpected * 22 +
    metrics.shotsOnTargetExpected * 3 +
    metrics.shotsExpected * 0.7,
    0,
    100
  )

  const underGoalScore = clamp(
    100 - (metrics.goalsExpected * 20 + metrics.shotsExpected * 1.2),
    0,
    100
  )

  const cornerOverScore = clamp(
    metrics.cornersExpected * 8 +
    metrics.shotsExpected * 0.7,
    0,
    100
  )

  const cornerUnderScore = clamp(
    100 - (metrics.cornersExpected * 7.5 + metrics.shotsExpected * 0.55),
    0,
    100
  )

  const homeDominanceScore = clamp(
    55 + metrics.strengthDiff * 18 + metrics.formDiff * 0.35,
    0,
    100
  )

  const awayDominanceScore = clamp(
    55 - metrics.strengthDiff * 18 - metrics.formDiff * 0.35,
    0,
    100
  )

  const drawProtectionHomeScore = clamp(homeDominanceScore + 10, 0, 100)
  const drawProtectionAwayScore = clamp(awayDominanceScore + 10, 0, 100)

  const bttsYesScore = clamp(
    metrics.bttsBase * 100 + metrics.shotsOnTargetExpected * 2,
    0,
    100
  )

  const bttsNoScore = clamp(
    100 - bttsYesScore + 8,
    0,
    100
  )

  const markets = []

  // Gols
  if (metrics.goalsExpected >= 2.95) {
    markets.push({
      family: 'gols',
      market: 'Mais de 2.5 gols',
      score: goalAttackScore
    })
  } else if (metrics.goalsExpected >= 2.3) {
    markets.push({
      family: 'gols',
      market: 'Mais de 1.5 gols',
      score: goalAttackScore
    })
  } else {
    markets.push({
      family: 'gols',
      market: 'Menos de 3.5 gols',
      score: underGoalScore
    })
  }

  // Escanteios
  if (metrics.cornersExpected >= 10.6) {
    markets.push({
      family: 'escanteios',
      market: 'Mais de 9.5 escanteios',
      score: cornerOverScore
    })
  } else if (metrics.cornersExpected >= 9.2) {
    markets.push({
      family: 'escanteios',
      market: 'Mais de 8.5 escanteios',
      score: cornerOverScore - 3
    })
  } else if (metrics.cornersExpected <= 8.4) {
    markets.push({
      family: 'escanteios',
      market: 'Menos de 10.5 escanteios',
      score: cornerUnderScore + 6
    })
  } else {
    markets.push({
      family: 'escanteios',
      market: 'Menos de 11.5 escanteios',
      score: cornerUnderScore
    })
  }

  // Resultado / proteção
  if (homeDominanceScore >= 80) {
    markets.push({
      family: 'resultado',
      market: `Vitória do ${home}`,
      score: homeDominanceScore
    })
  } else if (awayDominanceScore >= 80) {
    markets.push({
      family: 'resultado',
      market: `Vitória do ${away}`,
      score: awayDominanceScore
    })
  } else if (drawProtectionHomeScore >= drawProtectionAwayScore) {
    markets.push({
      family: 'resultado',
      market: `Dupla chance ${home} ou empate`,
      score: drawProtectionHomeScore
    })
  } else {
    markets.push({
      family: 'resultado',
      market: `Dupla chance ${away} ou empate`,
      score: drawProtectionAwayScore
    })
  }

  // Ambas
  if (metrics.goalsExpected >= 2.7 && metrics.shotsOnTargetExpected >= 8) {
    markets.push({
      family: 'ambas',
      market: 'Ambas marcam',
      score: bttsYesScore
    })
  } else {
    markets.push({
      family: 'ambas',
      market: 'Ambas não marcam',
      score: bttsNoScore
    })
  }

  return markets
    .map(item => ({
      ...item,
      score: round(clamp(item.score, 40, 96), 2),
      tier: tierFromScore(item.score),
      strength_label: labelFromScore(item.score)
    }))
    .sort((a, b) => b.score - a.score)
}

function buildGameReading(metrics, primaryMarket) {
  if (primaryMarket.family === 'gols' && primaryMarket.market.includes('Mais')) {
    return `Confronto com boa produção ofensiva, ${metrics.shotsExpected} finalizações projetadas e ${metrics.goalsExpected} gols esperados. O cenário favorece mercado de gols.`
  }

  if (primaryMarket.family === 'gols' && primaryMarket.market.includes('Menos')) {
    return `Jogo mais controlado, com projeção ofensiva moderada e menor tendência de placar elástico. O modelo aponta linha conservadora de gols como melhor leitura.`
  }

  if (primaryMarket.family === 'escanteios' && primaryMarket.market.includes('Mais')) {
    return `Partida com bom volume ofensivo e tendência de pressão lateral, chegando a ${metrics.cornersExpected} escanteios projetados. O mercado de cantos aparece como o mais forte.`
  }

  if (primaryMarket.family === 'escanteios' && primaryMarket.market.includes('Menos')) {
    return `Leitura de ritmo menos aberto nas laterais, com ${metrics.cornersExpected} escanteios projetados. O modelo vê valor em linha mais controlada de escanteios.`
  }

  if (primaryMarket.family === 'resultado' && primaryMarket.market.includes('Vitória')) {
    return `Há vantagem estatística clara para o lado recomendado, considerando força relativa, forma recente e equilíbrio do confronto.`
  }

  if (primaryMarket.family === 'resultado' && primaryMarket.market.includes('Dupla chance')) {
    return `Confronto com favoritismo moderado, mas sem domínio absoluto. Por isso, a proteção da dupla chance aparece como leitura mais segura.`
  }

  if (primaryMarket.family === 'ambas' && primaryMarket.market === 'Ambas marcam') {
    return `Os dois lados mostram capacidade ofensiva suficiente para produzir chances. O mercado de ambas marcam ganha força pelo equilíbrio ofensivo do confronto.`
  }

  return `O modelo combinou forma recente, produção ofensiva e equilíbrio do confronto para destacar esse mercado como a leitura principal da partida.`
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

function buildTopMarkets(markets) {
  const usedFamilies = new Set()
  const selected = []

  for (const market of markets) {
    if (usedFamilies.has(market.family)) continue
    usedFamilies.add(market.family)
    selected.push(market)
    if (selected.length === 4) break
  }

  return selected
}

async function updateMatchesBrain() {
  const matches = await loadAllMatches()

  const filtered = matches.filter(match => {
    const normalizedLeague = normalizeLeague(match.league)
    return normalizedLeague && isUpcomingMatch(match)
  })

  console.log(`Jogos filtrados no escopo principal: ${filtered.length}`)

  for (const match of filtered) {
    const normalizedLeague = normalizeLeague(match.league)
    const metrics = getMetrics(match)
    const marketBoard = buildScores(match, metrics)
    const topMarkets = buildTopMarkets(marketBoard)
    const primary = topMarkets[0]

    if (!primary) continue

    const payload = {
      league: normalizedLeague,

      pick: primary.market,
      strength_label: primary.strength_label,
      insight: buildGameReading(metrics, primary),

      expected_goals: round(metrics.goalsExpected, 2),
      expected_corners: round(metrics.cornersExpected, 2),
      expected_shots: round(metrics.shotsExpected, 0),
      expected_shots_on_target: round(metrics.shotsOnTargetExpected, 0),
      game_profile: metrics.profile,

      top_market_1: topMarkets[0]?.market || null,
      top_market_1_score: topMarkets[0]?.score || null,
      top_market_2: topMarkets[1]?.market || null,
      top_market_2_score: topMarkets[1]?.score || null,
      top_market_3: topMarkets[2]?.market || null,
      top_market_3_score: topMarkets[2]?.score || null,
      top_market_4: topMarkets[3]?.market || null,
      top_market_4_score: topMarkets[3]?.score || null
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

function marketKey(market) {
  if (!market) return 'desconhecido'
  if (market.includes('escanteios')) return 'escanteios'
  if (market.includes('gols')) return 'gols'
  if (market.includes('Ambas')) return 'ambas'
  if (market.includes('Dupla chance')) return 'dupla_chance'
  if (market.includes('Vitória')) return 'vitoria'
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

  const usable = (matches || []).filter(match => {
    const normalizedLeague = normalizeLeague(match.league)
    if (!normalizedLeague) return false
    if (!isUpcomingMatch(match)) return false
    if (!match.pick) return false
    if (!match.expected_goals || !match.expected_corners) return false
    return true
  })

  const candidates = usable.map(match => ({
    match_id: match.id,
    home_team: match.home_team,
    away_team: match.away_team,
    league: normalizeLeague(match.league),
    market: match.pick,
    probability: round(firstNumber(match, ['top_market_1_score'], 70) / 100, 4),
    score: firstNumber(match, ['top_market_1_score'], 70),
    market_key: marketKey(match.pick)
  }))
  .sort((a, b) => b.score - a.score)

  const selected = []
  const usedMatches = new Set()
  const usedLeagues = {}
  const usedMarkets = {}

  for (const item of candidates) {
    if (selected.length >= 6) break
    if (usedMatches.has(item.match_id)) continue
    if ((usedLeagues[item.league] || 0) >= 2) continue
    if ((usedMarkets[item.market_key] || 0) >= 2) continue

    selected.push(item)
    usedMatches.add(item.match_id)
    usedLeagues[item.league] = (usedLeagues[item.league] || 0) + 1
    usedMarkets[item.market_key] = (usedMarkets[item.market_key] || 0) + 1
  }

  // fallback relaxado
  for (const item of candidates) {
    if (selected.length >= 6) break
    if (usedMatches.has(item.match_id)) continue

    selected.push(item)
    usedMatches.add(item.match_id)
  }

  const rows = selected.slice(0, 6).map((item, index) => ({
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
    console.log('Iniciando Scoutly Brain V2...')
    await updateMatchesBrain()
    await rebuildDailyPicks()
    console.log('Scoutly Brain V2 finalizado com sucesso.')
  } catch (error) {
    console.error('Erro no Scoutly Brain V2:', error)
    process.exit(1)
  }
}

run()
   
