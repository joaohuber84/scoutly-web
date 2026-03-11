const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const APISPORTS_KEY = process.env.APISPORTS_KEY

const ALLOWED_LEAGUES = [
  // Inglaterra
  'Premier League',

  // Espanha
  'La Liga',
  'Copa del Rey',

  // Itália
  'Serie A',
  'Coppa Italia',

  // Alemanha
  'Bundesliga',

  // França
  'Ligue 1',

  // Holanda
  'Eredivisie',

  // Turquia
  'Süper Lig',
  'Super Lig',

  // Grécia
  'Super League 1',
  'Greece Super League',
  'Super League',

  // Arábia Saudita
  'Pro League',
  'Saudi Pro League',

  // Brasil
  'Serie A',
  'Serie B',
  'Copa do Brasil',
  'Brasileiro Serie A',
  'Brasileiro Serie B',
  'Campeonato Brasileiro Série A',
  'Campeonato Brasileiro Série B',

  // Argentina
  'Liga Profesional Argentina',
  'Primera División',
  'Primera Division',

  // Chile
  'Primera División',
  'Primera Division',
  'Campeonato Nacional',

  // Continentais Europa
  'UEFA Champions League',
  'UEFA Europa League',
  'UEFA Europa Conference League',

  // América do Sul
  'CONMEBOL Libertadores',
  'CONMEBOL Sudamericana',

  // CONCACAF
  'CONCACAF Champions Cup',
  'CONCACAF Champions League',

  // Ásia
  'AFC Champions League',
  'AFC Champions League Elite'
]

function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
}

function isAllowedLeague(leagueName, countryName) {
  const leagueNorm = normalizeText(leagueName)
  const countryNorm = normalizeText(countryName)

  const allowed = ALLOWED_LEAGUES.some((item) => normalizeText(item) === leagueNorm)
  if (allowed) return true

  // Regras extras por segurança
  if (leagueNorm === 'serie a' && countryNorm === 'brazil') return true
  if (leagueNorm === 'serie b' && countryNorm === 'brazil') return true
  if (leagueNorm === 'serie a' && countryNorm === 'italy') return true
  if (leagueNorm === 'primera division' && countryNorm === 'argentina') return true
  if (leagueNorm === 'primera division' && countryNorm === 'chile') return true
  if (leagueNorm === 'pro league' && countryNorm === 'saudi arabia') return true

  return false
}

function buildKickoff(dateString) {
  if (!dateString) return null
  const date = new Date(dateString)
  if (Number.isNaN(date.getTime())) return null
  return date.toISOString()
}

function buildMatchDate(dateString) {
  if (!dateString) return null
  const date = new Date(dateString)
  if (Number.isNaN(date.getTime())) return null
  return date.toISOString().slice(0, 10)
}

async function fetchFixturesForDate(dateStr) {
  const url = `https://v3.football.api-sports.io/fixtures?date=${dateStr}`

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'x-apisports-key': APISPORTS_KEY
    }
  })

  if (!response.ok) {
    throw new Error(`Erro ao buscar fixtures: ${response.status} ${response.statusText}`)
  }

  const json = await response.json()
  return json.response || []
}

async function upsertMatches(rows) {
  if (!rows.length) {
    console.log('Nenhum jogo válido para salvar.')
    return
  }

  const { error } = await supabase
    .from('matches')
    .upsert(rows, { onConflict: 'id' })

  if (error) {
    console.error('Erro ao salvar matches:', error)
    throw error
  }
}

async function cleanupOldMatches(todayDate) {
  const { error } = await supabase
    .from('matches')
    .delete()
    .lt('match_date', todayDate)

  if (error) {
    console.error('Erro ao limpar jogos antigos:', error)
  }
}

async function runScoutlySync() {
  try {
    console.log('🚀 Scoutly Sync iniciado')

    if (!APISPORTS_KEY) {
      throw new Error('APISPORTS_KEY não encontrada nos secrets do GitHub.')
    }

    const now = new Date()
    const today = now.toISOString().slice(0, 10)

    const fixtures = await fetchFixturesForDate(today)

    console.log(`Fixtures recebidas da API: ${fixtures.length}`)

    const filtered = fixtures.filter((item) => {
      const leagueName = item?.league?.name || ''
      const countryName = item?.league?.country || ''
      return isAllowedLeague(leagueName, countryName)
    })

    console.log(`Fixtures após filtro de competições: ${filtered.length}`)

    const rows = filtered.map((item) => {
      const fixture = item.fixture || {}
      const teams = item.teams || {}
      const league = item.league || {}

      const home = teams.home || {}
      const away = teams.away || {}

      const kickoff = buildKickoff(fixture.date)
      const matchDate = buildMatchDate(fixture.date)

      return {
        id: fixture.id,
        created_at: new Date().toISOString(),

        home_team: home.name || null,
        away_team: away.name || null,

        league: league.name || null,
        League: league.name || null,

        match_date: matchDate,
        kickoff: kickoff,

        home_logo: home.logo || null,
        away_logo: away.logo || null,

        avg_goals: null,
        avg_corners: null,
        avg_shots: null,
        insight: null,

        home_win_prob: null,
        draw_prob: null,
        away_win_prob: null,

        home_form: null,
        away_form: null,

        over25_prob: null,
        btts_prob: null,
        corners_over85_prob: null,

        pick: null,

        power_home: null,
        power_away: null,

        home_result_prob: null,
        draw_result_prob: null,
        away_result_prob: null,

        market_odds_over25: null,
        market_odds_btts: null,
        market_odds_corners85: null,

        over15_prob: null,
        under25_prob: null,
        under35_prob: null
      }
    })

    await upsertMatches(rows)
    await cleanupOldMatches(today)

    console.log(`✅ Scoutly Sync finalizado com ${rows.length} jogos salvos`)
  } catch (error) {
    console.error('❌ Erro no Scoutly Sync:', error)
    process.exit(1)
  }
}

runScoutlySync()
