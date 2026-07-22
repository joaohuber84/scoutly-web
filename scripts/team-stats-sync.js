#!/usr/bin/env node
/**
 * team-stats-sync.js — Sincroniza estatísticas de times por liga/temporada
 *
 * Fase 1: /teams/statistics → gols, cartões, forma
 * Fase 2: /fixtures recent  → corners, shots, fouls (que a API não dá via statistics)
 *
 * Roda 1x por dia às 05:00 UTC via team-stats-sync.yml
 */

const { createClient } = require('@supabase/supabase-js')

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const API_KEY      = process.env.APISPORTS_KEY
const SEASON       = 2026

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const TARGET_LEAGUES = [
  { leagueId: 1,   name: 'Copa do Mundo' },
  { leagueId: 71,  name: 'Brasileirão Série A' },
  { leagueId: 72,  name: 'Brasileirão Série B' },
  { leagueId: 73,  name: 'Copa do Brasil' },
  { leagueId: 75,  name: 'Copa do Nordeste' },
  { leagueId: 39,  name: 'Premier League' },
  { leagueId: 140, name: 'La Liga' },
  { leagueId: 135, name: 'Serie A' },
  { leagueId: 78,  name: 'Bundesliga' },
  { leagueId: 61,  name: 'Ligue 1' },
  { leagueId: 88,  name: 'Eredivisie' },
  { leagueId: 113, name: 'Allsvenskan' },
  { leagueId: 103, name: 'Eliteserien' },
  { leagueId: 253, name: 'MLS' },
  { leagueId: 128, name: 'Liga Argentina' },
  { leagueId: 97,  name: 'Copa Argentina' },
  { leagueId: 2,   name: 'UEFA Champions League', season: 2026 },
  { leagueId: 3,   name: 'UEFA Europa League', season: 2026 },
  { leagueId: 848, name: 'UEFA Conference League', season: 2026 },
]

const DELAY = 350

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }
function safe(v, d = 0) { const n = parseFloat(v); return isNaN(n) ? d : n }
function round(v, d = 2) { return Math.round(v * 10**d) / 10**d }

async function apiGet(path) {
  const res = await fetch(`https://v3.football.api-sports.io${path}`, {
    headers: { 'x-apisports-key': API_KEY }
  })
  const json = await res.json()
  return json?.response || []
}

async function fetchTeamsInLeague(leagueId) {
  const data = await apiGet(`/standings?league=${leagueId}&season=${SEASON}`)
  const groups = data?.[0]?.league?.standings || []
  const teams = []
  for (const group of groups) {
    for (const entry of group) {
      teams.push({ id: entry.team.id, name: entry.team.name, logo: entry.team.logo })
    }
  }
  if (teams.length) return teams
  // FALLBACK: competições em formato mata-mata (Champions/Europa/Conference League
  // nas fases preliminares, Copa do Brasil, Copa do Nordeste) não têm standings de grupo,
  // então o /standings acima vem vazio e esses times nunca eram sincronizados — H2H e
  // "últimos 5 jogos" ficavam sempre em branco para esses jogos. Aqui buscamos os times
  // direto pelos confrontos recentes/futuros da liga.
  try {
    const recent = await apiGet(`/fixtures?league=${leagueId}&season=${SEASON}&last=40`)
    await sleep(DELAY)
    const upcoming = await apiGet(`/fixtures?league=${leagueId}&season=${SEASON}&next=40`)
    const seen = new Map()
    for (const f of [...recent, ...upcoming]) {
      const home = f.teams?.home, away = f.teams?.away
      if (home?.id && !seen.has(home.id)) seen.set(home.id, { id: home.id, name: home.name, logo: home.logo })
      if (away?.id && !seen.has(away.id)) seen.set(away.id, { id: away.id, name: away.name, logo: away.logo })
    }
    return [...seen.values()]
  } catch {
    return []
  }
}

async function fetchTeamStatsFromAPI(teamId, leagueId) {
  const data = await apiGet(`/teams/statistics?team=${teamId}&league=${leagueId}&season=${SEASON}`)
  // /teams/statistics devolve um OBJETO único em response, não um array — data?.[0] sempre
  // dava undefined aqui, fazendo TODO time ser tratado como "sem jogos" e pulado (bug desde
  // pelo menos 29/jun). apiGet() já normaliza array-endpoints com json.response||[]; para este
  // endpoint específico precisamos aceitar tanto objeto quanto array por segurança.
  if (Array.isArray(data)) return data[0] || null
  if (data && typeof data === 'object' && Object.keys(data).length) return data
  return null
}

// NOVO: busca corners, shots e fouls de jogos reais recentes
async function fetchCornersShootsFouls(teamId, leagueId) {
  const fixtures = await apiGet(`/fixtures?team=${teamId}&league=${leagueId}&season=${SEASON}&status=FT&last=10`)
  if (!fixtures.length) return { cornersFor: null, cornersAgainst: null, shotsFor: null, shotsOnTarget: null, foulsCommitted: null }

  let totalCornersFor = 0, totalCornersAgainst = 0
  let totalShots = 0, totalShotsOT = 0, totalFouls = 0
  let gamesWithCorners = 0, gamesWithShots = 0, gamesWithFouls = 0

  for (const f of fixtures.slice(0, 10)) {
    try {
      const stats = await apiGet(`/fixtures/statistics?fixture=${f.fixture.id}&team=${teamId}`)
      await sleep(120)
      const s = stats?.[0]?.statistics || []
      const get = (label) => {
        const item = s.find(x => x.type?.toLowerCase() === label.toLowerCase())
        return item ? safe(item.value, 0) : null
      }
      const corners = get('Corner Kicks')
      const shots   = get('Total Shots')
      const sot     = get('Shots on Goal')
      const fouls   = get('Fouls')

      // corners against = total corners in match - corners for
      // We need both teams' stats for that
      const allStats = await apiGet(`/fixtures/statistics?fixture=${f.fixture.id}`)
      await sleep(120)
      let totalCornersInMatch = 0
      for (const ts of allStats) {
        const cs = ts.statistics?.find(x => x.type === 'Corner Kicks')
        if (cs) totalCornersInMatch += safe(cs.value, 0)
      }

      if (corners !== null) {
        totalCornersFor += corners
        totalCornersAgainst += Math.max(0, totalCornersInMatch - corners)
        gamesWithCorners++
      }
      if (shots !== null) { totalShots += shots; totalShotsOT += safe(sot, 0); gamesWithShots++ }
      if (fouls !== null) { totalFouls += fouls; gamesWithFouls++ }
    } catch {}
  }

  return {
    cornersFor:      gamesWithCorners > 0 ? round(totalCornersFor / gamesWithCorners) : null,
    cornersAgainst:  gamesWithCorners > 0 ? round(totalCornersAgainst / gamesWithCorners) : null,
    shotsFor:        gamesWithShots > 0   ? round(totalShots / gamesWithShots) : null,
    shotsOnTarget:   gamesWithShots > 0   ? round(totalShotsOT / gamesWithShots) : null,
    foulsCommitted:  gamesWithFouls > 0   ? round(totalFouls / gamesWithFouls) : null,
  }
}

function extractFromAPIStats(s) {
  if (!s) return {}
  const fixtures = s.fixtures || {}
  const goals    = s.goals || {}
  const cards    = s.cards || {}

  const played = safe(fixtures.played?.total, 0)
  if (!played) return {}

  const goalsFor     = safe(goals.for?.total?.total, 0)
  const goalsAgainst = safe(goals.against?.total?.total, 0)
  const homeGoalsFor = safe(goals.for?.total?.home, 0)
  const awayGoalsFor = safe(goals.for?.total?.away, 0)
  const homeGoalsAga = safe(goals.against?.total?.home, 0)
  const awayGoalsAga = safe(goals.against?.total?.away, 0)
  const playedHome   = safe(fixtures.played?.home, 1)
  const playedAway   = safe(fixtures.played?.away, 1)

  const yellowAvg = cards.yellow ? round(
    Object.values(cards.yellow).reduce((sum, v) => sum + safe(v?.total, 0), 0) / played
  ) : null
  const redAvg = cards.red ? round(
    Object.values(cards.red).reduce((sum, v) => sum + safe(v?.total, 0), 0) / played
  ) : null

  return {
    goals_for_avg:           played ? round(goalsFor / played) : null,
    goals_against_avg:       played ? round(goalsAgainst / played) : null,
    home_goals_for_avg:      playedHome ? round(homeGoalsFor / playedHome) : null,
    away_goals_for_avg:      playedAway ? round(awayGoalsFor / playedAway) : null,
    home_goals_against_avg:  playedHome ? round(homeGoalsAga / playedHome) : null,
    away_goals_against_avg:  playedAway ? round(awayGoalsAga / playedAway) : null,
    cards_yellow_avg: yellowAvg,
    cards_red_avg:    redAvg,
    matches_played:   played,
    form:             (s.form || '').slice(-5) || null,
    clean_sheet_pct:  played ? round(safe(s.clean_sheet?.total, 0) / played * 100) : null,
  }
}

async function run() {
  console.log('📊 Team Stats Sync iniciado —', new Date().toISOString())
  let totalOk = 0, totalFail = 0

  for (const league of TARGET_LEAGUES) {
    console.log(`\n🏆 ${league.name} (id=${league.leagueId})`)
    let teams
    try {
      teams = await fetchTeamsInLeague(league.leagueId)
      await sleep(DELAY)
      console.log(`   ${teams.length} times`)
      try { await supabase.from('debug_log').insert({ tag: 'league_teams', payload: { league: league.name, leagueId: league.leagueId, teamCount: teams.length } }) } catch {}
    } catch (e) {
      console.error(`   ❌ standings: ${e.message}`)
      try { await supabase.from('debug_log').insert({ tag: 'league_error', payload: { league: league.name, leagueId: league.leagueId, message: e.message } }) } catch {}
      continue
    }

    for (const team of teams) {
      try {
        // Fase 1: stats básicas (gols, cartões, forma)
        const apiStats = await fetchTeamStatsFromAPI(team.id, league.leagueId)
        await sleep(DELAY)

        const base = apiStats ? extractFromAPIStats(apiStats) : {}
        if (!base.matches_played) { console.log(`   ⏭️  ${team.name}: sem jogos`); continue }

        // Fase 2: corners, shots, fouls de fixtures reais
        console.log(`   🔍 ${team.name}: buscando corners/shots/fouls...`)
        const csf = await fetchCornersShootsFouls(team.id, league.leagueId)
        await sleep(DELAY)

        // Fase 3: últimos 6 jogos para exibição H2H (salvo no banco, sync não precisa chamar API)
        let recentFormJson = null
        try {
          const recentFixtures = await apiGet(`/fixtures?team=${team.id}&league=${league.leagueId}&season=${SEASON}&last=6`)
          await sleep(DELAY)
          if (!global.__debugLogged) {
            global.__debugLogged = true
            try {
              await supabase.from('debug_log').insert({
                tag: 'h2h_probe',
                payload: { team: team.name, teamId: team.id, leagueId: league.leagueId, season: SEASON, resultLength: Array.isArray(recentFixtures) ? recentFixtures.length : null, sample: Array.isArray(recentFixtures) ? recentFixtures[0] || null : recentFixtures }
              })
            } catch {}
          }
          const scores = [], matches = []
          for (const f of recentFixtures) {
            try {
              const isHome  = f.teams?.home?.id === team.id
              const myGoals = isHome ? f.goals?.home : f.goals?.away
              const ogGoals = isHome ? f.goals?.away : f.goals?.home
              if (myGoals === null || myGoals === undefined) continue
              const label = `${myGoals}-${ogGoals}`
              scores.push(label)
              matches.push({
                score: label,
                opponent: isHome ? f.teams?.away?.name : f.teams?.home?.name,
                opponentLogo: isHome ? f.teams?.away?.logo : f.teams?.home?.logo,
                isHome,
                date: f.fixture?.date
              })
            } catch (fixtureErr) {
              console.error(`      ⚠️ ${team.name}: fixture malformada ignorada — ${fixtureErr.message}`)
            }
          }
          recentFormJson = JSON.stringify({ scores, matches })
        } catch (formErr) {
          console.error(`   ⚠️ ${team.name}: falha ao buscar últimos jogos (H2H) — ${formErr.message}`)
          try {
            await supabase.from('debug_log').insert({ tag: 'h2h_error', payload: { team: team.name, message: formErr.message, stack: String(formErr.stack || '').slice(0,500) } })
          } catch {}
        }

        const row = {
          team_id:              team.id,
          league_id:            league.leagueId,
          season:               SEASON,
          team_name:            team.name,
          league:               league.name,
          logo:                 team.logo,
          ...base,
          corners_for_avg:      csf.cornersFor,
          corners_against_avg:  csf.cornersAgainst,
          shots_avg:            csf.shotsFor,
          shots_on_target_avg:  csf.shotsOnTarget,
          fouls_avg:            csf.foulsCommitted,
          recent_form_json:     recentFormJson,
          updated_at:           new Date().toISOString(),
        }

        const { error } = await supabase
          .from('team_statistics')
          .upsert(row, { onConflict: 'team_id,league_id,season' })

        if (error) throw error

        totalOk++
        console.log(`   ✅ ${team.name}: gols=${base.goals_for_avg} corners=${csf.cornersFor} shots=${csf.shotsFor} fouls=${csf.foulsCommitted}`)
      } catch (e) {
        totalFail++
        console.error(`   ❌ ${team.name}: ${e.message}`)
      }
    }
  }

  console.log(`\n📊 Concluído: ${totalOk} ok, ${totalFail} falhas`)
}

run().catch(e => { console.error('❌ Fatal:', e.message); process.exit(1) })
