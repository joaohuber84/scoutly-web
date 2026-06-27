#!/usr/bin/env node
/**
 * team-stats-sync.js — Sincroniza estatísticas de times por liga/temporada
 *
 * Usa /teams/statistics da API-Football que retorna médias reais de gols,
 * escanteios, cartões e finalizações dentro de uma liga específica.
 * Muito mais preciso que calcular dos match_stats (que mistura amistosos).
 *
 * Roda 1x por dia via team-stats-sync.yml
 */

const { createClient } = require('@supabase/supabase-js')

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const API_KEY      = process.env.APISPORTS_KEY
const SEASON       = 2026

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

// Ligas que monitoramos + seus IDs na API
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
]

const REQUEST_DELAY_MS = 400

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

function safe(v, def = 0) {
  const n = parseFloat(v)
  return isNaN(n) ? def : n
}

function round(v, d = 2) { return Math.round(v * 10 ** d) / 10 ** d }

async function fetchTeamsInLeague(leagueId) {
  const url = `https://v3.football.api-sports.io/standings?league=${leagueId}&season=${SEASON}`
  const res = await fetch(url, { headers: { 'x-apisports-key': API_KEY } })
  if (!res.ok) throw new Error(`API standings error ${res.status}`)
  const data = await res.json()
  const groups = data?.response?.[0]?.league?.standings || []
  const teams = []
  for (const group of groups) {
    for (const entry of group) {
      teams.push({ id: entry.team.id, name: entry.team.name, logo: entry.team.logo })
    }
  }
  return teams
}

async function fetchTeamStats(teamId, leagueId) {
  const url = `https://v3.football.api-sports.io/teams/statistics?team=${teamId}&league=${leagueId}&season=${SEASON}`
  const res = await fetch(url, { headers: { 'x-apisports-key': API_KEY } })
  if (!res.ok) throw new Error(`API team stats error ${res.status} for team ${teamId}`)
  const data = await res.json()
  return data?.response || null
}

async function upsertTeamStats(row) {
  const { error } = await supabase.from('team_statistics').upsert(row, { onConflict: 'team_id,league_id,season' })
  if (error) throw new Error(`Upsert failed for team ${row.team_id}: ${error.message}`)
}

function extractStats(s) {
  if (!s) return {}
  const goals    = s.goals || {}
  const shots    = s.shots || {}
  const cards    = s.cards || {}
  const corners  = s.corners || {}
  const fouls    = s.fouls || {}
  const fixtures = s.fixtures || {}

  const played = safe(fixtures.played?.total, 0)
  const playedHome = safe(fixtures.played?.home, 0)
  const playedAway = safe(fixtures.played?.away, 0)

  if (!played) return {}

  const totalGoalsFor = safe(goals.for?.total?.total, 0)
  const totalGoalsAgainst = safe(goals.against?.total?.total, 0)
  const homeGoalsFor = safe(goals.for?.total?.home, 0)
  const homeGoalsAgainst = safe(goals.against?.total?.home, 0)
  const awayGoalsFor = safe(goals.for?.total?.away, 0)
  const awayGoalsAgainst = safe(goals.against?.total?.away, 0)

  // Gols médias
  const goalsForAvg = played > 0 ? round(totalGoalsFor / played) : null
  const goalsAgainstAvg = played > 0 ? round(totalGoalsAgainst / played) : null
  const homeGoalsForAvg = playedHome > 0 ? round(homeGoalsFor / playedHome) : null
  const homeGoalsAgainstAvg = playedHome > 0 ? round(homeGoalsAgainst / playedHome) : null
  const awayGoalsForAvg = playedAway > 0 ? round(awayGoalsFor / playedAway) : null
  const awayGoalsAgainstAvg = playedAway > 0 ? round(awayGoalsAgainst / playedAway) : null

  // Escanteios
  const cornersForAvg = safe(corners.for?.average?.total, null)
  const cornersAgainstAvg = safe(corners.against?.average?.total, null)
  const homeCornersForAvg = safe(corners.for?.average?.home, null)
  const homeCornersAgainstAvg = safe(corners.against?.average?.home, null)
  const awayCornersForAvg = safe(corners.for?.average?.away, null)
  const awayCornersAgainstAvg = safe(corners.against?.average?.away, null)

  // Chutes
  const shotsAvg = safe(shots.total?.average, null)
  const shotsOnTargetAvg = safe(shots.on?.average, null)

  // Cartões
  const yellowAvg = cards.yellow ? round(
    Object.values(cards.yellow).reduce((sum, v) => sum + safe(v?.total, 0), 0) / played
  ) : null
  const redAvg = cards.red ? round(
    Object.values(cards.red).reduce((sum, v) => sum + safe(v?.total, 0), 0) / played
  ) : null

  // Faltas
  const foulsAvg = fouls.committed ? round(
    Object.values(fouls.committed).reduce((sum, v) => sum + safe(v?.total, 0), 0) / played
  ) : null

  // Percentuais de gols
  const over15Count = Object.values(goals.for?.minute || {}).reduce((sum, v) => {
    return sum // complexo de calcular diretamente, pula por ora
  }, 0)

  // Clean sheets
  const cleanSheets = safe(fixtures.wins?.total, 0)
  const cleanSheetPct = played > 0 ? round(safe(s.clean_sheet?.total, 0) / played * 100) : null

  // Forma
  const form = s.form || null

  return {
    goals_for_avg: goalsForAvg,
    goals_against_avg: goalsAgainstAvg,
    home_goals_for_avg: homeGoalsForAvg,
    home_goals_against_avg: homeGoalsAgainstAvg,
    away_goals_for_avg: awayGoalsForAvg,
    away_goals_against_avg: awayGoalsAgainstAvg,
    corners_for_avg: cornersForAvg > 0 ? cornersForAvg : null,
    corners_against_avg: cornersAgainstAvg > 0 ? cornersAgainstAvg : null,
    home_corners_for_avg: homeCornersForAvg > 0 ? homeCornersForAvg : null,
    home_corners_against_avg: homeCornersAgainstAvg > 0 ? homeCornersAgainstAvg : null,
    away_corners_for_avg: awayCornersForAvg > 0 ? awayCornersForAvg : null,
    away_corners_against_avg: awayCornersAgainstAvg > 0 ? awayCornersAgainstAvg : null,
    shots_avg: shotsAvg > 0 ? shotsAvg : null,
    shots_on_target_avg: shotsOnTargetAvg > 0 ? shotsOnTargetAvg : null,
    cards_yellow_avg: yellowAvg,
    cards_red_avg: redAvg,
    fouls_avg: foulsAvg,
    clean_sheet_pct: cleanSheetPct,
    matches_played: played,
    form: form ? form.slice(-5) : null,
  }
}

async function run() {
  console.log('📊 Team Stats Sync iniciado —', new Date().toISOString())

  let totalTeams = 0, totalOk = 0, totalFail = 0

  for (const league of TARGET_LEAGUES) {
    console.log(`\n🏆 ${league.name} (league_id=${league.leagueId})`)

    let teams
    try {
      teams = await fetchTeamsInLeague(league.leagueId)
      console.log(`   ${teams.length} times encontrados`)
      await sleep(REQUEST_DELAY_MS)
    } catch (err) {
      console.error(`   ❌ Falha buscando times: ${err.message}`)
      continue
    }

    for (const team of teams) {
      totalTeams++
      try {
        const stats = await fetchTeamStats(team.id, league.leagueId)
        await sleep(REQUEST_DELAY_MS)

        if (!stats || !stats.fixtures?.played?.total) {
          console.log(`   ⏭️  ${team.name}: sem dados`)
          continue
        }

        const extracted = extractStats(stats)
        if (!Object.keys(extracted).length) continue

        await upsertTeamStats({
          team_id: team.id,
          league_id: league.leagueId,
          season: SEASON,
          team_name: team.name,
          league: league.name,
          logo: team.logo,
          ...extracted,
          updated_at: new Date().toISOString(),
        })

        totalOk++
        console.log(`   ✅ ${team.name}: corners ${extracted.corners_for_avg ?? '?'} for / ${extracted.corners_against_avg ?? '?'} against`)
      } catch (err) {
        totalFail++
        console.error(`   ❌ ${team.name}: ${err.message}`)
      }
    }
  }

  console.log(`\n📊 Resumo: ${totalOk}/${totalTeams} times atualizados, ${totalFail} falhas`)
}

run().catch(err => {
  console.error('❌ Team Stats Sync falhou:', err.message)
  process.exit(1)
})
