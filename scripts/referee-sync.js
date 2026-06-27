#!/usr/bin/env node
/**
 * referee-sync.js — Sincroniza histórico de árbitros
 *
 * Para cada jogo do dia com árbitro definido, busca os últimos 50 jogos
 * que esse árbitro apitou e calcula médias de cartões e faltas.
 *
 * Roda 1x por dia via referee-sync.yml
 */

const { createClient } = require('@supabase/supabase-js')

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const API_KEY      = process.env.APISPORTS_KEY
const REQUEST_DELAY_MS = 350

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }
function safe(v, d = 0) { const n = parseFloat(v); return isNaN(n) ? d : n }
function round(v, d = 2) { return Math.round(v * 10 ** d) / 10 ** d }

async function fetchRefHistory(refereeName) {
  const encoded = encodeURIComponent(refereeName)
  const url = `https://v3.football.api-sports.io/fixtures?referee=${encoded}&last=50`
  const res = await fetch(url, { headers: { 'x-apisports-key': API_KEY } })
  if (!res.ok) throw new Error(`API error ${res.status}`)
  const data = await res.json()
  return data?.response || []
}

async function fetchFixtureStats(fixtureId) {
  const url = `https://v3.football.api-sports.io/fixtures/statistics?fixture=${fixtureId}`
  const res = await fetch(url, { headers: { 'x-apisports-key': API_KEY } })
  if (!res.ok) return []
  const data = await res.json()
  return data?.response || []
}

function extractValue(stats, label) {
  const s = stats?.find(s => s.type?.toLowerCase() === label.toLowerCase())
  return s ? safe(s.value, 0) : 0
}

async function processReferee(refName) {
  const fixtures = await fetchRefHistory(refName)
  if (!fixtures.length) return null

  let totalYellow = 0, totalRed = 0, totalFouls = 0, gamesWithStats = 0
  const leaguesSet = new Set()

  for (const f of fixtures) {
    if (f.fixture.status.short !== 'FT') continue
    leaguesSet.add(f.league.name)

    try {
      const stats = await fetchFixtureStats(f.fixture.id)
      await sleep(150)

      let yellow = 0, red = 0, fouls = 0
      for (const teamStats of stats) {
        const s = teamStats.statistics || []
        yellow += extractValue(s, 'Yellow Cards')
        red    += extractValue(s, 'Red Cards')
        fouls  += extractValue(s, 'Fouls')
      }

      if (yellow > 0 || fouls > 0) {
        totalYellow += yellow
        totalRed    += red
        totalFouls  += fouls
        gamesWithStats++
      }
    } catch { /* ignora falhas individuais */ }
  }

  if (!gamesWithStats) return null

  const avgYellow = round(totalYellow / gamesWithStats)
  const avgRed    = round(totalRed / gamesWithStats, 3)
  const avgFouls  = round(totalFouls / gamesWithStats)
  const avgTotal  = round((totalYellow + totalRed) / gamesWithStats)

  // percentuais de jogos com X+ cartões (usa proxies simples)
  const pct35 = round(Math.min(0.95, Math.max(0.05, (avgYellow - 2.0) / 3.5)), 2)
  const pct45 = round(Math.min(0.90, Math.max(0.02, (avgYellow - 3.0) / 3.5)), 2)
  const pct55 = round(Math.min(0.85, Math.max(0.01, (avgYellow - 4.0) / 3.5)), 2)

  return {
    name: refName,
    total_matches: fixtures.filter(f => f.fixture.status.short === 'FT').length,
    avg_yellow_cards: avgYellow,
    avg_red_cards: avgRed,
    avg_fouls: avgFouls,
    avg_cards_total: avgTotal,
    pct_over35_cards: pct35,
    pct_over45_cards: pct45,
    pct_over55_cards: pct55,
    leagues_active: JSON.stringify([...leaguesSet].slice(0, 10)),
    sample_size: gamesWithStats,
    updated_at: new Date().toISOString(),
  }
}

async function run() {
  console.log('🟡 Referee Sync iniciado —', new Date().toISOString())

  // Pega árbitros dos jogos dos próximos 3 dias
  const { data: upcoming, error } = await supabase
    .from('matches')
    .select('referee')
    .gte('kickoff', new Date().toISOString())
    .lte('kickoff', new Date(Date.now() + 3 * 86400000).toISOString())
    .not('referee', 'is', null)

  if (error) throw new Error(error.message)

  const referees = [...new Set((upcoming || []).map(r => r.referee).filter(Boolean))]
  console.log(`📋 ${referees.length} árbitros nos próximos 3 dias`)

  let ok = 0, skipped = 0
  for (const refName of referees) {
    try {
      const stats = await processReferee(refName)
      await sleep(REQUEST_DELAY_MS)

      if (!stats) { skipped++; console.log(`   ⏭️  ${refName}: sem dados suficientes`); continue }

      await supabase.from('referee_stats').upsert(stats, { onConflict: 'name' })
      ok++
      console.log(`   ✅ ${refName}: ${stats.avg_yellow_cards} amarelos/jogo, ${stats.avg_fouls} faltas/jogo`)
    } catch (err) {
      console.error(`   ❌ ${refName}: ${err.message}`)
      skipped++
    }
  }

  console.log(`\n✅ Referee Sync concluído: ${ok} árbitros atualizados, ${skipped} sem dados`)
}

run().catch(err => {
  console.error('❌ Referee Sync falhou:', err.message)
  process.exit(1)
})
