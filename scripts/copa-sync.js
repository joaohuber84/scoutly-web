#!/usr/bin/env node
/**
 * copa-sync.js — Sincronização dedicada da Copa do Mundo 2026
 *
 * Propósito: Garantir que os jogos da Copa do Mundo NUNCA sumam do banco,
 * independente de falhas no sync principal.
 *
 * Roda a cada hora via GitHub Actions (copa-sync.yml).
 * Não depende do sync principal — busca direto da API e grava no Supabase.
 */

const { createClient } = require("@supabase/supabase-js")

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const API_KEY      = process.env.APISPORTS_KEY
const TIMEZONE     = "America/Sao_Paulo"
const LEAGUE_ID    = 1
const SEASON       = 2026
const WINDOW_DAYS  = 8

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

function isoDate(d) { return d.toISOString().slice(0, 10) }

async function fetchCopaFixtures() {
  const now  = new Date()
  const from = isoDate(now)
  const to   = isoDate(new Date(now.getTime() + WINDOW_DAYS * 86400000))

  const url = `https://v3.football.api-sports.io/fixtures?league=${LEAGUE_ID}&season=${SEASON}&from=${from}&to=${to}&timezone=${TIMEZONE}`
  const res = await fetch(url, { headers: { "x-apisports-key": API_KEY } })
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  const data = await res.json()

  if (data.errors && Object.keys(data.errors).length > 0) {
    throw new Error(`API errors: ${JSON.stringify(data.errors)}`)
  }

  return data.response || []
}

async function upsertCopaMatch(fixture) {
  const f = fixture.fixture
  const home = fixture.teams.home
  const away = fixture.teams.away

  const { error } = await supabase.from("matches").upsert({
    id:        f.id,
    kickoff:   f.date,
    league:    "Copa do Mundo",
    country:   "World",
    region:    "international",
    priority:  100,
    home_team: home.name,
    away_team: away.name,
    home_logo: home.logo || null,
    away_logo: away.logo || null,
    updated_at: new Date().toISOString(),
  }, { onConflict: "id" })

  if (error) throw new Error(`Upsert failed for ${home.name} x ${away.name}: ${error.message}`)
}

async function refreshCopaAnalysis(fixtures) {
  const now = new Date().toISOString()
  let updated = 0

  for (const fixture of fixtures) {
    const f    = fixture.fixture
    const home = fixture.teams.home
    const away = fixture.teams.away
    if (new Date(f.date) <= new Date()) continue // só futuros

    try {
      // Busca team_statistics de cada time
      const [{ data: ts_h }, { data: ts_a }] = await Promise.all([
        supabase.from("team_statistics").select("*").eq("team_id", home.id).eq("league_id", LEAGUE_ID).eq("season", SEASON).single(),
        supabase.from("team_statistics").select("*").eq("team_id", away.id).eq("league_id", LEAGUE_ID).eq("season", SEASON).single(),
      ])

      // Busca jogos recentes de cada time (1 chamada por time)
      // Busca jogos recentes em QUALQUER liga (não só Copa) para ter ao menos 5 resultados
      const [hFixRes, aFixRes] = await Promise.all([
        fetch(`https://v3.football.api-sports.io/fixtures?team=${home.id}&last=10&status=FT`, { headers: { "x-apisports-key": API_KEY } }),
        fetch(`https://v3.football.api-sports.io/fixtures?team=${away.id}&last=10&status=FT`, { headers: { "x-apisports-key": API_KEY } }),
      ])
      const hFix = (await hFixRes.json())?.response || []
      const aFix = (await aFixRes.json())?.response || []

      const extractScores = (teamId, fixs) => {
        const scores = [], matches = []
        for (const fx of fixs) {
          const isHome = fx.teams?.home?.id === teamId
          const mg = isHome ? fx.goals?.home : fx.goals?.away
          const og = isHome ? fx.goals?.away : fx.goals?.home
          if (mg === null || mg === undefined) continue
          const label = `${mg}-${og}`
          scores.push(label)
          matches.push({ score: label, opponent: isHome ? fx.teams?.away?.name : fx.teams?.home?.name, opponentLogo: isHome ? fx.teams?.away?.logo : fx.teams?.home?.logo, isHome })
        }
        return { scores, matches }
      }

      const hScores = extractScores(home.id, hFix)
      const aScores = extractScores(away.id, aFix)

      const g = (ts, k, def) => { const v = parseFloat(ts?.[k]); return isNaN(v) ? def : v }

      const hGFor  = g(ts_h,'goals_for_avg',1.2),  hGAga = g(ts_h,'goals_against_avg',1.0)
      const aGFor  = g(ts_a,'goals_for_avg',1.1),   aGAga = g(ts_a,'goals_against_avg',1.0)
      const hCards = g(ts_h,'cards_yellow_avg',1.5), aCards = g(ts_a,'cards_yellow_avg',1.4)
      const hCorners = g(ts_h,'corners_for_avg',4.5), aCorners = g(ts_a,'corners_for_avg',4.2)
      const hFouls = g(ts_h,'fouls_avg',12.0), aFouls = g(ts_a,'fouls_avg',11.5)
      const hForm  = ts_h?.form || 'WDL', aForm = ts_a?.form || 'WDL'

      const eHG = Math.max(0.5, Math.min(2.8, hGFor*0.55 + aGAga*0.45) * 1.08)
      const eAG = Math.max(0.4, Math.min(2.4, aGFor*0.45 + hGAga*0.55) * 0.92)
      const totalG = eHG + eAG
      const expCorners = Math.max(7.0, Math.min(12.0, hCorners + aCorners))
      const expCards = Math.max(2.5, Math.min(7.0, (hCards + aCards) * 0.88))

      const pick1 = totalG > 2.4 ? 'Mais de 2.5 gols' :
                    hGFor > aGFor * 1.4 ? `Dupla chance ${home.name} ou empate` :
                    aGFor > hGFor * 1.4 ? `Dupla chance ${away.name} ou empate` : 'Mais de 1.5 gols'
      const pick2 = expCorners >= 9.5 ? 'Mais de 8.5 escanteios' : expCorners >= 8.5 ? 'Mais de 7.5 escanteios' : 'Mais de 1.5 gols'
      const pick3 = (hCards + aCards) > 3.5 ? 'Mais de 2.5 cartões' : 'Ambas marcam'

      // Árbitro
      const refName = f.referee || null
      let refStats = null
      if (refName) {
        const lastName = refName.split('. ').pop()?.split(' ')[0] || refName
        const { data: rs } = await supabase.from("referee_stats").select("*").ilike("name", `%${lastName}%`).limit(1).single()
        refStats = rs || null
        // Salva árbitro na partida
        await supabase.from("matches").update({ referee: refName }).eq("id", f.id)
      }

      const formData = {
        home_form_general: { avgGoalsFor:hGFor, avgGoalsAgainst:hGAga, avgShots:g(ts_h,'shots_avg',11), avgCorners:hCorners, avgCards:hCards, avgFouls:hFouls, matches:g(ts_h,'matches_played',3), recentScores:hScores.scores, recentMatches:hScores.matches, formStreak:hForm },
        away_form_general: { avgGoalsFor:aGFor, avgGoalsAgainst:aGAga, avgShots:g(ts_a,'shots_avg',10), avgCorners:aCorners, avgCards:aCards, avgFouls:aFouls, matches:g(ts_a,'matches_played',3), recentScores:aScores.scores, recentMatches:aScores.matches, formStreak:aForm },
        h2h: null,
      }

      // Usa RPC com COALESCE para preservar árbitro existente
      const { error } = await supabase.rpc('upsert_match_analysis', {
        p_match_id: f.id,
        p_home_strength: Math.round((hGFor*1.4 + hCards*0.15) * 100)/100,
        p_away_strength: Math.round((aGFor*1.35 + aCards*0.14) * 100)/100,
        p_expected_home_goals: Math.round(eHG * 100)/100,
        p_expected_away_goals: Math.round(eAG * 100)/100,
        p_expected_home_shots: Math.round(Math.max(7, Math.min(17, eHG*7+4))),
        p_expected_away_shots: Math.round(Math.max(6, Math.min(15, eAG*7+3))),
        p_expected_home_sot:   Math.round(Math.max(2, Math.min(6, (eHG*7+4)*0.30))),
        p_expected_away_sot:   Math.round(Math.max(2, Math.min(5, (eAG*7+3)*0.30))),
        p_expected_corners:    Math.round(expCorners * 10)/10,
        p_expected_cards:      Math.round(expCards * 10)/10,
        p_prob_over25:  Math.round(Math.max(0.30, Math.min(0.72, 0.22+(totalG-1.9)*0.22)) * 100)/100,
        p_prob_btts:    Math.round(Math.max(0.30, Math.min(0.62, 0.38+(hGFor*0.25+aGFor*0.25-0.5)*0.12)) * 100)/100,
        p_prob_corners: 0.55, p_prob_shots: 0.57, p_prob_sot: 0.50, p_prob_cards: 0.54,
        p_best_pick_1: pick1, p_best_pick_2: pick2, p_best_pick_3: pick3,
        p_aggressive_pick: null, p_analysis_text: null,
        p_form_data: formData,
        p_referee_name:      refStats?.name || refName || null,
        p_referee_avg_cards: refStats?.avg_yellow_cards || null,
        p_referee_avg_fouls: refStats?.avg_fouls || null,
      })

      if (error) console.error(`❌ Análise ${home.name} x ${away.name}: ${error.message}`)
      else { updated++; console.log(`✅ Análise ${home.name} x ${away.name}: ${pick1} | árbitro: ${refStats?.name || refName || 'N/D'}`) }
    } catch (err) {
      console.error(`❌ Análise ${home.name} x ${away.name}: ${err.message}`)
    }
  }
  return updated
}

async function run() {
  console.log("⚽ Copa Sync iniciado —", new Date().toISOString())

  if (!SUPABASE_URL || !SUPABASE_KEY || !API_KEY) {
    throw new Error("Variáveis de ambiente ausentes: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, APISPORTS_KEY")
  }

  const fixtures = await fetchCopaFixtures()
  console.log(`📡 API retornou ${fixtures.length} jogos da Copa do Mundo`)

  if (!fixtures.length) {
    console.log("⚠️  Nenhum jogo retornado — banco não é alterado")
    return
  }

  let ok = 0, fail = 0
  for (const fixture of fixtures) {
    try {
      await upsertCopaMatch(fixture)
      ok++
    } catch (err) {
      console.error(`❌ ${err.message}`)
      fail++
    }
  }

  console.log(`✅ Copa Sync concluído: ${ok} gravados, ${fail} falhas`)

  // Atualiza análise de cada jogo futuro da Copa
  const future = fixtures.filter(f => new Date(f.fixture.date) > new Date())
  console.log(`🔄 Atualizando análise de ${future.length} jogos futuros...`)
  const analysisUpdated = await refreshCopaAnalysis(future)
  console.log(`📊 ${analysisUpdated} análises atualizadas`)

  const { data: futureCount } = await supabase.from("matches").select("id", { count: "exact", head: true }).eq("league", "Copa do Mundo").gte("kickoff", new Date().toISOString())
  console.log(`📊 Jogos futuros da Copa no banco: ${futureCount?.length ?? "?"}`)
}

run().catch(err => {
  console.error("❌ Copa Sync falhou:", err.message)
  process.exit(1)
})
