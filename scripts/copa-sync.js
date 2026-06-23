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

  // Verifica quantos jogos futuros estão no banco
  const { data: future } = await supabase
    .from("matches")
    .select("id", { count: "exact", head: true })
    .eq("league", "Copa do Mundo")
    .gte("kickoff", new Date().toISOString())

  console.log(`📊 Jogos futuros da Copa no banco: ${future?.length ?? "?"}`)
}

run().catch(err => {
  console.error("❌ Copa Sync falhou:", err.message)
  process.exit(1)
})
