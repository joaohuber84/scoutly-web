const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY

if (!APISPORTS_KEY) {
  throw new Error("APISPORTS_KEY não encontrada.")
}

if (!SUPABASE_URL) {
  throw new Error("SUPABASE_URL não encontrada.")
}

if (!SUPABASE_KEY) {
  throw new Error("SUPABASE_SERVICE_ROLE_KEY não encontrada.")
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

async function api(path, params = {}) {
  const url = new URL(API + path)

  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") {
      url.searchParams.set(k, String(v))
    }
  })

  const r = await fetch(url, {
    headers: { "x-apisports-key": APISPORTS_KEY }
  })

  if (!r.ok) {
    const text = await r.text()
    throw new Error(`API erro ${r.status}: ${text}`)
  }

  const j = await r.json()
  return j.response || []
}

function avg(arr) {
  if (!arr.length) return 0
  return arr.reduce((a, b) => a + b, 0) / arr.length
}

function safeNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function round(value, decimals = 2) {
  return Number(safeNumber(value).toFixed(decimals))
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function toDateOnly(iso) {
  if (!iso) return null
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return null
  return d.toISOString().slice(0, 10)
}

function buildLeagueLabel(leagueObj) {
  const country = String(leagueObj?.country || "").trim()
  const leagueName = String(leagueObj?.name || "").trim()

  if (!country && !leagueName) return "Liga não informada"
  if (!country) return leagueName
  if (!leagueName) return country

  // evita confusão tipo Premier League / Super League / Pro League
  return `${country} - ${leagueName}`
}

function getStatValue(statistics = [], type) {
  const found = statistics.find((s) => s.type === type)
  if (!found) return 0

  const value = found.value

  if (value === null || value === undefined) return 0

  if (typeof value === "string") {
    const cleaned = value.replace("%", "").trim()
    const num = Number(cleaned)
    return Number.isFinite(num) ? num : 0
  }

  return safeNumber(value)
}

async function teamStats(teamId, leagueId, season) {
  const games = await api("/fixtures", {
    team: teamId,
    league: leagueId,
    season,
    last: 5,
    timezone: TIMEZONE
  })

  let goalsFor = []
  let goalsAgainst = []
  let corners = []
  let shots = []
  let shotsOnTarget = []
  let yellowCards = []
  let points = []

  for (const g of games) {
    const fixtureId = g?.fixture?.id
    if (!fixtureId) continue

    const isHome = g?.teams?.home?.id === teamId
    const gf = isHome ? safeNumber(g?.goals?.home) : safeNumber(g?.goals?.away)
    const ga = isHome ? safeNumber(g?.goals?.away) : safeNumber(g?.goals?.home)

    goalsFor.push(gf)
    goalsAgainst.push(ga)

    if (gf > ga) points.push(3)
    else if (gf === ga) points.push(1)
    else points.push(0)

    const stats = await api("/fixtures/statistics", {
      fixture: fixtureId
    })

    const team = stats.find((s) => s?.team?.id === teamId)
    if (!team) continue

    corners.push(getStatValue(team.statistics, "Corner Kicks"))
    shots.push(getStatValue(team.statistics, "Total Shots"))
    shotsOnTarget.push(getStatValue(team.statistics, "Shots on Goal"))
    yellowCards.push(getStatValue(team.statistics, "Yellow Cards"))
  }

  const gamesCount = Math.max(points.length, 1)

  return {
    goalsFor: round(avg(goalsFor)),
    goalsAgainst: round(avg(goalsAgainst)),
    corners: round(avg(corners)),
    shots: round(avg(shots)),
    shotsOnTarget: round(avg(shotsOnTarget)),
    yellowCards: round(avg(yellowCards)),
    form: round((points.reduce((a, b) => a + b, 0) / (gamesCount * 3)) * 100)
  }
}

function buildProbabilities(home, away) {
  const homeStrength =
    home.goalsFor * 1.25 +
    home.shotsOnTarget * 0.55 +
    home.corners * 0.12 +
    home.form * 0.018

  const awayStrength =
    away.goalsFor * 1.2 +
    away.shotsOnTarget * 0.5 +
    away.corners * 0.1 +
    away.form * 0.016

  const total = Math.max(homeStrength + awayStrength, 0.01)

  let homeWin = homeStrength / total
  let awayWin = awayStrength / total
  let draw = 1 - (homeWin + awayWin)

  // estabiliza o empate
  draw = clamp(0.18 + Math.abs(homeWin - awayWin) * -0.12, 0.18, 0.3)

  const remain = 1 - draw
  const ratioTotal = Math.max(homeWin + awayWin, 0.01)

  homeWin = (homeWin / ratioTotal) * remain
  awayWin = (awayWin / ratioTotal) * remain

  return {
    homeWin: round(homeWin, 4),
    draw: round(draw, 4),
    awayWin: round(awayWin, 4),
    powerHome: round(homeStrength, 4),
    powerAway: round(awayStrength, 4)
  }
}

function buildGoalAndCornerModel(home, away) {
  const avgGoals = round((home.goalsFor + away.goalsFor + home.goalsAgainst + away.goalsAgainst) / 2)
  const avgCorners = round((home.corners + away.corners) / 2)
  const avgShots = round((home.shots + away.shots) / 2)

  const over15Prob = clamp(avgGoals / 3.1, 0.18, 0.95)
  const over25Prob = clamp((avgGoals - 1.2) / 2.0, 0.08, 0.9)
  const under25Prob = clamp(1 - over25Prob, 0.1, 0.92)
  const under35Prob = clamp(1 - ((avgGoals - 1.8) / 2.4), 0.1, 0.95)
  const bttsProb = clamp((home.goalsFor + away.goalsFor) / 4, 0.12, 0.88)
  const cornersOver85Prob = clamp(avgCorners / 11.5, 0.12, 0.9)

  return {
    avgGoals: round(avgGoals),
    avgCorners: round(avgCorners),
    avgShots: round(avgShots),
    over15Prob: round(over15Prob, 4),
    over25Prob: round(over25Prob, 4),
    under25Prob: round(under25Prob, 4),
    under35Prob: round(under35Prob, 4),
    bttsProb: round(bttsProb, 4),
    cornersOver85Prob: round(cornersOver85Prob, 4)
  }
}

function buildBasePick(model, probs, homeTeam, awayTeam) {
  const candidates = [
    { market: "Mais de 1.5 gols", score: model.over15Prob },
    { market: "Menos de 3.5 gols", score: model.under35Prob },
    { market: "Ambas marcam", score: model.bttsProb },
    { market: "Ambas não marcam", score: 1 - model.bttsProb },
    { market: "Mais de 8.5 escanteios", score: model.cornersOver85Prob },
    { market: "Menos de 10.5 escanteios", score: 1 - Math.max(model.cornersOver85Prob - 0.15, 0) },
    { market: `Dupla chance ${homeTeam} ou empate`, score: probs.homeWin + probs.draw },
    { market: `Dupla chance ${awayTeam} ou empate`, score: probs.awayWin + probs.draw }
  ]

  candidates.sort((a, b) => b.score - a.score)
  return candidates[0]?.market || "Mais de 1.5 gols"
}

async function run() {
  console.log("🚀 Scoutly Sync matemático corrigido")

  const fixtures = await api("/fixtures", {
    next: 100,
    timezone: TIMEZONE
  })

  for (const f of fixtures) {
    const fixtureId = f?.fixture?.id
    const homeId = f?.teams?.home?.id
    const awayId = f?.teams?.away?.id
    const leagueId = f?.league?.id
    const season = f?.league?.season

    if (!fixtureId || !homeId || !awayId || !leagueId || !season) continue

    const home = await teamStats(homeId, leagueId, season)
    const away = await teamStats(awayId, leagueId, season)

    const model = buildGoalAndCornerModel(home, away)
    const probs = buildProbabilities(home, away)
    const pick = buildBasePick(
      model,
      probs,
      f?.teams?.home?.name || "Casa",
      f?.teams?.away?.name || "Visitante"
    )

    const kickoff = f?.fixture?.date || null
    const matchDate = toDateOnly(kickoff)

    const leagueLabel = buildLeagueLabel(f.league)

    await supabase.from("matches").upsert({
      id: fixtureId,
      home_team: f?.teams?.home?.name || null,
      away_team: f?.teams?.away?.name || null,
      league: leagueLabel,
      kickoff,
      match_date: matchDate,
      avg_goals: model.avgGoals,
      avg_corners: model.avgCorners,
      avg_shots: model.avgShots,
      home_logo: f?.teams?.home?.logo || null,
      away_logo: f?.teams?.away?.logo || null,

      home_win_prob: probs.homeWin,
      draw_prob: probs.draw,
      away_win_prob: probs.awayWin,

      home_result_prob: probs.homeWin,
      draw_result_prob: probs.draw,
      away_result_prob: probs.awayWin,

      power_home: probs.powerHome,
      power_away: probs.powerAway,

      home_form: home.form,
      away_form: away.form,

      over15_prob: model.over15Prob,
      over25_prob: model.over25Prob,
      under25_prob: model.under25Prob,
      under35_prob: model.under35Prob,
      btts_prob: model.bttsProb,
      corners_over85_prob: model.cornersOver85Prob,

      pick
    })

    await supabase.from("match_stats").upsert({
      match_id: fixtureId,

      home_shots: home.shots,
      home_shots_on_target: home.shotsOnTarget,
      home_corners: home.corners,
      home_yellow_cards: home.yellowCards,

      away_shots: away.shots,
      away_shots_on_target: away.shotsOnTarget,
      away_corners: away.corners,
      away_yellow_cards: away.yellowCards
    })
  }

  console.log("✅ Sync matemático corrigido concluído")
}

run().catch((error) => {
  console.error("❌ Erro no Scoutly Sync:", error)
  process.exit(1)
})

