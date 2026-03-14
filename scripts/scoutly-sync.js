const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API = "https://v3.football.api-sports.io"

async function api(path, params = {}) {
  const url = new URL(API + path)

  Object.entries(params).forEach(([k, v]) =>
    url.searchParams.set(k, v)
  )

  const r = await fetch(url, {
    headers: { "x-apisports-key": APISPORTS_KEY }
  })

  const j = await r.json()
  return j.response || []
}

function avg(arr) {
  if (!arr.length) return 0
  return arr.reduce((a, b) => a + b, 0) / arr.length
}

async function teamStats(teamId, league, season) {
  const games = await api("/fixtures", {
    team: teamId,
    league,
    season,
    last: 5
  })

  let goals = []
  let corners = []
  let shots = []

  for (const g of games) {
    const id = g.fixture.id

    const stats = await api("/fixtures/statistics", {
      fixture: id
    })

    const team = stats.find(s => s.team.id === teamId)

    if (!team) continue

    const map = {}

    team.statistics.forEach(s => {
      map[s.type] = s.value
    })

    goals.push(
      teamId === g.teams.home.id
        ? g.goals.home
        : g.goals.away
    )

    corners.push(Number(map["Corner Kicks"]) || 0)

    shots.push(Number(map["Total Shots"]) || 0)
  }

  return {
    goals: avg(goals),
    corners: avg(corners),
    shots: avg(shots)
  }
}

async function run() {
  console.log("Scoutly Sync matemático")

  const fixtures = await api("/fixtures", {
    next: 100
  })

  for (const f of fixtures) {
    const id = f.fixture.id

    const homeId = f.teams.home.id
    const awayId = f.teams.away.id

    const league = f.league.id
    const season = f.league.season

    const home = await teamStats(homeId, league, season)
    const away = await teamStats(awayId, league, season)

    const avgGoals = (home.goals + away.goals) / 2
    const avgCorners = (home.corners + away.corners) / 2
    const avgShots = (home.shots + away.shots) / 2

    await supabase.from("matches").upsert({
      id,
      home_team: f.teams.home.name,
      away_team: f.teams.away.name,
      league: f.league.name,
      kickoff: f.fixture.date,
      avg_goals: avgGoals,
      avg_corners: avgCorners,
      avg_shots: avgShots,
      home_logo: f.teams.home.logo,
      away_logo: f.teams.away.logo
    })
  }

  console.log("Sync matemático concluído")
}

run()
