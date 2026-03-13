const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_KEY ||
  process.env.SUPABASE_SERVICE_ROLE_KEY

if (!APISPORTS_KEY) {
  throw new Error("APISPORTS_KEY não encontrada.")
}

if (!SUPABASE_URL) {
  throw new Error("SUPABASE_URL não encontrada.")
}

if (!SUPABASE_KEY) {
  throw new Error("SUPABASE_SERVICE_KEY ou SUPABASE_SERVICE_ROLE_KEY não encontrada.")
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const API_BASE = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

const LEAGUES = [
  // EUROPA
  { id: 39, name: "Premier League", country: "England", season: 2025, code: "EN" },
  { id: 140, name: "La Liga", country: "Spain", season: 2025, code: "ES" },
  { id: 78, name: "Bundesliga", country: "Germany", season: 2025, code: "DE" },
  { id: 135, name: "Serie A", country: "Italy", season: 2025, code: "IT" },
  { id: 61, name: "Ligue 1", country: "France", season: 2025, code: "FR" },
  { id: 94, name: "Liga Portugal", country: "Portugal", season: 2025, code: "PT" },
  { id: 88, name: "Eredivisie", country: "Netherlands", season: 2025, code: "NL" },
  { id: 203, name: "Super Lig", country: "Turkey", season: 2025, code: "TR" },

  // AMÉRICA DO SUL
  { id: 71, name: "Brasileirão Série A", country: "Brazil", season: 2025, code: "BR" },
  { id: 128, name: "Liga Profesional Argentina", country: "Argentina", season: 2025, code: "AR" },

  // OUTRAS
  { id: 233, name: "Premier League", country: "Egypt", season: 2025, code: "EG" },
  { id: 235, name: "Premier League", country: "Russia", season: 2025, code: "RU" },
  { id: 307, name: "Pro League", country: "Saudi Arabia", season: 2025, code: "SA" },
  { id: 218, name: "Premier League", country: "Ukraine", season: 2025, code: "UA" },
  { id: 197, name: "Super League", country: "Zambia", season: 2025, code: "ZM" },
  { id: 186, name: "Ligue 1", country: "Algeria", season: 2025, code: "DZ" }
]

function toIsoDateBR(date = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  })

  return formatter.format(date)
}

function getDateRange() {
  const today = new Date()
  const todayStr = toIsoDateBR(today)

  const tomorrow = new Date(today)
  tomorrow.setDate(tomorrow.getDate() + 1)
  const tomorrowStr = toIsoDateBR(tomorrow)

  return {
    from: todayStr,
    to: tomorrowStr
  }
}

async function apiRequest(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`)

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value)
    }
  })

  const response = await fetch(url.toString(), {
    headers: {
      "x-apisports-key": APISPORTS_KEY
    }
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Erro API-Football (${response.status}): ${text}`)
  }

  return response.json()
}

function round(value, decimals = 2) {
  const num = Number(value)
  if (!Number.isFinite(num)) return null
  return Number(num.toFixed(decimals))
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function safeNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function calcProbabilities(homePower, awayPower) {
  const total = homePower + awayPower
  if (total <= 0) {
    return {
      homeWin: 0.33,
      draw: 0.34,
      awayWin: 0.33
    }
  }

  const rawHome = homePower / total
  const rawAway = awayPower / total
  const draw = clamp(0.22 + Math.abs(rawHome - rawAway) * -0.18, 0.18, 0.30)

  const remain = 1 - draw
  const homeWin = rawHome * remain
  const awayWin = rawAway * remain

  return {
    homeWin: round(homeWin, 4),
    draw: round(draw, 4),
    awayWin: round(awayWin, 4)
  }
}

function calcFormPercent(lastFixtures, teamId) {
  if (!Array.isArray(lastFixtures) || !lastFixtures.length) return null

  let points = 0
  let games = 0

  for (const item of lastFixtures) {
    const fixture = item.fixture
    const teams = item.teams
    const goals = item.goals

    if (!fixture || !teams || !goals) continue
    if (fixture.status?.short !== "FT") continue

    const isHome = teams.home?.id === teamId
    const isAway = teams.away?.id === teamId
    if (!isHome && !isAway) continue

    const gf = isHome ? safeNumber(goals.home) : safeNumber(goals.away)
    const ga = isHome ? safeNumber(goals.away) : safeNumber(goals.home)

    if (gf > ga) points += 3
    else if (gf === ga) points += 1

    games += 1
  }

  if (!games) return null
  return round((points / (games * 3)) * 100, 2)
}

function buildInsight(avgGoals, avgCorners, avgShots, homeProb, awayProb) {
  if (avgGoals >= 3) {
    return "A leitura Scoutly indica um confronto com tendência ofensiva e boa chance de gols."
  }

  if (avgCorners >= 9.5) {
    return "A leitura Scoutly projeta um jogo com volume alto de jogadas pelos lados e tendência de escanteios."
  }

  if (homeProb >= 0.6) {
    return "A leitura Scoutly vê vantagem clara para o mandante dentro do cenário da partida."
  }

  if (awayProb >= 0.6) {
    return "A leitura Scoutly vê vantagem clara para o visitante dentro do cenário da partida."
  }

  if (avgGoals <= 2.2) {
    return "A leitura Scoutly projeta um jogo mais controlado, com tendência de menos gols."
  }

  return "A leitura Scoutly indica um confronto equilibrado, sem amplo favoritismo."
}

function buildPick(avgGoals, avgCorners, homeProb, awayProb, bttsProb, under35Prob, over15Prob) {
  const options = [
    { label: "Mais de 1.5 gols", score: over15Prob || 0 },
    { label: "Menos de 3.5 gols", score: under35Prob || 0 },
    { label: "Ambas marcam", score: bttsProb || 0 },
    { label: "Ambas não marcam", score: 1 - (bttsProb || 0) },
    { label: "Mais de 8.5 escanteios", score: avgCorners >= 8.5 ? clamp(avgCorners / 12, 0, 0.95) : 0 },
    { label: "Menos de 10.5 escanteios", score: avgCorners <= 10.5 ? clamp((12 - avgCorners) / 12, 0, 0.95) : 0 },
    { label: "Dupla chance casa ou empate", score: clamp((homeProb || 0) + 0.18, 0, 0.95) },
    { label: "Dupla chance visitante ou empate", score: clamp((awayProb || 0) + 0.18, 0, 0.95) }
  ]

  options.sort((a, b) => b.score - a.score)
  return options[0]?.label || "Mais de 1.5 gols"
}

async function fetchLastFixtures(teamId, season, leagueId) {
  try {
    const data = await apiRequest("/fixtures", {
      team: teamId,
      league: leagueId,
      season,
      last: 5,
      timezone: TIMEZONE
    })

    return data.response || []
  } catch (error) {
    console.error(`Erro ao buscar últimos jogos do time ${teamId}:`, error.message)
    return []
  }
}

async function fetchFixtureStatistics(fixtureId) {
  try {
    const data = await apiRequest("/fixtures/statistics", {
      fixture: fixtureId
    })

    return data.response || []
  } catch (error) {
    console.error(`Erro ao buscar estatísticas do fixture ${fixtureId}:`, error.message)
    return []
  }
}

function getStatValue(statsArray, typeName) {
  const found = statsArray.find((item) => item.type === typeName)
  if (!found) return 0

  const value = found.value

  if (value === null || value === undefined) return 0

  if (typeof value === "string" && value.includes("%")) {
    return safeNumber(value.replace("%", ""))
  }

  return safeNumber(value)
}

async function runScoutlySync() {
  console.log("🚀 Scoutly Sync iniciado...")

  const { from, to } = getDateRange()
  console.log(`📅 Buscando jogos entre ${from} e ${to}`)

  const allMatches = []
  const allStats = []

  for (const league of LEAGUES) {
    console.log(`🔎 Buscando liga: ${league.country} - ${league.name}`)

    let fixtures = []

    try {
      const data = await apiRequest("/fixtures", {
        league: league.id,
        season: league.season,
        from,
        to,
        timezone: TIMEZONE
      })

      fixtures = data.response || []
    } catch (error) {
      console.error(`Erro na liga ${league.name}:`, error.message)
      continue
    }

    for (const item of fixtures) {
      const fixture = item.fixture
      const teams = item.teams
      const goals = item.goals

      if (!fixture || !teams?.home || !teams?.away) continue

      const fixtureId = fixture.id
      const homeId = teams.home.id
      const awayId = teams.away.id

      const kickoff = fixture.date || null
      const matchDate = kickoff ? kickoff.slice(0, 10) : null

      const lastHome = await fetchLastFixtures(homeId, league.season, league.id)
      const lastAway = await fetchLastFixtures(awayId, league.season, league.id)
      const fixtureStats = await fetchFixtureStatistics(fixtureId)

      const homeStats = fixtureStats.find((x) => x.team?.id === homeId)?.statistics || []
      const awayStats = fixtureStats.find((x) => x.team?.id === awayId)?.statistics || []

      const homeShots = getStatValue(homeStats, "Total Shots")
      const awayShots = getStatValue(awayStats, "Total Shots")
      const homeShotsOnTarget = getStatValue(homeStats, "Shots on Goal")
      const awayShotsOnTarget = getStatValue(awayStats, "Shots on Goal")
      const homeCorners = getStatValue(homeStats, "Corner Kicks")
      const awayCorners = getStatValue(awayStats, "Corner Kicks")
      const homeYellow = getStatValue(homeStats, "Yellow Cards")
      const awayYellow = getStatValue(awayStats, "Yellow Cards")

      const avgGoals =
        round(
          safeNumber(goals.home) +
            safeNumber(goals.away) +
            1.1,
          2
        ) || 2.2

      const avgCorners =
        round(
          homeCorners + awayCorners > 0
            ? homeCorners + awayCorners
            : 8.2,
          2
        ) || 8.2

      const avgShots =
        round(
          homeShots + awayShots > 0
            ? homeShots + awayShots
            : 20,
          2
        ) || 20

      const homePower =
        round(
          (homeShotsOnTarget * 0.35) +
            (homeShots * 0.08) +
            (homeCorners * 0.05) +
            0.4,
          4
        ) || 0.4

      const awayPower =
        round(
          (awayShotsOnTarget * 0.35) +
            (awayShots * 0.08) +
            (awayCorners * 0.05) +
            0.4,
          4
        ) || 0.4

      const probs = calcProbabilities(homePower, awayPower)

      const homeForm = calcFormPercent(lastHome, homeId)
      const awayForm = calcFormPercent(lastAway, awayId)

      const over15Prob = clamp(avgGoals / 3.1, 0.15, 0.97)
      const over25Prob = clamp((avgGoals - 1.2) / 2.2, 0.05, 0.95)
      const under25Prob = clamp(1 - over25Prob, 0.05, 0.95)
      const under35Prob = clamp(1 - ((avgGoals - 1.8) / 2.6), 0.08, 0.97)
      const bttsProb = clamp(
        ((homeShotsOnTarget + awayShotsOnTarget) / 10) * 0.65 +
          (avgGoals / 4) * 0.35,
        0.08,
        0.95
      )
      const cornersOver85Prob = clamp(avgCorners / 12, 0.08, 0.95)

      const pick = buildPick(
        avgGoals,
        avgCorners,
        probs.homeWin,
        probs.awayWin,
        bttsProb,
        under35Prob,
        over15Prob
      )

      const insight = buildInsight(
        avgGoals,
        avgCorners,
        avgShots,
        probs.homeWin,
        probs.awayWin
      )

      allMatches.push({
        id: fixtureId,
        created_at: new Date().toISOString(),
        match_date: matchDate,
        league: league.name,
        kickoff,
        home_team: teams.home.name,
        away_team: teams.away.name,
        home_logo: teams.home.logo,
        away_logo: teams.away.logo,
        avg_goals: avgGoals,
        avg_corners: avgCorners,
        avg_shots: avgShots,
        insight,
        home_win_prob: probs.homeWin,
        draw_prob: probs.draw,
        away_win_prob: probs.awayWin,
        home_form: homeForm,
        away_form: awayForm,
        over25_prob: round(over25Prob, 4),
        btts_prob: round(bttsProb, 4),
        corners_over85_prob: round(cornersOver85Prob, 4),
        pick,
        power_home: homePower,
        power_away: awayPower,
        home_result_prob: probs.homeWin,
        draw_result_prob: probs.draw,
        away_result_prob: probs.awayWin,
        market_odds_over25: null,
        market_odds_btts: null,
        market_odds_corners85: null,
        over15_prob: round(over15Prob, 4),
        under25_prob: round(under25Prob, 4),
        under35_prob: round(under35Prob, 4)
      })

      allStats.push({
        match_id: fixtureId,
        created_at: new Date().toISOString(),
        home_shots: homeShots,
        home_shots_on_target: homeShotsOnTarget,
        home_corners: homeCorners,
        home_yellow_cards: homeYellow,
        away_shots: awayShots,
        away_shots_on_target: awayShotsOnTarget,
        away_corners: awayCorners,
        away_yellow_cards: awayYellow
      })
    }
  }

  console.log(`📦 Matches encontradas: ${allMatches.length}`)
  console.log(`📊 Stats encontradas: ${allStats.length}`)

  if (allMatches.length > 0) {
    const { error } = await supabase
      .from("matches")
      .upsert(allMatches, { onConflict: "id" })

    if (error) {
      console.error("Erro ao salvar matches:", error)
      throw error
    }
  }

  if (allStats.length > 0) {
    const { error } = await supabase
      .from("match_stats")
      .upsert(allStats, { onConflict: "match_id" })

    if (error) {
      console.error("Erro ao salvar match_stats:", error)
      throw error
    }
  }

  console.log("✅ Scoutly Sync finalizado com sucesso.")
}

runScoutlySync().catch((error) => {
  console.error("❌ Erro no Scoutly Sync:", error)
  process.exit(1)
})
