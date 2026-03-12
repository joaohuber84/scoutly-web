const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const ALLOWED_LEAGUES = {
  "Brasileirão Série A": 1.35,
  "Copa Do Brasil": 1.3,
  "UEFA Europa League": 1.28,
  "UEFA Europa Conference League": 1.22,
  "Liga Profesional Argentina": 1.18,
  "Premier League": 1.35,
  "La Liga": 1.32,
  "Serie A": 1.3,
  "Bundesliga": 1.28,
  "Ligue 1": 1.22
}

function getLeagueName(match) {
  return match.league || match.League || "Unknown"
}

function getLeagueWeight(league) {
  return ALLOWED_LEAGUES[league] || 0
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function round2(value) {
  return Math.round(value * 100) / 100
}

function getTier(probability) {
  if (probability >= 0.78) return "Muito forte"
  if (probability >= 0.68) return "Boa"
  if (probability >= 0.58) return "Moderada"
  return "Oportunidade extra"
}

function makeCandidate(match, market, probability, extraScore = 0) {
  const league = getLeagueName(match)
  const leagueWeight = getLeagueWeight(league)

  if (!leagueWeight) return null
  if (!probability || probability < 0.56) return null

  const avgCorners = Number(match.avg_corners || 0)
  const avgGoals = Number(match.avg_goals || 0)
  const avgShots = Number(match.avg_shots || 0)

  const score =
    probability * 100 * leagueWeight +
    extraScore +
    clamp(avgShots, 0, 30) * 0.35 +
    clamp(avgCorners, 0, 15) * 0.8 +
    clamp(avgGoals, 0, 5) * 1.4

  return {
    match_id: match.id,
    home_team: match.home_team,
    away_team: match.away_team,
    league,
    kickoff: match.kickoff || null,
    market,
    probability: round2(probability),
    tier: getTier(probability),
    score: round2(score),
    is_opportunity: probability >= 0.58
  }
}

function buildCandidates(match) {
  const candidates = []

  const cornersOver85Prob = Number(match.corners_over85_prob || 0)
  const over15Prob = Number(match.over15_prob || 0)
  const over25Prob = Number(match.over25_prob || 0)
  const under25Prob = Number(match.under25_prob || 0)
  const under35Prob = Number(match.under35_prob || 0)
  const bttsProb = Number(match.btts_prob || 0)

  const avgCorners = Number(match.avg_corners || 0)
  const avgGoals = Number(match.avg_goals || 0)
  const avgShots = Number(match.avg_shots || 0)

  if (cornersOver85Prob >= 0.6) {
    candidates.push(
      makeCandidate(
        match,
        "OVER 8.5 ESCANTEIOS",
        cornersOver85Prob,
        avgCorners >= 9 ? 8 : 0
      )
    )
  }

  if (under35Prob >= 0.62) {
    candidates.push(
      makeCandidate(
        match,
        "MENOS DE 3.5 GOLS",
        under35Prob,
        avgGoals <= 2.8 ? 6 : 0
      )
    )
  }

  if (over15Prob >= 0.68) {
    candidates.push(
      makeCandidate(
        match,
        "OVER 1.5 GOLS",
        over15Prob,
        avgGoals >= 2.1 ? 7 : 0
      )
    )
  }

  if (over25Prob >= 0.6) {
    candidates.push(
      makeCandidate(
        match,
        "OVER 2.5 GOLS",
        over25Prob,
        avgGoals >= 2.5 ? 8 : 0
      )
    )
  }

  if (under25Prob >= 0.62) {
    candidates.push(
      makeCandidate(
        match,
        "MENOS DE 2.5 GOLS",
        under25Prob,
        avgGoals <= 2.2 ? 7 : 0
      )
    )
  }

  if (bttsProb >= 0.6) {
    candidates.push(
      makeCandidate(
        match,
        "AMBAS MARCAM",
        bttsProb,
        avgShots >= 20 ? 6 : 0
      )
    )
  }

  const noBttsProb = 1 - bttsProb
  if (noBttsProb >= 0.62) {
    candidates.push(
      makeCandidate(
        match,
        "AMBAS NÃO MARCAM",
        noBttsProb,
        avgGoals <= 2.4 ? 6 : 0
      )
    )
  }

  return candidates.filter(Boolean)
}

function pickBestCandidatePerMatch(matches) {
  const best = []

  for (const match of matches) {
    const candidates = buildCandidates(match)
      .sort((a, b) => b.score - a.score)

    if (candidates.length) {
      best.push(candidates[0])
    }
  }

  return best
}

function diversifyTopPicks(candidates, limit = 5) {
  const sorted = [...candidates].sort((a, b) => b.score - a.score)

  const result = []
  const marketCount = {}
  const leagueCount = {}

  for (const item of sorted) {
    if (result.length >= limit) break

    const marketUsed = marketCount[item.market] || 0
    const leagueUsed = leagueCount[item.league] || 0

    if (marketUsed >= 2) continue
    if (leagueUsed >= 2) continue

    result.push(item)
    marketCount[item.market] = marketUsed + 1
    leagueCount[item.league] = leagueUsed + 1
  }

  if (result.length < limit) {
    for (const item of sorted) {
      if (result.length >= limit) break
      if (result.find(r => r.match_id === item.match_id)) continue
      result.push(item)
    }
  }

  return result.slice(0, limit).map((item, index) => ({
    ...item,
    rank: index + 1
  }))
}

async function runBrain() {
  console.log("Scoutly Brain iniciado")

  const now = new Date().toISOString()

  const { data: matches, error: matchesError } = await supabase
    .from("matches")
    .select(`
      id,
      home_team,
      away_team,
      league,
      League,
      kickoff,
      avg_goals,
      avg_corners,
      avg_shots,
      btts_prob,
      over15_prob,
      over25_prob,
      under25_prob,
      under35_prob,
      corners_over85_prob
    `)
    .gte("kickoff", now)
    .order("kickoff", { ascending: true })
    .limit(80)

  if (matchesError) {
    console.error("Erro ao buscar matches:", matchesError)
    return
  }

  if (!matches || !matches.length) {
    console.log("Nenhum jogo futuro encontrado")
    return
  }

  const filteredMatches = matches.filter(match => {
    const league = getLeagueName(match)
    return getLeagueWeight(league) > 0
  })

  console.log("Jogos futuros encontrados:", matches.length)
  console.log("Jogos válidos após filtro de ligas:", filteredMatches.length)

  const bestPerMatch = pickBestCandidatePerMatch(filteredMatches)
  const topPicks = diversifyTopPicks(bestPerMatch, 5)

  if (!topPicks.length) {
    console.log("Nenhuma pick válida encontrada")
    return
  }

  const { error: deleteError } = await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  if (deleteError) {
    console.error("Erro ao limpar daily_picks:", deleteError)
    return
  }

  const rows = topPicks.map(item => ({
    rank: item.rank,
    match_id: item.match_id,
    home_team: item.home_team,
    away_team: item.away_team,
    league: item.league,
    market: item.market,
    probability: item.probability,
    is_opportunity: item.is_opportunity
  }))

  const { error: insertError } = await supabase
    .from("daily_picks")
    .insert(rows)

  if (insertError) {
    console.error("Erro ao inserir daily_picks:", insertError)
    return
  }

  console.log("Top picks inseridas com sucesso:")
  console.table(rows)
  console.log("Brain finalizado")
}

runBrain().catch(err => {
  console.error("Erro fatal no brain:", err)
})
