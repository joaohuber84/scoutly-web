const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value))
}

function round(value, decimals = 6) {
  return Number(value.toFixed(decimals))
}

function probToOdds(prob) {
  const safeProb = clamp(prob, 0.05, 0.95)
  return round(1 / safeProb, 2)
}

async function runScoutlyBrain() {

  console.log("Scoutly Brain iniciado")

  const { data: matches, error } = await supabase
    .from('matches')
    .select('*')

  if (error) {
    console.error("Erro ao buscar matches", error)
    return
  }

  if (!matches || matches.length === 0) {
    console.log("Nenhum jogo encontrado")
    return
  }

  console.log(`Jogos encontrados: ${matches.length}`)

  for (const match of matches) {

    const home_strength = round(Math.random() * 2)
    const away_strength = round(Math.random() * 2)

    const expected_home_goals = round(Math.random() * 2)
    const expected_away_goals = round(Math.random() * 2)

    const expected_home_shots = round(Math.random() * 15)
    const expected_away_shots = round(Math.random() * 15)

    const expected_home_sot = round(Math.random() * 6)
    const expected_away_sot = round(Math.random() * 6)

    const expected_corners = round(Math.random() * 12)
    const expected_cards = round(Math.random() * 6)

    const prob_over25 = clamp(Math.random())
    const prob_btts = clamp(Math.random())

    const home_win_prob = clamp(Math.random())
    const away_win_prob = clamp(Math.random())
    const draw_prob = clamp(1 - (home_win_prob + away_win_prob))

    const avg_goals = expected_home_goals + expected_away_goals
    const avg_shots = expected_home_shots + expected_away_shots
    const avg_corners = expected_corners

    const best_pick_1 = "Over 2.5 Goals"
    const best_pick_2 = "BTTS"
    const best_pick_3 = "Over 8.5 Corners"

    const pick = best_pick_1

    const insight = `Modelo aponta valor em ${pick}`

    const home_form = home_strength > away_strength ? "Strong" : "Average"
    const away_form = away_strength > home_strength ? "Strong" : "Average"

    const market_odds_over25 = probToOdds(prob_over25)
    const market_odds_btts = probToOdds(prob_btts)
    const market_odds_corners85 = probToOdds(prob_over25)

    const power_home = home_strength
    const power_away = away_strength

    const home_result_prob = home_win_prob
    const draw_result_prob = draw_prob
    const away_result_prob = away_win_prob

    const over15_prob = clamp(Math.random())
    const under25_prob = clamp(Math.random())
    const under35_prob = clamp(Math.random())
    const corners_over85_prob = clamp(Math.random())

    // salva análise detalhada

    await supabase
      .from('match_analysis')
      .upsert({
        match_id: match.id,
        home_strength,
        away_strength,
        expected_home_goals,
        expected_away_goals,
        expected_home_shots,
        expected_away_shots,
        expected_home_sot,
        expected_away_sot,
        expected_corners,
        expected_cards,
        prob_over25,
        prob_btts,
        best_pick_1,
        best_pick_2,
        best_pick_3
      })

    // atualiza tabela matches

    await supabase
      .from('matches')
      .update({
        avg_goals,
        avg_corners,
        avg_shots,
        insight,
        home_win_prob,
        draw_prob,
        away_win_prob,
        home_form,
        away_form,
        over25_prob: prob_over25,
        btts_prob: prob_btts,
        corners_over85_prob,
        pick,
        power_home,
        power_away,
        home_result_prob,
        draw_result_prob,
        away_result_prob,
        market_odds_over25,
        market_odds_btts,
        market_odds_corners85,
        over15_prob,
        under25_prob,
        under35_prob
      })
      .eq('id', match.id)

  }

  console.log("Scoutly Brain finalizado")

}

runScoutlyBrain()
