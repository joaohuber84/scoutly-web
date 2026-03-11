const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

async function runScoutlyBrain() {
  console.log('🧠 Scoutly Brain iniciado')

  const { data: matches, error: matchesError } = await supabase
    .from('matches')
    .select('*')

  if (matchesError) {
    console.error('Erro ao buscar matches:', matchesError)
    process.exit(1)
  }

  if (!matches || matches.length === 0) {
    console.log('Nenhum jogo encontrado')
    return
  }

  console.log('Jogos encontrados:', matches.length)

  for (const match of matches) {
    const home_strength = Math.random() * 2
    const away_strength = Math.random() * 2

    const expected_home_goals = Math.random() * 2
    const expected_away_goals = Math.random() * 2

    const expected_home_shots = Math.random() * 15
    const expected_away_shots = Math.random() * 15

    const expected_home_sot = Math.random() * 6
    const expected_away_sot = Math.random() * 6

    const expected_corners = Math.random() * 12
    const expected_cards = Math.random() * 6

    const prob_over25 = Math.random()
    const prob_btts = Math.random()
    const prob_corners = Math.random()
    const prob_shots = Math.random()
    const prob_sot = Math.random()
    const prob_cards = Math.random()

    const best_pick_1 = 'Over 2.5 Goals'
    const best_pick_2 = 'BTTS'
    const best_pick_3 = 'Over 8.5 Corners'

    const { error: insertError } = await supabase
      .from('match_analysis')
      .insert({
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
        prob_corners,
        prob_shots,
        prob_sot,
        prob_cards,
        best_pick_1,
        best_pick_2,
        best_pick_3
      })

    if (insertError) {
      console.error(`Erro ao inserir análise do jogo ${match.id}:`, insertError)
    }
  }

  console.log('✅ Scoutly Brain finalizado')
}

runScoutlyBrain()
