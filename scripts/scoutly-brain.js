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
    try {
      // Base artificial por enquanto (depois a gente troca por modelo real)
      const home_strength = round(0.5 + Math.random() * 1.5)
      const away_strength = round(0.5 + Math.random() * 1.5)

      const homeEdge = 0.15
      const expected_home_goals = round(clamp((home_strength + homeEdge) * 0.9, 0.2, 3.5))
      const expected_away_goals = round(clamp(away_strength * 0.85, 0.2, 3.5))

      const expected_home_shots = round(clamp(expected_home_goals * 5 + Math.random() * 4, 3, 22))
      const expected_away_shots = round(clamp(expected_away_goals * 5 + Math.random() * 4, 3, 22))

      const expected_home_sot = round(clamp(expected_home_shots * 0.32 + Math.random(), 1, 10))
      const expected_away_sot = round(clamp(expected_away_shots * 0.32 + Math.random(), 1, 10))

      const expected_corners = round(clamp((expected_home_shots + expected_away_shots) * 0.22 + Math.random() * 2, 4, 16))
      const expected_cards = round(clamp(2 + Math.random() * 4, 1, 8))

      const totalXg = expected_home_goals + expected_away_goals
      const totalShots = expected_home_shots + expected_away_shots
      const totalSot = expected_home_sot + expected_away_sot

      // Probabilidades principais
      let home_win_prob = clamp(0.45 + (home_strength - away_strength) * 0.18 + homeEdge * 0.2)
      let away_win_prob = clamp(0.30 + (away_strength - home_strength) * 0.18)
      let draw_prob = clamp(1 - (home_win_prob + away_win_prob), 0.08, 0.34)

      // Normaliza para somar 1
      const sum3 = home_win_prob + away_win_prob + draw_prob
      home_win_prob = round(home_win_prob / sum3)
      away_win_prob = round(away_win_prob / sum3)
      draw_prob = round(draw_prob / sum3)

      const prob_over25 = round(clamp((totalXg - 1.7) / 1.6))
      const prob_btts = round(clamp(((expected_home_goals * expected_away_goals) / 1.8)))
      const prob_corners = round(clamp((expected_corners - 7.5) / 3.5))
      const prob_shots = round(clamp((totalShots - 18) / 10))
      const prob_sot = round(clamp((totalSot - 6.5) / 4))
      const prob_cards = round(clamp((expected_cards - 3) / 2.5))

      const over15_prob = round(clamp((totalXg - 0.8) / 1.4))
      const under25_prob = round(1 - prob_over25)
      const under35_prob = round(clamp((3.8 - totalXg) / 2.2))

      const corners_over85_prob = prob_corners

      // Picks
      const candidates = [
        { market: 'Over 2.5 Goals', probability: prob_over25 },
        { market: 'BTTS', probability: prob_btts },
        { market: 'Over 8.5 Corners', probability: prob_corners },
        { market: 'Shots Market', probability: prob_shots },
        { market: 'Shots on Target Market', probability: prob_sot },
        { market: 'Cards Market', probability: prob_cards }
      ].sort((a, b) => b.probability - a.probability)

      const best_pick_1 = candidates[0]?.market || 'Over 2.5 Goals'
      const best_pick_2 = candidates[1]?.market || 'BTTS'
      const best_pick_3 = candidates[2]?.market || 'Over 8.5 Corners'
      const pick = best_pick_1

      // Campos extras da tabela matches
      const avg_goals = round(totalXg)
      const avg_corners = round(expected_corners)
      const avg_shots = round(totalShots)
      const insight = `Jogo com tendência para ${pick}`
      const home_form = home_strength >= away_strength ? 'Boa' : 'Regular'
      const away_form = away_strength > home_strength ? 'Boa' : 'Regular'

      const power_home = round(home_strength)
      const power_away = round(away_strength)

      const home_result_prob = home_win_prob
      const draw_result_prob = draw_prob
      const away_result_prob = away_win_prob

      const market_odds_over25 = probToOdds(prob_over25)
      const market_odds_btts = probToOdds(prob_btts)
      const market_odds_corners85 = probToOdds(prob_corners)

      // 1) Salva / atualiza match_analysis
      const { error: analysisError } = await supabase
        .from('match_analysis')
        .upsert(
          [{
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
          }],
          { onConflict: 'match_id' }
        )

      if (analysisError) {
        console.error(`Erro ao salvar match_analysis do jogo ${match.id}:`, analysisError)
        continue
      }

      // 2) Atualiza a tabela matches com os campos finais que o app usa
      const { error: matchesUpdateError } = await supabase
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
          under35_prob,
          League: match.league || null
        })
        .eq('id', match.id)

      if (matchesUpdateError) {
        console.error(`Erro ao atualizar matches do jogo ${match.id}:`, matchesUpdateError)
        continue
      }
    } catch (err) {
      console.error(`Erro inesperado no jogo ${match.id}:`, err)
    }
  }

  console.log('✅ Scoutly Brain finalizado')
}

runScoutlyBrain()

 
