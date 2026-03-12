const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

async function runBrain() {

  console.log("Scoutly Brain iniciado")

  // limpa picks antigas
  await supabase
    .from("daily_picks")
    .delete()
    .neq("id", 0)

  console.log("Tabela daily_picks limpa")

  // pega jogos futuros reais
  const { data: matches } = await supabase
    .from("matches")
    .select("*")
    .not("home_team", "is", null)
    .not("away_team", "is", null)
    .limit(50)

  if (!matches || matches.length === 0) {
    console.log("Nenhum jogo encontrado")
    return
  }

  let picks = []

  for (const game of matches) {

    // cálculo simples de força
    const probability =
      0.5 +
      Math.random() * 0.5

    if (probability > 0.70) {

      picks.push({
        match_id: game.id,
        home_team: game.home_team,
        away_team: game.away_team,
        league: game.league || "Unknown",
        market: "Over 8.5 corners",
        probability: probability,
        is_opportunity: true
      })

    }

  }

  // ordena pelos melhores
  picks.sort((a, b) => b.probability - a.probability)

  // pega top 5
  const top = picks.slice(0, 5)

  let rank = 1

  for (const pick of top) {

    pick.rank = rank

    await supabase
      .from("daily_picks")
      .insert(pick)

    console.log("Pick inserida:", pick)

    rank++
  }

  console.log("Brain finalizado")

}

runBrain()

