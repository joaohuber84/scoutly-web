const { createClient } = require("@supabase/supabase-js")

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

  // pega jogos
  const { data: matches } = await supabase
    .from("matches")
    .select("*")
    .limit(20)

  if (!matches) {
    console.log("Nenhum jogo encontrado")
    return
  }

  let rank = 1

  for (const game of matches) {

    // lógica simples inicial
    const probability = Math.random()

    if (probability > 0.65) {

      const pick = {
        rank: rank,
        match_id: game.id,
        home_team: game.home_team,
        away_team: game.away_team,
        league: game.league || "Unknown",
        market: "Over 8.5 corners",
        probability: probability,
        is_opportunity: true
      }

      await supabase
        .from("daily_picks")
        .insert(pick)

      console.log("Pick inserida:", pick)

      rank++
    }
  }

  console.log("Brain finalizado")
}

runBrain()
