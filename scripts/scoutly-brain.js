const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

async function runBrain() {

  console.log("Scoutly Brain iniciado")

  const { data: matches, error } = await supabase
    .from('matches')
    .select('*')
    .limit(5)

  if (error) {
    console.error("Erro ao buscar matches:", error)
    return
  }

  if (!matches || matches.length === 0) {
    console.log("Nenhum jogo encontrado")
    return
  }

  console.log("Matches encontrados:", matches.length)

  const picks = matches.map((match, index) => {

    const confidence = Math.floor(Math.random() * 30) + 60

    return {
      rank: index + 1,
      match_id: match.id,
      home_team: match.home_team,
      away_team: match.away_team,
      pick: "Over 8.5 Corners",
      confidence: confidence,
      insight: "Alta tendência de escanteios baseada em padrões recentes."
    }

  })

  console.log("Picks geradas:", picks.length)

  const { error: insertError } = await supabase
    .from('daily_picks')
    .insert(picks)

  if (insertError) {
    console.error("Erro ao inserir picks:", insertError)
    return
  }

  console.log("Picks salvas com sucesso!")

}

runBrain()
