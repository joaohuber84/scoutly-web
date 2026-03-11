const { createClient } = require("@supabase/supabase-js");

const APISPORTS_KEY = process.env.APISPORTS_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!APISPORTS_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Faltam variáveis de ambiente.");
}

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: { persistSession: false }
  }
);

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "x-apisports-key": APISPORTS_KEY,
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Erro API-Football ${res.status}: ${text}`);
  }

  return res.json();
}

function isoDateParts() {
  return new Date().toISOString().split("T")[0];
}

async function main() {
  const today = isoDateParts();

  console.log("Buscando jogos reais do dia:", today);

  const fixturesUrl = `https://v3.football.api-sports.io/fixtures?date=${today}`;
  const fixturesData = await fetchJson(fixturesUrl);

  const fixtures = fixturesData.response || [];
  console.log("Quantidade de jogos encontrados:", fixtures.length);

  if (!fixtures.length) {
    console.log("Nenhum jogo encontrado para hoje.");
    return;
  }

  const matches = fixtures.map((item) => ({
    home_team: item.teams?.home?.name || null,
    away_team: item.teams?.away?.name || null,
    league: item.league?.name || null,
    match_date: item.fixture?.date
      ? new Date(item.fixture.date).toISOString().slice(0, 10)
      : null,
    kickoff: item.fixture?.date || null,
    home_logo: item.teams?.home?.logo || null,
    away_logo: item.teams?.away?.logo || null,

    // campos extras opcionais, enviados como null por enquanto
    avg_goals: null,
    avg_corners: null,
    avg_shots: null,
    insight: null,
    home_win_prob: null,
    draw_prob: null,
    away_win_prob: null,
    home_form: null,
    away_form: null,
    over25_prob: null,
    btts_prob: null,
    corners_over85_prob: null,
    pick: null,
    power_home: null,
    power_away: null,
    home_result_prob: null,
    draw_result_prob: null,
    away_result_prob: null,
    market_odds_over25: null,
    market_odds_btts: null,
    market_odds_corners85: null,
    over15_prob: null,
    under25_prob: null,
    under35_prob: null,
  }));

  console.log(`Limpando tabela matches antes de inserir ${matches.length} jogos...`);

  const { error: deleteError } = await supabase
    .from("matches")
    .delete()
    .not("id", "is", null);

  if (deleteError) {
    throw new Error(`Erro ao limpar matches: ${deleteError.message}`);
  }

  console.log(`Gravando ${matches.length} jogos no Supabase...`);

  const chunkSize = 50;
  for (let i = 0; i < matches.length; i += chunkSize) {
    const chunk = matches.slice(i, i + chunkSize);

    const { error: insertError } = await supabase
      .from("matches")
      .insert(chunk);

    if (insertError) {
      throw new Error(`Erro ao gravar lote no Supabase: ${insertError.message}`);
    }

    console.log(
      `Lote ${Math.floor(i / chunkSize) + 1} gravado com sucesso (${chunk.length} jogos).`
    );
  }

  console.log("Jogos salvos com sucesso no Supabase.");
}

main().catch((err) => {
  console.error("Erro geral no Scoutly Sync:", err.message || err);
  process.exit(1);
});
