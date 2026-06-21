// supabase/functions/football-proxy/index.ts
//
// Proxy server-side para a API-Football. A chave APISPORTS_KEY nunca
// chega ao navegador — fica guardada como secret na própria função.
//
// Deploy:
//   supabase secrets set APISPORTS_KEY=705f52b4ec1295f3c369365c2d71cb71
//   supabase functions deploy football-proxy --no-verify-jwt
//
// Uso no front:
//   GET /functions/v1/football-proxy?endpoint=fixtures&live=all
//   GET /functions/v1/football-proxy?endpoint=fixtures/statistics&fixture=123
//   GET /functions/v1/football-proxy?endpoint=fixtures/events&fixture=123
//   GET /functions/v1/football-proxy?endpoint=standings&league=71&season=2026
//   GET /functions/v1/football-proxy?endpoint=players/topscorers&league=71&season=2026
//   GET /functions/v1/football-proxy?endpoint=fixtures&team=6&last=10

const APISPORTS_KEY = Deno.env.get("APISPORTS_KEY") ?? "";
const API_BASE = "https://v3.football.api-sports.io";

// Apenas os endpoints que o Scoutly realmente usa. Qualquer coisa fora
// disso é recusada — isso evita que alguém transforme seu proxy num
// "relay" aberto pra qualquer rota da API-Football.
const ALLOWED_ENDPOINTS = new Set([
  "fixtures",
  "fixtures/statistics",
  "fixtures/events",
  "standings",
  "players/topscorers",
]);

// Apenas chamadas vindas do seu próprio site (e localhost, pra dev).
// Ajuste/remova o localhost quando não precisar mais testar local.
const ALLOWED_ORIGINS = new Set([
  "https://scoutlypro.app",
  "http://localhost:3000",
  "http://127.0.0.1:5500",
]);

function buildCorsHeaders(origin: string | null): HeadersInit {
  const allow = origin && ALLOWED_ORIGINS.has(origin) ? origin : "https://scoutlypro.app";
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
    "Content-Type": "application/json",
  };
}

Deno.serve(async (req: Request) => {
  const origin = req.headers.get("origin");
  const cors = buildCorsHeaders(origin);

  if (req.method === "OPTIONS") {
    return new Response(null, { headers: cors });
  }

  if (!APISPORTS_KEY) {
    return new Response(
      JSON.stringify({ error: "APISPORTS_KEY não configurada no servidor." }),
      { status: 500, headers: cors }
    );
  }

  const url = new URL(req.url);
  const endpoint = url.searchParams.get("endpoint") || "";

  if (!ALLOWED_ENDPOINTS.has(endpoint)) {
    return new Response(
      JSON.stringify({ error: `Endpoint não permitido: ${endpoint}` }),
      { status: 400, headers: cors }
    );
  }

  // Repassa todos os outros query params, exceto "endpoint"
  const apiParams = new URLSearchParams(url.searchParams);
  apiParams.delete("endpoint");

  const apiUrl = `${API_BASE}/${endpoint}?${apiParams.toString()}`;

  try {
    const apiRes = await fetch(apiUrl, {
      headers: { "x-apisports-key": APISPORTS_KEY },
    });
    const data = await apiRes.json();
    return new Response(JSON.stringify(data), {
      status: apiRes.status,
      headers: cors,
    });
  } catch (err) {
    return new Response(
      JSON.stringify({ error: "Falha ao consultar API-Football", detail: String(err) }),
      { status: 502, headers: cors }
    );
  }
});
