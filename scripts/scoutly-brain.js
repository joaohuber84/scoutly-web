const { createClient } = require("@supabase/supabase-js")

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY

if (!SUPABASE_URL) {
  throw new Error("SUPABASE_URL não encontrada nas variáveis de ambiente.")
}

if (!SUPABASE_KEY) {
  throw new Error(
    "SUPABASE_SERVICE_KEY / SUPABASE_SERVICE_ROLE_KEY não encontrada nas variáveis de ambiente."
  )
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const TIMEZONE = "America/Sao_Paulo"

function toNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function round1(value) {
  return Math.round(toNumber(value) * 10) / 10
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function formatDateInTZ(date, timeZone = TIMEZONE) {
  return new Intl.DateTimeFormat("sv-SE", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date)
}

function getTodayInTZ() {
  return formatDateInTZ(new Date(), TIMEZONE)
}

function getKickoffDateOnly(kickoff) {
  if (!kickoff) return null
  const d = new Date(kickoff)
  if (Number.isNaN(d.getTime())) return null
  return formatDateInTZ(d, TIMEZONE)
}

function normalizeText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
}

function buildMatchLabel(row) {
  return `${row.home_team} x ${row.away_team}`
}

function getStrengthLabel(score) {
  if (score >= 0.86) return "Muito forte"
  if (score >= 0.74) return "Boa"
  return "Moderada"
}

function getRhythmLabel(avgShots) {
  const shots = toNumber(avgShots)
  if (shots >= 24) return "Alto"
  if (shots >= 16) return "Moderado"
  return "Baixo"
}

function safeLeague(row) {
  return row.league || "Liga"
}

function getGameProfile(row) {
  const avgGoals = toNumber(row.avg_goals)
  const avgShots = toNumber(row.avg_shots)
  const avgCorners = toNumber(row.avg_corners)

  const over15Prob = toNumber(row.over15_prob)
  const over25Prob = toNumber(row.over25_prob)
  const under25Prob = toNumber(row.under25_prob)
  const under35Prob = toNumber(row.under35_prob)
  const bttsProb = toNumber(row.btts_prob)
  const bttsNoProb = clamp(1 - bttsProb, 0, 1)

  if (
    avgGoals >= 2.8 ||
    over25Prob >= 0.67 ||
    (avgShots >= 24 && bttsProb >= 0.62)
  ) {
    return "ofensivo"
  }

  if (
    avgGoals <= 2.1 &&
    under25Prob >= 0.70 &&
    bttsNoProb >= 0.68 &&
    avgShots <= 18
  ) {
    return "defensivo"
  }

  if (
    avgCorners >= 9.2 &&
    avgShots >= 21 &&
    avgGoals >= 2.2
  ) {
    return "corners"
  }

  if (
    under35Prob >= 0.78 &&
    avgGoals <= 2.5 &&
    avgShots <= 20
  ) {
    return "controlado"
  }

  return "equilibrado"
}

function buildInsight(row, bestPick, profile) {
  const avgGoals = round1(row.avg_goals)
  const avgCorners = round1(row.avg_corners)
  const avgShots = Math.round(toNumber(row.avg_shots))
  const rhythm = getRhythmLabel(avgShots).toLowerCase()

  const market = normalizeText(bestPick.market)

  if (market.includes("mais de 2.5 gols")) {
    return `A leitura Scoutly projeta um jogo mais aberto, com potencial real para 3 ou mais gols. A média esperada está em ${avgGoals} gols, com ritmo ofensivo ${rhythm}, reforçando essa linha como a melhor interpretação da partida.`
  }

  if (market.includes("mais de 1.5 gols")) {
    return `A leitura Scoutly projeta um confronto com boa chance de pelo menos 2 gols. A média esperada está em ${avgGoals} gols, com ritmo ofensivo ${rhythm} e cenário favorável para essa linha.`
  }

  if (market.includes("menos de 2.5 gols")) {
    return `A leitura Scoutly indica um jogo travado, com baixa projeção ofensiva e controle no placar. A expectativa está em ${avgGoals} gols, com ritmo ${rhythm}, sustentando a linha de menos de 2.5 gols.`
  }

  if (market.includes("menos de 3.5 gols")) {
    return `A leitura Scoutly indica um jogo controlado, sem expectativa de explosão ofensiva. A projeção está em ${avgGoals} gols, com ritmo ${rhythm}, tornando a linha de menos de 3.5 gols uma opção consistente.`
  }

  if (market.includes("ambas não marcam")) {
    return `A leitura Scoutly vê um confronto com baixa tendência de gols dos dois lados. A projeção ofensiva é moderada, e o cenário sugere maior chance de uma das equipes passar em branco.`
  }

  if (market.includes("ambas marcam")) {
    return `A leitura Scoutly identifica espaço para gols dos dois lados. A expectativa ofensiva, o ritmo ${rhythm} e o equilíbrio do confronto criam um cenário interessante para ambas marcam.`
  }

  if (market.includes("escanteios")) {
    return `A leitura Scoutly projeta cerca de ${avgCorners} escanteios, com ritmo ${rhythm}. Esse comportamento torna a linha de escanteios uma das melhores oportunidades estatísticas deste jogo.`
  }

  if (market.includes("dupla chance")) {
    return `A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate. O equilíbrio da partida ainda pede segurança, e por isso a dupla chance aparece como leitura mais sólida.`
  }

  return `A leitura Scoutly classifica este confronto como ${profile}, combinando projeção de ${avgGoals} gols, ${avgCorners} escanteios e ritmo ${rhythm} para destacar essa oportunidade.`
}

function pushCandidate(candidates, item) {
  candidates.push({
    ...item,
    probability: clamp(toNumber(item.probability), 0, 1),
    score: clamp(toNumber(item.score), 0, 1),
  })
}

function buildMarketCandidates(row) {
  const candidates = []

  const avgGoals = toNumber(row.avg_goals)
  const avgCorners = toNumber(row.avg_corners)
  const avgShots = toNumber(row.avg_shots)

  const over15Prob = toNumber(row.over15_prob)
  const over25Prob = toNumber(row.over25_prob)
  const under25Prob = toNumber(row.under25_prob)
  const under35Prob = toNumber(row.under35_prob)
  const bttsProb = toNumber(row.btts_prob)
  const bttsNoProb = clamp(1 - bttsProb, 0, 1)
  const cornersOver85Prob = toNumber(row.corners_over85_prob)

  const homeWin =
    row.home_result_prob != null
      ? toNumber(row.home_result_prob)
      : toNumber(row.home_win_prob)

  const draw =
    row.draw_result_prob != null
      ? toNumber(row.draw_result_prob)
      : toNumber(row.draw_prob)

  const awayWin =
    row.away_result_prob != null
      ? toNumber(row.away_result_prob)
      : toNumber(row.away_win_prob)

  const homeOrDraw = clamp(homeWin + draw, 0, 1)
  const awayOrDraw = clamp(awayWin + draw, 0, 1)

  const profile = getGameProfile(row)

  function pushCandidate(list, candidate) {
    list.push({
      ...candidate,
      probability: clamp(candidate.probability, 0, 1),
      score: clamp(candidate.score, 0, 1),
    })
  }

  // OFENSIVOS

  if (over25Prob >= 0.64) {
    let score =
      over25Prob +
      (avgGoals >= 2.7 ? 0.05 : 0) +
      (avgShots >= 22 ? 0.04 : 0)

    if (profile === "ofensivo") score += 0.06
    if (profile === "defensivo") score -= 0.10
    if (profile === "controlado") score -= 0.04

    pushCandidate(candidates, {
      market: "Mais de 2.5 gols",
      probability: over25Prob,
      score,
      family: "gols",
      subfamily: "over",
      macro: "ofensivo",
    })
  }

  if (over15Prob >= 0.75) {
    let score =
      over15Prob +
      (avgGoals >= 2.3 ? 0.04 : 0) +
      (avgShots >= 20 ? 0.04 : 0)

    if (profile === "ofensivo") score += 0.04
    if (profile === "defensivo") score -= 0.05
    if (profile === "controlado") score -= 0.02

    pushCandidate(candidates, {
      market: "Mais de 1.5 gols",
      probability: over15Prob,
      score,
      family: "gols",
      subfamily: "over",
      macro: "ofensivo",
    })
  }

  if (bttsProb >= 0.63) {
    let score =
      bttsProb +
      (avgGoals >= 2.6 ? 0.04 : 0) +
      (avgShots >= 21 ? 0.03 : 0)

    if (profile === "ofensivo") score += 0.05
    if (profile === "defensivo") score -= 0.09
    if (profile === "controlado") score -= 0.04

    pushCandidate(candidates, {
      market: "Ambas marcam",
      probability: bttsProb,
      score,
      family: "btts",
      subfamily: "yes",
      macro: "ofensivo",
    })
  }

  // DEFENSIVOS

  if (under25Prob >= 0.76) {
    let score =
      under25Prob +
      (avgGoals <= 2.1 ? 0.02 : 0) +
      (avgShots <= 17 ? 0.01 : 0)

    if (profile === "defensivo") score += 0.02
    if (profile === "ofensivo") score -= 0.10
    if (profile === "controlado") score += 0.01

    pushCandidate(candidates, {
      market: "Menos de 2.5 gols",
      probability: under25Prob,
      score,
      family: "gols",
      subfamily: "under",
      macro: "defensivo",
    })
  }

  if (under35Prob >= 0.80) {
    let score =
      under35Prob +
      (avgGoals <= 2.6 ? 0.01 : 0) +
      (avgShots <= 20 ? 0.01 : 0)

    if (profile === "controlado") score += 0.02
    if (profile === "defensivo") score += 0.01
    if (profile === "ofensivo") score -= 0.08

    pushCandidate(candidates, {
      market: "Menos de 3.5 gols",
      probability: under35Prob,
      score,
      family: "gols",
      subfamily: "under",
      macro: "defensivo",
    })
  }

  if (bttsNoProb >= 0.72) {
    let score =
      bttsNoProb +
      (avgGoals <= 2.2 ? 0.01 : 0) +
      (avgShots <= 18 ? 0.01 : 0)

    if (profile === "defensivo") score += 0.02
    if (profile === "ofensivo") score -= 0.10
    if (profile === "controlado") score += 0.01

    pushCandidate(candidates, {
      market: "Ambas não marcam",
      probability: bttsNoProb,
      score,
      family: "btts",
      subfamily: "no",
      macro: "defensivo",
    })
  }

  // ESCANTEIOS

  if (cornersOver85Prob >= 0.62) {
    let score =
      cornersOver85Prob +
      (avgCorners >= 8.7 ? 0.05 : 0) +
      (avgShots >= 20 ? 0.02 : 0)

    if (profile === "corners") score += 0.04
    if (profile === "ofensivo") score += 0.01

    pushCandidate(candidates, {
      market: "Mais de 8.5 escanteios",
      probability: cornersOver85Prob,
      score,
      family: "escanteios",
      subfamily: "over",
      macro: "estatistico",
    })
  }

  const cornersUnder105Prob = clamp(1 - Math.max(cornersOver85Prob - 0.18, 0), 0, 1)

  if (avgCorners <= 8.8 && cornersUnder105Prob >= 0.64) {
    let score =
      cornersUnder105Prob +
      (avgCorners <= 8.3 ? 0.03 : 0)

    if (profile === "corners") score -= 0.02

    pushCandidate(candidates, {
      market: "Menos de 10.5 escanteios",
      probability: cornersUnder105Prob,
      score,
      family: "escanteios",
      subfamily: "under",
      macro: "estatistico",
    })
  }

  // PROTEÇÃO

  if (homeOrDraw >= 0.72 && homeWin >= awayWin) {
    let score = homeOrDraw + (homeWin > awayWin ? 0.02 : 0)
    if (homeWin >= 0.50) score += 0.02

    pushCandidate(candidates, {
      market: `Dupla chance ${row.home_team} ou empate`,
      probability: homeOrDraw,
      score,
      family: "dupla",
      subfamily: "home_draw",
      macro: "protecao",
    })
  }

  if (awayOrDraw >= 0.72 && awayWin > homeWin) {
    let score = awayOrDraw + (awayWin > homeWin ? 0.02 : 0)
    if (awayWin >= 0.50) score += 0.02

    pushCandidate(candidates, {
      market: `Dupla chance ${row.away_team} ou empate`,
      probability: awayOrDraw,
      score,
      family: "dupla",
      subfamily: "away_draw",
      macro: "protecao",
    })
  }

  candidates.forEach(c => {
    if (c.macro === "ofensivo") {
      c.score = clamp(c.score + 0.04, 0, 1)
    }

    if (c.macro === "defensivo" && c.subfamily === "under") {
      c.score = clamp(c.score - 0.04, 0, 1)
    }

    if (c.market === "Ambas não marcam") {
      c.score = clamp(c.score - 0.01, 0, 1)
    }

  return candidates.sort((a, b) => b.score - a.score)
}

function chooseBestAndAlternatives(candidates) {
  if (!candidates.length) return { best: null, alternatives: [] }

  const sorted = [...candidates].sort((a, b) => b.score - a.score)
  const best = sorted[0]

  const alternatives = sorted
    .slice(1)
    .filter((item, index, arr) => {
      return arr.findIndex((x) => x.market === item.market) === index
    })
    .slice(0, 2)

  return { best, alternatives }
}

function buildAnalysisFromRow(row) {
  const candidates = buildMarketCandidates(row)
  if (!candidates.length) return null

  const profile = getGameProfile(row)
  const { best, alternatives } = chooseBestAndAlternatives(candidates)

  if (!best) return null

  const rhythm = getRhythmLabel(row.avg_shots)

  return {
    match_id: row.id,
    home_team: row.home_team,
    away_team: row.away_team,
    league: safeLeague(row),
    kickoff: row.kickoff,
    home_logo: row.home_logo,
    away_logo: row.away_logo,
    avg_goals: round1(row.avg_goals),
    avg_corners: round1(row.avg_corners),
    avg_shots: Math.round(toNumber(row.avg_shots)),
    game_profile: profile,
    main_pick: best.market,
    main_probability: best.probability,
    main_score: best.score,
    main_family: best.family,
    main_subfamily: best.subfamily,
    main_macro: best.macro,
    strength: getStrengthLabel(best.score),
    rhythm,
    insight: buildInsight(row, best, profile),
    alternatives,
  }
}

function chooseFeaturedAndTop5(analyses) {
  const sorted = [...analyses].sort((a, b) => {
    if (b.main_score !== a.main_score) return b.main_score - a.main_score

    const aTime = a.kickoff ? new Date(a.kickoff).getTime() : Number.MAX_SAFE_INTEGER
    const bTime = b.kickoff ? new Date(b.kickoff).getTime() : Number.MAX_SAFE_INTEGER
    return aTime - bTime
  })

  const featured = sorted[0] || null
  const remaining = sorted.filter((item) => !featured || item.match_id !== featured.match_id)

  const top5 = []
  const usedMarkets = {}
  const usedMacros = {}
  const requiredMacros = ["ofensivo","equilibrado","estatistico"]
  
  for (const item of remaining) {
    const marketCount = usedMarkets[item.main_pick] || 0
    const macroCount = usedMacros[item.main_macro] || 0

    if (marketCount >= 2) continue
    if (macroCount >= 2 && item.main.macro === "defensivo") continue

    top5.push(item)
    usedMarkets[item.main_pick] = marketCount + 1
    usedMacros[item.main_macro] = macroCount + 1
if (top5.length === 5) break
  }
  
for (const macro of requiredMacros) {
  if (top5.find(x => x.main_macro === macro)) continue
    
  const found = remaining.find(x =>
    x.main_macro === macro &&
    !top5.find(t => t.match_id === x.match_id)
  )

  if (found) {
    top5.push(found)
    usedMarkets[found.main_pick] = (usedMarkets[found.main_pick] || 0) + 1
    usedMacros[found.main_macro] = (usedMacros[found.main_macro] || 0) + 1
  }
    
    if (top5.length === 5) break
  }

  if (top5.length < 5) {
    for (const item of remaining) {
      if (top5.find((x) => x.match_id === item.match_id)) continue
      top5.push(item)
      if (top5.length === 5) break
    }
  }

console.log("DEBUG TOP5 FINAL:")
top5.forEach((item, index) => {
  console.log(
    index + 1,
    buildMatchLabel(item),
    "->",
    item.main_pick,
    "| macro:",
    item.main_macro,
    "| score:",
    item.main_score
  )
})
  
  return { featured, top5 }
}

async function loadTodaysMatches() {
  const today = getTodayInTZ()

  const { data, error } = await supabase
    .from("matches")
    .select(`
      id,
      home_team,
      away_team,
      league,
      kickoff,
      match_date,
      home_logo,
      away_logo,
      avg_goals,
      avg_corners,
      avg_shots,
      insight,
      pick,
      home_win_prob,
      draw_prob,
      away_win_prob,
      home_result_prob,
      draw_result_prob,
      away_result_prob,
      over15_prob,
      over25_prob,
      under25_prob,
      under35_prob,
      btts_prob,
      corners_over85_prob
    `)
    .order("kickoff", { ascending: true, nullsFirst: false })

  if (error) throw error

console.log("DEBUG DATA HOJE:", today)
console.log("DEBUG TOTAL RAW MATCHES:", (data || []).length)

;(data || []).slice(0, 15).forEach((row, index) => {
  const kickoffDay = getKickoffDateOnly(row.kickoff)
  const matchDay = row.match_date ? String(row.match_date) : null

  console.log(
    `RAW ${index + 1}:`,
    row.home_team,
    "x",
    row.away_team,
    "| kickoff:",
    row.kickoff,
    "| kickoffDay:",
    kickoffDay,
    "| match_date:",
    matchDay,
    "| league:",
    row.league
  )
})
  
  const filtered = (data || []).filter((row) => {
    const kickoffDay = getKickoffDateOnly(row.kickoff)
    const matchDay = row.match_date ? String(row.match_date) : null

    return kickoffDay === today || matchDay === today
  })

  return filtered
    .filter((row) => row.home_team && row.away_team && row.league)
    .filter((row) => {
     const hasCoreNumbers = true

      return hasCoreNumbers
    })
}

async function updateMatchesInsights(analyses) {
  for (const item of analyses) {
    const { error } = await supabase
      .from("matches")
      .update({
        pick: item.main_pick,
        insight: item.insight,
      })
      .eq("id", item.match_id)

    if (error) {
      console.error(`Erro ao atualizar match ${item.match_id}:`, error.message)
    }
  }
}

async function rebuildDailyPicks(featured, top5) {
  const { error: deleteError } = await supabase.from("daily_picks").delete().neq("id", 0)

  if (deleteError) throw deleteError

  const rows = []

  if (featured) {
    rows.push({
      rank: 0,
      match_id: featured.match_id,
      home_team: featured.home_team,
      away_team: featured.away_team,
      league: featured.league,
      market: featured.main_pick,
      probability: round1(featured.main_probability * 100) / 100,
      is_opportunity: true,
    })
  }

  top5.forEach((item, index) => {
    rows.push({
      rank: index + 1,
      match_id: item.match_id,
      home_team: item.home_team,
      away_team: item.away_team,
      league: item.league,
      market: item.main_pick,
      probability: round1(item.main_probability * 100) / 100,
      is_opportunity: true,
    })
  })

  if (!rows.length) {
    console.log("Nenhuma dica elegível para gravar em daily_picks.")
    return
  }

  const { error: insertError } = await supabase.from("daily_picks").insert(rows)

  if (insertError) throw insertError
}

async function runScoutlyBrain() {
  console.log("🧠 Scoutly Brain V3.0 iniciado...")

  const matches = await loadTodaysMatches()
  console.log(`📦 Jogos carregados para análise: ${matches.length}`)

  const analyses = matches
    .map(buildAnalysisFromRow)
    .filter(Boolean)

  if (!analyses.length) {
    console.log("⚠️ Nenhuma análise válida encontrada para hoje.")
    await supabase.from("daily_picks").delete().neq("id", 0)
    return
  }

  await updateMatchesInsights(analyses)

  const { featured, top5 } = chooseFeaturedAndTop5(analyses)

  await rebuildDailyPicks(featured, top5)

  console.log("✅ Scoutly Brain V3.0 finalizado com sucesso.")
  if (featured) {
    console.log(
      `⭐ Dica do dia: ${buildMatchLabel(featured)} -> ${featured.main_pick} [${featured.main_macro}]`
    )
  }
  console.log(`🔥 Top 5 gerado com ${top5.length} jogos.`)
}

runScoutlyBrain().catch((error) => {
  console.error("❌ Erro no Scoutly Brain V3.0:", error)
  process.exit(1)
})
