const { createClient } = require("@supabase/supabase-js")
const WebSocket = require("ws")

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY

if (!SUPABASE_URL) throw new Error("SUPABASE_URL não encontrada.")
if (!SUPABASE_KEY) throw new Error("SUPABASE_SERVICE_KEY não encontrada.")

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
  realtime: { transport: WebSocket },
  global: { headers: { "X-Client-Info": "scoutly-brain-v4" } },
})

/**
 * SCOUTLY BRAIN V4.3
 * [FIX 1] chooseBestAndAlternatives — removido usedFamilies
 * [FIX 2] buildMarketCandidates — thresholds relaxed 0.52→0.50
 * [FIX 3] rebuildDailyPicks — delete só picks FUTUROS → histórico preservado
 * [FIX 4] RADAR_BLACKLIST — todas as copas nacionais removidas.
 *         Agora aparecem em competições E no radar quando tiverem picks fortes.
 *         Mantida apenas "Copa Sul-Sudeste" (sem dados confiáveis na API-Football).
 */

const TIMEZONE = "America/Sao_Paulo"
const PAST_GRACE_HOURS = 3
const RADAR_SIZE = 40
// Ligas prioritárias: sem limite de jogos por liga no radar (Copa do Mundo pode ter 4+ jogos/dia)
const RADAR_UNCAPPED_LEAGUES = new Set([
  "Copa do Mundo","Copa do Mundo 2026","FIFA World Cup","FIFA World Cup 2026","World Cup",
  "Copa do Brasil","Brasileirão Série A","Brasileirão Série B",
])
const TICKET_MIN_SIZE = 2
const TICKET_MAX_SIZE = 3

const LEAGUE_TIER = {
  "Brasileirão Série A":1,"Brasileirão Série B":1,"Copa do Brasil":1,
  "Premier League":1,"FA Cup":1,"EFL Cup":1,"Championship":1,
  "La Liga":1,"Copa del Rey":1,"Serie A":1,"Coppa Italia":1,
  "Bundesliga":1,"DFB-Pokal":1,"Ligue 1":1,"Coupe de France":1,
  "Liga Argentina":1,"Copa Argentina":1,"Primeira Liga":1,"Taça de Portugal":1,
  "Liga MX":1,"Copa MX":1,"MLS":1,"Saudi Pro League":1,
  "UEFA Champions League":2,"UEFA Europa League":2,"UEFA Conference League":2,
  "Libertadores":2,"Sul-Americana":2,
  "Copa do Mundo":1,"Copa do Mundo 2026":1,"FIFA World Cup":1,"FIFA World Cup 2026":1,"World Cup":1,"Eurocopa":1,
  "Copa América":2,"Nations League":2,"Mundial de Clubes":2,
  "CONCACAF Champions Cup":2,"Eliminatórias Sul-Americanas":2,
  "Eliminatórias Europeias":2,"Eliminatórias Africanas":2,
  "Eliminatórias Asiáticas":2,"Eliminatórias CONCACAF":2,"Copa Africana":2,
  "Eredivisie":3,"KNVB Cup":3,"Super Lig":3,"Belgian Pro League":3,
  "Austrian Bundesliga":3,"Superliga":3,"Super League Greece":3,
  "Scottish Premiership":3,"Allsvenskan":3,"Eliteserien":3,
  "Amistosos Internacionais":2,"Liga Chilena":3,"Liga Colombiana":3,
  "Liga Peruana":3,"Liga Uruguaia":3,"Copa do Nordeste":3,
  "Copa Verde":3,"Recopa Sul-Americana":3,"Supercopa Argentina":3,
  "Taça da Liga":4,"Copa da Turquia":4,"Copa da Bélgica":4,"Copa da Áustria":4,
  "Copa da Grécia":4,"Copa da Dinamarca":4,"Scottish Cup":4,"Copa Chile":4,
  "Copa Colombia":4,"Leagues Cup":4,"Copa Sul-Sudeste":4,
}

// Amistosos e ligas sem confiabilidade bloqueadas do radar
const RADAR_BLACKLIST = new Set([
  "Copa Sul-Sudeste",
  "Amistosos Internacionais",
])

function getLeagueTierScore(league) {
  const tier = LEAGUE_TIER[String(league||"")] ?? 4
  return tier === 1 ? 1000 : tier === 2 ? 500 : tier === 3 ? 200 : 50
}

const FAMILY_SCORE_WEIGHT = {
  gols:1.0, resultado:0.98, escanteios:0.97, cards:0.95,
  btts:0.92, sot:0.91, shots:0.88, outro:0.7,
}

const CORNER_OVER_LINES  = [6.5,7.5,8.5]
const CORNER_UNDER_LINES = [11.5,12.5,13.5]
const CARDS_OVER_LINES   = [1.5,2.5,3.5,4.5]
const SHOTS_OVER_LINES   = [17.5,19.5,21.5,23.5]
const SOT_OVER_LINES     = [5.5,6.5,7.5,8.5]

function toNumber(value,fallback=0){const n=Number(value);return Number.isFinite(n)?n:fallback}
function round1(value){return Math.round(toNumber(value)*10)/10}
function round2(value){return Math.round(toNumber(value)*100)/100}
function clamp(value,min,max){return Math.max(min,Math.min(max,value))}
function maybeJson(value){if(!value)return null;if(typeof value==="object")return value;if(typeof value==="string"){try{return JSON.parse(value)}catch(_){return null}}return null}
function formatDateInTZ(date,timeZone=TIMEZONE){return new Intl.DateTimeFormat("sv-SE",{timeZone,year:"numeric",month:"2-digit",day:"2-digit"}).format(date)}
function getTodayInTZ(){return formatDateInTZ(new Date(),TIMEZONE)}
function getKickoffDateOnly(kickoff){if(!kickoff)return null;const d=new Date(kickoff);if(Number.isNaN(d.getTime()))return null;return formatDateInTZ(d,TIMEZONE)}
function getKickoffMs(kickoff){if(!kickoff)return Number.MAX_SAFE_INTEGER;const d=new Date(kickoff);if(Number.isNaN(d.getTime()))return Number.MAX_SAFE_INTEGER;return d.getTime()}
function getDayOffsetFromToday(kickoff){const kickoffDay=getKickoffDateOnly(kickoff);if(!kickoffDay)return 999;const today=getTodayInTZ();const todayMs=new Date(`${today}T00:00:00`).getTime();const kickoffMs=new Date(`${kickoffDay}T00:00:00`).getTime();if(Number.isNaN(todayMs)||Number.isNaN(kickoffMs))return 999;return Math.round((kickoffMs-todayMs)/(24*60*60*1000))}
function buildMatchLabel(row){return`${row.home_team} x ${row.away_team}`}
function compareByKickoff(a,b){const aTime=getKickoffMs(a.kickoff),bTime=getKickoffMs(b.kickoff);if(aTime!==bTime)return aTime-bTime;if(b.main_score!==a.main_score)return b.main_score-a.main_score;return String(a.league||"").localeCompare(String(b.league||""))}
function compareByScoreThenKickoff(a,b){if(b.main_score!==a.main_score)return b.main_score-a.main_score;if(b.main_probability!==a.main_probability)return b.main_probability-a.main_probability;return compareByKickoff(a,b)}
function safeLeague(row){return row.league||"Liga"}
function hasMinimumAnalysis(row){
  // Tier 1 leagues always pass — Copa do Mundo, Premier League, etc.
  const tier = getLeagueTierScore(row.league);
  if (tier >= 90) return true;
  const values=[row.expected_home_goals,row.expected_away_goals,row.expected_home_shots,row.expected_away_shots,row.expected_home_sot,row.expected_away_sot,row.expected_corners,row.expected_cards,row.over25_prob,row.btts_prob,row.prob_shots,row.prob_sot,row.prob_cards];
  return values.some((v)=>v!==null&&Number(v)>0)
}
function getStrengthLabel(score){if(score>=0.78)return"Forte";if(score>=0.7)return"Boa";return"Moderada"}
function getRhythmLabel(avgShots){const shots=toNumber(avgShots);if(shots>=24)return"Alto";if(shots>=16)return"Moderado";return"Baixo"}
function marketFamily(market){const m=String(market||"").trim().toLowerCase();if(!m)return"outro";if(m.includes("escanteio"))return"escanteios";if(m.includes("finaliza")&&m.includes("no gol"))return"sot";if(m.includes("finaliza"))return"shots";if(m.includes("cart"))return"cards";if(m.includes("ambas"))return"btts";if(m.includes("dupla chance")||m.includes("empate")||m.includes("vitória")||m.includes("vitoria"))return"resultado";if(m.includes("gol"))return"gols";return"outro"}
function pickDynamicOverLine(value,lines,margin=0.25){const eligible=lines.filter((line)=>value>=line+margin);if(!eligible.length)return lines[0];return eligible[eligible.length-1]}
function pickDynamicUnderLine(value,lines){const eligible=lines.filter((line)=>value<=line-0.25);if(!eligible.length)return lines[lines.length-1];return eligible[0]}
function buildSubfamily(prefix,side,line){const clean=String(line).replace(".5","5").replace(".","");return`${prefix}_${side}${clean}`}

async function loadBaseTables(){
  const{data:matches,error:matchesError}=await supabase.from("matches").select(`id,kickoff,league,country,region,priority,home_team,away_team,home_logo,away_logo,metrics,markets,probabilities,pick,probability,insight,game_profile,confidence_score,updated_at`).order("kickoff",{ascending:true,nullsFirst:false})
  if(matchesError)throw matchesError
  const{data:analysis,error:analysisError}=await supabase.from("match_analysis").select(`match_id,home_strength,away_strength,expected_home_goals,expected_away_goals,expected_home_shots,expected_away_shots,expected_home_sot,expected_away_sot,expected_corners,expected_cards,prob_over25,prob_btts,prob_corners,prob_shots,prob_sot,prob_cards,best_pick_1,best_pick_2,best_pick_3,aggressive_pick,analysis_text`)
  if(analysisError)throw analysisError
  return{matches:matches||[],analysis:analysis||[]}
}

function mergeMatchRow(matchRow,analysisMap){
  const analysis=analysisMap.get(String(matchRow.id))||{}
  const metrics=maybeJson(matchRow.metrics)||{}
  const markets=maybeJson(matchRow.markets)||{}
  const probabilities=maybeJson(matchRow.probabilities)||{}
  const expectedHomeGoals=toNumber(analysis.expected_home_goals,0)
  const expectedAwayGoals=toNumber(analysis.expected_away_goals,0)
  const expectedHomeShots=toNumber(analysis.expected_home_shots,0)
  const expectedAwayShots=toNumber(analysis.expected_away_shots,0)
  const expectedHomeSOT=toNumber(analysis.expected_home_sot,0)
  const expectedAwaySOT=toNumber(analysis.expected_away_sot,0)
  const expectedCorners=toNumber(analysis.expected_corners,0)
  const expectedCards=toNumber(analysis.expected_cards,0)
  const avgGoals=expectedHomeGoals>0||expectedAwayGoals>0?round1(expectedHomeGoals+expectedAwayGoals):round1(toNumber(metrics.goals,0))
  const avgShots=expectedHomeShots>0||expectedAwayShots>0?Math.round(expectedHomeShots+expectedAwayShots):Math.round(toNumber(metrics.shots,0))
  const avgShotsOnTarget=expectedHomeSOT>0||expectedAwaySOT>0?Math.round(expectedHomeSOT+expectedAwaySOT):Math.round(toNumber(metrics.shots_on_target,0)||toNumber(metrics.sot,0))
  const avgCorners=expectedCorners>0?round1(expectedCorners):round1(toNumber(metrics.corners,0)||toNumber(markets.corners,0))
  const avgCards=expectedCards>0?round1(expectedCards):round1(toNumber(metrics.cards,0)||toNumber(markets.cards,0))
  const homeWinProb=clamp(toNumber(probabilities.home,0),0,1)
  const drawProb=clamp(toNumber(probabilities.draw,0),0,1)
  const awayWinProb=clamp(toNumber(probabilities.away,0),0,1)
  const over15Prob=clamp(toNumber(markets.over15,0),0,1)
  const over25Prob=clamp(toNumber(analysis.prob_over25,0)||toNumber(markets.over25,0),0,1)
  const bttsProb=clamp(toNumber(analysis.prob_btts,0)||toNumber(markets.btts,0),0,1)
  const shotsProb=clamp(toNumber(analysis.prob_shots,0)||toNumber(markets.shots,0),0,1)
  const sotProb=clamp(toNumber(analysis.prob_sot,0)||toNumber(markets.shots_on_target,0),0,1)
  const cardsProb=clamp(toNumber(analysis.prob_cards,0)||toNumber(markets.cards,0),0,1)
  const cornersProb=clamp(toNumber(analysis.prob_corners,0)||toNumber(markets.corners,0),0,1)
  const under25Prob=clamp(1-over25Prob,0,1)
  const under35Prob=clamp(toNumber(markets.under35,0)||clamp(1-Math.max(over25Prob-0.18,0),0,1),0,1)
  // Copa do Mundo defaults when form data is empty
  const leagueTier = getLeagueTierScore(matchRow.league);
  const hasData = avgGoals > 0 || avgShots > 0 || over25Prob > 0;
  const isTopIntl = leagueTier >= 90 && !hasData;
  const finalAvgGoals    = isTopIntl ? 2.35 : avgGoals;
  const finalAvgShots    = isTopIntl ? 22   : avgShots;
  const finalAvgSOT      = isTopIntl ? 7    : avgShotsOnTarget;
  const finalAvgCorners  = isTopIntl ? 9.5  : avgCorners;
  const finalAvgCards    = isTopIntl ? 3.2  : avgCards;
  const finalOver25      = isTopIntl ? 0.51 : over25Prob;
  const finalBtts        = isTopIntl ? 0.46 : bttsProb;
  const finalShotsProb   = isTopIntl ? 0.55 : shotsProb;
  const finalSOTProb     = isTopIntl ? 0.52 : sotProb;
  const finalCardsProb   = isTopIntl ? 0.48 : cardsProb;
  const finalCornersProb = isTopIntl ? 0.54 : cornersProb;
  const confidenceScore=clamp(toNumber(matchRow.confidence_score,0)||toNumber(matchRow.probability,0)||over25Prob||over15Prob||(isTopIntl?0.52:0),0,1)
  return{id:matchRow.id,kickoff:matchRow.kickoff,league:matchRow.league,country:matchRow.country,region:matchRow.region,priority:toNumber(matchRow.priority,0),home_team:matchRow.home_team,away_team:matchRow.away_team,home_logo:matchRow.home_logo,away_logo:matchRow.away_logo,metrics,markets,probabilities,home_strength:round1(toNumber(analysis.home_strength,0)),away_strength:round1(toNumber(analysis.away_strength,0)),avg_goals:isTopIntl?finalAvgGoals:avgGoals,avg_corners:isTopIntl?finalAvgCorners:avgCorners,avg_shots:isTopIntl?finalAvgShots:avgShots,avg_shots_on_target:isTopIntl?finalAvgSOT:avgShotsOnTarget,avg_cards:isTopIntl?finalAvgCards:avgCards,avg_fouls:null,over15_prob:round2(isTopIntl?0.67:over15Prob),over25_prob:round2(isTopIntl?finalOver25:over25Prob),under25_prob:round2(isTopIntl?1-finalOver25:under25Prob),under35_prob:round2(isTopIntl?0.49:under35Prob),btts_prob:round2(isTopIntl?finalBtts:bttsProb),prob_corners:round2(isTopIntl?finalCornersProb:cornersProb),prob_shots:round2(isTopIntl?finalShotsProb:shotsProb),prob_sot:round2(isTopIntl?finalSOTProb:sotProb),prob_cards:round2(isTopIntl?finalCardsProb:cardsProb),home_win_prob:round2(homeWinProb),draw_prob:round2(drawProb),away_win_prob:round2(awayWinProb),expected_home_goals:round2(expectedHomeGoals),expected_away_goals:round2(expectedAwayGoals),expected_home_shots:round2(expectedHomeShots),expected_away_shots:round2(expectedAwayShots),expected_home_sot:round2(expectedHomeSOT),expected_away_sot:round2(expectedAwaySOT),expected_corners:round2(expectedCorners),expected_cards:round2(expectedCards),confidence_score:round2(confidenceScore),best_pick_1:analysis.best_pick_1||matchRow.pick||null,best_pick_2:analysis.best_pick_2||null,best_pick_3:analysis.best_pick_3||null,aggressive_pick:analysis.aggressive_pick||null,analysis_text:analysis.analysis_text||matchRow.insight||null,game_profile:matchRow.game_profile||null}
}

async function loadActiveMatches(){
  const now=new Date()
  const minTime=new Date(now.getTime()-PAST_GRACE_HOURS*60*60*1000)
  const{matches,analysis}=await loadBaseTables()
  console.log("DEBUG DATA HOJE:",getTodayInTZ())
  console.log("DEBUG TOTAL RAW MATCHES:",matches.length)
  console.log("DEBUG TOTAL ANALYSIS:",analysis.length)
  const analysisMap=new Map(analysis.map((row)=>[String(row.match_id),row]))
  const merged=matches.map((row)=>mergeMatchRow(row,analysisMap))
  const finalRows=merged.filter((row)=>{const kickoffDate=row.kickoff?new Date(row.kickoff):null;const kickoffValid=kickoffDate&&!Number.isNaN(kickoffDate.getTime());return kickoffValid&&kickoffDate.getTime()>=minTime.getTime()}).filter((row)=>row.home_team&&row.away_team&&row.league).filter((row)=>hasMinimumAnalysis(row)).sort(compareByKickoff)
  console.log("DEBUG TOTAL FILTRADOS ATIVOS:",finalRows.length)
  return finalRows
}

function getGameProfile(row){
  if(row.game_profile&&String(row.game_profile).trim())return String(row.game_profile).trim()
  const avgGoals=toNumber(row.avg_goals),avgShots=toNumber(row.avg_shots)
  const avgCorners=toNumber(row.avg_corners),avgSOT=toNumber(row.avg_shots_on_target)
  const avgCards=toNumber(row.avg_cards),over25Prob=toNumber(row.over25_prob)
  const under25Prob=toNumber(row.under25_prob),under35Prob=toNumber(row.under35_prob)
  const bttsProb=toNumber(row.btts_prob),bttsNoProb=clamp(1-bttsProb,0,1)
  if(avgGoals>=2.8||over25Prob>=0.67||(avgShots>=24&&bttsProb>=0.62))return"ofensivo"
  if(avgCorners>=8.8&&avgShots>=21&&avgGoals>=2.1)return"estatistico"
  if(avgShots>=22&&avgSOT>=7&&avgCorners<9.3)return"volume"
  if(avgSOT>=7&&avgGoals>=2.2&&avgShots<=24)return"precisao"
  if(avgCards>=3.8&&avgGoals<=3.0)return"disciplinar"
  if(avgGoals<=2.1&&under25Prob>=0.72&&bttsNoProb>=0.7&&avgShots<=18)return"defensivo"
  if(under35Prob>=0.79&&avgGoals<=2.5&&avgShots<=20)return"controlado"
  return"equilibrado"
}

function buildInsight(row,bestPick,profile){
  const avgGoals=round1(row.avg_goals),avgCorners=round1(row.avg_corners)
  const avgShots=Math.round(toNumber(row.avg_shots)),avgSOT=Math.round(toNumber(row.avg_shots_on_target))
  const avgCards=round1(row.avg_cards),rhythm=getRhythmLabel(avgShots).toLowerCase()
  const market=String(bestPick.market||"").trim().toLowerCase()
  if(market.includes("mais de 2.5 gols"))return`A leitura Scoutly projeta um jogo mais aberto, com potencial real para 3 ou mais gols. A média esperada está em ${avgGoals} gols, com ritmo ofensivo ${rhythm}, reforçando essa linha como uma leitura forte da partida.`
  if(market.includes("mais de 1.5 gols"))return`A leitura Scoutly projeta um confronto com boa chance de pelo menos 2 gols. A média esperada está em ${avgGoals} gols, com ritmo ofensivo ${rhythm} e cenário favorável para essa linha.`
  if(market.includes("menos de 2.5 gols"))return`A leitura Scoutly indica um jogo travado, com baixa projeção ofensiva e controle no placar. A expectativa está em ${avgGoals} gols, com ritmo ${rhythm}, sustentando a linha de menos de 2.5 gols.`
  if(market.includes("menos de 3.5 gols"))return`A leitura Scoutly indica um jogo controlado, sem expectativa de explosão ofensiva. A projeção está em ${avgGoals} gols, com ritmo ${rhythm}, tornando a linha de menos de 3.5 gols uma opção consistente.`
  if(market.includes("ambas não marcam")||market.includes("ambas nao marcam"))return`A leitura Scoutly vê um confronto com baixa tendência de gols dos dois lados. A projeção ofensiva é moderada, e o cenário sugere maior chance de uma das equipes passar em branco.`
  if(market.includes("ambas marcam"))return`A leitura Scoutly identifica espaço para gols dos dois lados. A expectativa ofensiva, o ritmo ${rhythm} e o equilíbrio do confronto criam um cenário interessante para ambas marcam.`
  if(market.includes("escanteios")){if(market.includes("mais de"))return`A leitura Scoutly projeta cerca de ${avgCorners} escanteios, com ritmo ${rhythm}, pressão ofensiva e volume suficiente para sustentar uma linha de over corners com boa coerência estatística.`;return`A leitura Scoutly projeta cerca de ${avgCorners} escanteios, indicando um cenário mais controlado para o mercado de cantos, sem necessidade de esticar demais a linha.`}
  if(market.includes("finalizações")&&market.includes("no gol"))return`A leitura Scoutly projeta cerca de ${avgSOT} finalizações no gol, indicando um cenário de boa produção ofensiva com precisão suficiente para sustentar esse mercado.`
  if(market.includes("finalizações"))return`A leitura Scoutly projeta cerca de ${avgShots} finalizações totais, com ritmo ${rhythm} e sustentação estatística clara para transformar volume ofensivo em oportunidade real de mercado.`
  if(market.includes("cart"))return`A leitura Scoutly projeta cerca de ${avgCards} cartões, sugerindo um confronto com nível de contato e tensão suficiente para transformar disciplina em oportunidade de mercado.`
  if(market.includes("dupla chance"))return`A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate. O equilíbrio da partida ainda pede segurança, e por isso a dupla chance aparece como uma leitura sólida.`
  return`A leitura Scoutly classifica este confronto como ${profile}, combinando projeção de ${avgGoals} gols, ${avgCorners} escanteios, ${avgShots} finalizações e ${avgSOT} no gol para destacar essa oportunidade.`
}

function pushCandidate(candidates,item){const family=item.family||marketFamily(item.market);const familyWeight=FAMILY_SCORE_WEIGHT[family]||0.7;const adjustedScore=clamp(toNumber(item.score)*familyWeight,0,1);candidates.push({...item,family,probability:clamp(toNumber(item.probability),0,1),score:adjustedScore})}

function buildCornerCandidates(row,profile){
  const candidates=[];const avgCorners=toNumber(row.avg_corners),avgShots=toNumber(row.avg_shots);const avgGoals=toNumber(row.avg_goals),avgSOT=toNumber(row.avg_shots_on_target)
  function add(market,probability,score,subfamily,macro="estatistico"){pushCandidate(candidates,{market,probability,score,family:"escanteios",subfamily,macro})}
  // Boost usado APENAS para probabilidade — NÃO para seleção de linha
  // Antes: boost empurrava 9.3 → 9.65-9.9, gerando 7.5/8.5 para quase todos
  const boostedOverCorners=avgCorners+(avgShots>=20?0.35:0)+(avgShots>=23?0.25:0)+(avgSOT>=7?0.2:0)+(avgGoals>=2.6?0.15:0)+(profile==="estatistico"?0.2:0)+(profile==="volume"?0.18:0)+(profile==="precisao"?0.1:0)
  // Escanteios: máximo 7.5 no pick principal — NUNCA 8.5 (75% hit rate histórico, muito arriscado)
  // João quer apenas linhas seguras: 6.5 (90%) e 7.5 quando muito justificado
  if(avgCorners>=6.0){
    const line = pickDynamicOverLine(avgCorners, [6.5, 7.5], SAFE_MARGIN)  // 8.5 REMOVIDO
    const probability=clamp(0.6+(boostedOverCorners-line)*0.11+(avgShots>=21?0.02:0),0.6,0.91)
    const score=clamp(0.68+(boostedOverCorners-line)*0.09+(profile==="estatistico"?0.03:0)+(avgSOT>=7?0.02:0),0.6,0.9)
    add(`Mais de ${line} escanteios`,probability,score,buildSubfamily("corners","over",line))
  }
  const controlledCorners=avgCorners-(avgShots<=18?0.3:0)-(avgShots<=16?0.2:0)-(avgGoals<=2.1?0.15:0)-(profile==="defensivo"?0.18:0)-(profile==="controlado"?0.2:0)
  if(controlledCorners<=10.6){const referenceUnder=controlledCorners+4.0;const line=pickDynamicUnderLine(referenceUnder,CORNER_UNDER_LINES);const probability=clamp(0.61+(line-referenceUnder)*0.07+(avgShots<=18?0.02:0),0.58,0.89);const score=clamp(0.66+(line-referenceUnder)*0.06+(profile==="controlado"?0.03:0)+(profile==="defensivo"?0.02:0),0.58,0.87);add(`Menos de ${line} escanteios`,probability,score,buildSubfamily("corners","under",line))}
  return candidates
}

function buildCardsCandidates(row,profile){
  const candidates=[];const avgCards=toNumber(row.avg_cards),avgGoals=toNumber(row.avg_goals)
  function add(market,probability,score,subfamily){pushCandidate(candidates,{market,probability,score,family:"cards",subfamily,macro:"disciplina"})}
  const adjustedCards=avgCards+(avgGoals<=2.4?0.25:0)+(avgGoals<=2.0?0.2:0)+(profile==="disciplinar"?0.3:0)+(profile==="defensivo"?0.15:0)
  if(adjustedCards>=1.8){const line=pickDynamicOverLine(adjustedCards,CARDS_OVER_LINES);const probability=clamp(0.6+(adjustedCards-line)*0.12,0.58,0.9);const score=clamp(0.66+(adjustedCards-line)*0.1+(profile==="disciplinar"?0.03:0),0.6,0.88);add(`Mais de ${line} cartões`,probability,score,buildSubfamily("cards","over",line))}
  return candidates
}

function buildShotsCandidates(row,profile){
  const candidates=[];const avgShots=toNumber(row.avg_shots),avgGoals=toNumber(row.avg_goals)
  function add(market,probability,score,subfamily){pushCandidate(candidates,{market,probability,score,family:"shots",subfamily,macro:"volume"})}
  const adjustedShots=avgShots+(avgGoals>=2.3?0.8:0)+(profile==="volume"?0.4:0)
  // Margem 1.0 para finalizações (igual aos escanteios — evita picks arriscados)
  const adjustedShotsMargin = 1.0
  if(adjustedShots>=15.8){const line=pickDynamicOverLine(adjustedShots,SHOTS_OVER_LINES,adjustedShotsMargin);const probability=clamp(0.6+(adjustedShots-line)*0.09,0.56,0.86);const score=clamp(0.64+(adjustedShots-line)*0.08,0.58,0.86);add(`Mais de ${line} finalizações`,probability,score,buildSubfamily("shots","over",line))}
  return candidates
}

function buildSOTCandidates(row,profile){
  const candidates=[];const avgSOT=toNumber(row.avg_shots_on_target),avgGoals=toNumber(row.avg_goals)
  function add(market,probability,score,subfamily){pushCandidate(candidates,{market,probability,score,family:"sot",subfamily,macro:"precisao"})}
  const adjustedSOT=avgSOT+(avgGoals>=2.3?0.4:0)+(profile==="precisao"?0.4:0)
  if(adjustedSOT>=4.4){const line=pickDynamicOverLine(adjustedSOT,SOT_OVER_LINES);const probability=clamp(0.59+(adjustedSOT-line)*0.08,0.55,0.84);const score=clamp(0.62+(adjustedSOT-line)*0.075,0.56,0.83);add(`Mais de ${line} finalizações no gol`,probability,score,buildSubfamily("sot","over",line))}
  return candidates
}

function buildGoalsCandidates(row){
  const candidates=[];const avgGoals=toNumber(row.avg_goals),over25=toNumber(row.over25_prob),under25=toNumber(row.under25_prob)
  function add(market,probability,score,subfamily,macro){pushCandidate(candidates,{market,probability,score,family:"gols",subfamily,macro})}
  if(avgGoals>=2.0){const prob=clamp(over25+0.18,0.62,0.92);add("Mais de 1.5 gols",prob,prob,"gols_over15","ofensivo")}
  // Margem de segurança: Mais de 2.5 só quando projeção >= 3.0 (não 2.2)
  // Switzerland x Colombia com 2.60 gols não deve recomendar 2.5
  // Mais de 2.5 gols: só quando projeção >= 3.5 (muito conservador)
  // Com threshold 3.0 ainda aparecia em muitos jogos. 3.5 garante margem real.
  if(avgGoals>=3.5){const prob=clamp(over25,0.6,0.9);add("Mais de 2.5 gols",prob,prob,"gols_over25","ofensivo")}
  if(avgGoals<=2.4){const prob=clamp(under25,0.6,0.9);add("Menos de 2.5 gols",prob,prob,"gols_under25","defensivo")}
  if(avgGoals<=3.0){const prob=clamp(row.under35_prob,0.62,0.92);add("Menos de 3.5 gols",prob,prob,"gols_under35","defensivo")}
  return candidates
}

function buildBTTS(row){
  const candidates=[];const btts=toNumber(row.btts_prob),avgGoals=toNumber(row.avg_goals)
  function add(market,probability,score,subfamily,macro){pushCandidate(candidates,{market,probability,score,family:"btts",subfamily,macro})}
  if(btts>=0.58&&avgGoals>=2.2)add("Ambas marcam",btts,btts,"btts_yes","ofensivo")
  if(btts<=0.45&&avgGoals<=2.4)add("Ambas não marcam",1-btts,1-btts,"btts_no","defensivo")
  return candidates
}

function buildResultCandidates(row,profile){
  const candidates=[];const homeWin=toNumber(row.home_win_prob),draw=toNumber(row.draw_prob),awayWin=toNumber(row.away_win_prob)
  const homeOrDraw=clamp(homeWin+draw,0,1),awayOrDraw=clamp(awayWin+draw,0,1),diff=Math.abs(homeWin-awayWin)
  function add(market,probability,score,subfamily){pushCandidate(candidates,{market,probability,score,family:"resultado",subfamily,macro:"protecao"})}
  if(homeWin>=awayWin&&homeOrDraw>=0.68){const probability=clamp(homeOrDraw+(diff>=0.1?0.02:0),0.68,0.92);const score=clamp(probability+(profile==="equilibrado"?0.02:0),0.66,0.9);add(`Dupla chance ${row.home_team} ou empate`,probability,score,"resultado_home_draw")}
  if(awayWin>homeWin&&awayOrDraw>=0.68){const probability=clamp(awayOrDraw+(diff>=0.1?0.02:0),0.68,0.92);const score=clamp(probability+(profile==="equilibrado"?0.02:0),0.66,0.9);add(`Dupla chance ${row.away_team} ou empate`,probability,score,"resultado_away_draw")}
  return candidates
}

function buildMarketCandidates(row,options={}){
  const relaxed=options.relaxed===true
  const profile=getGameProfile(row)
  let candidates=[]
  candidates.push(...buildGoalsCandidates(row,profile))
  candidates.push(...buildBTTS(row,profile))
  candidates.push(...buildResultCandidates(row,profile))
  candidates.push(...buildCornerCandidates(row,profile))
  candidates.push(...buildCardsCandidates(row,profile))
  candidates.push(...buildShotsCandidates(row,profile))
  candidates.push(...buildSOTCandidates(row,profile))
  candidates=candidates.map((item)=>{let score=toNumber(item.score);if(item.family==="gols")score+=0.03;if(item.family==="resultado")score+=0.025;if(item.family==="escanteios")score+=0.02;if(item.family==="cards")score+=0.015;if(profile==="ofensivo"&&item.macro==="ofensivo")score+=0.02;if(profile==="defensivo"&&item.macro==="defensivo")score+=0.02;if(profile==="estatistico"&&item.family==="escanteios")score+=0.02;if(profile==="disciplinar"&&item.family==="cards")score+=0.02;if(profile==="equilibrado"&&item.family==="resultado")score+=0.015;if(profile==="volume"&&item.family==="shots")score+=0.02;if(profile==="precisao"&&item.family==="sot")score+=0.02;return{...item,score:clamp(score,0,1)}})
  const minProbability=relaxed?0.50:0.56
  const minScore=relaxed?0.50:0.58
  return candidates.filter((c)=>c.probability>=minProbability&&c.score>=minScore).sort((a,b)=>b.score-a.score)
}

function chooseBestAndAlternatives(candidates,row){
  if(!candidates.length)return{best:null,alternatives:[]}
  const sorted=[...candidates].sort((a,b)=>b.score-a.score)
  const best=sorted[0]
  const alternatives=[]
  const usedMarkets=new Set([best.market])
  for(const item of sorted.slice(1)){if(!item?.market)continue;if(usedMarkets.has(item.market))continue;alternatives.push(item);usedMarkets.add(item.market);if(alternatives.length>=2)break}
  if(alternatives.length<2&&row){const relaxed=buildMarketCandidates(row,{relaxed:true});for(const item of relaxed){if(!item?.market)continue;if(usedMarkets.has(item.market))continue;alternatives.push(item);usedMarkets.add(item.market);if(alternatives.length>=2)break}}
  return{best,alternatives}
}

function buildAnalysisFromRow(row){
  const candidates=buildMarketCandidates(row)
  if(!candidates.length)return null
  const profile=getGameProfile(row)
  const{best,alternatives}=chooseBestAndAlternatives(candidates,row)
  if(!best)return null
  const aggressivePick=alternatives.find((x)=>String(x.subfamily||"").includes("gols_over")||String(x.subfamily||"").includes("corners_over")||String(x.subfamily||"").includes("cards_over"))?.market||row.aggressive_pick||null
  return{match_id:row.id,home_team:row.home_team,away_team:row.away_team,league:safeLeague(row),kickoff:row.kickoff,home_logo:row.home_logo,away_logo:row.away_logo,avg_goals:round1(row.avg_goals),avg_corners:round1(row.avg_corners),avg_shots:Math.round(toNumber(row.avg_shots)),avg_shots_on_target:Math.round(toNumber(row.avg_shots_on_target)),avg_cards:round1(row.avg_cards),avg_fouls:null,home_strength:round1(row.home_strength),away_strength:round1(row.away_strength),home_win_prob:round2(row.home_win_prob),draw_prob:round2(row.draw_prob),away_win_prob:round2(row.away_win_prob),over25_prob:round2(row.over25_prob),under25_prob:round2(row.under25_prob),under35_prob:round2(row.under35_prob),btts_prob:round2(row.btts_prob),prob_corners:round2(row.prob_corners),prob_shots:round2(row.prob_shots),prob_sot:round2(row.prob_sot),prob_cards:round2(row.prob_cards),game_profile:profile,main_pick:best.market,main_probability:round2(best.probability),main_score:round2(best.score),main_family:best.family,main_subfamily:best.subfamily,main_macro:best.macro,strength:getStrengthLabel(best.score),rhythm:getRhythmLabel(row.avg_shots),insight:buildInsight(row,best,profile),best_pick_1:best.market,best_pick_2:alternatives[0]?.market||null,best_pick_3:alternatives[1]?.market||null,aggressive_pick:aggressivePick,alternatives_count:alternatives.filter(Boolean).length}
}

function chooseRadar(analyses){
  const now=Date.now()
  const active=analyses.filter((item)=>{const kickoffMs=getKickoffMs(item.kickoff);return kickoffMs>=now-PAST_GRACE_HOURS*60*60*1000}).filter((item)=>!RADAR_BLACKLIST.has(String(item.league||"")))
  const today=active.filter((item)=>getDayOffsetFromToday(item.kickoff)<=0)
  const future=active.filter((item)=>getDayOffsetFromToday(item.kickoff)>0)
  function sortPool(arr){return[...arr].sort((a,b)=>{const ta=getKickoffMs(a.kickoff),tb=getKickoffMs(b.kickoff);const blockA=Math.floor(ta/(30*60*1000)),blockB=Math.floor(tb/(30*60*1000));if(blockA!==blockB)return blockA-blockB;const tierA=LEAGUE_TIER[String(a.league||"")]??4,tierB=LEAGUE_TIER[String(b.league||"")]??4;if(tierA!==tierB)return tierA-tierB;return b.main_score-a.main_score})}
  const pool=[...sortPool(today),...sortPool(future)]
  function buildRadarPass(pool,allowedTiers,currentRadar,currentUsed){
    const radar=[...currentRadar];const usedMatchIds=new Set(currentUsed.matchIds);const usedExactMarkets={...currentUsed.exactMarkets};const usedFamilies={...currentUsed.families};const usedLeagues={...currentUsed.leagues}
    for(const item of pool){if(radar.length>=RADAR_SIZE)break;if(usedMatchIds.has(item.match_id))continue;const tier=LEAGUE_TIER[String(item.league||"")]??4;if(!allowedTiers.includes(tier))continue;const exactMarketCount=usedExactMarkets[item.main_pick]||0;const familyCount=usedFamilies[item.main_family]||0;const leagueCount=usedLeagues[item.league]||0;if(exactMarketCount>=2)continue;const leagueCap=RADAR_UNCAPPED_LEAGUES.has(String(item.league||""))?Infinity:2;if(leagueCount>=leagueCap)continue;if(item.main_family==="gols"&&familyCount>=4)continue;if(item.main_family==="resultado"&&familyCount>=3)continue;if(item.main_family==="escanteios"&&familyCount>=3)continue;if(item.main_family==="cards"&&familyCount>=3)continue;if(item.main_family==="btts"&&familyCount>=2)continue;if(item.main_family==="shots"&&familyCount>=2)continue;if(item.main_family==="sot"&&familyCount>=2)continue;radar.push(item);usedMatchIds.add(item.match_id);usedExactMarkets[item.main_pick]=exactMarketCount+1;usedFamilies[item.main_family]=familyCount+1;usedLeagues[item.league]=leagueCount+1}
    return{radar,used:{matchIds:Array.from(usedMatchIds),exactMarkets:usedExactMarkets,families:usedFamilies,leagues:usedLeagues}}
  }
  const pass1=buildRadarPass(pool,[1],[],{matchIds:[],exactMarkets:{},families:{},leagues:{}})
  const pass2=pass1.radar.length<RADAR_SIZE?buildRadarPass(pool,[2],pass1.radar,pass1.used):pass1
  const pass3=pass2.radar.length<RADAR_SIZE?buildRadarPass(pool,[3],pass2.radar,pass2.used):pass2
  let radar=pass3.radar
  if(radar.length<RADAR_SIZE){const usedIds=new Set(pass3.used.matchIds);const backup=[...active].sort(compareByScoreThenKickoff);for(const item of backup){if(radar.length>=RADAR_SIZE)break;if(usedIds.has(item.match_id))continue;radar.push(item);usedIds.add(item.match_id)}}
  const tierCount={1:0,2:0,3:0,4:0}
  radar.forEach(item=>{const t=LEAGUE_TIER[String(item.league||"")]??4;tierCount[t]=(tierCount[t]||0)+1})
  console.log(`📊 Distribuição por tier: T1=${tierCount[1]||0} T2=${tierCount[2]||0} T3=${tierCount[3]||0} T4=${tierCount[4]||0}`)
  console.log(`🏆 Pass1: ${pass1.radar.length} | Pass2: ${pass2.radar.length} | Pass3: ${pass3.radar.length}`)
  return radar.sort(compareByKickoff)
}

function buildTicketFromRadar(radar){
  const ranked=[...radar].sort(compareByScoreThenKickoff)
  const desiredSize=ranked.length>=3?TICKET_MAX_SIZE:TICKET_MIN_SIZE
  const ticket=[];const usedMatches=new Set();const usedFamilies=new Set()
  for(const item of ranked){if(usedMatches.has(item.match_id))continue;if(usedFamilies.has(item.main_family))continue;ticket.push(item);usedMatches.add(item.match_id);usedFamilies.add(item.main_family);if(ticket.length===desiredSize)break}
  if(ticket.length<desiredSize){for(const item of ranked){if(usedMatches.has(item.match_id))continue;ticket.push(item);usedMatches.add(item.match_id);if(ticket.length===desiredSize)break}}
  return ticket.sort(compareByKickoff)
}

async function updateMatchAnalysisFromBrain(analyses){
  for(const item of analyses){const{error}=await supabase.from("match_analysis").update({best_pick_1:item.best_pick_1,best_pick_2:item.best_pick_2,best_pick_3:item.best_pick_3,aggressive_pick:item.aggressive_pick,analysis_text:item.insight}).eq("match_id",item.match_id);if(error)console.error(`Erro ao atualizar match_analysis ${item.match_id}:`,error.message)}
}

async function rebuildDailyPicks(radar,ticket){
  const now=new Date().toISOString()
  const{error:deleteError}=await supabase
    .from("daily_picks")
    .delete()
    .gte("kickoff",now)
  if(deleteError)throw deleteError
  const orderedRadar=[...radar].sort(compareByKickoff)
  const rows=orderedRadar.map((item,index)=>{const isInTicket=ticket.some((t)=>String(t.match_id)===String(item.match_id));return{rank:index+1,match_id:item.match_id,home_team:item.home_team,away_team:item.away_team,league:item.league,market:item.main_pick,probability:round2(item.main_probability),is_opportunity:!isInTicket,home_logo:item.home_logo||null,away_logo:item.away_logo||null,kickoff:item.kickoff||null,created_at:new Date().toISOString()}})
  if(!rows.length){console.log("Nenhuma dica elegível para gravar em daily_picks.");return}
  const{error:insertError}=await supabase.from("daily_picks").insert(rows)
  if(insertError)throw insertError
}

async function runScoutlyBrain(){
  console.log("🧠 Scoutly Brain V4.3 iniciado...")
  console.log("✅ [FIX 4] RADAR_BLACKLIST: todas as copas nacionais removidas — aparecem em competições e no radar")
  const matches=await loadActiveMatches()
  console.log(`📦 Jogos ativos carregados: ${matches.length}`)
  const analyses=matches.map(buildAnalysisFromRow).filter(Boolean)
  console.log(`🧪 Análises válidas: ${analyses.length}`)
  if(!analyses.length){console.log("⚠️ Nenhuma análise válida encontrada.");const now=new Date().toISOString();await supabase.from("daily_picks").delete().gte("kickoff",now);return}
  await updateMatchAnalysisFromBrain(analyses)
  const radar=chooseRadar(analyses)
  const ticket=buildTicketFromRadar(radar)
  console.log("🎯 RADAR FINAL:")
  radar.forEach((item,index)=>{const tier=LEAGUE_TIER[item.league]??4;const alt2=item.best_pick_2?` | alt2: ${item.best_pick_2}`:" | alt2: NULL";const alt3=item.best_pick_3?` | alt3: ${item.best_pick_3}`:"";console.log(`  ${index+1}. [T${tier}] ${buildMatchLabel(item)} → ${item.main_pick}${alt2}${alt3} | score:${item.main_score}`)})
  console.log("🎟️ BILHETE FINAL:")
  ticket.forEach((item,index)=>{console.log(`  ${index+1}. ${buildMatchLabel(item)} → ${item.main_pick} | score:${item.main_score}`)})
  await rebuildDailyPicks(radar,ticket)
  console.log("✅ Scoutly Brain V4.3 finalizado.")
  console.log(`📡 Radar: ${radar.length} jogo(s) | 🎫 Bilhete: ${ticket.length} jogo(s)`)
}

runScoutlyBrain().catch((error)=>{console.error("❌ Erro no Scoutly Brain V4.3:",error);process.exit(1)})
