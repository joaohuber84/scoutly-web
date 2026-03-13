import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
process.env.SUPABASE_URL,
process.env.SUPABASE_SERVICE_KEY
)

async function runScoutlyBrain() {

const { data: matches } = await supabase
.from("matches")
.select("*")
.eq("status","scheduled")

const { data: stats } = await supabase
.from("match_stats")
.select("*")

const { data: analysis } = await supabase
.from("match_analysis")
.select("*")

let picks = []

matches.forEach(match => {

const matchStats = stats.find(s => s.match_id === match.id)
const matchAnalysis = analysis.find(a => a.match_id === match.id)

if(!matchStats || !matchAnalysis) return

const xg = matchAnalysis.expected_goals || 0
const corners = matchAnalysis.projected_corners || 0
const shots = matchAnalysis.projected_shots || 0
const shotsOn = matchAnalysis.projected_shots_on_target || 0

let opportunities = []

/* UNDER GOALS */

if(xg <= 2.4){

opportunities.push({
market:"MENOS DE 3.5 GOLS",
strength:"Muito forte",
score:92
})

}

/* BTTS */

if(xg <= 2.1){

opportunities.push({
market:"AMBAS NÃO MARCAM",
strength:"Muito forte",
score:90
})

}

/* DOUBLE CHANCE */

if(matchAnalysis.home_strength > matchAnalysis.away_strength){

opportunities.push({
market:`DUPLA CHANCE ${match.home_team} OU EMPATE`,
strength:"Muito forte",
score:88
})

}

/* CORNERS */

if(corners <= 9){

opportunities.push({
market:"MENOS DE 10.5 ESCANTEIOS",
strength:"Muito forte",
score:89
})

}

if(corners >= 11){

opportunities.push({
market:"MAIS DE 8.5 ESCANTEIOS",
strength:"Muito forte",
score:87
})

}

/* SHOTS */

if(shots >= 20){

opportunities.push({
market:"MAIS DE 17.5 FINALIZAÇÕES",
strength:"Muito forte",
score:86
})

}

if(shotsOn >= 7){

opportunities.push({
market:"MAIS DE 5.5 CHUTES NO GOL",
strength:"Muito forte",
score:85
})

}

if(opportunities.length === 0) return

const bestOpportunity = opportunities.sort((a,b)=>b.score-a.score)[0]

picks.push({

match_id:match.id,
home_team:match.home_team,
away_team:match.away_team,
league:match.league,
kickoff:match.kickoff,

market:bestOpportunity.market,
strength:bestOpportunity.strength,
score:bestOpportunity.score,

expected_goals:xg,
projected_corners:corners,
projected_shots:shots,
projected_shots_on_target:shotsOn,

})

})

/* ORDER BY QUALITY */

picks.sort((a,b)=>b.score-a.score)

/* DIVERSIFY MARKETS */

let usedMarkets = {}
let finalPicks = []

for(const pick of picks){

if(finalPicks.length >= 5) break

if(!usedMarkets[pick.market]){

finalPicks.push(pick)
usedMarkets[pick.market] = true

}

}

/* FALLBACK */

if(finalPicks.length < 5){

for(const pick of picks){

if(finalPicks.length >= 5) break

if(!finalPicks.find(p=>p.match_id === pick.match_id)){

finalPicks.push(pick)

}

}

}

/* DELETE OLD PICKS */

await supabase
.from("daily_picks")
.delete()
.eq("day", new Date().toISOString().slice(0,10))

/* SAVE NEW PICKS */

for(let i=0;i<finalPicks.length;i++){

await supabase
.from("daily_picks")
.insert({

day:new Date().toISOString().slice(0,10),

match_id:finalPicks[i].match_id,

market:finalPicks[i].market,
strength:finalPicks[i].strength,
score:finalPicks[i].score,

rank:i+1

})

}

console.log("SCOUTLY BRAIN V2.3 GERADO")

}

runScoutlyBrain()

