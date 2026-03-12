const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
process.env.SUPABASE_URL,
process.env.SUPABASE_SERVICE_ROLE_KEY
)

function generateOpportunities(matches){

const leaguesWhitelist = [
"Premier League",
"La Liga",
"Bundesliga",
"Serie A",
"Ligue 1",
"Eredivisie",
"Brasileirão Série A",
"Brasileirão Série B",
"MLS",
"Liga Argentina",
"Champions League",
"Europa League",
"Conference League"
]

let opportunities = []

for(const match of matches){

if(!leaguesWhitelist.includes(match.league)) continue

let score = 0

if(match.avg_goals >= 2.5) score += 2
if(match.avg_shots >= 20) score += 2
if(match.avg_corners >= 9) score += 2
if(match.home_form > match.away_form) score += 1

if(score >= 4){

opportunities.push({
match_id: match.id,
home_team: match.home_team,
away_team: match.away_team,
league: match.league,
pick: "Over 1.5 Goals",
confidence: score
})

}

}

return opportunities

}

async function run(){

const { data: matches } = await supabase
.from("matches")
.select("*")

if(!matches){
console.log("No matches found")
return
}

const opportunities = generateOpportunities(matches)

console.log("Generated opportunities:", opportunities.length)

for(const opp of opportunities){

await supabase
.from("daily_picks")
.insert({
match_id: opp.match_id,
home_team: opp.home_team,
away_team: opp.away_team,
league: opp.league,
market: opp.pick,
probability: opp.confidence/5,
is_opportunity: true
})

}

console.log("Daily picks updated")

}

run()
