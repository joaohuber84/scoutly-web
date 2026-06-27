const { createClient } = require("@supabase/supabase-js")

const APISPORTS_KEY = process.env.APISPORTS_KEY || ""
const SUPABASE_URL = process.env.SUPABASE_URL || ""
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_KEY ||
  process.env.SUPABASE_KEY ||
  ""

if (!APISPORTS_KEY) throw new Error("APISPORTS_KEY não encontrada.")
if (!SUPABASE_URL) throw new Error("SUPABASE_URL não encontrada.")
if (!SUPABASE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY não encontrada.")

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)
const API = "https://v3.football.api-sports.io"
const TIMEZONE = "America/Sao_Paulo"

/**
 * SCOUTLY SYNC V13.7
 * [FIX 1] clearFutureWindow: daily_picks agora só apaga picks com kickoff futuro
 *         Picks de jogos já acontecidos ficam preservados para o verify.js processar
 * [FIX 2] RADAR_BLACKLIST: todas as copas nacionais removidas.
 *         Agora aparecem em competições e no radar quando tiverem picks fortes.
 *         Mantidas apenas Copa Sul-Sudeste e Copa Verde (sem dados na API-Football).
 */

const WINDOW_HOURS = 168
const REQUEST_DELAY_MS = 350
const FORM_LIMIT_GENERAL = 10
const FORM_LIMIT_HOME_AWAY = 5
const MAX_RECENT_FIXTURES_FETCH = 20
const H2H_LIMIT = 10
const H2H_WEIGHT = 0.25
const FORM_WEIGHT = 0.75
const MIN_REQUIRED_RECENT_MATCHES = 3
const MIN_REQUIRED_STATS_MATCHES = 2
const MAX_DAILY_PICKS = 20
const MAX_SAME_MARKET_IN_DAILY = 2
const MAX_SAME_LEAGUE_IN_DAILY = 2
const MAX_INTERNATIONAL_IN_DAILY = 8
const MAX_BRAZIL_IN_DAILY = 6

const LEAGUE_TIER = {
  "UEFA Champions League":1,"Libertadores":1,"Copa do Mundo":1,"FIFA World Cup":1,"FIFA World Cup 2026":1,"Eurocopa":1,
  "Copa América":1,"Brasileirão Série A":1,"Premier League":1,"La Liga":1,
  "Serie A":1,"Bundesliga":1,"Ligue 1":1,"Nations League":1,"Copa do Brasil":1,
  "UEFA Europa League":2,"Sul-Americana":2,"Eredivisie":2,"MLS":2,"Liga Argentina":2,
  "Primeira Liga":2,"Brasileirão Série B":2,"UEFA Conference League":2,"Liga MX":2,
  "Super Lig":2,"Belgian Pro League":2,"Saudi Pro League":2,"Eliminatórias Sul-Americanas":2,
  "Eliminatórias Europeias":2,"Mundial de Clubes":2,"CONCACAF Champions Cup":2,
  "Copa del Rey":3,"Coppa Italia":3,"DFB-Pokal":3,"FA Cup":3,"Coupe de France":3,
  "Austrian Bundesliga":3,"Super League Greece":3,"Superliga":3,"Copa Argentina":3,
  "Amistosos Internacionais":3,"Championship":3,"Scottish Premiership":3,
  "Allsvenskan":3,"Eliteserien":3,"Liga Chilena":3,"Liga Colombiana":3,
  "Liga Peruana":3,"Liga Uruguaia":3,"Copa Africana":3,
  "EFL Cup":4,"KNVB Cup":4,"Taça de Portugal":4,"Taça da Liga":4,
  "Copa da Turquia":4,"Copa da Bélgica":4,"Copa da Áustria":4,"Copa da Grécia":4,
  "Copa da Dinamarca":4,"Scottish Cup":4,"Copa Chile":4,"Copa Colombia":4,
  "Copa MX":4,"Leagues Cup":4,"Copa Verde":4,"Copa do Nordeste":4,
  "Copa Sul-Sudeste":4,"Recopa Sul-Americana":4,"Copa Asiática":4,"Copa da Escócia":4,
}

// Mantidas apenas competições sem dados confiáveis na API-Football.
// Todas as copas nacionais foram removidas — agora aparecem em competições
// e no radar quando tiverem picks fortes.
const RADAR_BLACKLIST = new Set([
  "Copa Sul-Sudeste",
  "Copa Verde",
])

function getLeagueTierScore(league) {
  const tier = LEAGUE_TIER[String(league||"")] || 4
  return tier === 1 ? 1000 : tier === 2 ? 500 : tier === 3 ? 200 : 50
}
const MIN_PROBABILITY_GENERAL = 0.68
const MIN_PROBABILITY_EQUILIBRADO = 0.72
const MIN_PROBABILITY_DEFENSIVO = 0.72
const MIN_PROBABILITY_INTERNATIONAL = 0.70
const NATIONAL_TEAM_DOUBLE_CHANCE_CAP = 0.82
const NATIONAL_TEAM_WIN_CAP = 0.74
const STRONG_MISMATCH_DOUBLE_CHANCE_BLOCK = 0.18

const apiCache = new Map()
const fixtureStatsCache = new Map()
const teamRecentFixturesCache = new Map()
const teamContextCache = new Map()
const competitionFixturesCache = new Map()
const h2hCache = new Map()

const TARGET_COMPETITIONS = [
  { mode:"country", country:"Brazil", type:"league", names:["Serie A","Brasileirão Série A","Campeonato Brasileiro Série A","Brasileiro Série A"], display:"Brasileirão Série A", region:"brazil", priority:94 },
  { mode:"country", country:"Brazil", type:"league", names:["Serie B","Brasileirão Série B","Campeonato Brasileiro Série B","Brasileiro Série B"], display:"Brasileirão Série B", region:"brazil", priority:88 },
  { mode:"country", country:"Brazil", type:"cup", names:["Copa do Brasil","Copa Do Brasil"], display:"Copa do Brasil", region:"brazil", priority:91 },
  { mode:"country", country:"Brazil", type:"cup", names:["Copa do Nordeste","Copa Nordeste","Nordeste"], display:"Copa do Nordeste", region:"brazil", priority:82 },
  { mode:"country", country:"Argentina", type:"league", names:["Liga Profesional Argentina","Liga Profesional","Primera División","Primera Division","Superliga"], display:"Liga Argentina", region:"general", priority:84 },
  { mode:"country", country:"Argentina", type:"cup", names:["Copa Argentina","Copa de la Liga Profesional","Copa de la Liga"], display:"Copa Argentina", region:"general", priority:76 },
  { mode:"country", country:"Chile", type:"league", names:["Primera División","Primera Division"], display:"Liga Chilena", region:"general", priority:76 },
  { mode:"country", country:"Colombia", type:"league", names:["Liga BetPlay Dimayor","Primera A","Categoría Primera A","Liga Dimayor"], display:"Liga Colombiana", region:"general", priority:76 },
  { mode:"country", country:"Peru", type:"league", names:["Liga 1","Liga 1 Betsson"], display:"Liga Peruana", region:"general", priority:74 },
  { mode:"country", country:"Uruguay", type:"league", names:["Primera División","Primera Division"], display:"Liga Uruguaia", region:"general", priority:74 },
  { mode:"country", country:"England", type:"league", names:["Premier League"], display:"Premier League", region:"general", priority:100 },
  { mode:"country", country:"England", type:"cup", names:["FA Cup","Emirates FA Cup"], display:"FA Cup", region:"general", priority:78 },
  { mode:"country", country:"England", type:"cup", names:["EFL Cup","League Cup","Carabao Cup"], display:"EFL Cup", region:"general", priority:74 },
  { mode:"country", country:"England", type:"league", names:["Championship"], display:"Championship", region:"general", priority:80 },
  { mode:"country", country:"Spain", type:"league", names:["La Liga","LaLiga"], display:"La Liga", region:"general", priority:98 },
  { mode:"country", country:"Spain", type:"cup", names:["Copa del Rey","Copa Del Rey"], display:"Copa del Rey", region:"general", priority:76 },
  { mode:"country", country:"Italy", type:"league", names:["Serie A"], display:"Serie A", region:"general", priority:97 },
  { mode:"country", country:"Italy", type:"cup", names:["Coppa Italia","Coppa Italia Frecciarossa"], display:"Coppa Italia", region:"general", priority:76 },
  { mode:"country", country:"Germany", type:"league", names:["Bundesliga","1. Bundesliga"], display:"Bundesliga", region:"general", priority:96 },
  { mode:"country", country:"Germany", type:"cup", names:["DFB Pokal","DFB-Pokal","DFB Cup"], display:"DFB-Pokal", region:"general", priority:76 },
  { mode:"country", country:"France", type:"league", names:["Ligue 1","Ligue 1 Uber Eats"], display:"Ligue 1", region:"general", priority:95 },
  { mode:"country", country:"France", type:"cup", names:["Coupe de France","Coupe De France"], display:"Coupe de France", region:"general", priority:74 },
  { mode:"country", country:"Netherlands", type:"league", names:["Eredivisie"], display:"Eredivisie", region:"general", priority:90 },
  { mode:"country", country:"Netherlands", type:"cup", names:["KNVB Cup","KNVB Beker"], display:"KNVB Cup", region:"general", priority:72 },
  { mode:"country", country:"Portugal", type:"league", names:["Primeira Liga","Liga Portugal Betclic","Liga NOS","Liga Portugal"], display:"Primeira Liga", region:"general", priority:89 },
  { mode:"country", country:"Portugal", type:"cup", names:["Taça de Portugal","Taca de Portugal","Portuguese Cup"], display:"Taça de Portugal", region:"general", priority:72 },
  { mode:"country", country:"Portugal", type:"cup", names:["Taça da Liga","Taca da Liga"], display:"Taça da Liga", region:"general", priority:70 },
  { mode:"country", country:"Turkey", type:"league", names:["Süper Lig","Super Lig"], display:"Super Lig", region:"general", priority:78 },
  { mode:"country", country:"Turkey", type:"cup", names:["Turkish Cup","Ziraat Türkiye Kupası","Ziraat Kupasi"], display:"Copa da Turquia", region:"general", priority:70 },
  { mode:"country", country:"Denmark", type:"league", names:["Superliga","Superligaen","3F Superliga"], display:"Superliga", region:"general", priority:75 },
  { mode:"country", country:"Greece", type:"league", names:["Super League 1","Super League","Super League Greece"], display:"Super League Greece", region:"general", priority:74 },
  { mode:"country", country:"Belgium", type:"league", names:["Pro League","Jupiler Pro League","Belgian Pro League"], display:"Belgian Pro League", region:"general", priority:85 },
  { mode:"country", country:"Belgium", type:"cup", names:["Belgian Cup","Croky Cup"], display:"Copa da Bélgica", region:"general", priority:70 },
  { mode:"country", country:"Austria", type:"league", names:["Bundesliga","Austrian Bundesliga"], display:"Austrian Bundesliga", region:"general", priority:84 },
  { mode:"country", country:"Austria", type:"cup", names:["Austrian Cup","ÖFB Cup","OFB Cup"], display:"Copa da Áustria", region:"general", priority:70 },
  { mode:"country", country:"Greece", type:"cup", names:["Greek Cup","Greece Cup"], display:"Copa da Grécia", region:"general", priority:70 },
  { mode:"country", country:"Denmark", type:"cup", names:["Danish Cup","DBU Pokalen"], display:"Copa da Dinamarca", region:"general", priority:70 },
  { mode:"country", country:"Scotland", type:"league", names:["Premiership","Scottish Premiership"], display:"Scottish Premiership", region:"general", priority:78 },
  { mode:"country", country:"Scotland", type:"cup", names:["Scottish Cup","Scottish FA Cup"], display:"Scottish Cup", region:"general", priority:70 },
  { mode:"country", country:"Sweden", type:"league", names:["Allsvenskan"], display:"Allsvenskan", region:"general", priority:74 },
  { mode:"country", country:"Norway", type:"league", names:["Eliteserien"], display:"Eliteserien", region:"general", priority:74 },
  { mode:"search", search:"Saudi", display:"Saudi Pro League", region:"general", priority:85 },
  { mode:"country", country:"USA", type:"league", names:["Major League Soccer","MLS"], display:"MLS", region:"america", priority:90 },
  { mode:"country", country:"USA", type:"cup", names:["Leagues Cup"], display:"Leagues Cup", region:"america", priority:76 },
  { mode:"country", country:"Mexico", type:"league", names:["Liga MX","Liga BBVA MX"], display:"Liga MX", region:"general", priority:79 },
  { mode:"country", country:"Mexico", type:"cup", names:["Copa MX","Copa por México"], display:"Copa MX", region:"general", priority:70 },
  { mode:"search", search:"CONCACAF Champions", display:"CONCACAF Champions Cup", region:"america", priority:88 },
  { mode:"search", search:"UEFA Champions League", display:"UEFA Champions League", region:"general", priority:98 },
  { mode:"search", search:"UEFA Europa League", display:"UEFA Europa League", region:"general", priority:93 },
  { mode:"search", search:"UEFA Europa Conference League", display:"UEFA Conference League", region:"general", priority:88 },
  { mode:"search", search:"Conference League", display:"UEFA Conference League", region:"general", priority:88 },
  { mode:"search", search:"CONMEBOL Libertadores", display:"Libertadores", region:"brazil", priority:92 },
  { mode:"search", search:"Copa Libertadores", display:"Libertadores", region:"brazil", priority:92 },
  { mode:"search", search:"CONMEBOL Sudamericana", display:"Sul-Americana", region:"brazil", priority:86 },
  { mode:"search", search:"Copa Sudamericana", display:"Sul-Americana", region:"brazil", priority:86 },
  { mode:"search", search:"CONMEBOL Recopa", display:"Recopa Sul-Americana", region:"brazil", priority:80 },
  { mode:"search", search:"UEFA Nations League", display:"Nations League", region:"international", priority:95 },
  { mode:"search", search:"International Friendlies", display:"Amistosos Internacionais", region:"international", priority:90 },
  { mode:"search", search:"Friendlies", display:"Amistosos Internacionais", region:"international", priority:88 },
  { mode:"search", search:"World Cup - Qualification Europe", display:"Eliminatórias Europeias", region:"international", priority:94 },
  { mode:"search", search:"CONMEBOL World Cup Qualifiers", display:"Eliminatórias Sul-Americanas", region:"international", priority:96 },
  { mode:"search", search:"World Cup - Qualification South America", display:"Eliminatórias Sul-Americanas", region:"international", priority:96 },
  { mode:"search", search:"World Cup - Qualification Africa", display:"Eliminatórias Africanas", region:"international", priority:88 },
  { mode:"search", search:"World Cup - Qualification Asia", display:"Eliminatórias Asiáticas", region:"international", priority:88 },
  { mode:"search", search:"World Cup - Qualification CONCACAF", display:"Eliminatórias CONCACAF", region:"international", priority:88 },
  { mode:"search", search:"Copa America", display:"Copa América", region:"international", priority:98 },
  { mode:"search", search:"UEFA European Championship", display:"Eurocopa", region:"international", priority:98 },
  // Copa do Mundo gerenciada exclusivamente pelo copa-sync.js (roda a cada hora)
  // { mode:"id", leagueId:1, season:2026, display:"Copa do Mundo 2026", region:"international", priority:100 },
  // { mode:"search", search:"FIFA World Cup", season:2026, display:"Copa do Mundo", region:"international", priority:100 },
  { mode:"search", search:"FIFA Club World Cup", display:"Mundial de Clubes", region:"general", priority:96 },
  { mode:"search", search:"Africa Cup of Nations", display:"Copa Africana", region:"international", priority:90 },
]

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }
function safeNumber(v, fb = 0) { const n = Number(v); return Number.isFinite(n) ? n : fb }
function round(v, d = 2) { return Number(Number(v || 0).toFixed(d)) }
function clamp(v, min, max) { return Math.max(min, Math.min(max, v)) }
function sum(arr) { return arr.reduce((a, v) => a + v, 0) }
function isoDate(d) { return d.toISOString().slice(0, 10) }
function normalizeText(v) {
  return String(v || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/\s+/g, " ").trim()
}
function uniqBy(arr, getKey) {
  const seen = new Set()
  return arr.filter(item => { const k = getKey(item); if (seen.has(k)) return false; seen.add(k); return true })
}
function makeApiCacheKey(path, params = {}) {
  return `${path}?${Object.entries(params).sort(([a],[b]) => a.localeCompare(b)).map(([k,v]) => `${k}=${v}`).join("&")}`
}

async function api(path, params = {}) {
  const key = makeApiCacheKey(path, params)
  if (apiCache.has(key)) return apiCache.get(key)
  await sleep(REQUEST_DELAY_MS)
  const url = new URL(API + path)
  Object.entries(params).forEach(([k,v]) => { if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v)) })
  const res = await fetch(url, { headers: { "x-apisports-key": APISPORTS_KEY } })
  if (!res.ok) { const t = await res.text(); throw new Error(`API ${res.status} em ${path}: ${t}`) }
  const json = await res.json()
  if (json.errors && Object.keys(json.errors).length > 0) throw new Error(`API error em ${path}: ${JSON.stringify(json.errors)}`)
  const data = json.response || []
  apiCache.set(key, data)
  return data
}

function getSyncWindowRange() {
  const now = new Date()
  const start = new Date(now); start.setHours(0,0,0,0)
  const end = new Date(now.getTime() + WINDOW_HOURS * 3600000)
  return { start, end }
}

function hasForbiddenMarker(v = "") {
  const s = normalizeText(v)
  return ["u17","u18","u19","u20","u21","u23","under 17","under 18","under 19","under 20","under 21","under 23",
    "sub 17","sub 18","sub 19","sub 20","sub 21","sub 23","women","woman","female","feminino","feminina",
    "femenil","frauen","vrouwen","reserve","reserves","youth"].some(m => s.includes(m))
}

function isExactTargetLeague(target, rawName, country) {
  const raw = normalizeText(rawName)
  const c = normalizeText(country || target.country || "")
  if (hasForbiddenMarker(raw)) return false
  if (target.country === "France" && target.display === "Ligue 1") {
    if (raw.includes("ligue 2") || raw.includes("national") || raw.includes("b") ) return false
    return c === "france" && (raw === "ligue 1" || raw === "ligue 1 uber eats" || raw === "ligue 1 mcdonald's")
  }
  if (target.country === "Germany" && target.display === "Bundesliga") return c === "germany" && (raw === "bundesliga" || raw === "1. bundesliga")
  if (target.country === "Belgium" && target.display === "Belgian Pro League") return c === "belgium" && (raw === "pro league" || raw === "jupiler pro league")
  if (target.country === "Mexico" && target.display === "Liga MX") return c === "mexico" && (raw === "liga mx" || raw === "liga bbva mx")
  if (target.country === "Netherlands" && target.display === "Eredivisie") return c === "netherlands" && raw === "eredivisie"
  if (target.country === "Austria" && target.display === "Austrian Bundesliga") return c === "austria" && raw === "bundesliga"
  if (target.country === "England" && target.display === "Premier League") return c === "england" && raw === "premier league"
  if (target.country === "Spain" && target.display === "La Liga") return c === "spain" && (raw === "la liga" || raw === "laliga")
  if (target.country === "Italy" && target.display === "Serie A") return c === "italy" && raw === "serie a"
  if (target.country === "Turkey" && target.display === "Super Lig") return c === "turkey" && (raw === "süper lig" || raw === "super lig")
  if (target.country === "Greece" && target.display === "Super League Greece") return c === "greece" && (raw === "super league 1" || raw === "super league" || raw === "super league greece")
  if (target.country === "Scotland" && target.display === "Scottish Premiership") return c === "scotland" && (raw === "premiership" || raw === "scottish premiership")
  return null
}

function isLikelyClubName(name = "") {
  const v = normalizeText(name)
  return ["fc","sc","afc","cf","ac","club","united","city","rovers","athletic","atletico","deportivo","sporting","jk","fk","bk","if"].some(m => v.includes(m))
}

function isClubFriendlyFixture(fixture) {
  if (isLikelyClubName(fixture?.teams?.home?.name || "") || isLikelyClubName(fixture?.teams?.away?.name || "")) return true
  if (fixture?.teams?.home?.national === true && fixture?.teams?.away?.national === true) return false
  return false
}

function isInternationalCompetition(comp, fixture = null) {
  const region = normalizeText(comp?.region || "")
  const display = normalizeText(comp?.display || "")
  const leagueName = normalizeText(fixture?.league?.name || "")
  const country = normalizeText(comp?.country || fixture?.league?.country || "")
  return region === "international" || display.includes("amistosos") || display.includes("nations league") ||
    display.includes("eliminatorias") || display.includes("eurocopa") || display.includes("copa america") ||
    display.includes("copa do mundo") || display.includes("copa africana") ||
    leagueName.includes("friendlies") || country === "world"
}

function isLikelyNationalTeamMatch(fixture, comp) {
  if (!isInternationalCompetition(comp, fixture)) return false
  if (fixture?.teams?.home?.national === true && fixture?.teams?.away?.national === true) return true
  if (isClubFriendlyFixture(fixture)) return false
  return true
}

function normalizeCompetitionName(country, rawName, fallbackDisplay) {
  const name = String(rawName || "").trim()
  const c = String(country || "").trim()
  const norm = normalizeText(name)
  if (c === "Brazil") {
    if (name === "Serie A") return "Brasileirão Série A"
    if (name === "Serie B") return "Brasileirão Série B"
    if (norm.includes("copa do brasil")) return "Copa do Brasil"
    if (norm.includes("nordeste")) return "Copa do Nordeste"
    if (norm.includes("verde")) return "Copa Verde"
    if (norm.includes("sul-sudeste") || norm.includes("sul sudeste")) return "Copa Sul-Sudeste"
    if (norm.includes("women") || norm.includes("feminino") || norm.includes("feminina")) return "Brasileirão Feminino"
  }
  if (c === "Argentina") {
    if (["Liga Profesional Argentina","Liga Profesional","Primera División","Primera Division","Superliga"].includes(name)) return "Liga Argentina"
    if (norm.includes("copa argentina") || norm.includes("copa de la liga")) return "Copa Argentina"
  }
  if (c === "Chile") { if (norm.includes("primeira")) return "Liga Chilena"; if (norm.includes("copa chile")) return "Copa Chile" }
  if (c === "Colombia") { if (norm.includes("liga") || norm.includes("primeira")) return "Liga Colombiana"; if (norm.includes("copa")) return "Copa Colombia" }
  if (c === "Peru") return "Liga Peruana"
  if (c === "Uruguay") return "Liga Uruguaia"
  if (c === "England") {
    if (name === "Premier League") return "Premier League"
    if (norm.includes("fa cup") || norm.includes("emirates fa cup")) return "FA Cup"
    if (norm.includes("efl cup") || norm.includes("league cup") || norm.includes("carabao")) return "EFL Cup"
    if (name === "Championship") return "Championship"
  }
  if (c === "Spain") {
    if (name === "La Liga" || name === "LaLiga") return "La Liga"
    if (norm.includes("copa del rey")) return "Copa del Rey"
  }
  if (c === "Italy") {
    if (name === "Serie A") return "Serie A"
    if (norm.includes("coppa italia")) return "Coppa Italia"
  }
  if (c === "Germany") {
    if (norm === "bundesliga" || norm === "1. bundesliga") return "Bundesliga"
    if (norm.includes("dfb") || norm.includes("pokal")) return "DFB-Pokal"
  }
  if (c === "France") {
    if (norm.includes("ligue 1")) return "Ligue 1"
    if (norm.includes("coupe de france")) return "Coupe de France"
  }
  if (c === "Netherlands") { if (norm === "eredivisie") return "Eredivisie"; if (norm.includes("knvb")) return "KNVB Cup" }
  if (c === "Portugal") {
    if (name === "Primeira Liga" || norm.includes("liga portugal")) return "Primeira Liga"
    if (norm.includes("taca de portugal") || norm.includes("taça de portugal")) return "Taça de Portugal"
    if (norm.includes("taca da liga") || norm.includes("taça da liga")) return "Taça da Liga"
  }
  if (c === "Turkey") {
    if (norm.includes("super lig") || norm.includes("süper lig")) return "Super Lig"
    if (norm.includes("cup") || norm.includes("kupa") || norm.includes("kupasi")) return "Copa da Turquia"
  }
  if (c === "Greece") { if (norm.includes("super league")) return "Super League Greece"; if (norm.includes("cup")) return "Copa da Grécia" }
  if (c === "Belgium") { if (norm === "pro league" || norm.includes("jupiler")) return "Belgian Pro League"; if (norm.includes("cup") || norm.includes("croky")) return "Copa da Bélgica" }
  if (c === "Austria") {
    if (norm === "bundesliga") return "Austrian Bundesliga"
    if (norm.includes("cup") || norm.includes("ofb")) return "Copa da Áustria"
  }
  if (c === "Denmark") {
    if (norm.includes("superliga") || norm.includes("superligaen")) return "Superliga"
    if (norm.includes("cup") || norm.includes("dbu")) return "Copa da Dinamarca"
  }
  if (c === "Scotland") { if (norm.includes("premiership")) return "Scottish Premiership"; if (norm.includes("cup")) return "Scottish Cup" }
  if (c === "Sweden") return "Allsvenskan"
  if (c === "Norway") return "Eliteserien"
  if (c === "Saudi Arabia" && norm.includes("pro league")) return "Saudi Pro League"
  if (c === "USA") { if (name === "Major League Soccer" || name === "MLS") return "MLS"; if (norm.includes("leagues cup")) return "Leagues Cup" }
  if (c === "Mexico") { if (norm.includes("liga mx") || norm.includes("liga bbva")) return "Liga MX"; if (norm.includes("copa")) return "Copa MX" }
  if (name === "UEFA Europa Conference League") return "UEFA Conference League"
  if (name === "CONMEBOL Libertadores") return "Libertadores"
  if (name === "CONMEBOL Sudamericana") return "Sul-Americana"
  if (norm.includes("recopa")) return "Recopa Sul-Americana"
  if (norm.includes("nations league")) return "Nations League"
  if (norm.includes("friendlies")) return "Amistosos Internacionais"
  if (norm.includes("club world cup")) return "Mundial de Clubes"
  if (norm.includes("africa cup") || norm.includes("afcon")) return "Copa Africana"
  return fallbackDisplay || name || c || "Competição"
}

function isExactBrazilRegionalMatch(targetDisplay, country, rawName) {
  const norm = normalizeText(`${country || ""} ${rawName || ""}`)
  if (targetDisplay === "Copa do Nordeste") return norm.includes("brazil") && norm.includes("nordeste")
  if (targetDisplay === "Copa Verde") return norm.includes("brazil") && norm.includes("verde")
  if (targetDisplay === "Copa Sul-Sudeste") return norm.includes("brazil") && (norm.includes("sul-sudeste") || norm.includes("sul sudeste"))
  return true
}

async function resolveCountryCompetitions(target) {
  const leagues = await api("/leagues", { country: target.country, current: true })
  const normalizedNames = new Set((target.names || []).map(x => normalizeText(x)))
  return leagues.filter(item => {
    const rawName = String(item?.league?.name || "")
    const leagueType = String(item?.league?.type || "").toLowerCase()
    const seasonCurrent = item?.seasons?.find(s => s.current) || 
      item?.seasons?.sort((a,b) => b.year - a.year)?.[0]
    if (!seasonCurrent) return false
    if (target.type && leagueType !== target.type) return false
    if (hasForbiddenMarker(rawName)) return false
    if (target.country === "Italy" && normalizeText(rawName).includes("women")) return false
    const rawNorm = normalizeText(rawName)
    if (target.type === "league") {
      if (rawNorm.includes("ligue 2")) return false
      if (rawNorm.includes("serie b") && target.country !== "Brazil") return false
      if (rawNorm.includes("2. bundesliga") || rawNorm.includes("bundesliga 2")) return false
      if (rawNorm.includes("segunda") && !["Argentina","Spain","Mexico"].includes(target.country)) return false
      if (rawNorm.includes("segunda division") && target.country === "Spain") return false
      if (rawNorm.includes("championship") && target.country !== "England") return false
    }
    const exactDecision = isExactTargetLeague(target, rawName, item.country?.name || target.country)
    if (exactDecision !== null) return exactDecision
    return Array.from(normalizedNames).some(n => normalizeText(rawName) === n)
  }).map(item => {
    const currentSeason = item?.seasons?.find(s => s.current) || item?.seasons?.[0]
    return {
      leagueId: item.league.id, season: currentSeason.year,
      country: item.country?.name || target.country, rawName: item.league.name,
      display: normalizeCompetitionName(item.country?.name || target.country, item.league.name, target.display),
      region: target.region, priority: target.priority,
    }
  })
}

async function resolveSearchCompetition(target) {
  const leagues = await api("/leagues", { search: target.search })
  const items = leagues.map(item => {
    // Use explicit target.season if provided, then current, then fallback
    const currentSeason = (target.season ? { year: target.season } : null) ||
      item?.seasons?.find(s => s.current) ||
      item?.seasons?.sort((a,b) => b.year - a.year)?.[0]
    if (!currentSeason) return null
    const country = item?.country?.name || null
    const rawName = String(item?.league?.name || "").trim()
    const haystack = normalizeText(`${country || ""} ${rawName}`)
    const countryLower = String(country || "").toLowerCase()
    if (hasForbiddenMarker(rawName)) return null
    if (haystack.includes("open cup")) return null
    if (target.display === "Saudi Pro League") {
      const rawLower = normalizeText(rawName)
      if (!countryLower.includes("saudi")) return null
      if (!(rawLower === "pro league" || rawLower.includes("saudi pro league") || rawLower.includes("spl"))) return null
    }
    if (!isExactBrazilRegionalMatch(target.display, country, rawName)) return null
    if (target.display === "Amistosos Internacionais" && !haystack.includes("friend")) return null
    if (target.display === "Nations League" && !haystack.includes("nations")) return null
    return {
      leagueId: item.league.id, season: currentSeason.year, country, rawName,
      display: normalizeCompetitionName(country, rawName, target.display),
      region: target.region, priority: target.priority,
    }
  }).filter(Boolean)
  return uniqBy(items, x => `${x.leagueId}:${x.season}`)
}

async function resolveTargetCompetitions() {
  const resolved = []
  for (const target of TARGET_COMPETITIONS) {
    try {
      let items
      if (target.mode === "id") {
        // Direct league ID — no search needed, just use provided leagueId + season
        items = [{ leagueId: target.leagueId, season: target.season, country: target.country||null,
          rawName: target.display, display: target.display, region: target.region, priority: target.priority }]
      } else if (target.mode === "country") {
        items = await resolveCountryCompetitions(target)
      } else {
        items = await resolveSearchCompetition(target)
      }
      resolved.push(...items)
    } catch (err) {
      console.error(`Falha resolvendo ${target.display || target.search || target.country}:`, err.message)
    }
  }
  return uniqBy(resolved, x => `${x.leagueId}:${x.season}`)
}

async function fetchFixturesForCompetition(comp) {
  const cacheKey = `${comp.leagueId}:${comp.season}`
  if (competitionFixturesCache.has(cacheKey)) return competitionFixturesCache.get(cacheKey)
  const { start, end } = getSyncWindowRange()
  // Competições de prioridade alta (Copa do Mundo, Brasileirão A/B, Copa do Brasil, ...) tentam
  // de novo em caso de falha transitória (rate limit, erro de rede pontual) — o sync faz uma
  // sequência longa de chamadas à API (70+ competições), então um soluço isolado é esperado de
  // vez em quando. Sem retry, esse soluço apagava a competição inteira até o próximo sync.
  const maxAttempts = comp.priority >= 95 ? 3 : 1
  let lastErr = null
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const fixtures = await api("/fixtures", { league: comp.leagueId, season: comp.season, from: isoDate(start), to: isoDate(end), timezone: TIMEZONE })
      const cleaned = fixtures.filter(f => {
        if (hasForbiddenMarker(f?.teams?.home?.name || "") || hasForbiddenMarker(f?.teams?.away?.name || "") || hasForbiddenMarker(f?.league?.name || "")) return false
        if (normalizeText(f?.league?.name || "").includes("open cup")) return false
        if (normalizeText(comp.display).includes("amistosos") && isClubFriendlyFixture(f)) return false
        return true
      }).map(f => ({ ...f, __comp: comp }))
      competitionFixturesCache.set(cacheKey, cleaned)
      return cleaned
    } catch (err) {
      lastErr = err
      if (attempt < maxAttempts) {
        console.warn(`⚠️  Tentativa ${attempt}/${maxAttempts} falhou buscando fixtures de ${comp.display}: ${err.message} — tentando de novo...`)
        await sleep(REQUEST_DELAY_MS * 4)
      }
    }
  }
  console.error(`Falha buscando fixtures de ${comp.display}:`, lastErr?.message)
  competitionFixturesCache.set(cacheKey, [])
  return []
}

function isCompletedFixture(f) {
  return ["FT","AET","PEN"].includes(String(f?.fixture?.status?.short || "").toUpperCase())
}

async function getFixtureStatistics(fixtureId) {
  if (fixtureStatsCache.has(fixtureId)) return fixtureStatsCache.get(fixtureId)
  try {
    const stats = await api("/fixtures/statistics", { fixture: fixtureId })
    fixtureStatsCache.set(fixtureId, stats)
    return stats
  } catch (err) {
    fixtureStatsCache.set(fixtureId, [])
    return []
  }
}

function extractStatValue(statistics = [], type) {
  const found = statistics.find(x => x.type === type)
  if (!found || found.value === null || found.value === undefined) return 0
  if (typeof found.value === "string") { const n = Number(found.value.replace("%","").trim()); return Number.isFinite(n) ? n : 0 }
  return safeNumber(found.value)
}

async function fetchRecentFinishedFixtures(teamId, limit = MAX_RECENT_FIXTURES_FETCH) {
  const cacheKey = `${teamId}:${limit}`
  if (teamRecentFixturesCache.has(cacheKey)) return teamRecentFixturesCache.get(cacheKey)
  try {
    const fixtures = await api("/fixtures", { team: teamId, last: limit, timezone: TIMEZONE })
    const cleaned = fixtures.filter(isCompletedFixture).sort((a,b) => new Date(b.fixture.date) - new Date(a.fixture.date))
    teamRecentFixturesCache.set(cacheKey, cleaned)
    return cleaned
  } catch (err) {
    teamRecentFixturesCache.set(cacheKey, [])
    return []
  }
}

async function fetchH2HFixtures(homeTeamId, awayTeamId) {
  const cacheKey = `h2h:${homeTeamId}:${awayTeamId}`
  if (h2hCache.has(cacheKey)) return h2hCache.get(cacheKey)
  try {
    const fixtures = await api("/fixtures/headtohead", { h2h: `${homeTeamId}-${awayTeamId}`, last: H2H_LIMIT })
    const completed = fixtures.filter(isCompletedFixture).sort((a,b) => new Date(b.fixture.date) - new Date(a.fixture.date))
    h2hCache.set(cacheKey, completed)
    return completed
  } catch (err) {
    console.error(`Falha H2H ${homeTeamId} vs ${awayTeamId}:`, err.message)
    h2hCache.set(cacheKey, [])
    return []
  }
}

function getGoalsForAgainst(fixture, teamId) {
  const isHome = fixture?.teams?.home?.id === teamId
  const gf = isHome ? safeNumber(fixture?.goals?.home) : safeNumber(fixture?.goals?.away)
  const ga = isHome ? safeNumber(fixture?.goals?.away) : safeNumber(fixture?.goals?.home)
  return { gf, ga, isHome }
}

function buildScoreLabelForTeam(fixture, teamId) {
  const { gf, ga } = getGoalsForAgainst(fixture, teamId)
  return `${gf}-${ga}`
}

function weightedAverage(rows, key, fallback = 0) {
  if (!rows.length) return fallback
  const weights = rows.map((_,i) => Math.max(1, rows.length - i))
  const totalWeight = sum(weights)
  if (!totalWeight) return fallback
  return rows.reduce((acc, row, i) => acc + safeNumber(row[key], 0) * weights[i], 0) / totalWeight
}

function splitVenueFixtures(fixtures, teamId, wantHome, limit = FORM_LIMIT_HOME_AWAY) {
  return fixtures.filter(f => { const isHome = f?.teams?.home?.id === teamId; return wantHome ? isHome : !isHome }).slice(0, limit)
}

function detectFormStreak(fixtures, teamId, limit = 5) {
  const recent = fixtures.slice(0, limit)
  if (!recent.length) return "sem dados"
  const results = recent.map(f => { const { gf, ga } = getGoalsForAgainst(f, teamId); return gf > ga ? "W" : gf === ga ? "D" : "L" })
  const wins = results.filter(r => r === "W").length
  const losses = results.filter(r => r === "L").length
  if (wins >= 4) return "em alta"
  if (losses >= 4) return "em queda"
  if (wins >= 3) return "boa fase"
  if (losses >= 3) return "má fase"
  return "irregular"
}

async function collectProfileFromFixtures(teamId, fixturesSubset) {
  if (!fixturesSubset.length) return { matches:0, statsMatches:0, avgGoalsFor:0, avgGoalsAgainst:0, avgShots:0, avgShotsOnTarget:0, avgCorners:0, avgCards:0, avgFouls:0, recentScores:[], formStreak:"sem dados" }
  const rows = []
  for (const fixture of fixturesSubset) {
    const { gf, ga } = getGoalsForAgainst(fixture, teamId)
    const stats = await getFixtureStatistics(fixture.fixture.id)
    const teamStats = stats.find(s => s.team.id === teamId)?.statistics || []
    rows.push({ goalsFor:gf, goalsAgainst:ga, shots:extractStatValue(teamStats,"Total Shots"), shotsOnTarget:extractStatValue(teamStats,"Shots on Goal"), corners:extractStatValue(teamStats,"Corner Kicks"), fouls:extractStatValue(teamStats,"Fouls"), cards:extractStatValue(teamStats,"Yellow Cards")+extractStatValue(teamStats,"Red Cards"), scoreLabel:buildScoreLabelForTeam(fixture,teamId),
      opponent: fixture.teams?.home?.id === teamId ? (fixture.teams?.away?.name||"?") : (fixture.teams?.home?.name||"?"),
      opponentLogo: fixture.teams?.home?.id === teamId ? (fixture.teams?.away?.logo||null) : (fixture.teams?.home?.logo||null),
      isHome: fixture.teams?.home?.id === teamId,
      date: fixture.fixture?.date || null
    })
  }
  // Only keep rows where the API returned at least some data
  // But if very few valid rows, fall back to all rows with at least goals
  const strictRows = rows.filter(r => r.shots > 0 || r.shotsOnTarget > 0 || r.corners > 0 || r.fouls > 0 || r.cards > 0)
  const statsRows = strictRows.length >= 3 ? strictRows : rows.filter(r => r.goalsFor !== null || r.goalsAgainst !== null)
  return { matches:rows.length, statsMatches:statsRows.length, avgGoalsFor:round(weightedAverage(rows,"goalsFor")), avgGoalsAgainst:round(weightedAverage(rows,"goalsAgainst")), avgShots:round(weightedAverage(statsRows,"shots")), avgShotsOnTarget:round(weightedAverage(statsRows,"shotsOnTarget")), avgCorners:(()=>{ const v=round(weightedAverage(statsRows,"corners")); return v>0?v:null; })(),
    avgCards:round(weightedAverage(statsRows,"cards")), avgFouls:round(weightedAverage(statsRows,"fouls")), recentScores:rows.map(r=>r.scoreLabel).slice(0,5),
    recentMatches:rows.slice(0,5).map(r=>({score:r.scoreLabel,opponent:r.opponent,opponentLogo:r.opponentLogo,isHome:r.isHome,date:r.date})),
    formStreak:detectFormStreak(fixturesSubset,teamId) }
}

async function buildH2HProfile(homeTeamId, awayTeamId) {
  const fixtures = await fetchH2HFixtures(homeTeamId, awayTeamId)
  if (fixtures.length < 3) return null
  const homeRows = [], awayRows = []
  for (const f of fixtures.slice(0, H2H_LIMIT)) {
    const { gf:hGF, ga:hGA } = getGoalsForAgainst(f, homeTeamId)
    const { gf:aGF, ga:aGA } = getGoalsForAgainst(f, awayTeamId)
    homeRows.push({ goalsFor:hGF, goalsAgainst:hGA })
    awayRows.push({ goalsFor:aGF, goalsAgainst:aGA })
  }
  return {
    matches: fixtures.length,
    homeAvgGoalsFor: round(weightedAverage(homeRows,"goalsFor")),
    homeAvgGoalsAgainst: round(weightedAverage(homeRows,"goalsAgainst")),
    awayAvgGoalsFor: round(weightedAverage(awayRows,"goalsFor")),
    awayAvgGoalsAgainst: round(weightedAverage(awayRows,"goalsAgainst")),
    recentScores: fixtures.slice(0,5).map(f => {
      const hg = safeNumber(f?.goals?.home), ag = safeNumber(f?.goals?.away)
      const hn = f?.teams?.home?.name || "?", an = f?.teams?.away?.name || "?"
      return `${hn} ${hg}-${ag} ${an}`
    }),
  }
}

async function fetchTeamStatisticsFromDB(teamId, leagueId) {
  // Busca estatísticas de liga específica do banco (populado pelo team-stats-sync)
  // Muito mais preciso que calcular de match_stats genéricos
  const { data, error } = await supabase
    .from('team_statistics')
    .select('*')
    .eq('team_id', teamId)
    .eq('league_id', leagueId)
    .eq('season', 2026)
    .single()
  return error ? null : data
}

async function buildTeamContext(teamId, leagueId = null) {
  const cacheKey = `${teamId}:${leagueId || 'any'}`
  if (teamContextCache.has(cacheKey)) return teamContextCache.get(cacheKey)

  const allFixtures = await fetchRecentFinishedFixtures(teamId, MAX_RECENT_FIXTURES_FETCH)

  // [FIX ESCANTEIOS] Filtra amistosos do perfil — amistosos têm ~4.4 escanteios vs
  // ~9.4 no Brasileirão. Misturar eles contamina as projeções de escanteios/cartões.
  const competitiveFixtures = allFixtures.filter(f => {
    const leagueName = normalizeText(f?.league?.name || '')
    return !leagueName.includes('friend') && !leagueName.includes('amistoso') &&
           !leagueName.includes('club friendly') && f?.league?.id !== 667
  })

  // Usa fixtures competitivos se tiver pelo menos 3; caso contrário usa tudo
  const fixtures = competitiveFixtures.length >= 3 ? competitiveFixtures : allFixtures

  const general = await collectProfileFromFixtures(teamId, fixtures.slice(0, FORM_LIMIT_GENERAL))
  const home    = await collectProfileFromFixtures(teamId, splitVenueFixtures(fixtures, teamId, true, FORM_LIMIT_HOME_AWAY))
  const away    = await collectProfileFromFixtures(teamId, splitVenueFixtures(fixtures, teamId, false, FORM_LIMIT_HOME_AWAY))

  // [MELHORIA] Enriquece com estatísticas de liga específica quando disponível
  // Especialmente para corners e cards que são muito sensíveis ao tipo de competição
  let leagueStats = null
  if (leagueId) {
    try { leagueStats = await fetchTeamStatisticsFromDB(teamId, leagueId) } catch {}
  }

  if (leagueStats) {
    // Substitui corners e cards com dados de liga real (muito mais precisos)
    const blendLeague = (leagueVal, calcVal) =>
      leagueVal != null ? round(leagueVal * 0.75 + (calcVal || 0) * 0.25) : calcVal

    const cornersForAvg = blendLeague(leagueStats.corners_for_avg, general.avgCorners)
    const cornersAgainstAvg = blendLeague(leagueStats.corners_against_avg, null)
    const cardsAvg = blendLeague(
      leagueStats.cards_yellow_avg != null ? leagueStats.cards_yellow_avg + (leagueStats.cards_red_avg || 0) : null,
      general.avgCards
    )

    general.avgCorners = cornersForAvg
    general.avgCornersAgainst = cornersAgainstAvg
    general.avgCards = cardsAvg || general.avgCards
    general.avgShots = blendLeague(leagueStats.shots_avg, general.avgShots) || general.avgShots
    general.avgShotsOnTarget = blendLeague(leagueStats.shots_on_target_avg, general.avgShotsOnTarget) || general.avgShotsOnTarget
    general._leagueStats = true
    console.log(`   📊 Liga stats: corners ${cornersForAvg} for/${cornersAgainstAvg} against, cards ${cardsAvg}`)
  }

  const payload = { general, home, away }
  teamContextCache.set(cacheKey, payload)
  return payload
}

function blendValue(primary, fallback, primaryWeight = 0.68) {
  return safeNumber(primary,0)*primaryWeight + safeNumber(fallback,0)*(1-primaryWeight)
}

function buildSideProfile(teamContext, side) {
  const sideProfile = side === "home" ? teamContext.home : teamContext.away
  const general = teamContext.general
  const hasSideData = sideProfile.matches >= 2
  const hasSideStats = sideProfile.statsMatches >= 2
  return {
    matches:sideProfile.matches, statsMatches:sideProfile.statsMatches,
    avgGoalsFor:round(hasSideData?blendValue(sideProfile.avgGoalsFor,general.avgGoalsFor,0.70):general.avgGoalsFor),
    avgGoalsAgainst:round(hasSideData?blendValue(sideProfile.avgGoalsAgainst,general.avgGoalsAgainst,0.70):general.avgGoalsAgainst),
    avgShots:round(hasSideStats?blendValue(sideProfile.avgShots,general.avgShots,0.72):general.avgShots),
    avgShotsOnTarget:round(hasSideStats?blendValue(sideProfile.avgShotsOnTarget,general.avgShotsOnTarget,0.72):general.avgShotsOnTarget),
    avgCorners:round(hasSideStats?blendValue(sideProfile.avgCorners,general.avgCorners,0.72):general.avgCorners),
    avgCards:round(hasSideStats?blendValue(sideProfile.avgCards,general.avgCards,0.66):general.avgCards),
    avgFouls:round(hasSideStats?blendValue(sideProfile.avgFouls,general.avgFouls,0.66):general.avgFouls),
    recentScores:sideProfile.recentScores?.length?sideProfile.recentScores:general.recentScores||[],
    recentMatches:sideProfile.recentMatches?.length?sideProfile.recentMatches:general.recentMatches||[],
    formStreak:sideProfile.formStreak||general.formStreak||"sem dados",
  }
}

function isUsableTeamProfile(sideProfile, generalProfile) {
  return safeNumber(generalProfile.matches,0) >= MIN_REQUIRED_RECENT_MATCHES && safeNumber(generalProfile.statsMatches,0) >= MIN_REQUIRED_STATS_MATCHES
}

function buildExpectedMetrics(homeProfile, awayProfile, h2hProfile = null) {
  let homeGoalsFor = homeProfile.avgGoalsFor
  let homeGoalsAgainst = homeProfile.avgGoalsAgainst
  let awayGoalsFor = awayProfile.avgGoalsFor
  let awayGoalsAgainst = awayProfile.avgGoalsAgainst
  if (h2hProfile && h2hProfile.matches >= 3) {
    homeGoalsFor = round(homeGoalsFor*FORM_WEIGHT + h2hProfile.homeAvgGoalsFor*H2H_WEIGHT)
    homeGoalsAgainst = round(homeGoalsAgainst*FORM_WEIGHT + h2hProfile.homeAvgGoalsAgainst*H2H_WEIGHT)
    awayGoalsFor = round(awayGoalsFor*FORM_WEIGHT + h2hProfile.awayAvgGoalsFor*H2H_WEIGHT)
    awayGoalsAgainst = round(awayGoalsAgainst*FORM_WEIGHT + h2hProfile.awayAvgGoalsAgainst*H2H_WEIGHT)
    console.log(`   📊 H2H blend aplicado (${h2hProfile.matches} confrontos diretos)`)
  }
  const expectedHomeGoals = clamp(round(homeGoalsFor*0.60+awayGoalsAgainst*0.40),0.25,3.8)
  const expectedAwayGoals = clamp(round(awayGoalsFor*0.56+homeGoalsAgainst*0.44),0.20,3.4)
  const expectedGoals = round(expectedHomeGoals+expectedAwayGoals)
  const expectedHomeShots = clamp(round(homeProfile.avgShots*0.68+expectedHomeGoals*2.8),4,24)
  const expectedAwayShots = clamp(round(awayProfile.avgShots*0.64+expectedAwayGoals*2.5),4,22)
  const expectedHomeSOT = clamp(round(homeProfile.avgShotsOnTarget*0.70+expectedHomeGoals*0.95),1,9)
  const expectedAwaySOT = clamp(round(awayProfile.avgShotsOnTarget*0.67+expectedAwayGoals*0.90),1,8)
  const expectedShots = clamp(round(expectedHomeShots+expectedAwayShots),8,42)
  const expectedSOT = clamp(round(expectedHomeSOT+expectedAwaySOT),2,16)
  const pressureFactor = expectedShots>=24?0.45:expectedShots>=20?0.25:0.10
  const expectedCorners = clamp(round(homeProfile.avgCorners*0.50+awayProfile.avgCorners*0.46+expectedShots*0.050+expectedSOT*0.045+pressureFactor),4.5,13.2)
  const expectedCards = clamp(round(homeProfile.avgCards*0.50+awayProfile.avgCards*0.50+(homeProfile.avgFouls+awayProfile.avgFouls)*0.025),1.2,7.0)
  const expectedFouls = clamp(round(homeProfile.avgFouls*0.52+awayProfile.avgFouls*0.48),8,30)
  return { expectedGoals, expectedHomeGoals, expectedAwayGoals, expectedHomeShots, expectedAwayShots, expectedHomeSOT, expectedAwaySOT, expectedShots, expectedSOT, expectedCorners, expectedCards, expectedFouls }
}

function poisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0
  let f = 1; for (let i = 2; i <= k; i++) f *= i
  return (Math.exp(-lambda)*Math.pow(lambda,k))/f
}

function cumulativePoisson(lambda, maxK = 10) {
  const arr = []; let total = 0
  for (let k = 0; k <= maxK; k++) { const p = poisson(lambda,k); total += p; arr.push(p) }
  if (total < 0.999) arr.push(1-total)
  return arr
}

function buildProbabilities(metrics) {
  const homeDist = cumulativePoisson(metrics.expectedHomeGoals,8)
  const awayDist = cumulativePoisson(metrics.expectedAwayGoals,8)
  let homeWin=0,draw=0,awayWin=0,over15=0,over25=0,btts=0,under35=0
  for (let h=0; h<homeDist.length; h++) {
    for (let a=0; a<awayDist.length; a++) {
      const p=homeDist[h]*awayDist[a]; const total=h+a
      if(h>a)homeWin+=p; else if(h===a)draw+=p; else awayWin+=p
      if(total>=2)over15+=p; if(total>=3)over25+=p; if(total<=3)under35+=p; if(h>=1&&a>=1)btts+=p
    }
  }
  return {
    home:round(clamp(homeWin,0.05,0.88)), draw:round(clamp(draw,0.06,0.42)), away:round(clamp(awayWin,0.05,0.88)),
    over15:round(clamp(over15,0.10,0.97)), over25:round(clamp(over25,0.08,0.94)), btts:round(clamp(btts,0.08,0.92)),
    under35:round(clamp(under35,0.12,0.97)), corners:round(clamp((metrics.expectedCorners-6.8)/4.0,0.08,0.93)),
    shots:round(clamp((metrics.expectedShots-15)/17,0.08,0.93)), sot:round(clamp((metrics.expectedSOT-4.5)/7.5,0.08,0.93)),
    cards:round(clamp((metrics.expectedCards-2.2)/3.8,0.08,0.93)),
  }
}

function buildMarkets(metrics, probs) {
  return { over15:probs.over15, over25:probs.over25, btts:probs.btts, under35:probs.under35, corners:round(metrics.expectedCorners), cards:round(metrics.expectedCards), shots:round(metrics.expectedShots), shots_on_target:round(metrics.expectedSOT), fouls:round(metrics.expectedFouls) }
}

function buildMetrics(metrics) {
  return { goals:round(metrics.expectedGoals), corners:round(metrics.expectedCorners), shots:round(metrics.expectedShots), shots_on_target:round(metrics.expectedSOT), cards:round(metrics.expectedCards), fouls:round(metrics.expectedFouls) }
}

function lineScore(prob, family, market) {
  let score = safeNumber(prob,0)
  if (market==="Mais de 1.5 gols") score+=0.04
  if (market==="Mais de 2.5 gols") score+=0.02
  if (market==="Menos de 3.5 gols") score+=0.03
  if (market==="Menos de 2.5 gols") score+=0.01
  if (family==="dupla_chance") score+=0.015
  if (family==="resultado") score+=0.008
  if (market==="Empate") score-=0.10
  if (score>0.90) score-=0.04
  return round(score)
}

function detectMarketFamily(market="") {
  const m=normalizeText(market)
  if(m.includes("escanteio"))return"escanteios"
  if(m.includes("finalizacoes")||m.includes("finalizações"))return"shots"
  if(m.includes("no gol"))return"sot"
  if(m.includes("cart"))return"cards"
  if(m.includes("ambas"))return"ambas"
  if(m.includes("dupla chance"))return"dupla_chance"
  if(m.includes("vitoria")||m.includes("vitória")||m.includes("empate"))return"resultado"
  if(m.includes("gol"))return"gols"
  return"outro"
}

function detectDirection(market="") {
  const m=normalizeText(market)
  if(m.includes("mais de"))return"over"
  if(m.includes("menos de"))return"under"
  return null
}

function extractLine(market="") {
  const m=String(market||"").replace(",",".")
  const match=m.match(/(\d+(\.\d+)?)/)
  return match?Number(match[1]):null
}

function buildCornerCandidatesSync(metrics) {
  const candidates=[]; const c=safeNumber(metrics.expectedCorners,0); const shots=safeNumber(metrics.expectedShots,0)
  const add=(market,prob)=>{ if(!market)return; candidates.push({market,probability:round(prob),score:lineScore(prob,"escanteios",market),family:"escanteios"}) }
  if(c>=6.5)add("Mais de 6.5 escanteios",clamp((c-5.5)/2.0+(shots>=20?0.02:0),0.60,0.90))
  if(c>=7.4)add("Mais de 7.5 escanteios",clamp((c-6.2)/2.1+(shots>=22?0.02:0),0.58,0.87))
  if(c>=8.6)add("Mais de 8.5 escanteios",clamp((c-7.1)/2.3+(shots>=24?0.02:0),0.55,0.83))
  if(c<=9.0)add("Menos de 12.5 escanteios",clamp((13.2-c)/3.8,0.63,0.91))
  if(c<=8.2)add("Menos de 11.5 escanteios",clamp((12.0-c)/3.3,0.60,0.88))
  if(c<=7.2)add("Menos de 10.5 escanteios",clamp((10.8-c)/3.0,0.56,0.84))
  return candidates.sort((a,b)=>b.score-a.score)
}

function buildShotsCandidatesSync(metrics, probs) {
  const candidates=[]; const shots=safeNumber(metrics.expectedShots,0); const base=safeNumber(probs.shots,0)
  const add=(market,prob)=>{ if(!market)return; candidates.push({market,probability:round(prob),score:lineScore(prob,"shots",market),family:"shots"}) }
  if(shots>=18.5)add("Mais de 17.5 finalizações",clamp(Math.max(base,0.61)+(shots-18.5)*0.02,0.61,0.90))
  if(shots>=20.0)add("Mais de 19.5 finalizações",clamp(Math.max(base-0.01,0.58)+(shots-20.0)*0.02,0.58,0.87))
  if(shots>=22.0)add("Mais de 21.5 finalizações",clamp(Math.max(base-0.03,0.55)+(shots-22.0)*0.02,0.55,0.83))
  return candidates.sort((a,b)=>b.score-a.score)
}

function buildSOTCandidatesSync(metrics, probs) {
  const candidates=[]; const sot=safeNumber(metrics.expectedSOT,0); const base=safeNumber(probs.sot,0)
  const add=(market,prob)=>{ if(!market)return; candidates.push({market,probability:round(prob),score:lineScore(prob,"sot",market),family:"sot"}) }
  if(sot>=5.8)add("Mais de 5.5 finalizações no gol",clamp(Math.max(base,0.61)+(sot-5.8)*0.03,0.61,0.91))
  if(sot>=6.8)add("Mais de 6.5 finalizações no gol",clamp(Math.max(base-0.01,0.58)+(sot-6.8)*0.03,0.58,0.88))
  if(sot>=7.8)add("Mais de 7.5 finalizações no gol",clamp(Math.max(base-0.03,0.55)+(sot-7.8)*0.03,0.55,0.84))
  return candidates.sort((a,b)=>b.score-a.score)
}

function buildTeamCornerCandidates(payload) {
  const { homeTeam, awayTeam, metrics } = payload
  const candidates=[]
  const add=(market,prob)=>{ if(!market)return; candidates.push({market,probability:round(prob),score:lineScore(prob,"escanteios",market),family:"escanteios"}) }
  const totalCorners=safeNumber(metrics.expectedCorners??metrics.expected_corners,0)
  const homeShots=safeNumber(metrics.expectedHomeShots??metrics.expected_home_shots,0)
  const awayShots=safeNumber(metrics.expectedAwayShots??metrics.expected_away_shots,0)
  const totalShots=Math.max(homeShots+awayShots,1)
  const homeShare=clamp(homeShots/totalShots,0.35,0.65)
  const awayShare=clamp(awayShots/totalShots,0.35,0.65)
  const eHC=round(totalCorners*homeShare); const eAC=round(totalCorners*awayShare)
  const addTeamLines=(teamName,expectedCorners)=>{
    if(!teamName)return
    if(expectedCorners>=3.2)add(`${teamName} mais de 2.5 escanteios`,clamp(0.60+(expectedCorners-3.2)*0.07,0.60,0.88))
    if(expectedCorners>=4.0)add(`${teamName} mais de 3.5 escanteios`,clamp(0.57+(expectedCorners-4.0)*0.07,0.57,0.85))
    if(expectedCorners>=4.9)add(`${teamName} mais de 4.5 escanteios`,clamp(0.54+(expectedCorners-4.9)*0.07,0.54,0.82))
  }
  addTeamLines(homeTeam,eHC); addTeamLines(awayTeam,eAC)
  return candidates.sort((a,b)=>b.score-a.score)
}

function buildCardsCandidatesSync(metrics, probs) {
  const candidates=[]; const cards=safeNumber(metrics.expectedCards,0); const base=safeNumber(probs.cards,0)
  const add=(market,prob)=>{ if(!market)return; candidates.push({market,probability:round(prob),score:lineScore(prob,"cards",market),family:"cards"}) }
  if(cards>=2.8)add("Mais de 2.5 cartões",clamp(Math.max(base,0.60)+(cards-2.8)*0.03,0.60,0.90))
  if(cards>=3.6)add("Mais de 3.5 cartões",clamp(Math.max(base-0.01,0.57)+(cards-3.6)*0.03,0.57,0.87))
  if(cards>=4.6)add("Mais de 4.5 cartões",clamp(Math.max(base-0.03,0.54)+(cards-4.6)*0.03,0.54,0.83))
  return candidates.sort((a,b)=>b.score-a.score)
}

function buildCandidateMarkets(payload) {
  const { homeTeam, awayTeam, metrics, probabilities, isNationalTeamsGame } = payload
  const candidates=[]; const add=(market,probability,family)=>{ if(!market)return; candidates.push({market,probability:round(probability),score:lineScore(probability,family,market),family}) }
  let homeProb=safeNumber(probabilities.home,0), drawProb=safeNumber(probabilities.draw,0), awayProb=safeNumber(probabilities.away,0)
  if(isNationalTeamsGame){ homeProb=Math.min(homeProb,NATIONAL_TEAM_WIN_CAP); awayProb=Math.min(awayProb,NATIONAL_TEAM_WIN_CAP) }
  const homeOrDraw=clamp(homeProb+drawProb,0,1); const awayOrDraw=clamp(awayProb+drawProb,0,1)
  const under25=clamp(1-probabilities.over25,0,1); const bttsNo=clamp(1-probabilities.btts,0,1)
  if(probabilities.over25>=0.66)add("Mais de 2.5 gols",probabilities.over25,"gols")
  if(probabilities.over15>=0.76)add("Mais de 1.5 gols",probabilities.over15,"gols")
  if(under25>=0.74)add("Menos de 2.5 gols",under25,"gols")
  if(probabilities.under35>=0.80)add("Menos de 3.5 gols",probabilities.under35,"gols")
  if(probabilities.btts>=0.63)add("Ambas marcam",probabilities.btts,"ambas")
  if(bttsNo>=0.72)add("Ambas não marcam",bttsNo,"ambas")
  buildShotsCandidatesSync(metrics,probabilities).forEach(i=>candidates.push(i))
  buildSOTCandidatesSync(metrics,probabilities).forEach(i=>candidates.push(i))
  buildCardsCandidatesSync(metrics,probabilities).forEach(i=>candidates.push(i))
  buildCornerCandidatesSync(metrics).forEach(i=>candidates.push(i))
  buildTeamCornerCandidates({homeTeam,awayTeam,metrics}).forEach(i=>candidates.push(i))
  const mismatch=Math.abs(homeProb-awayProb)
  if(homeProb>=0.62)add("Vitória do mandante",homeProb,"resultado")
  if(awayProb>=0.62)add("Vitória do visitante",awayProb,"resultado")
  if(homeOrDraw>=0.74){ const allowed=!isNationalTeamsGame||mismatch<=STRONG_MISMATCH_DOUBLE_CHANCE_BLOCK||homeProb>=awayProb; if(allowed)add(`Dupla chance ${homeTeam} ou empate`,isNationalTeamsGame?Math.min(homeOrDraw,NATIONAL_TEAM_DOUBLE_CHANCE_CAP):homeOrDraw,"dupla_chance") }
  if(awayOrDraw>=0.74){ const allowed=!isNationalTeamsGame||mismatch<=STRONG_MISMATCH_DOUBLE_CHANCE_BLOCK||awayProb>=homeProb; if(allowed)add(`Dupla chance ${awayTeam} ou empate`,isNationalTeamsGame?Math.min(awayOrDraw,NATIONAL_TEAM_DOUBLE_CHANCE_CAP):awayOrDraw,"dupla_chance") }
  const profile=buildGameProfile(metrics,probabilities)
  candidates.forEach(item=>{
    if(profile==="volume"&&item.family==="shots")item.score=round(item.score+0.03)
    if(profile==="precisao"&&item.family==="sot")item.score=round(item.score+0.03)
    if(profile==="disciplinar"&&item.family==="cards")item.score=round(item.score+0.03)
    if((profile==="volume"||profile==="precisao")&&item.family==="escanteios")item.score=round(item.score-0.02)
  })
  return candidates.sort((a,b)=>b.score-a.score)
}

function chooseMainPick(candidates) {
  if(!candidates.length)return{market:"Menos de 3.5 gols",probability:0.6,score:0.6,family:"gols"}
  return[...candidates].sort((a,b)=>b.score-a.score)[0]
}

function chooseExtraPicks(candidates, mainPick) {
  const filtered=candidates.filter(i=>i.market!==mainPick.market)
  const picked=[]; const usedMarketKeys=new Set([normalizeText(mainPick.market)])
  const mainFamily=detectMarketFamily(mainPick.market); const mainDirection=detectDirection(mainPick.market); const mainLine=extractLine(mainPick.market)
  for(const item of filtered){
    const marketKey=normalizeText(item.market); if(usedMarketKeys.has(marketKey))continue
    const family=detectMarketFamily(item.market)
    if(family===mainFamily&&!["escanteios","shots","sot","cards"].includes(family))continue
    if(["escanteios","shots","sot","cards"].includes(family)&&family===mainFamily){
      const direction=detectDirection(item.market); const line=extractLine(item.market)
      if(!direction||line===null||!mainDirection||mainLine===null)continue
      if(direction!==mainDirection)continue
      if(direction==="under"&&line>=mainLine)continue
      if(direction==="over"&&line<=mainLine)continue
    }
    picked.push(item); usedMarketKeys.add(marketKey)
    if(picked.length===2)break
  }
  return picked
}

function normalizeLeagueByTeams(comp, fixture) {
  let leagueDisplay=comp.display; let country=comp.country||fixture?.league?.country||null
  const leagueNameRaw=fixture?.league?.name||""; const leagueId=fixture?.league?.id||comp?.leagueId||null
  const normLeague=normalizeText(leagueNameRaw); const normCountry=normalizeText(country)
  // ── Nomes canônicos das competições internacionais ──
  if(leagueId===1||normLeague.includes("world cup")||normLeague.includes("copa do mundo")){leagueDisplay="Copa do Mundo";country="World"}
  if(leagueId===4||normLeague.includes("euro ")&&!normLeague.includes("league")){leagueDisplay="Eurocopa";country="World"}
  if(leagueId===9||normLeague.includes("copa america")){leagueDisplay="Copa América";country="World"}
  if(leagueId===218){leagueDisplay="Austrian Bundesliga";country="Austria"}
  if(leagueId===203){leagueDisplay="Super Lig";country="Turkey"}
  if(normLeague.includes("nordeste")){leagueDisplay="Copa do Nordeste";country="Brazil"}
  if(normLeague.includes("verde")){leagueDisplay="Copa Verde";country="Brazil"}
  if(normLeague.includes("sul-sudeste")||normLeague.includes("sul sudeste")){leagueDisplay="Copa Sul-Sudeste";country="Brazil"}
  if((normLeague.includes("women")||normLeague.includes("feminino")||normLeague.includes("feminina"))&&normCountry==="brazil"){leagueDisplay="Brasileirão Feminino"}
  return{leagueDisplay,country}
}

function buildGameProfile(metrics, probabilities) {
  if(metrics.expectedGoals>=2.9||probabilities.over25>=0.66)return"ofensivo"
  if(metrics.expectedCorners>=9.3&&metrics.expectedShots>=22)return"estatistico"
  if(metrics.expectedShots>=22&&metrics.expectedSOT>=7&&metrics.expectedCorners<9.2)return"volume"
  if(metrics.expectedSOT>=7&&metrics.expectedGoals>=2.2&&metrics.expectedShots<=23)return"precisao"
  if(metrics.expectedCards>=4.2&&metrics.expectedGoals<=2.8)return"disciplinar"
  if(metrics.expectedGoals<=2.0&&probabilities.under35>=0.8)return"controlado"
  if(metrics.expectedGoals<=1.7&&metrics.expectedShots<=17)return"defensivo"
  return"equilibrado"
}

function buildInsightSync(mainPick, metrics, profile) {
  const goals=round(metrics.expectedGoals,1),corners=round(metrics.expectedCorners,1),shots=Math.round(metrics.expectedShots),sot=Math.round(metrics.expectedSOT),cards=round(metrics.expectedCards,1)
  if(mainPick==="Mais de 2.5 gols")return`A leitura Scoutly projeta um jogo mais aberto, com produção ofensiva suficiente para 3 ou mais gols. O cenário combina ${goals} gols esperados e ${shots} finalizações projetadas.`
  if(mainPick==="Mais de 1.5 gols")return`A leitura Scoutly projeta um confronto com boa chance de pelo menos 2 gols. A média esperada está em ${goals} gols, com cenário ofensivo sustentável.`
  if(mainPick==="Menos de 2.5 gols")return`O modelo identifica um cenário mais travado, com baixa explosão ofensiva e bom suporte estatístico para até 2 gols.`
  if(mainPick==="Menos de 3.5 gols")return`O modelo identifica um cenário mais controlado, com boa sustentação estatística para até 3 gols.`
  if(normalizeText(mainPick).includes("escanteios")){
    if(normalizeText(mainPick).includes("mais de"))return`A leitura Scoutly projeta cerca de ${corners} escanteios, com contexto de jogo favorável para uma linha de cantos mais alta.`
    return`A leitura Scoutly projeta cerca de ${corners} escanteios, indicando um cenário mais controlado para o mercado de cantos.`
  }
  if(normalizeText(mainPick).includes("finalizações")&&!normalizeText(mainPick).includes("no gol"))return`A leitura Scoutly projeta cerca de ${shots} finalizações totais, sustentando um cenário de volume ofensivo consistente.`
  if(normalizeText(mainPick).includes("no gol"))return`A leitura Scoutly projeta cerca de ${sot} finalizações no gol, indicando boa produção ofensiva com precisão suficiente.`
  if(normalizeText(mainPick).includes("cart"))return`A leitura Scoutly projeta cerca de ${cards} cartões, sugerindo confronto com tensão suficiente para transformar disciplina em oportunidade.`
  if(normalizeText(mainPick).includes("dupla chance"))return`A leitura Scoutly aponta vantagem competitiva para um dos lados, mas com proteção ao empate.`
  if(mainPick==="Ambas não marcam")return`A leitura Scoutly vê um confronto com menor troca ofensiva, sustentando o cenário de uma equipe passar em branco.`
  if(mainPick==="Ambas marcam")return`A leitura Scoutly identifica espaço para gols dos dois lados, combinando projeção ofensiva e comportamento recente.`
  return`A leitura Scoutly classifica este confronto como ${profile}, cruzando projeção de gols, escanteios, finalizações, no gol e cartões para destacar a melhor oportunidade.`
}

function isInternationalNationalTeamsFixture(fixture, comp) {
  if(!isInternationalCompetition(comp,fixture))return true
  if(normalizeText(comp?.display||"").includes("amistosos")||normalizeText(fixture?.league?.name||"").includes("friend")){
    if(isClubFriendlyFixture(fixture))return false
  }
  return true
}

function hasMinimumMatchData(homeContext, awayContext) {
  return isUsableTeamProfile(buildSideProfile(homeContext,"home"),homeContext.general) &&
         isUsableTeamProfile(buildSideProfile(awayContext,"away"),awayContext.general)
}

async function clearFutureWindow(freshFixtureIds) {
  const now = new Date().toISOString()
  const { start, end } = getSyncWindowRange()

  // Jogos já encerrados: sempre seguro limpar, não depende do fetch desta rodada
  const { data: oldRows, error: oldError } = await supabase
    .from("matches").select("id").lte("kickoff", now)
  if (oldError) throw new Error(`Supabase select old matches: ${oldError.message}`)
  const oldIds = (oldRows || []).map(x => x.id)
  if (oldIds.length) {
    await supabase.from("match_stats").delete().in("match_id", oldIds)
    await supabase.from("match_analysis").delete().in("match_id", oldIds)
    await supabase.from("daily_picks").delete().in("match_id", oldIds)
    const { error } = await supabase.from("matches").delete().in("id", oldIds)
    if (error) throw new Error(`Supabase delete old matches: ${error.message}`)
  }

  // [FIX DEFINITIVO] Copa do Mundo é gerenciada exclusivamente pelo copa-sync.js
  // (roda a cada hora). O sync principal NÃO deve deletar/reinserir Copa — quando
  // isso acontecia, a reinserção falhava e a Copa desaparecia do banco a cada 2h.
  const { data: futureRows, error: futureError } = await supabase
    .from("matches").select("id, league").gte("kickoff", start.toISOString()).lte("kickoff", end.toISOString())
  if (futureError) throw new Error(`Supabase select future matches: ${futureError.message}`)
  const currentFutureIds = new Set((futureRows || []).map(x => x.id))
  const futureIdsToClear = (futureRows || [])
    .filter(r => freshFixtureIds.has(r.id) && r.league !== "Copa do Mundo")
    .map(r => r.id)
  if (futureIdsToClear.length) {
    await supabase.from("match_stats").delete().in("match_id", futureIdsToClear)
    await supabase.from("match_analysis").delete().in("match_id", futureIdsToClear)
    const { error } = await supabase.from("matches").delete().in("id", futureIdsToClear)
    if (error) throw new Error(`Supabase delete future matches: ${error.message}`)
  }

  // daily_picks futuros: mesma lógica de preservação — só limpa picks de jogos que vão
  // ser regerados nesta rodada, ou picks órfãos cujo jogo já não existe mais em matches.
  // Picks de competições que falharam no fetch ficam intactos no radar.
  const { data: futurePickRows, error: pickSelectError } = await supabase
    .from("daily_picks").select("id, match_id").gte("kickoff", now)
  if (pickSelectError) throw new Error(`Supabase select future daily_picks: ${pickSelectError.message}`)
  const pickIdsToDelete = (futurePickRows || [])
    .filter(p => freshFixtureIds.has(p.match_id) || !currentFutureIds.has(p.match_id))
    .map(p => p.id)
  if (pickIdsToDelete.length) {
    const { error: dailyError } = await supabase.from("daily_picks").delete().in("id", pickIdsToDelete)
    if (dailyError) throw new Error(`Supabase delete daily_picks: ${dailyError.message}`)
  }

  return futureIdsToClear.length
}

async function upsertMatch(match) {
  const{error}=await supabase.from("matches").upsert({ id:match.id,kickoff:match.kickoff,league:match.league,country:match.country||null,region:match.region||null,priority:match.priority||null,home_team:match.home_team||null,away_team:match.away_team||null,home_logo:match.home_logo||null,away_logo:match.away_logo||null,referee:match.referee||null,probabilities:match.probabilities||null,markets:match.markets||null,metrics:match.metrics||null,pick:match.pick||null,probability:match.probability||null,insight:match.insight||null,updated_at:new Date().toISOString() },{onConflict:"id"})
  if(error)throw error
}

async function upsertMatchStats(row) {
  const{error}=await supabase.from("match_stats").upsert({ match_id:row.match_id,home_shots:row.home_shots,home_shots_on_target:row.home_shots_on_target,home_corners:row.home_corners,home_yellow_cards:row.home_yellow_cards,away_shots:row.away_shots,away_shots_on_target:row.away_shots_on_target,away_corners:row.away_corners,away_yellow_cards:row.away_yellow_cards,created_at:new Date().toISOString() },{onConflict:"match_id"})
  if(error)throw error
}

async function upsertMatchAnalysis(row) {
  const{error}=await supabase.from("match_analysis").upsert({ match_id:row.match_id,home_strength:row.home_strength,away_strength:row.away_strength,expected_home_goals:row.expected_home_goals,expected_away_goals:row.expected_away_goals,expected_home_shots:row.expected_home_shots,expected_away_shots:row.expected_away_shots,expected_home_sot:row.expected_home_sot,expected_away_sot:row.expected_away_sot,expected_corners:row.expected_corners,expected_cards:row.expected_cards,prob_over25:row.prob_over25,prob_btts:row.prob_btts,prob_corners:row.prob_corners,prob_shots:row.prob_shots,prob_sot:row.prob_sot,prob_cards:row.prob_cards,best_pick_1:row.best_pick_1,best_pick_2:row.best_pick_2,best_pick_3:row.best_pick_3,aggressive_pick:row.aggressive_pick,analysis_text:row.analysis_text,data:row.form_data||null,created_at:new Date().toISOString() },{onConflict:"match_id"})
  if(error)throw error
}

async function buildAndStoreMatches(fixtureLists) {
  const{start,end}=getSyncWindowRange()
  const allFixtures=uniqBy(fixtureLists.flat().filter(f=>{ const kickoff=f?.fixture?.date; if(!kickoff)return false; const dt=new Date(kickoff); return!isNaN(dt.getTime())&&dt>=start&&dt<=end }),(x)=>x?.fixture?.id)
  console.log(`📅 Fixtures na janela ativa: ${allFixtures.length}`)
  const freshFixtureIds=new Set(allFixtures.map(f=>f?.fixture?.id).filter(Boolean))
  const cleared=await clearFutureWindow(freshFixtureIds)
  console.log(`🧹 Limpeza prévia concluída: ${cleared}`)
  const stored=[]
  for(const fixture of allFixtures){
    try{
      const comp=fixture.__comp; if(!comp)continue
      if(!isInternationalNationalTeamsFixture(fixture,comp)){
        console.log(`⛔ Ignorado amistoso de clube: ${fixture?.teams?.home?.name} x ${fixture?.teams?.away?.name}`)
        continue
      }
      const{leagueDisplay,country}=normalizeLeagueByTeams(comp,fixture)
      const baseMatchPayload={
        id:fixture?.fixture?.id,kickoff:fixture?.fixture?.date||null,league:leagueDisplay,country,region:comp.region,priority:comp.priority||70,
        home_team:fixture?.teams?.home?.name||null,away_team:fixture?.teams?.away?.name||null,
        home_logo:fixture?.teams?.home?.logo||null,away_logo:fixture?.teams?.away?.logo||null,
        referee:fixture?.fixture?.referee||null,
        probabilities:null,markets:null,metrics:null,pick:null,probability:null,insight:null,
      }
      await upsertMatch(baseMatchPayload)
      const homeTeamId=fixture?.teams?.home?.id; const awayTeamId=fixture?.teams?.away?.id
      const leagueIdForStats=fixture?.league?.id||comp?.leagueId||null
      if(!homeTeamId||!awayTeamId){ stored.push(baseMatchPayload); console.log(`🟡 Sem análise (time_id ausente): ${leagueDisplay} | ${baseMatchPayload.home_team} x ${baseMatchPayload.away_team}`); continue }
      const homeContext=await buildTeamContext(homeTeamId,leagueIdForStats)
      const awayContext=await buildTeamContext(awayTeamId,leagueIdForStats)
      if(!hasMinimumMatchData(homeContext,awayContext)){ stored.push(baseMatchPayload); console.log(`🟡 Sem análise (dados mínimos insuficientes): ${leagueDisplay} | ${baseMatchPayload.home_team} x ${baseMatchPayload.away_team}`); continue }
      const homeProfile=buildSideProfile(homeContext,"home")
      const awayProfile=buildSideProfile(awayContext,"away")
      const h2hProfile=await buildH2HProfile(homeTeamId,awayTeamId)
      const metricsExp=buildExpectedMetrics(homeProfile,awayProfile,h2hProfile)
      const probabilities=buildProbabilities(metricsExp)
      const markets=buildMarkets(metricsExp,probabilities)
      const metrics=buildMetrics(metricsExp)
      const hasUsableMetrics=metrics.goals>0&&metrics.corners>0&&metrics.shots>0&&metrics.shots_on_target>0
      if(!hasUsableMetrics){ stored.push(baseMatchPayload); console.log(`🟡 Sem análise (métricas insuficientes): ${leagueDisplay} | ${baseMatchPayload.home_team} x ${baseMatchPayload.away_team}`); continue }
      const isNationalTeamsGame=isLikelyNationalTeamMatch(fixture,comp)
      const candidates=buildCandidateMarkets({ homeTeam:fixture?.teams?.home?.name||"Mandante",awayTeam:fixture?.teams?.away?.name||"Visitante",metrics:metricsExp,probabilities,isNationalTeamsGame })
      if(!candidates.length){ stored.push(baseMatchPayload); console.log(`🟡 Sem análise (sem mercados coerentes): ${leagueDisplay} | ${baseMatchPayload.home_team} x ${baseMatchPayload.away_team}`); continue }
      const mainPick=chooseMainPick(candidates)
      const extraPicks=chooseExtraPicks(candidates,mainPick)
      const pick2=extraPicks[0]?.market||null; const pick3=extraPicks[1]?.market||null
      const gameProfile=buildGameProfile(metricsExp,probabilities)
      const insight=buildInsightSync(mainPick.market,metricsExp,gameProfile)
      const analyzedMatchPayload={...baseMatchPayload,probabilities:{home:probabilities.home,draw:probabilities.draw,away:probabilities.away},markets,metrics,pick:mainPick.market,probability:mainPick.probability,insight}
      await upsertMatch(analyzedMatchPayload)
      await upsertMatchStats({ match_id:analyzedMatchPayload.id,home_shots:Math.round(metricsExp.expectedHomeShots),home_shots_on_target:Math.round(metricsExp.expectedHomeSOT),home_corners:Math.max(1,Math.round(homeProfile.avgCorners)),home_yellow_cards:Math.max(0,Math.round(homeProfile.avgCards)),away_shots:Math.round(metricsExp.expectedAwayShots),away_shots_on_target:Math.round(metricsExp.expectedAwaySOT),away_corners:Math.max(1,Math.round(awayProfile.avgCorners)),away_yellow_cards:Math.max(0,Math.round(awayProfile.avgCards)) })
      const formData={
        home_form_general:{matches:homeContext.general.matches,avgGoalsFor:homeContext.general.avgGoalsFor,avgGoalsAgainst:homeContext.general.avgGoalsAgainst,avgShots:homeContext.general.avgShots,avgCorners:homeContext.general.avgCorners,avgCards:homeContext.general.avgCards,recentScores:homeContext.general.recentScores,recentMatches:homeContext.general.recentMatches||[],formStreak:homeContext.general.formStreak},
        home_form_home:{matches:homeContext.home.matches,avgGoalsFor:homeContext.home.avgGoalsFor,avgGoalsAgainst:homeContext.home.avgGoalsAgainst,avgShots:homeContext.home.avgShots,avgCorners:homeContext.home.avgCorners,avgCards:homeContext.home.avgCards,recentScores:homeContext.home.recentScores,recentMatches:homeContext.home.recentMatches||[],formStreak:homeContext.home.formStreak},
        home_form_away:{matches:homeContext.away.matches,avgGoalsFor:homeContext.away.avgGoalsFor,avgGoalsAgainst:homeContext.away.avgGoalsAgainst,avgShots:homeContext.away.avgShots,avgCorners:homeContext.away.avgCorners,avgCards:homeContext.away.avgCards,recentScores:homeContext.away.recentScores,recentMatches:homeContext.away.recentMatches||[],formStreak:homeContext.away.formStreak},
        away_form_general:{matches:awayContext.general.matches,avgGoalsFor:awayContext.general.avgGoalsFor,avgGoalsAgainst:awayContext.general.avgGoalsAgainst,avgShots:awayContext.general.avgShots,avgCorners:awayContext.general.avgCorners,avgCards:awayContext.general.avgCards,recentScores:awayContext.general.recentScores,recentMatches:awayContext.general.recentMatches||[],formStreak:awayContext.general.formStreak},
        away_form_home:{matches:awayContext.home.matches,avgGoalsFor:awayContext.home.avgGoalsFor,avgGoalsAgainst:awayContext.home.avgGoalsAgainst,avgShots:awayContext.home.avgShots,avgCorners:awayContext.home.avgCorners,avgCards:awayContext.home.avgCards,recentScores:awayContext.home.recentScores,recentMatches:awayContext.home.recentMatches||[],formStreak:awayContext.home.formStreak},
        away_form_away:{matches:awayContext.away.matches,avgGoalsFor:awayContext.away.avgGoalsFor,avgGoalsAgainst:awayContext.away.avgGoalsAgainst,avgShots:awayContext.away.avgShots,avgCorners:awayContext.away.avgCorners,avgCards:awayContext.away.avgCards,recentScores:awayContext.away.recentScores,recentMatches:awayContext.away.recentMatches||[],formStreak:awayContext.away.formStreak},
        h2h:h2hProfile?{matches:h2hProfile.matches,homeAvgGoalsFor:h2hProfile.homeAvgGoalsFor,homeAvgGoalsAgainst:h2hProfile.homeAvgGoalsAgainst,awayAvgGoalsFor:h2hProfile.awayAvgGoalsFor,awayAvgGoalsAgainst:h2hProfile.awayAvgGoalsAgainst,recentScores:h2hProfile.recentScores}:null,
      }
      await upsertMatchAnalysis({ match_id:analyzedMatchPayload.id,home_strength:round(homeProfile.avgGoalsFor*1.4+homeProfile.avgShotsOnTarget*0.55+homeProfile.avgCorners*0.25),away_strength:round(awayProfile.avgGoalsFor*1.35+awayProfile.avgShotsOnTarget*0.52+awayProfile.avgCorners*0.23),expected_home_goals:round(metricsExp.expectedHomeGoals),expected_away_goals:round(metricsExp.expectedAwayGoals),expected_home_shots:round(metricsExp.expectedHomeShots),expected_away_shots:round(metricsExp.expectedAwayShots),expected_home_sot:round(metricsExp.expectedHomeSOT),expected_away_sot:round(metricsExp.expectedAwaySOT),expected_corners:round(metricsExp.expectedCorners),expected_cards:round(metricsExp.expectedCards),prob_over25:round(probabilities.over25),prob_btts:round(probabilities.btts),prob_corners:round(probabilities.corners),prob_shots:round(probabilities.shots),prob_sot:round(probabilities.sot),prob_cards:round(probabilities.cards),best_pick_1:mainPick.market,best_pick_2:pick2,best_pick_3:pick3,aggressive_pick:candidates.find(x=>x.market==="Mais de 2.5 gols"||x.market==="Ambas marcam"||x.market.includes("Mais de 8.5 escanteios")||x.market.includes("Mais de 19.5 finalizações")||x.market.includes("Mais de 6.5 finalizações no gol"))?.market||null,analysis_text:insight,form_data:formData })
      stored.push({...analyzedMatchPayload,game_profile:gameProfile})
      console.log(`✅ ${leagueDisplay} | ${analyzedMatchPayload.home_team} x ${analyzedMatchPayload.away_team} | ${mainPick.market}${h2hProfile?` [H2H: ${h2hProfile.matches}]`:""}`)
    }catch(err){
      console.error(`❌ Falha processando fixture ${fixture?.fixture?.id}:`,err.message)
    }
  }
  return stored
}

async function rebuildDailyPicks(matches) {
  if(!matches.length)return 0
  const sorted=[...matches]
    .filter(m=>m.id&&m.pick&&m.metrics&&safeNumber(m.metrics.goals,0)>0)
    .filter(m=>!RADAR_BLACKLIST.has(String(m.league||"")))
    .map(m=>({ ...m, _tierScore: getLeagueTierScore(m.league) }))
    .sort((a,b)=>{
      const scoreA = a._tierScore + safeNumber(a.probability,0)*100
      const scoreB = b._tierScore + safeNumber(b.probability,0)*100
      if(scoreB!==scoreA)return scoreB-scoreA
      return safeNumber(b.priority,0)-safeNumber(a.priority,0)
    })
  const selected=[]; const marketCount={},leagueCount={},familyCount={},regionCount={}
  const detectFamily=market=>{ const m=normalizeText(market); if(m.includes("escanteio"))return"escanteios"; if(m.includes("finalizacoes")||m.includes("finalizações"))return"shots"; if(m.includes("no gol"))return"sot"; if(m.includes("cart"))return"cards"; if(m.includes("ambas"))return"ambas"; if(m.includes("dupla chance"))return"dupla_chance"; if(m.includes("vitoria")||m.includes("vitória")||m.includes("empate"))return"resultado"; if(m.includes("gol"))return"gols"; return"outro" }
  for(const match of sorted){
    const market=String(match.pick||""); const league=String(match.league||""); const family=detectFamily(market); const region=String(match.region||"general"); const gameProfile=String(match.game_profile||"")
    marketCount[market]=marketCount[market]||0; leagueCount[league]=leagueCount[league]||0; familyCount[family]=familyCount[family]||0; regionCount[region]=regionCount[region]||0
    if(safeNumber(match.probability,0)<MIN_PROBABILITY_GENERAL)continue
    if(gameProfile==="defensivo"&&safeNumber(match.probability,0)<MIN_PROBABILITY_DEFENSIVO)continue
    if(gameProfile==="equilibrado"&&safeNumber(match.probability,0)<MIN_PROBABILITY_EQUILIBRADO)continue
    if(region==="international"&&safeNumber(match.probability,0)<MIN_PROBABILITY_INTERNATIONAL)continue
    if(LEAGUE_TIER[league]===4&&selected.length>=MAX_DAILY_PICKS/2)continue
    if(marketCount[market]>=MAX_SAME_MARKET_IN_DAILY)continue
    if(leagueCount[league]>=MAX_SAME_LEAGUE_IN_DAILY)continue
    if(family==="gols"&&familyCount[family]>=4)continue
    if(family==="escanteios"&&familyCount[family]>=3)continue
    if(family==="shots"&&familyCount[family]>=2)continue
    if(family==="sot"&&familyCount[family]>=2)continue
    if(family==="cards"&&familyCount[family]>=2)continue
    if(family==="ambas"&&familyCount[family]>=2)continue
    if(family==="dupla_chance"&&familyCount[family]>=3)continue
    if(family==="resultado"&&familyCount[family]>=2)continue
    if(region==="international"&&regionCount[region]>=MAX_INTERNATIONAL_IN_DAILY)continue
    if(region==="brazil"&&regionCount[region]>=MAX_BRAZIL_IN_DAILY)continue
    selected.push(match); marketCount[market]+=1; leagueCount[league]+=1; familyCount[family]+=1; regionCount[region]+=1
    if(selected.length>=MAX_DAILY_PICKS)break
  }
  const rows=selected.map((m,index)=>({ rank:index+1,match_id:m.id,league:m.league,home_team:m.home_team,away_team:m.away_team,market:m.pick,probability:round(m.probability),kickoff:m.kickoff,is_opportunity:false,home_logo:m.home_logo||null,away_logo:m.away_logo||null,created_at:new Date().toISOString() }))
  if(!rows.length)return 0
  const{error}=await supabase.from("daily_picks").insert(rows)
  if(error)throw new Error(`Supabase daily_picks: ${error.message}`)
  return rows.length
}

async function run() {
  console.log("🚀 Scoutly Sync V13.7 iniciado")
  console.log("✅ [FIX] RADAR_BLACKLIST: copas nacionais removidas — aparecem em competições e radar")
  console.log("✅ [FIX] Copa da Turquia, Copa da Bélgica, Copa da Áustria, Copa da Grécia, Copa da Dinamarca, Scottish Cup agora são buscadas e analisadas")
  const{start,end}=getSyncWindowRange()
  console.log(`📆 Janela ativa: ${start.toISOString()} -> ${end.toISOString()}`)
  const competitions=await resolveTargetCompetitions()
  console.log(`🏆 Competições resolvidas: ${competitions.length}`)
  const fixtureLists=[]
  for(const comp of competitions){
    const list=await fetchFixturesForCompetition(comp)
    fixtureLists.push(list)
    if(list.length>0){
      console.log(`📌 ${comp.display}: ${list.length} fixture(s)`)
    } else if(comp.priority>=95){
      // Competição de alta prioridade voltou zerada — provável falha na API (rate limit,
      // erro transitório, etc). Os dados antigos dela são preservados (ver clearFutureWindow),
      // mas isso merece atenção: ficará desatualizada até um sync que funcione.
      console.log(`⚠️  ALERTA: ${comp.display} (prioridade ${comp.priority}) voltou com 0 fixtures nesta rodada — dados antigos preservados, mas não atualizados`)
    }
  }
  const storedMatches=await buildAndStoreMatches(fixtureLists)
  const picksCount=await rebuildDailyPicks(storedMatches)
  console.log(`\n📊 Resumo final:`)
  console.log(`   Matches gravados: ${storedMatches.length}`)
  console.log(`   Daily picks gerados: ${picksCount}`)
  console.log("✅ Scoutly Sync V13.7 concluído")
}

run().catch(err=>{ console.error("❌ Erro fatal no Scoutly Sync V13.7:",err); process.exit(1) })
