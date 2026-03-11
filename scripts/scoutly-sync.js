const { createClient } = require("@supabase/supabase-js");

const APISPORTS_KEY = process.env.APISPORTS_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!APISPORTS_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Faltam variáveis de ambiente.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function todayISO() {
  const d = new Date();
  return d.toISOString().slice(0, 10);
