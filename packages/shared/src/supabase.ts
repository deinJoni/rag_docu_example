import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { loadEnv } from "./env.js";

let cached: SupabaseClient | undefined;

export function createSupabaseClient(): SupabaseClient {
  if (cached) return cached;
  const env = loadEnv();
  cached = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
  return cached;
}
