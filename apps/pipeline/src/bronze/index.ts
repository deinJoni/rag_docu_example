import { createSupabaseClient, loadEnv } from "@rag/shared";

export async function run() {
  const env = loadEnv();
  const supabase = createSupabaseClient();
  console.log(`[bronze] listing bucket: ${env.SUPABASE_BUCKET}`);
  const { data, error } = await supabase.storage.from(env.SUPABASE_BUCKET).list();
  if (error) throw error;
  console.log(`[bronze] found ${data?.length ?? 0} entries (top level)`);
}
