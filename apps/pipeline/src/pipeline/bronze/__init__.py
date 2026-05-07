from rag_shared import get_settings, get_supabase


def run() -> None:
    settings = get_settings()
    client = get_supabase()
    print(f"[bronze] listing bucket: {settings.supabase_bucket}")
    entries = client.storage.from_(settings.supabase_bucket).list()
    print(f"[bronze] found {len(entries)} entries (top level)")
