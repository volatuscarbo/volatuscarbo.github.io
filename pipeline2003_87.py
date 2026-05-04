from supabase import create_client

SUPABASE_URL = "https://wgsknwmczjjqucfueanx.supabase.co"
SUPABASE_KEY = "YOUR_KEY"

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def version_exists(version_id: str) -> bool:
    res = sb.table("versions") \
        .select("version_id") \
        .eq("version_id", version_id) \
        .limit(1) \
        .execute()

    return len(res.data) > 0


def insert_version(celex, version):
    """
    version must contain:
      - version_id (UUID or unique hash)
      - version_date
      - raw_text or metadata
    """

    vid = version["version_id"]

    # ✅ IMPORTANT: dedupe ONLY by version_id
    if version_exists(vid):
        print(f"⏭️ Version exists: {vid}")
        return

    sb.table("versions").insert({
        "celex": celex,
        "version_id": vid,
        "version_date": version.get("version_date"),
        "content": version.get("content"),
        "source_url": version.get("source_url")
    }).execute()

    print(f"✅ Inserted version: {vid}")


def process_celex(celex):
    print(f"\n📘 Processing {celex}")

    versions = fetch_versions_from_source(celex)

    if not versions:
        print("⚠️ No versions found from source")
        return

    print(f"📄 Found {len(versions)} versions")

    for v in versions:
        insert_version(celex, v)


def run_pipeline(celex_list):
    print("🚀 EU Compliance Intelligence System")

    for celex in celex_list:
        process_celex(celex)
