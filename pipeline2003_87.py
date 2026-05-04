from supabase import create_client

SUPABASE_URL = "https://wgsknwmczjjqucfueanx.supabase.co"
SUPABASE_KEY = "sb_publishable_9gyHR3xwGCSjUpJs4T060w_sEGFs8FQ"

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def insert_version(celex, version):

    vid = version.get("version_id")
    if not vid:
        print("⚠️ Missing version_id, skipping")
        return

    try:
        sb.table("versions").upsert({
            "celex": celex,
            "version_id": vid,
            "version_date": version.get("version_date"),
            "content": version.get("content"),
            "source_url": version.get("source_url")
        }, on_conflict="version_id").execute()

        print(f"✅ Upserted: {vid}")

    except Exception as e:
        print(f"❌ Failed {vid}: {e}")


def process_celex(celex):
    print(f"\n📘 Processing {celex}")

    versions = fetch_versions_from_source(celex)

    if not versions:
        print("⚠️ No versions found")
        return

    print(f"📄 Found {len(versions)} versions")

    for v in versions:
        insert_version(celex, v)


def run_pipeline(celex_list):
    print("🚀 EU Compliance Intelligence System")

    for celex in celex_list:
        process_celex(celex)
