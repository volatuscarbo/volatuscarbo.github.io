from supabase import create_client

SUPABASE_URL = "https://wgsknwmczjjqucfueanx.supabase.co"
SUPABASE_KEY = "YOUR_KEY"

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_versions_from_source(celex):
    """
    MUST return ALL versions.
    Ensure pagination is implemented here.
    """
    raise NotImplementedError("Implement source fetch with pagination")


def normalize_version(v):
    return {
        "version_id": v.get("version_id"),
        "version_date": v.get("version_date"),
        "content": v.get("content") or "",
        "source_url": v.get("source_url")
    }


def insert_version(celex, version):
    v = normalize_version(version)

    if not v["version_id"]:
        print("⚠️ Skipping version with missing version_id")
        return

    try:
        # ✅ UPSERT = no duplicates + no race conditions
        res = sb.table("versions").upsert(
            {
                "celex": celex,
                "version_id": v["version_id"],
                "version_date": v["version_date"],
                "content": v["content"],
                "source_url": v["source_url"]
            },
            on_conflict="version_id"
        ).execute()

        if res.data:
            print(f"✅ Upserted: {v['version_id']}")
        else:
            print(f"⚠️ No response data for {v['version_id']}")

    except Exception as e:
        print(f"❌ Insert failed for {v['version_id']}: {e}")


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
