from supabase import create_client
import hashlib

SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_KEY"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# -------------------------
# HASH FUNCTION (ONLY FOR CHANGE DETECTION)
# -------------------------
def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# -------------------------
# FETCH LATEST VERSION ONLY
# -------------------------
def get_latest_version(act_id):
    res = supabase.table("act_versions") \
        .select("*") \
        .eq("act_id", act_id) \
        .order("version_number", desc=True) \
        .limit(1) \
        .execute()

    return res.data[0] if res.data else None


# -------------------------
# CREATE NEW VERSION
# -------------------------
def create_version(act_id, celex, new_text, new_hash, latest_version):
    new_version_number = 1 if not latest_version else latest_version["version_number"] + 1

    supabase.table("act_versions").insert({
        "act_id": act_id,
        "celex": celex,
        "content": new_text,
        "content_hash": new_hash,
        "version_number": new_version_number,
        "is_latest": True
    }).execute()

    # mark old latest as not latest
    if latest_version:
        supabase.table("act_versions") \
            .update({"is_latest": False}) \
            .eq("id", latest_version["id"]) \
            .execute()

    print(f"✅ Created version {new_version_number}")


# -------------------------
# MAIN PIPELINE
# -------------------------
def process_celex(act_id, celex, new_text):
    print("\n" + "=" * 60)
    print(f"📘 Processing CELEX: {celex}")
    print("=" * 60)

    new_hash = hash_text(new_text)

    print("\n🔐 New hash:", new_hash)

    latest = get_latest_version(act_id)

    if latest:
        print("📄 Latest version:", latest["version_number"])
        print("🔐 Old hash:", latest["content_hash"])

        if latest["content_hash"] == new_hash:
            print("⏭ No change detected → skipping")
            return
    else:
        print("🆕 No previous version found")

    create_version(act_id, celex, new_text, new_hash, latest)


# -------------------------
# EXAMPLE USAGE
# -------------------------
if __name__ == "__main__":

    # Example input (replace with real EUR-Lex fetch later)
    test_data = [
        {
            "act_id": "ec657d4c-eb3c-4855-9a40-3d9688934c47",
            "celex": "32003L0087",
            "text": "Article 1 defines scope of application."
        },
        {
            "act_id": "331f64b3-7c4f-42c2-ad82-e1e4d7bec8f2",
            "celex": "32018R2066",
            "text": "Monitoring and reporting framework established."
        },
        {
            "act_id": "919d4fb5-a896-41cf-a641-7d587d895850",
            "celex": "32018R2067",
            "text": "Verification procedures updated."
        }
    ]

    for item in test_data:
        process_celex(
            item["act_id"],
            item["celex"],
            item["text"]
        )
