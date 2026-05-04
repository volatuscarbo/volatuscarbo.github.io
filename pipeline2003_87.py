import os
import hashlib
import difflib
from datetime import date
from supabase import create_client
import time

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

CELEX_LIST = [
    "32003L0087",
    "32018R2066",
    "32018R2067"
]


# -------------------------
# DEBUG HELPERS
# -------------------------

def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def debug(title, data):
    print("\n" + "="*60)
    print(f"🔍 {title}")
    print("="*60)
    print(data)
    print("="*60 + "\n")


def safe_execute(query, label="query"):
    res = query.execute()

    if hasattr(res, "error") and res.error:
        print(f"❌ ERROR in {label}: {res.error}")
    else:
        print(f"✅ {label} OK | rows: {len(res.data) if res.data else 0}")

    return res


# -------------------------
# DB FUNCTIONS
# -------------------------

def get_or_create_act(celex: str):
    res = supabase.table("acts").select("*").eq("celex", celex).execute()

    if res.data:
        print(f"📌 Act exists: {celex}")
        return res.data[0]

    print(f"➕ Creating act: {celex}")

    inserted = supabase.table("acts").insert({
        "celex": celex,
        "title": f"Act {celex}",
        "type": "unknown"
    }).execute()

    debug("ACT INSERT RESULT", inserted.data)

    return inserted.data[0]


def get_latest_version(act_id: str):
    res = supabase.table("act_versions") \
        .select("*") \
        .eq("act_id", act_id) \
        .eq("is_latest", True) \
        .limit(1) \
        .execute()

    if res.data:
        print(f"📄 Found latest version: v{res.data[0]['version_number']}")
    else:
        print("📭 No previous version found")

    return res.data[0] if res.data else None


def compute_diff(old_text, new_text):
    diff = difflib.unified_diff(
        old_text.splitlines(),
        new_text.splitlines(),
        lineterm=""
    )
    return "\n".join(diff)


# -------------------------
# MOCK DATA FETCH (DEBUG IMPORTANT)
# -------------------------

def fetch_legislation_text(celex: str):
    text = f"Full consolidated text for {celex} - {time.time()}"
    
    debug("FETCHED TEXT", text[:200])
    
    return text


# -------------------------
# INSERT VERSION
# -------------------------

def insert_version(act, new_text: str, effective_date=None):
    act_id = act["id"]

    print(f"\n🚀 Processing act_id={act_id}, celex={act['celex']}")

    latest = get_latest_version(act_id)

    new_hash = hash_text(new_text)

    print(f"🔐 New hash: {new_hash}")

    if latest:
        print(f"🔐 Old hash: {latest['content_hash']}")

        if latest["content_hash"] == new_hash:
            print("⏭ NO CHANGE → skipping insert")
            return
    else:
        print("🆕 First version for this act")

    version_number = 1 if not latest else latest["version_number"] + 1

    print(f"📦 Creating version {version_number}")

    new_version = supabase.table("act_versions").insert({
        "act_id": act_id,
        "version_number": version_number,
        "celex": act["celex"],
        "full_text": new_text,
        "effective_date": effective_date or date.today().isoformat(),
        "is_latest": True,
        "content_hash": new_hash,
        "previous_version_id": latest["id"] if latest else None
    }).execute()

    if new_version.data:
        print(f"✅ INSERT SUCCESS: version {version_number}")
    else:
        print("❌ INSERT FAILED (no data returned)")
        debug("INSERT RESPONSE", new_version)

    # update previous
    if latest:
        supabase.table("act_versions") \
            .update({"is_latest": False}) \
            .eq("id", latest["id"]) \
            .execute()

        diff = compute_diff(latest["full_text"], new_text)

        supabase.table("act_version_diffs").insert({
            "act_id": act_id,
            "from_version_id": latest["id"],
            "to_version_id": new_version.data[0]["id"] if new_version.data else None,
            "diff_text": diff
        }).execute()

        print("🧾 Diff stored")


# -------------------------
# MAIN
# -------------------------

def run():
    print("\n==============================")
    print("🚀 STARTING LEGAL PIPELINE DEBUG")
    print("==============================\n")

    for celex in CELEX_LIST:
        print(f"\n📘 CELEX: {celex}")

        act = get_or_create_act(celex)

        debug("ACT OBJECT", act)

        text = fetch_legislation_text(celex)

        insert_version(act, text)

    print("\n🎯 DONE")


if __name__ == "__main__":
    run()
