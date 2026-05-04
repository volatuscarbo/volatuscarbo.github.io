import os
import hashlib
import difflib
from datetime import date
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

CELEX_LIST = [
    "32003L0087",
    "32018R2066",
    "32018R2067"
]


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_or_create_act(celex: str):
    res = supabase.table("acts").select("*").eq("celex", celex).execute()

    if res.data:
        return res.data[0]

    inserted = supabase.table("acts").insert({
        "celex": celex,
        "title": f"Act {celex}",
        "type": "unknown"
    }).execute()

    return inserted.data[0]


def get_latest_version(act_id: str):
    res = supabase.table("act_versions") \
        .select("*") \
        .eq("act_id", act_id) \
        .eq("is_latest", True) \
        .limit(1) \
        .execute()

    return res.data[0] if res.data else None


def compute_diff(old_text, new_text):
    diff = difflib.unified_diff(
        old_text.splitlines(),
        new_text.splitlines(),
        lineterm=""
    )
    return "\n".join(diff)


def insert_version(act, new_text: str, effective_date=None):
    act_id = act["id"]
    latest = get_latest_version(act_id)

    new_hash = hash_text(new_text)

    # avoid duplicates
    if latest and latest["content_hash"] == new_hash:
        print(f"⏭ No change for {act['celex']}")
        return

    version_number = 1 if not latest else latest["version_number"] + 1

    # insert new version
    new_version = supabase.table("act_versions").insert({
        "act_id": act_id,
        "version_number": version_number,
        "celex": act["celex"],
        "full_text": new_text,
        "effective_date": effective_date or date.today().isoformat(),
        "is_latest": True,
        "content_hash": new_hash,
        "previous_version_id": latest["id"] if latest else None
    }).execute().data[0]

    # update previous latest
    if latest:
        supabase.table("act_versions") \
            .update({"is_latest": False}) \
            .eq("id", latest["id"]) \
            .execute()

        diff = compute_diff(latest["full_text"], new_text)

        supabase.table("act_version_diffs").insert({
            "act_id": act_id,
            "from_version_id": latest["id"],
            "to_version_id": new_version["id"],
            "diff_text": diff
        }).execute()

    print(f"✅ Inserted version {version_number} for {act['celex']}")


def fetch_legislation_text(celex: str):
    """
    Placeholder:
    Replace with your scraper / EUR-Lex API / pipeline output
    """
    return f"Full consolidated text for {celex} at version time"


def run():
    for celex in CELEX_LIST:
        act = get_or_create_act(celex)
        text = fetch_legislation_text(celex)
        insert_version(act, text)


if __name__ == "__main__":
    run()
