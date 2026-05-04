from supabase import create_client
import hashlib
import re

# -------------------------
# CONFIG
# -------------------------
SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_KEY"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_VERSIONS = "act-versions"
TABLE_DIFFS = "act_versions_diffs"


# -------------------------
# HASH (change detection only)
# -------------------------
def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# -------------------------
# NORMALIZE (removes noise)
# -------------------------
def normalize(text):
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


# -------------------------
# ALWAYS FETCH LATEST FROM DB (NO CACHE)
# -------------------------
def get_latest_version(act_id):
    res = supabase.table(TABLE_VERSIONS) \
        .select("*") \
        .eq("act_id", act_id) \
        .order("version_number", desc=True) \
        .limit(1) \
        .execute()

    return res.data[0] if res.data else None


# -------------------------
# SAFE VERSION NUMBERING
# -------------------------
def get_next_version(latest):
    if not latest:
        return 1
    return int(latest["version_number"]) + 1


# -------------------------
# DIFF GENERATOR
# -------------------------
def generate_diff(old, new):
    old_lines = old.splitlines()
    new_lines = new.splitlines()

    diff = []
    for line in new_lines:
        if line not in old_lines:
            diff.append(f"+ {line}")

    for line in old_lines:
        if line not in new_lines:
            diff.append(f"- {line}")

    return "\n".join(diff)


# -------------------------
# INSERT VERSION (SAFE)
# -------------------------
def insert_version(act_id, celex, text, hash_value, version_number):
    print(f"📦 INSERT → version {version_number}")

    res = supabase.table(TABLE_VERSIONS).insert({
        "act_id": act_id,
        "celex": celex,
        "content": text,
        "content_hash": hash_value,
        "version_number": version_number,
        "is_latest": True
    }).execute()

    if not res.data:
        print("❌ INSERT FAILED")
        print(res)
        return None

    print(f"✅ INSERT OK → v{version_number}")
    return res.data[0]


# -------------------------
# UPDATE OLD LATEST
# -------------------------
def update_old_latest(latest):
    if not latest:
        return

    supabase.table(TABLE_VERSIONS) \
        .update({"is_latest": False}) \
        .eq("id", latest["id"]) \
        .execute()


# -------------------------
# INSERT DIFF
# -------------------------
def insert_diff(act_id, celex, from_v, to_v, diff_text):
    supabase.table(TABLE_DIFFS).insert({
        "act_id": act_id,
        "celex": celex,
        "from_version": from_v,
        "to_version": to_v,
        "diff": diff_text
    }).execute()

    print(f"🧾 Diff stored: v{from_v} → v{to_v}")


# -------------------------
# MAIN PIPELINE (FIXED)
# -------------------------
def process_celex(act_id, celex, new_text):

    print("\n" + "=" * 70)
    print(f"📘 CELEX: {celex}")
    print("=" * 70)

    # ALWAYS re-fetch latest from DB
    latest = get_latest_version(act_id)

    new_hash = hash_text(normalize(new_text))

    print("🔐 New hash:", new_hash)

    if latest:
        print("📄 Latest version:", latest["version_number"])
        print("🔐 Old hash:", latest["content_hash"])

        # NO CHANGE → STOP
        if latest["content_hash"] == new_hash:
            print("⏭ No change detected → skipping")
            return
    else:
        print("🆕 First version")

    # -------------------------
    # VERSION LOGIC
    # -------------------------
    next_version = get_next_version(latest)

    inserted = insert_version(
        act_id,
        celex,
        new_text,
        new_hash,
        next_version
    )

    if not inserted:
        print("🚨 Insert failed → abort diff")
        return

    # -------------------------
    # UPDATE OLD LATEST
    # -------------------------
    update_old_latest(latest)

    # -------------------------
    # CREATE DIFF (ONLY IF NOT FIRST VERSION)
    # -------------------------
    if latest:
        diff_text = generate_diff(latest["content"], new_text)

        insert_diff(
            act_id,
            celex,
            latest["version_number"],
            next_version,
            diff_text
        )


# -------------------------
# DEBUG VIEW ALL VERSIONS
# -------------------------
def show_all(act_id):
    print("\n📜 FULL VERSION HISTORY")

    res = supabase.table(TABLE_VERSIONS) \
        .select("*") \
        .eq("act_id", act_id) \
        .order("version_number") \
        .execute()

    for r in res.data:
        print(f"v{r['version_number']} | {r['content_hash']}")


# -------------------------
# TEST RUN
# -------------------------
if __name__ == "__main__":

    data = [
        ("ec657d4c-eb3c-4855-9a40-3d9688934c47", "32003L0087",
         "Article 1 defines scope."),
        ("ec657d4c-eb3c-4855-9a40-3d9688934c47", "32003L0087",
         "Article 1 defines scope. Article 5 added compliance rules."),
        ("ec657d4c-eb3c-4855-9a40-3d9688934c47", "32003L0087",
         "Article 1 revised. Article 5 expanded enforcement."),
    ]

    for act_id, celex, text in data:
        process_celex(act_id, celex, text)

    show_all(data[0][0])
