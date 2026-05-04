import os
import requests
import hashlib
from datetime import datetime

# =========================
# CONFIG
# =========================

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing SUPABASE_URL or SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# CELEX input (GitHub Actions or local fallback)
CELEX = os.environ.get("CELEX", "32003L0087")

# =========================
# ACT MANAGEMENT
# =========================

def get_or_create_act(celex):
    print("🔍 Checking act:", celex, flush=True)

    url = f"{SUPABASE_URL}/rest/v1/acts?celex=eq.{celex}&select=id"

    res = requests.get(url, headers=HEADERS).json()

    if isinstance(res, list) and len(res) > 0:
        act_id = res[0]["id"]
        print("✔ Found act:", act_id, flush=True)
        return act_id

    print("➕ Creating act", flush=True)

    res = requests.post(
        f"{SUPABASE_URL}/rest/v1/acts",
        headers=HEADERS,
        json={"celex": celex, "title": celex}
    ).json()

    return res[0]["id"]

# =========================
# EUR-Lex XML FETCH
# =========================

def fetch_xml(celex):
    url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/XML/?uri=CELEX:{celex}"

    print("🌐 Fetching XML:", url, flush=True)

     headers = {"User-Agent": "Mozilla/5.0"}

    for i in range(retries):
        res = requests.get(url, headers=headers)

        print(f"try {i+1} status:", res.status_code)

        # ✅ success
        if res.status_code == 200:
            return res.text

        # ⏳ still processing
        if res.status_code == 202:
            time.sleep(2 * (i + 1))  # exponential backoff
            continue

        # ❌ real error
        raise Exception(f"EUR-Lex error {res.status_code}: {res.text[:200]}")

    

    return res.text

# =========================
# HASHING
# =========================

def hash_content(xml_text):
    return hashlib.sha256(xml_text.encode("utf-8")).hexdigest()

# =========================
# EXISTING VERSIONS
# =========================

def get_existing_hashes(act_id):
    url = f"{SUPABASE_URL}/rest/v1/act_versions?act_id=eq.{act_id}&select=content_hash"

    res = requests.get(url, headers=HEADERS).json()

    if not isinstance(res, list):
        return set()

    return {r["content_hash"] for r in res if "content_hash" in r}

# =========================
# INSERT VERSION
# =========================

def insert_version(act_id, celex, xml_text, existing_hashes):
    content_hash = hash_content(xml_text)

    if content_hash in existing_hashes:
        print("⏭ No change detected (duplicate)", flush=True)
        return False

    print("🆕 New version detected", flush=True)

    url = f"{SUPABASE_URL}/rest/v1/act_versions"

    payload = {
        "act_id": act_id,
        "celex_version": celex,
        "version_date": datetime.utcnow().isoformat(),
        "content_xml": xml_text,
        "content_hash": content_hash
    }

    res = requests.post(url, headers=HEADERS, json=payload)

    if res.status_code not in (200, 201):
        raise Exception(f"Insert failed: {res.text}")

    print("✔ Inserted version:", celex, flush=True)
    return True

# =========================
# MAIN PIPELINE
# =========================

def run():
    print("🚀 Starting CELEX pipeline:", CELEX, flush=True)

    # 1. Get or create act
    act_id = get_or_create_act(CELEX)

    # 2. Fetch XML from EUR-Lex
    xml_text = fetch_xml(CELEX)

    # 3. Get existing versions
    existing = get_existing_hashes(act_id)

    # 4. Insert if new
    inserted = insert_version(act_id, CELEX, xml_text, existing)

    # 5. Summary
    print("✅ DONE", flush=True)
    print("Inserted:", inserted, flush=True)

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    run()
