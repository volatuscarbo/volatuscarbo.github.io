import requests
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime

SUPABASE_URL = "https://eeuedfwjnaupakapmvwi.supabase.co"
SUPABASE_KEY = "sb_secret_KGEAj-UMur_awENGB3NIIA_YndbMfYy"

CELEX = "32003L0087"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# -----------------------------
# 1. GET OR CREATE ACT
# -----------------------------
def get_act():
    url = f"{SUPABASE_URL}/rest/v1/acts?celex=eq.{CELEX}"
    res = requests.get(url, headers=HEADERS).json()
    print("acts_id =", res[0]["id"])
    if res:
        return res[0]["id"]

    insert = requests.post(
        f"{SUPABASE_URL}/rest/v1/acts",
        headers=HEADERS,
        json={"celex": CELEX, "title": "Directive 2003/87/EC"}
    ).json()

    return insert[0]["id"]


# -----------------------------
# 2. FETCH ALL VERSIONS
# -----------------------------
def fetch_versions():
    url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{CELEX}"
    html = requests.get(url).text

    soup = BeautifulSoup(html, "html.parser")

    versions = []

    for link in soup.select("[data-celex]"):
        celex_version = link.get("data-celex")
        date = link.get("data-date")

        if not celex_version or not date:
            continue

        version_url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri={celex_version}"
        v_html = requests.get(version_url).text

        content_hash = hashlib.sha256(v_html.encode()).hexdigest()

        versions.append({
            "celex_version": celex_version,
            "date": date,
            "content": v_html,
            "hash": content_hash
        })

    return versions


# -----------------------------
# 3. GET EXISTING VERSIONS
# -----------------------------
def get_existing_versions(acts_id):
    url = f"{SUPABASE_URL}/rest/v1/act_versions?act_id=eq.{acts_id}&select=celex_version"
    res = requests.get(url, headers=HEADERS).json()
    return set(v["celex_version"] for v in res)


# -----------------------------
# 4. INSERT NEW ONLY
# -----------------------------
def insert_new_versions(acts_id, versions, existing):
    inserted = 0

    for v in versions:
        if v["celex_version"] in existing:
            continue

        payload = {
            "acts_id": acts_id,
            "celex_version": v["celex_version"],
            "version_date": v["date"],
            "content": v["content"],
            "hash": v["hash"],
            "is_latest": False
        }

        requests.post(
            f"{SUPABASE_URL}/rest/v1/act_versions",
            headers=HEADERS,
            json=payload
        )

        inserted += 1

    return inserted


# -----------------------------
# 5. UPDATE LATEST FLAG
# -----------------------------
def update_latest(acts_id):
    # reset
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/act_versions?acts_id=eq.{acts_id}",
        headers=HEADERS,
        json={"is_latest": False}
    )

    # set newest
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/act_versions?acts_id=eq.{act_id}&order=version_date.desc&limit=1",
        headers=HEADERS,
        json={"is_latest": True}
    )


# -----------------------------
# MAIN
# -----------------------------
def run():
    print(f"🚀 Updating {CELEX}")

    acts_id = get_act()
    print("acts_id =", acts_id)
    versions = fetch_versions()
    existing = get_existing_versions(acts_id)

    inserted = insert_new_versions(acts_id, versions, existing)

    update_latest(acts_id)

    print(f"✅ Inserted {inserted} new versions")


if __name__ == "__main__":
    run()
