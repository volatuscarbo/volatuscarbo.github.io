print("FILE Started")
import requests
import hashlib
import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime

SUPABASE_URL = "https://eeuedfwjnaupakapmvwi.supabase.co"
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

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
    print("Start get_act", flush=True)
    
    url = f"{SUPABASE_URL}/rest/v1/acts?celex=eq.{CELEX}"

    res = requests.get(url, headers=HEADERS).json()
    print("DEBUG response:", res, flush=True)

    # 🔒 validate response type
    if not isinstance(res, list):
        raise Exception(f"Unexpected response from Supabase: {res}")

    # ✅ existing act found
    if len(res) > 0:
        act_id = res[0].get("id")
        print("acts_id =", act_id, flush=True)
        return act_id

    # 🆕 insert new act
    insert = requests.post(
        f"{SUPABASE_URL}/rest/v1/acts",
        headers=HEADERS,
        json={"celex": CELEX, "title": "Directive 2003/87/EC"}
    ).json()

    print("DEBUG insert response:", insert, flush=True)

    if isinstance(insert, list) and len(insert) > 0:
        return insert[0]["id"]

    raise Exception(f"Insert failed: {insert}")


# -----------------------------
# 2. FETCH ALL VERSIONS
# -----------------------------
def fetch_versions():
    url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{CELEX}"
    print("url :",url)
    res = requests.get(url, headers={
    "User-Agent": "Mozilla/5.0"
    })

    print("status:", res.status_code)
    print("final url:", res.url)
    print("html length:", len(res.text))
    print("preview:", res.text[:500])
    html = requests.get(url).text
    print("html :",html)
    soup = BeautifulSoup(html, "html.parser")
    
    versions = []

    for link in soup.select("[data-celex]"):
        celex_version = link.get("data-celex")
        date = link.get("data-date")
        print("celex_version :",celex_version)
        print("Date :",date)
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
        print("Versions :",versions)
    return versions


# -----------------------------
# 3. GET EXISTING VERSIONS
# -----------------------------
def get_existing_versions(acts_id):
    url = f"{SUPABASE_URL}/rest/v1/act_versions"

    params = {
        "act_id": f"eq.{acts_id}",
        "select": "celex_version"
    }

    res = requests.get(url, headers=HEADERS, params=params)
    data = res.json()

    return {
        row.get("celex_version")
        for row in data
        if isinstance(row, dict)
    }


# -----------------------------
# 4. INSERT NEW ONLY
# -----------------------------
def insert_new_versions(acts_id, versions, existing):
    inserted = 0

    for v in versions:
        if v["celex_version"] in existing:
            continue

        payload = {
            "act_id": acts_id,
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
        f"{SUPABASE_URL}/rest/v1/act_versions?act_id=eq.{acts_id}",
        headers=HEADERS,
        json={"is_latest": False}
    )

    # set newest
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/act_versions?act_id=eq.{acts_id}&order=version_date.desc&limit=1",
        headers=HEADERS,
        json={"is_latest": True}
    )


# -----------------------------
# MAIN
# -----------------------------
def run():
    
    
    acts_id = get_act()
    print("acts_id:  ", acts_id, flush=True)    
    versions = fetch_versions()
    existing = get_existing_versions(acts_id)

    inserted = insert_new_versions(acts_id, versions, existing)

    update_latest(acts_id)

    print(f"✅ Inserted {inserted} new versions")


if __name__ == "__main__":
    run()
