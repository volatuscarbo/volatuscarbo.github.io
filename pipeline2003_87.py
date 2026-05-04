import requests
import hashlib
import os
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client

# =========================
# CONFIG
# =========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BASE_CELEX = "32003L0087"  # Directive 2003/87/EC

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# HELPERS
# =========================
def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# =========================
# STEP 1: GET LATEST VERSION
# =========================
def get_latest_consolidated():
    # EUR-Lex consolidated URL pattern
    url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:0{BASE_CELEX}"

    r = requests.get(url)
    r.raise_for_status()

    # crude detection of consolidation date
    today = datetime.utcnow().date()

    return {
        "celex": f"0{BASE_CELEX}-{today.strftime('%Y%m%d')}",
        "date": today,
        "html": r.text,
        "url": url
    }

# =========================
# STEP 2: PARSE ARTICLES
# =========================
def parse_articles(html):
    soup = BeautifulSoup(html, "html.parser")

    articles = {}

    for tag in soup.find_all(["h2", "h3"]):
        text = tag.get_text().strip()

        if text.startswith("Article"):
            number = text.replace("Article", "").strip()
            content = []

            for sib in tag.find_next_siblings():
                if sib.name in ["h2", "h3"]:
                    break
                content.append(sib.get_text(" ", strip=True))

            full_text = " ".join(content)

            articles[number] = {
                "content": full_text,
                "hash": hash_text(full_text)
            }

    return articles

# =========================
# STEP 3: GET OR CREATE ACT
# =========================
def get_act():
    res = sb.table("legal_acts").select("*").eq("celex", BASE_CELEX).execute()

    if res.data:
        return res.data[0]["id"]

    res = sb.table("legal_acts").insert({
        "celex": BASE_CELEX,
        "title": "EU ETS Directive"
    }).execute()

    return res.data[0]["id"]

# =========================
# STEP 4: CHECK VERSION
# =========================
def version_exists(act_id, date):
    res = sb.table("versions") \
        .select("*") \
        .eq("act_id", act_id) \
        .eq("version_date", str(date)) \
        .execute()

    return len(res.data) > 0

# =========================
# STEP 5: LOAD LAST VERSION
# =========================
def load_last_articles(act_id):
    res = sb.table("versions") \
        .select("id") \
        .eq("act_id", act_id) \
        .order("version_date", desc=True) \
        .limit(1) \
        .execute()

    if not res.data:
        return None, {}

    version_id = res.data[0]["id"]

    rows = sb.table("articles") \
        .select("*") \
        .eq("version_id", version_id) \
        .execute()

    return version_id, {
        r["article_number"]: r for r in rows.data
    }

# =========================
# STEP 6: DIFF
# =========================
def diff_articles(old, new):
    changes = []

    all_keys = set(old.keys()).union(new.keys())

    for k in all_keys:
        if k not in old:
            changes.append((k, "added", None, new[k]["hash"]))
        elif k not in new:
            changes.append((k, "removed", old[k]["hash"], None))
        elif old[k]["hash"] != new[k]["hash"]:
            changes.append((k, "modified", old[k]["hash"], new[k]["hash"]))

    return changes

# =========================
# MAIN PIPELINE
# =========================
def run():
    print("🚀 Running EU law tracker")

    act_id = get_act()

    latest = get_latest_consolidated()

    if version_exists(act_id, latest["date"]):
        print("✅ No new version")
        return

    print("📄 Parsing new version")
    new_articles = parse_articles(latest["html"])

    print("📚 Loading previous version")
    _, old_articles = load_last_articles(act_id)

    print("🔬 Diffing")
    changes = diff_articles(old_articles, new_articles)

    print("💾 Saving version")
    v = sb.table("versions").insert({
        "act_id": act_id,
        "celex": latest["celex"],
        "version_date": str(latest["date"]),
        "label": f"Consolidated {latest['date']}",
        "source_url": latest["url"]
    }).execute()

    version_id = v.data[0]["id"]

    print("💾 Saving articles")
    for k, v in new_articles.items():
        sb.table("articles").insert({
            "version_id": version_id,
            "article_number": k,
            "content": v["content"],
            "hash": v["hash"]
        }).execute()

    print("💾 Saving diffs")
    for (k, t, old_h, new_h) in changes:
        sb.table("article_changes").insert({
            "version_id": version_id,
            "article_number": k,
            "change_type": t,
            "previous_hash": old_h,
            "new_hash": new_h,
            "summary": t
        }).execute()

    print("✅ Done")

if __name__ == "__main__":
    run()
