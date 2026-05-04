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

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

TRACKED_ACTS = [
    "32003L0087",
    "32018R2066",
    "32018R2067"
]

# =========================
# HELPERS
# =========================
def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# =========================
# STEP 1: FETCH EUR-LEX
# =========================
def get_latest_consolidated(celex):
    url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:0{celex}"

    r = requests.get(url)
    r.raise_for_status()

    today = datetime.utcnow().date()

    return {
        "celex": f"0{celex}-{today.strftime('%Y%m%d')}",
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
# STEP 3: ACT MANAGEMENT
# =========================
def get_act(celex):
    res = sb.table("legal_acts").select("*").eq("celex", celex).execute()

    if res.data:
        return res.data[0]["id"]

    res = sb.table("legal_acts").insert({
        "celex": celex,
        "title": f"EU Act {celex}"
    }).execute()

    return res.data[0]["id"]

# =========================
# STEP 4: VERSION CHECK
# =========================
def version_exists(act_id, date):
    res = sb.table("versions") \
        .select("id") \
        .eq("act_id", act_id) \
        .eq("version_date", str(date)) \
        .execute()

    return len(res.data) > 0

# =========================
# STEP 5: LOAD PREVIOUS STATE
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
# STEP 6: DIFF ENGINE
# =========================
def diff_articles(old, new):
    changes = []

    all_keys = set(old.keys()).union(new.keys())

    for k in all_keys:
        if k not in old:
            changes.append({
                "article": k,
                "type": "added",
                "old_hash": None,
                "new_hash": new[k]["hash"],
                "old_text": None,
                "new_text": new[k]["content"]
            })

        elif k not in new:
            changes.append({
                "article": k,
                "type": "removed",
                "old_hash": old[k]["hash"],
                "new_hash": None,
                "old_text": old[k]["content"],
                "new_text": None
            })

        elif old[k]["hash"] != new[k]["hash"]:
            changes.append({
                "article": k,
                "type": "modified",
                "old_hash": old[k]["hash"],
                "new_hash": new[k]["hash"],
                "old_text": old[k]["content"],
                "new_text": new[k]["content"]
            })

    return changes

# =========================
# STEP 7: IMPACT CLASSIFICATION
# =========================
def classify_change(text):
    t = text.lower()

    if "monitoring" in t or "reporting" in t:
        return "MRR_IMPACT", 5

    if "allowance" in t or "allocation" in t:
        return "FREE_ALLOCATION_IMPACT", 4

    if "penalty" in t or "fine" in t:
        return "COMPLIANCE_PENALTY_IMPACT", 5

    if "scope" in t or "installation" in t:
        return "SCOPE_CHANGE_IMPACT", 5

    if "definition" in t:
        return "DEFINITION_CHANGE", 3

    return "GENERAL_CHANGE", 1

# =========================
# STEP 8: PROCESS ACT
# =========================
def process_act(celex):
    print(f"\n📘 Processing {celex}")

    act_id = get_act(celex)
    latest = get_latest_consolidated(celex)

    if version_exists(act_id, latest["date"]):
        print("✅ No new version")
        return

    print("📄 Parsing")
    new_articles = parse_articles(latest["html"])

    print("📚 Loading previous")
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

    # =========================
    # SAVE ARTICLES
    # =========================
    sb.table("articles").insert([
        {
            "version_id": version_id,
            "article_number": k,
            "content": v["content"],
            "hash": v["hash"]
        }
        for k, v in new_articles.items()
    ]).execute()

    # =========================
    # SAVE RAW CHANGES
    # =========================
    sb.table("article_changes").insert([
        {
            "version_id": version_id,
            "article_number": c["article"],
            "change_type": c["type"],
            "previous_hash": c["old_hash"],
            "new_hash": c["new_hash"],
            "summary": (c["new_text"] or "")[:300]
        }
        for c in changes
    ]).execute()

    # =========================
    # IMPACT ANALYSIS (NEW CORE FEATURE)
    # =========================
    impacts = []

    for c in changes:
        combined_text = (c["old_text"] or "") + " " + (c["new_text"] or "")
        change_type, score = classify_change(combined_text)

        impacts.append({
            "version_id": version_id,
            "article_number": c["article"],
            "change_type": c["type"],
            "legal_change_type": change_type,
            "impact_score": score,
            "summary": combined_text[:300]
        })

    sb.table("article_impacts").insert(impacts).execute()

    # =========================
    # ALERT (OPTIONAL)
    # =========================
    if any(i["impact_score"] >= 4 for i in impacts):
        print("🚨 HIGH IMPACT REGULATORY CHANGE DETECTED")

    print("✅ Done:", celex)

# =========================
# MAIN
# =========================
def run():
    print("🚀 EU Compliance Intelligence System")

    for celex in TRACKED_ACTS:
        try:
            process_act(celex)
        except Exception as e:
            print(f"❌ Error {celex}: {e}")

if __name__ == "__main__":
    run()
