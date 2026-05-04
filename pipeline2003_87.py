import requests
import hashlib
import os
import re
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
    "32010R0998",
    "32021L2118"
]

# =========================
# HASH
# =========================
def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# =========================
# FETCH DOCUMENT
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
# UNIVERSAL PARSER (ROBUST)
# =========================
def parse_eu_document(html):
    soup = BeautifulSoup(html, "html.parser")

    articles = extract_structured(soup)

    if len(articles) < 3:
        articles = extract_regex(soup)

    if len(articles) < 3:
        articles = extract_fallback(soup)

    return articles


def extract_structured(soup):
    articles = {}

    for tag in soup.find_all(["h2", "h3", "h4", "p", "div"]):
        text = tag.get_text(" ", strip=True)

        match = re.match(r"^Article\s+(\d+[a-zA-Z0-9\-]*)", text, re.IGNORECASE)
        if not match:
            continue

        article_number = match.group(1)

        content = []
        for sib in tag.find_next_siblings():
            sib_text = sib.get_text(" ", strip=True)

            if re.match(r"^Article\s+\d", sib_text, re.IGNORECASE):
                break

            content.append(sib_text)

        full_text = " ".join(content).strip()

        if full_text:
            articles[article_number] = {
                "content": full_text,
                "hash": hash_text(full_text)
            }

    return articles


def extract_regex(soup):
    text = soup.get_text("\n", strip=True)

    pattern = re.compile(
        r"(Article\s+\d+[a-zA-Z0-9\-]*)\s*(.*?)(?=Article\s+\d+|\Z)",
        re.DOTALL | re.IGNORECASE
    )

    articles = {}

    for m in pattern.finditer(text):
        article_number = re.sub(r"[^\dA-Za-z\-]", "", m.group(1).replace("Article", ""))
        body = m.group(2).strip()

        if body:
            articles[article_number] = {
                "content": body,
                "hash": hash_text(body)
            }

    return articles


def extract_fallback(soup):
    text = soup.get_text("\n", strip=True)

    lines = text.split("\n")

    articles = {}
    current = None
    buffer = []

    for line in lines:
        if re.match(r"^Article\s+\d", line, re.IGNORECASE):
            if current and buffer:
                articles[current] = {
                    "content": " ".join(buffer),
                    "hash": hash_text(" ".join(buffer))
                }

            current = line.replace("Article", "").strip()
            buffer = []
        else:
            if current:
                buffer.append(line)

    if current and buffer:
        articles[current] = {
            "content": " ".join(buffer),
            "hash": hash_text(" ".join(buffer))
        }

    return articles

# =========================
# ACT MANAGEMENT
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
# VERSION CHECK
# =========================
def version_exists(act_id, date):
    res = sb.table("versions") \
        .select("id") \
        .eq("act_id", act_id) \
        .eq("version_date", str(date)) \
        .execute()

    return len(res.data) > 0

# =========================
# LOAD PREVIOUS VERSION
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
# DIFF ENGINE
# =========================
def diff_articles(old, new):
    changes = []

    all_keys = set(old.keys()).union(new.keys())

    for k in all_keys:
        if k not in old:
            changes.append({
                "article": k,
                "type": "added",
                "old_text": None,
                "new_text": new[k]["content"]
            })

        elif k not in new:
            changes.append({
                "article": k,
                "type": "removed",
                "old_text": old[k]["content"],
                "new_text": None
            })

        elif old[k]["hash"] != new[k]["hash"]:
            changes.append({
                "article": k,
                "type": "modified",
                "old_text": old[k]["content"],
                "new_text": new[k]["content"]
            })

    return changes

# =========================
# IMPACT CLASSIFIER
# =========================
def classify_change(text):
    t = (text or "").lower()

    if "monitoring" in t or "reporting" in t:
        return "MRR_IMPACT", 5

    if "allowance" in t or "allocation" in t:
        return "FREE_ALLOCATION_IMPACT", 4

    if "penalty" in t or "fine" in t:
        return "COMPLIANCE_PENALTY_IMPACT", 5

    if "scope" in t:
        return "SCOPE_CHANGE_IMPACT", 5

    if "definition" in t:
        return "DEFINITION_CHANGE", 3

    return "GENERAL_CHANGE", 1

# =========================
# OBLIGATION EXTRACTION
# =========================
def extract_obligations(text):
    text = text or ""

    return re.findall(r"\b(shall|must|required to|ensure)\b[^.]{0,200}", text, re.IGNORECASE)

# =========================
# PROCESS ACT
# =========================
def process_act(celex):
    print(f"\n📘 Processing {celex}")

    act_id = get_act(celex)
    latest = get_latest_consolidated(celex)

    if version_exists(act_id, latest["date"]):
        print("✅ No new version")
        return

    print("📄 Parsing")
    new_articles = parse_eu_document(latest["html"])

    if not new_articles:
        print("⚠️ No articles found — skipping")
        return

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
    # SAVE CHANGES
    # =========================
    sb.table("article_changes").insert([
        {
            "version_id": version_id,
            "article_number": c["article"],
            "change_type": c["type"],
            "summary": (c["new_text"] or "")[:300]
        }
        for c in changes
    ]).execute()

    # =========================
    # IMPACT + OBLIGATIONS
    # =========================
    impacts = []
    obligations_rows = []

    for c in changes:
        combined = (c["old_text"] or "") + " " + (c["new_text"] or "")

        change_type, score = classify_change(combined)

        impacts.append({
            "version_id": version_id,
            "article_number": c["article"],
            "change_type": c["type"],
            "legal_change_type": change_type,
            "impact_score": score,
            "summary": combined[:300]
        })

        obligations_rows.append({
            "version_id": version_id,
            "article_number": c["article"],
            "obligations": extract_obligations(combined)
        })

    if impacts:
        sb.table("article_impacts").insert(impacts).execute()

    if obligations_rows:
        sb.table("article_obligations").insert(obligations_rows).execute()

    if any(i["impact_score"] >= 4 for i in impacts):
        print("🚨 HIGH IMPACT REGULATORY CHANGE")

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
