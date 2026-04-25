
import sys
import re
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 30

# The relations page lists all acts that amend or are related to the ETS Directive
# More reliable than scraping the document page itself
DISCOVER_URLS = [
    "https://eur-lex.europa.eu/legal-content/EN/ALL/?uri=CELEX:32003L0087",
]

# Fix: \d{4} for the 4-digit year; extended types to catch R (Regulation), D (Decision)
CELEX_PATTERN = re.compile(r'CELEX[:/](3\d{4}[A-Z]\d{4})')

# -----------------------------
def discover_celex():
    """
    Scrape EUR-Lex for CELEX codes related to the ETS Directive.
    Returns a sorted, deduplicated list of CELEX IDs.
    """
    found = set()

    for url in DISCOVER_URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()

            if len(r.content) < 500:
                print(f"⚠️ Response from {url} suspiciously small — skipping")
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                match = CELEX_PATTERN.search(a["href"])
                if match:
                    found.add(match.group(1))

            print(f"  ✅ {url} → {len(found)} code(s) found so far")

        except requests.RequestException as exc:
            print(f"  ❌ Failed to fetch {url}: {exc}")

    return sorted(found)   # fix: deterministic order

# -----------------------------
if __name__ == "__main__":
    try:
        result = discover_celex()
        if not result:
            print("⚠️ No CELEX codes found — check URLs and regex")
            sys.exit(1)
        print(result)
    except Exception:
        import traceback
        print("❌ Discovery failed")
        traceback.print_exc()
        sys.exit(1)
