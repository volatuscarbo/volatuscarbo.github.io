import sys
import re
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 30

DISCOVER_URLS = [
    "https://eur-lex.europa.eu/legal-content/EN/ALL/?uri=CELEX:32003L0087",
]

# Fixed: \d{4} for 4-digit year, [A-Z] to catch L/R/D types
CELEX_PATTERN = re.compile(r'CELEX[:/](3\d{4}[A-Z]\d{4})')

def discover_celex():
    found = set()
    for url in DISCOVER_URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            if len(r.content) < 500:
                print(f"⚠️ Response too small from {url} — likely an error page")
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                match = CELEX_PATTERN.search(a["href"])
                if match:
                    found.add(match.group(1))
            print(f"  ✅ {url} → {len(found)} code(s) found so far")
        except requests.RequestException as exc:
            print(f"  ❌ Failed to fetch {url}: {exc}")
    return sorted(found)

if __name__ == "__main__":
    try:
        result = discover_celex()
        if not result:
            print("⚠️ No CELEX codes found — check URL and regex")
            sys.exit(1)
        print(result)
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
