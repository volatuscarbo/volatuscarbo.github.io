import re
import sys
import requests
from bs4 import BeautifulSoup

# -----------------------------
# CONFIG
# -----------------------------
TIMEOUT = 30
CELEX_PATTERN = re.compile(r'CELEX[:/](3\d{4}[A-Z]\d{4})')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# EUR-Lex pages to scrape for related CELEX codes
DISCOVER_URLS = [
    "https://eur-lex.europa.eu/legal-content/EN/ALL/?uri=CELEX:32003L0087",
]

# Hardcoded fallback — used when EUR-Lex blocks the request or returns a bot-check page.
# Update this list when new ETS legislation is adopted.
KNOWN_ETS_CELEX = [
    "32003L0087",   # Original ETS Directive
    "32004L0101",   # Linking Directive
    "32008L0101",   # Aviation inclusion
    "32009L0029",   # Revised ETS — Phase 3
    "32014L0064",   # Small emitters opt-out
    "32018L0410",   # Phase 4 reform
    "32023L0959",   # Fit for 55 — ETS revision
]

# -----------------------------
# HELPERS
# -----------------------------
def _prime_session(session):
    """
    Load the EUR-Lex homepage so the session acquires consent cookies.
    Failure here is non-fatal — we log and continue.
    """
    try:
        r = session.get("https://eur-lex.europa.eu", headers=HEADERS, timeout=TIMEOUT)
        print(f"  Session primed — status {r.status_code}, "
              f"cookies: {list(session.cookies.keys())}")
    except Exception as exc:
        print(f"  ⚠️ Could not prime session (non-fatal): {exc}")


def _scrape_url(session, url):
    """
    Fetch one EUR-Lex page and return all CELEX codes found in href attributes.
    Returns an empty set if the page is unreachable or looks like a bot-check page.
    """
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()

        print(f"  Status: {r.status_code}, Size: {len(r.content)} bytes")

        if len(r.content) < 500:
            print(f"  ⚠️ Response too small — likely a consent/bot-check page")
            print(f"  Content preview: {r.text[:300]}")
            return set()

        soup = BeautifulSoup(r.text, "html.parser")
        found = set()
        for a in soup.find_all("a", href=True):
            match = CELEX_PATTERN.search(a["href"])
            if match:
                found.add(match.group(1))

        print(f"  ✅ {url} → {len(found)} code(s) found")
        return found

    except requests.RequestException as exc:
        print(f"  ❌ Request failed for {url}: {exc}")
        return set()


# -----------------------------
# MAIN FUNCTION
# -----------------------------
def discover_celex():
    """
    Discover CELEX codes for ETS-related legislation by scraping EUR-Lex.

    Strategy:
      1. Prime a requests.Session with the EUR-Lex homepage to pick up consent cookies.
      2. Scrape each URL in DISCOVER_URLS and collect matching CELEX codes.
      3. If scraping returns nothing (blocked, bot-check, network error),
         fall back to the hardcoded KNOWN_ETS_CELEX list.

    Returns a sorted, deduplicated list of CELEX ID strings.
    """
    print("🔍 Discovering CELEX codes...")
    found = set()

    session = requests.Session()
    _prime_session(session)

    for url in DISCOVER_URLS:
        codes = _scrape_url(session, url)
        found.update(codes)

    if not found:
        print("  ⚠️ No codes found via scraping — using hardcoded fallback list")
        found.update(KNOWN_ETS_CELEX)
    else:
        # Always include the known list so we never miss a core document
        before = len(found)
        found.update(KNOWN_ETS_CELEX)
        added = len(found) - before
        if added:
            print(f"  ℹ️ Added {added} code(s) from hardcoded list not found by scraping")

    result = sorted(found)
    print(f"  📋 Total CELEX codes to process: {len(result)}")
    return result


# -----------------------------
# ENTRYPOINT
# -----------------------------
if __name__ == "__main__":
    try:
        result = discover_celex()
        if not result:
            print("❌ No CELEX codes found")
            sys.exit(1)
        print("\nCELEX codes:")
        for c in result:
            print(f"  {c}")
    except Exception:
        import traceback
        print("❌ Discovery failed")
        traceback.print_exc()
        sys.exit(1)
