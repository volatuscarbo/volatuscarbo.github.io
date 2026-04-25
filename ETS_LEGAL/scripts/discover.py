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
        r = session.get("https://eur-lex.europa.eu", headers=HEADERS, timeout=
