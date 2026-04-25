import json
import os
import sys
import time
import traceback

import requests

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT  = 30
MAX_RETRIES = 3
RETRY_DELAY = 5   # seconds between retries
MIN_CONTENT_BYTES = 500   # anything smaller is likely an error page

# -----------------------------
# PATH RESOLUTION
# -----------------------------
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR        = os.path.join(BASE_DIR, "data")
RAW_DIR         = os.path.join(DATA_DIR, "raw")
AMENDMENTS_FILE = os.path.join(DATA_DIR, "amendments.json")

# -----------------------------
def download_celex(celex, session):
    """
    Try HTML first, fall back to XML.
    Returns the format downloaded ('html' or 'xml'), or None on failure.
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    for fmt in ["HTML", "XML"]:
        url  = f"https://eur-lex.europa.eu/legal-content/EN/TXT/{fmt}/?uri=CELEX:{celex}"
        path = os.path.join(RAW_DIR, f"{celex}.{fmt.lower()}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
                r.raise_for_status()

                # Validate content — guard against soft-error pages
                if len(r.content) < MIN_CONTENT_BYTES:
                    raise ValueError(
                        f"Response too small ({len(r.content)} bytes) — "
                        f"likely an error page"
                    )

                with open(path, "w", encoding="utf-8") as f:
                    f.write(r.text)
                print(f"  ✅ Saved {fmt}: {path}")
                return fmt.lower()              # success — stop trying formats

            except Exception as exc:
                print(f"  ⚠️ {celex} ({fmt}) attempt {attempt}/{MAX_RETRIES}: {exc}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

    # All formats and retries exhausted
    print(f"  ❌ {celex}: all download attempts failed")
    return None

# -----------------------------
def download_all(celex_list):
    """
    Downloads all CELEX documents.
    Returns a list of CELEX IDs that failed — empty list means full success.
    """
    failed = []
    with requests.Session() as session:         # reuse TCP connection
        for c in celex_list:
            print(f"⬇️  Downloading {c}...")
            result = download_celex(c, session)
            if result is None:
                failed.append(c)
            time.sleep(1)                       # basic rate limiting

    if failed:
        print(f"⚠️ {len(failed)}/{len(celex_list)} download(s) failed: {failed}")
    return failed

# -----------------------------
if __name__ == "__main__":
    try:
        with open(AMENDMENTS_FILE, "r", encoding="utf-8") as f:
            celex = json.load(f)
        failed = download_all([x["celex"] for x in celex])
        if failed:
            sys.exit(1)
    except FileNotFoundError:
        print("amendments.json not found — run discover step first")
        sys.exit(1)
    except Exception:
        print("❌ Download script failed")
        traceback.print_exc()
        sys.exit(1)
