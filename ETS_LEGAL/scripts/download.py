import json
import os
import sys
import time
import traceback
import requests

# -----------------------------
# CONFIG
# -----------------------------
TIMEOUT          = 30
MAX_RETRIES      = 3
RETRY_DELAY      = 5
MIN_CONTENT_BYTES = 500

BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR        = os.path.join(BASE_DIR, "data")
RAW_DIR         = os.path.join(DATA_DIR, "raw")
AMENDMENTS_FILE = os.path.join(DATA_DIR, "amendments.json")

# CELLAR is the Publications Office's official programmatic API.
# It is not subject to the same IP blocks as the EUR-Lex website.
CELLAR_BASE = "https://publications.europa.eu/resource/celex/{celex}"

# EUR-Lex direct URLs kept as fallback
EURLEX_BASE = "https://eur-lex.europa.eu/legal-content/EN/TXT/{fmt}/?uri=CELEX:{celex}"

# Headers for CELLAR content negotiation
CELLAR_HEADERS = {
    "User-Agent":      "ETS-Legal-Monitor/1.0 (frank.adriaansen@etsverification.com)",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}

# Headers for EUR-Lex fallback
EURLEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# -----------------------------
# HELPERS
# -----------------------------
def _fetch_with_retry(session, url, headers, label):
    """
    Attempt to fetch a URL up to MAX_RETRIES times.
    Returns the response text on success, or None on failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()

            if len(r.content) < MIN_CONTENT_BYTES:
                raise ValueError(
                    f"Response too small ({len(r.content)} bytes) — "
                    f"status {r.status_code}, likely blocked or error page"
                )

            return r.text

        except Exception as exc:
            print(f"    ⚠️ {label} attempt {attempt}/{MAX_RETRIES}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return None


# -----------------------------
# CORE DOWNLOAD FUNCTION
# -----------------------------
def download_celex(celex, session):
    """
    Download a CELEX document, trying CELLAR first then EUR-Lex as fallback.
    Saves as {celex}.html in RAW_DIR.
    Returns True on success, False on failure.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    out_path = os.path.join(RAW_DIR, f"{celex}.html")

    # --- Attempt 1: CELLAR API ---
    cellar_url = CELLAR_BASE.format(celex=celex)
    text = _fetch_with_retry(session, cellar_url, CELLAR_HEADERS, f"CELLAR {celex}")
    if text:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"  ✅ Saved via CELLAR: {out_path}")
        return True

    print(f"  ⚠️ CELLAR failed for {celex} — trying EUR-Lex direct...")

    # --- Attempt 2: EUR-Lex direct (HTML then XML) ---
    for fmt in ["HTML", "XML"]:
        eurlex_url = EURLEX_BASE.format(fmt=fmt, celex=celex)
        ext = fmt.lower()
        text = _fetch_with_retry(session, eurlex_url, EURLEX_HEADERS, f"EUR-Lex {fmt} {celex}")
        if text:
            save_path = os.path.join(RAW_DIR, f"{celex}.{ext}")
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"  ✅ Saved via EUR-Lex {fmt}: {save_path}")
            return True

    print(f"  ❌ {celex}: all sources failed")
    return False


# -----------------------------
# BATCH DOWNLOAD
# -----------------------------
def download_all(celex_list):
    """
    Download all documents in celex_list.
    Returns a list of CELEX IDs that failed — empty means full success.
    """
    failed = []
    with requests.Session() as session:
        for i, c in enumerate(celex_list, 1):
            print(f"⬇️  [{i}/{len(celex_list)}] Downloading {c}...")
            if not download_celex(c, session):
                failed.append(c)
            time.sleep(1)   # polite rate limiting

    if failed:
        print(f"⚠️ {len(failed)}/{len(celex_list)} download(s) failed: {failed}")
    return failed


# -----------------------------
# ENTRYPOINT
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
