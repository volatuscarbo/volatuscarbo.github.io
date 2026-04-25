import requests
import os
import json

HEADERS = {"User-Agent": "Mozilla/5.0"}

# -----------------------------
# PATH RESOLUTION (IMPORTANT)
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
AMENDMENTS_FILE = os.path.join(DATA_DIR, "amendments.json")

# -----------------------------
def download_celex(celex):
    os.makedirs(RAW_DIR, exist_ok=True)

    for fmt in ["HTML", "XML"]:
        url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/{fmt}/?uri=CELEX:{celex}"
        path = os.path.join(RAW_DIR, f"{celex}.{fmt.lower()}")

        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()

            with open(path, "w", encoding="utf-8") as f:
                f.write(r.text)

            print(f"Saved {path}")

        except Exception as e:
            print(f"Failed {celex} ({fmt}): {e}")

# -----------------------------
def download_all(celex_list):
    for c in celex_list:
        print("Downloading", c)
        download_celex(c)

# -----------------------------
if __name__ == "__main__":
    try:
        with open(AMENDMENTS_FILE, "r", encoding="utf-8") as f:
            celex = json.load(f)

        download_all([x["celex"] for x in celex])

    except FileNotFoundError:
        print("amendments.json not found — run discover step first")
    except Exception as e:
        print("Error:", e)
