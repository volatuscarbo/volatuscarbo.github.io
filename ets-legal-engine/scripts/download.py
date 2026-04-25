import requests
import os

BASE = "data/raw"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def download_celex(celex):
    os.makedirs(BASE, exist_ok=True)

    for fmt in ["HTML", "XML"]:
        url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/{fmt}/?uri=CELEX:{celex}"
        path = f"{BASE}/{celex}.{fmt.lower()}"

        r = requests.get(url, headers=HEADERS)
        with open(path, "w", encoding="utf-8") as f:
            f.write(r.text)

def download_all(celex_list):
    for c in celex_list:
        print("Downloading", c)
        download_celex(c)

if __name__ == "__main__":
    import json
    celex = json.load(open("data/amendments.json"))
    download_all([x["celex"] for x in celex])
