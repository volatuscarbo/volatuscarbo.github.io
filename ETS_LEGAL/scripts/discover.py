import requests
from bs4 import BeautifulSoup
import re

URL = "https://eur-lex.europa.eu/legal-content/EN/ALL/?uri=CELEX:32003L0087"

def discover_celex():
    html = requests.get(URL).text
    soup = BeautifulSoup(html, "html.parser")

    celex = set()

    for a in soup.find_all("a", href=True):
        match = re.search(r'CELEX:(3\d{3}[LR]\d{4})', a["href"])
        if match:
            celex.add(match.group(1))

    return list(celex)

if __name__ == "__main__":
    print(discover_celex())
