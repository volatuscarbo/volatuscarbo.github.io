from bs4 import BeautifulSoup

def parse_html(file):
    with open(file, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    law = {}
    current_article = None

    for el in soup.find_all("p"):
        text = el.get_text(strip=True)

        if text.startswith("Article"):
            current_article = text.split()[1]
            law[current_article] = []

        elif current_article:
            law[current_article].append(text)

    return law
