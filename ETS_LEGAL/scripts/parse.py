import os
from bs4 import BeautifulSoup


# Tags that may contain article body text in EUR-Lex HTML
BODY_TAGS = {"p", "div", "li", "td", "span"}


def parse_html(file):
    """
    Parse a EUR-Lex HTML document into a dict of {article_id: [lines]}.
    Returns an empty dict if no articles are found (caller should check).
    Raises FileNotFoundError or ValueError with a clear message on failure.
    """
    if not os.path.exists(file):
        raise FileNotFoundError(f"parse_html: file not found — {file}")

    try:
        # Pass bytes so BeautifulSoup can detect encoding from the HTML charset declaration
        with open(file, "rb") as f:
            soup = BeautifulSoup(f, "html.parser")
    except Exception as exc:
        raise ValueError(f"parse_html: could not read {file}: {exc}") from exc

    law = {}
    current_article = None

    for el in soup.find_all(True):   # all tags
        if el.name not in BODY_TAGS and el.name not in {"h1","h2","h3","h4","h5"}:
            continue

        text = el.get_text(strip=True)
        if not text:
            continue   # fix: skip empty paragraphs

        # Detect article headers more precisely:
        # must start with "Article" followed by a number/identifier,
        # and be short (not an inline cross-reference mid-paragraph)
        words = text.split()
        if (
            words[0] == "Article"
            and len(words) >= 2          # fix: guard against bare "Article"
            and len(text) < 60          # fix: exclude long cross-reference sentences
        ):
            article_id = words[1]       # e.g. "1", "10a", "2(1)"

            # fix: warn on duplicate article IDs rather than silently overwriting
            if article_id in law:
                print(f"  ⚠️ parse_html: duplicate article ID '{article_id}' in {file} — appending")
                law[article_id].append(f"[DUPLICATE HEADER: {text}]")
            else:
                current_article = article_id
                law[current_article] = []

        elif current_article:
            law[current_article].append(text)

    # fix: warn if nothing was parsed — helps diagnose HTML structure changes
    if not law:
        print(f"  ⚠️ parse_html: no articles found in {file} — HTML structure may have changed")

    return law
