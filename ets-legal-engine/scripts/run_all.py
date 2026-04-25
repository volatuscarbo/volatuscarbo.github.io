import json
from discover import discover_celex
from download import download_all
from parse import parse_html
from diff_engine import diff_laws
import os

def load_json(path):
    if not os.path.exists(path):
        return []
    return json.load(open(path))

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def update_amendments():
    found = discover_celex()
    existing = load_json("data/amendments.json")

    existing_ids = {x["celex"] for x in existing}

    for c in found:
        if c not in existing_ids:
            existing.append({"celex": c})

    save_json("data/amendments.json", existing)
    return existing

def run():
    amendments = update_amendments()

    celex_ids = [x["celex"] for x in amendments]

    download_all(celex_ids)

    files = [f"data/raw/{c}.html" for c in celex_ids if os.path.exists(f"data/raw/{c}.html")]

    if len(files) < 2:
        return

    old = parse_html(files[-2])
    new = parse_html(files[-1])

    diff = diff_laws(old, new)

    save_json("data/diffs.json", diff)

if __name__ == "__main__":
    run()
