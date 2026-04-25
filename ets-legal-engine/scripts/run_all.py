import json
import os

from discover import discover_celex
from download import download_all
from parse import parse_html
from diff_engine import diff_laws

# -----------------------------
# PATH FIX (IMPORTANT)
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")

AMENDMENTS_FILE = os.path.join(DATA_DIR, "amendments.json")
DIFF_FILE = os.path.join(DATA_DIR, "diffs.json")

# -----------------------------
# HELPERS
# -----------------------------
def load_json(path):
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump([], f)
        return []
    return json.load(open(path))

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# -----------------------------
# CORE LOGIC
# -----------------------------
def update_amendments():
    print("Discovering CELEX...")
    found = discover_celex()

    existing = load_json(AMENDMENTS_FILE)
    existing_ids = {x["celex"] for x in existing}

    for c in found:
        if c not in existing_ids:
            existing.append({"celex": c})

    save_json(AMENDMENTS_FILE, existing)
    print(f"Stored {len(existing)} amendments")
    return existing

def run():
    print("Running ETS Legal Engine...")

    amendments = update_amendments()
    celex_ids = [x["celex"] for x in amendments]

    print("Downloading files...")
    download_all(celex_ids)

    files = [
        os.path.join(RAW_DIR, f"{c}.html")
        for c in celex_ids
        if os.path.exists(os.path.join(RAW_DIR, f"{c}.html"))
    ]

    print(f"Found {len(files)} files")

    if len(files) < 2:
        print("Not enough files to diff yet")
        return

    old = parse_html(files[-2])
    new = parse_html(files[-1])

    diff = diff_laws(old, new)

    save_json(DIFF_FILE, diff)
    print("Diff saved")

# -----------------------------
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        import traceback
        traceback.print_exc()
        exit(1)
