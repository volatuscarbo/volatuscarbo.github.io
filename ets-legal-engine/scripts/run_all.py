import json
import os
import traceback

from scripts.download import download_all
from scripts.discover import discover_celex
from scripts.parse import parse_html
from scripts.diff_engine import diff_laws

# -----------------------------
# PATH CONFIG (CI SAFE)
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
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump([], f)
        return []

    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return []

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# -----------------------------
# STEP 1: DISCOVER
# -----------------------------
def update_amendments():
    print("🔍 Discovering CELEX codes...")

    found = discover_celex() or []
    print("DISCOVERED:", found)

    existing = load_json(AMENDMENTS_FILE)
    existing_ids = {x["celex"] for x in existing if "celex" in x}

    for c in found:
        if c not in existing_ids:
            existing.append({"celex": c})

    save_json(AMENDMENTS_FILE, existing)

    print(f"📦 Stored amendments: {len(existing)}")
    return existing

# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run():
    print("🚀 Starting ETS Legal Engine pipeline...")

    # STEP 1
    amendments = update_amendments()
    celex_ids = [x["celex"] for x in amendments if "celex" in x]

    if not celex_ids:
        print("⚠️ No CELEX IDs found. Exiting.")
        return

    # STEP 2
    print("⬇️ Downloading documents...")
    download_all(celex_ids)

    # STEP 3
    files = []
    for c in celex_ids:
        html_path = os.path.join(RAW_DIR, f"{c}.html")
        if os.path.exists(html_path):
            files.append(html_path)

    files = sorted(files)

    print(f"📄 Found {len(files)} downloaded files")

    # Not enough data to diff
    if len(files) < 2:
        print("ℹ️ Not enough files for diff yet")
        return

    # STEP 4
    print("⚖️ Parsing documents...")
    old = parse_html(files[-2])
    new = parse_html(files[-1])

    # STEP 5
    print("🔬 Generating diff...")
    diff = diff_laws(old, new)

    save_json(DIFF_FILE, diff)

    print("✅ Diff saved successfully")

# -----------------------------
# ENTRYPOINT
# -----------------------------
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("❌ PIPELINE FAILED")
        traceback.print_exc()
        exit(1)
