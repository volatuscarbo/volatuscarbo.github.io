import json
import os
import shutil
import sys
import traceback

from scripts.download import download_all
from scripts.discover import discover_celex
from scripts.parse import parse_html
from scripts.diff_engine import diff_laws

# -----------------------------
# PATH CONFIG (CI SAFE)
# -----------------------------
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR        = os.path.join(BASE_DIR, "data")
RAW_DIR         = os.path.join(DATA_DIR, "raw")
PREV_DIR        = os.path.join(DATA_DIR, "previous")   # <-- new: stores last-run snapshots
AMENDMENTS_FILE = os.path.join(DATA_DIR, "amendments.json")
DIFF_FILE       = os.path.join(DATA_DIR, "diffs.json")

# -----------------------------
# HELPERS
# -----------------------------
def load_json(path):
    dir_path = os.path.dirname(path)
    if dir_path:                                        # fix: guard against empty dirname
        os.makedirs(dir_path, exist_ok=True)
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
    dir_path = os.path.dirname(path)
    if dir_path:                                        # fix: guard against empty dirname
        os.makedirs(dir_path, exist_ok=True)
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

    # STEP 1: Discover
    amendments = update_amendments()
    celex_ids = [x["celex"] for x in amendments if "celex" in x]
    if not celex_ids:
        print("❌ No CELEX IDs found — check discover_celex().")
        sys.exit(1)                                     # fix: exit with error code, not silently

    # STEP 2: Download
    print("⬇️ Downloading documents...")
    result = download_all(celex_ids)
    # download_all may return failed IDs — handle defensively
    if isinstance(result, list) and result:
        print("❌ PIPELINE FAILED")
        traceback.print_exc()
        sys.exit(1) 
