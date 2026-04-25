import json
import os
import shutil
import sys
import traceback

from scripts.download import download_all
from scripts.discover import discover_celex
from scripts.parse import parse_html
from scripts.diff_engine import diff_laws

BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR        = os.path.join(BASE_DIR, "data")
RAW_DIR         = os.path.join(DATA_DIR, "raw")
PREV_DIR        = os.path.join(DATA_DIR, "previous")
AMENDMENTS_FILE = os.path.join(DATA_DIR, "amendments.json")
DIFF_FILE       = os.path.join(DATA_DIR, "diffs.json")

def load_json(path):
    dir_path = os.path.dirname(path)
    if dir_path:
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
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

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

def run():
    print("🚀 Starting ETS Legal Engine pipeline...")

    amendments = update_amendments()
    celex_ids = [x["celex"] for x in amendments if "celex" in x]
    if not celex_ids:
        print("❌ No CELEX IDs found — check discover_celex()")
        sys.exit(1)   # fail loudly so CI goes red

    print("⬇️ Downloading documents...")
    failed_downloads = download_all(celex_ids)
    if failed_downloads:
        print(f"⚠️ Failed downloads: {failed_downloads}")

    downloaded = []
    missing = []
    for c in celex_ids:
        path = os.path.join(RAW_DIR, f"{c}.html")
        if os.path.exists(path):
            downloaded.append((c, path))
        else:
            missing.append(c)

    if missing:
        print(f"⚠️ No file for: {missing}")
    print(f"📄 {len(downloaded)} file(s) available")

    if not downloaded:
        print("❌ No downloaded files. Exiting.")
        sys.exit(1)

    print("⚖️ Comparing against previous versions...")
    os.makedirs(PREV_DIR, exist_ok=True)

    all_diffs = []
    for celex, current_path in downloaded:
        prev_path = os.path.join(PREV_DIR, f"{celex}.html")
        if not os.path.exists(prev_path):
            print(f"  ℹ️ {celex}: first run — saving baseline")
            shutil.copy2(current_path, prev_path)
            continue
        try:
            old_doc = parse_html(prev_path)
            new_doc = parse_html(current_path)
            diff = diff_laws(old_doc, new_doc, celex=celex)
            if diff:
                all_diffs.append({"celex": celex, "changes": diff})
                print(f"  🔬 {celex}: {len(diff)} change(s)")
            else:
                print(f"  ✅ {celex}: no changes")
            shutil.copy2(current_path, prev_path)
        except Exception as exc:
            print(f"  ❌ {celex}: {exc}")
            traceback.print_exc()

    save_json(DIFF_FILE, all_diffs)
    print(f"✅ Done. {len(all_diffs)} document(s) with changes.")

if __name__ == "__main__":
    try:
        run()
    except Exception:
        print("❌ PIPELINE FAILED")
        traceback.print_exc()
        sys.exit(1)
