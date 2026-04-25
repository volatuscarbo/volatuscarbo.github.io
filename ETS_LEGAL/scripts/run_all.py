def run():
    print("🚀 Starting ETS Legal Engine pipeline...")

    # Write empty diffs.json immediately so the file always exists in the repo
    save_json(DIFF_FILE, [])

    # STEP 1: Discover
    amendments = update_amendments()
    celex_ids = [x["celex"] for x in amendments if "celex" in x]
    if not celex_ids:
        print("❌ No CELEX IDs found — check discover_celex()")
        sys.exit(1)

    # STEP 2: Download
    print("⬇️ Downloading documents...")
    failed_downloads = download_all(celex_ids)
    if failed_downloads:
        print(f"⚠️ Failed downloads: {failed_downloads}")

    # STEP 3: Collect downloaded files
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
        print("❌ No downloaded files — check CELLAR/EUR-Lex connectivity")
        sys.exit(1)

    # STEP 4: Diff each document against its previous version
    print("⚖️ Comparing against previous versions...")
    os.makedirs(PREV_DIR, exist_ok=True)

    all_diffs = []
    first_run_count = 0

    for celex, current_path in downloaded:
        prev_path = os.path.join(PREV_DIR, f"{celex}.html")

        if not os.path.exists(prev_path):
            print(f"  ℹ️ {celex}: first run — saving as baseline (no diff yet)")
            shutil.copy2(current_path, prev_path)
            first_run_count += 1
            continue

        try:
            old_doc = parse_html(prev_path)
            new_doc = parse_html(current_path)
            diff = diff_laws(old_doc, new_doc, celex=celex)
            if diff:
                all_diffs.append({"celex": celex, "changes": diff})
                print(f"  🔬 {celex}: {len(diff)} change(s) detected")
            else:
                print(f"  ✅ {celex}: no changes")
            shutil.copy2(current_path, prev_path)
        except Exception as exc:
            print(f"  ❌ {celex}: diff failed — {exc}")
            traceback.print_exc()

    # STEP 5: Save results
    save_json(DIFF_FILE, all_diffs)

    if first_run_count:
        print(f"ℹ️ {first_run_count} document(s) saved as baseline — "
              f"diffs will appear from the next run onwards")
    print(f"✅ Pipeline complete. {len(all_diffs)} document(s) with changes.")
