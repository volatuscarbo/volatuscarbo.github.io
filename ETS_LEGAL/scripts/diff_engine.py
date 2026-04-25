import difflib

def diff_text(old, new, fromfile="previous", tofile="current"):
    if isinstance(old, str):
        old = old.splitlines()
    if isinstance(new, str):
        new = new.splitlines()
    return list(difflib.unified_diff(old, new, fromfile=fromfile, tofile=tofile, lineterm=""))

def diff_laws(old_law, new_law, celex=""):
    if old_law is None or new_law is None:
        raise ValueError(f"diff_laws received None for {celex or 'unknown'}")
    if not isinstance(old_law, dict) or not isinstance(new_law, dict):
        raise TypeError(f"Expected dicts, got {type(old_law).__name__} / {type(new_law).__name__}")

    changes = []

    # Detect removed articles
    for article in old_law:
        if article not in new_law:
            changes.append({"type": "ARTICLE_REMOVED", "article": article})

    # Detect added and modified articles
    for article in new_law:
        if article not in old_law:
            changes.append({"type": "ARTICLE_ADDED", "article": article})
        else:
            try:
                diff = diff_text(
                    old_law[article], new_law[article],
                    fromfile=f"{celex}/previous/{article}",
                    tofile=f"{celex}/current/{article}",
                )
                if diff:
                    changes.append({"type": "MODIFIED", "article": article, "diff": diff})
            except Exception as exc:
                print(f"  ⚠️ Could not diff article '{article}' in {celex}: {exc}")
                changes.append({"type": "DIFF_ERROR", "article": article, "error": str(exc)})

    return changes
