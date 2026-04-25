import difflib


def diff_text(old, new, fromfile="previous", tofile="current"):
    """
    Returns a unified diff between two texts.
    Accepts either a string or a list of lines — normalises both.
    """
    # fix: normalise to list of lines regardless of input type
    if isinstance(old, str):
        old = old.splitlines()
    if isinstance(new, str):
        new = new.splitlines()

    return list(
        difflib.unified_diff(old, new, fromfile=fromfile, tofile=tofile, lineterm="")
    )


def diff_laws(old_law, new_law, celex=""):
    """
    Compares two parsed law dicts and returns a list of changes.
    Detects added, removed, and modified articles.

    Args:
        old_law: dict of {article_id: lines} from the previous version
        new_law: dict of {article_id: lines} from the current version
        celex:   optional document ID for clearer error messages
    """
    # fix: validate inputs before iterating
    if old_law is None or new_law is None:
        raise ValueError(
            f"diff_laws received None input for {celex or 'unknown document'} — "
            "check parse_html return value"
        )
    if not isinstance(old_law, dict) or not isinstance(new_law, dict):
        raise TypeError(
            f"diff_laws expects dicts, got {type(old_law).__name__} / "
            f"{type(new_law).__name__}"
        )

    changes = []

    # fix: detect removed articles (were in old, gone from new)
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
                    old_law[article],
                    new_law[article],
                    fromfile=f"{celex}/previous/{article}",
                    tofile=f"{celex}/current/{article}",
                )
                if diff:
                    changes.append({
                        "type": "MODIFIED",
                        "article": article,
                        "diff": diff,
                    })
            except Exception as exc:
                # fix: log and continue — one bad article does not abort the rest
                print(f"  ⚠️ Could not diff article '{article}' in {celex}: {exc}")
                changes.append({
                    "type": "DIFF_ERROR",
                    "article": article,
                    "error": str(exc),
                })

    return changes
