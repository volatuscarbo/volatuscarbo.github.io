import difflib

def diff_text(old, new):
    return list(difflib.unified_diff(old, new, lineterm=""))

def diff_laws(old_law, new_law):
    changes = []

    for article in new_law:
        if article not in old_law:
            changes.append({"type": "ARTICLE_ADDED", "article": article})
        else:
            diff = diff_text(old_law[article], new_law[article])
            if diff:
                changes.append({
                    "type": "MODIFIED",
                    "article": article,
                    "diff": diff
                })

    return changes
