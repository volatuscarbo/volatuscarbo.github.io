"""
ETS_LEGAL/scripts/run_all.py
============================
Monitors EUR-Lex for amendments to the seven core EU ETS directives.
Downloads via the CELLAR API (publications.europa.eu) which is not
IP-restricted and works reliably from GitHub Actions.
 
Run:
    cd ETS_LEGAL
    python -m scripts.run_all
 
Output:
    diffs.json          — results for the GitHub Actions email step
    baselines/<celex>.txt — stored full text of each directive (git-committed)
 
First run: saves baselines, no diffs produced.
Subsequent runs: compares against baselines, writes diffs if text changed.
"""
 
from __future__ import annotations
 
import difflib
import json
import re
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
 
import requests
from bs4 import BeautifulSoup
 
# ── Logging ──────────────────────────────────────────────────────────────────
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
 
# ── Paths ─────────────────────────────────────────────────────────────────────
 
SCRIPT_DIR   = Path(__file__).parent
ROOT_DIR     = SCRIPT_DIR.parent          # ETS_LEGAL/
BASELINE_DIR = ROOT_DIR / "baselines"
DIFFS_PATH   = ROOT_DIR / "diffs.json"
 
BASELINE_DIR.mkdir(exist_ok=True)
 
# ── The seven core EU ETS directives ─────────────────────────────────────────
# CELEX format: 3 (legislation) + 4-digit year + L (directive) + number
# Use the *consolidated* CELEX where available so we track the living text.
 
DIRECTIVES = [
    {
        "celex": "32003L0087",
        "title": "EU ETS Directive 2003/87/EC (establishing the scheme)",
    },
    {
        "celex": "32004L0101",
        "title": "Linking Directive 2004/101/EC (project mechanisms)",
    },
    {
        "celex": "32008L0101",
        "title": "Aviation Directive 2008/101/EC (aviation in ETS)",
    },
    {
        "celex": "32009L0029",
        "title": "Phase 3 Amending Directive 2009/29/EC",
    },
    {
        "celex": "32018L0410",
        "title": "Phase 4 Directive 2018/410/EU",
    },
    {
        "celex": "32023L0958",
        "title": "Revised Aviation Directive 2023/958/EU",
    },
    {
        "celex": "32023L0959",
        "title": "ETS2 Directive 2023/959/EU (buildings, road transport, maritime)",
    },
]
 
# ── HTTP helpers ──────────────────────────────────────────────────────────────
 
CELLAR_URL  = "https://publications.europa.eu/resource/celex/{celex}"
EURLEX_URL  = "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:{celex}"
EURLEX_HOME = "https://eur-lex.europa.eu"
 
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}
 
RETRY_DELAYS = [5, 15, 30]   # seconds between retries
 
 
def _make_session() -> requests.Session:
    """Return a requests Session with shared headers."""
    s = requests.Session()
    s.headers.update(HEADERS)
    return s
 
 
def _prime_eurlex_session(session: requests.Session) -> None:
    """
    Hit the EUR-Lex homepage to obtain the required consent cookie.
    Without this, EUR-Lex returns truncated / empty responses.
    """
    try:
        resp = session.get(EURLEX_HOME, timeout=20)
        resp.raise_for_status()
        log.info("EUR-Lex session primed (status %s)", resp.status_code)
    except Exception as exc:
        log.warning("Could not prime EUR-Lex session: %s", exc)
 
 
def _fetch_url(session: requests.Session, url: str, label: str) -> str | None:
    """
    GET url with retries. Returns the response text, or None on failure.
    Detects WAF / empty-body responses that look like successes but aren't.
    """
    for attempt, delay in enumerate([0] + RETRY_DELAYS, start=1):
        if delay:
            log.info("  Retry %d/%d for %s in %ds …", attempt, len(RETRY_DELAYS) + 1, label, delay)
            time.sleep(delay)
        try:
            resp = session.get(url, timeout=30, allow_redirects=True)
        except requests.RequestException as exc:
            log.warning("  [%s] request error: %s", label, exc)
            continue
 
        # EUR-Lex WAF returns HTTP 202 with a 0-byte body when blocking
        if resp.status_code == 202 and not resp.text.strip():
            log.warning("  [%s] WAF block (HTTP 202, empty body)", label)
            continue
 
        if resp.status_code == 404:
            log.warning("  [%s] 404 — CELEX may not exist at this source", label)
            return None
 
        if not resp.ok:
            log.warning("  [%s] HTTP %s", label, resp.status_code)
            continue
 
        text = resp.text.strip()
        if len(text) < 500:
            log.warning("  [%s] suspiciously short response (%d chars)", label, len(text))
            continue
 
        log.info("  [%s] downloaded %d chars", label, len(text))
        return text
 
    log.error("  [%s] all attempts failed", label)
    return None
 
 
def _html_to_text(html: str) -> str:
    """
    Strip HTML tags and normalise whitespace to produce clean plain text
    suitable for diffing. Removes scripts, styles, nav elements.
    """
    soup = BeautifulSoup(html, "html.parser")
 
    # Remove non-content elements
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "aside", "noscript", "iframe"]):
        tag.decompose()
 
    text = soup.get_text(separator="\n")
 
    # Collapse runs of blank lines to at most two
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Strip leading/trailing whitespace per line
    lines = [line.strip() for line in text.splitlines()]
    # Drop completely empty leading/trailing lines
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
 
    return "\n".join(lines)
 
 
# ── Baseline management ───────────────────────────────────────────────────────
 
def _baseline_path(celex: str) -> Path:
    return BASELINE_DIR / f"{celex}.txt"
 
 
def _load_baseline(celex: str) -> str | None:
    p = _baseline_path(celex)
    if p.exists():
        return p.read_text(encoding="utf-8")
    return None
 
 
def _save_baseline(celex: str, text: str) -> None:
    _baseline_path(celex).write_text(text, encoding="utf-8")
    log.info("  Baseline saved → baselines/%s.txt (%d lines)", celex, text.count("\n"))
 
 
# ── Diff computation ──────────────────────────────────────────────────────────
 
def _compute_diff(celex: str, title: str, old: str, new: str) -> dict | None:
    """
    Returns a diff record if the text changed, else None.
    The diff_snippet contains the first 3000 chars of the unified diff.
    """
    if old == new:
        return None
 
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)
 
    unified = list(difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile=f"{celex}_baseline",
        tofile=f"{celex}_current",
        lineterm="",
    ))
 
    added   = sum(1 for l in unified if l.startswith("+") and not l.startswith("+++"))
    removed = sum(1 for l in unified if l.startswith("-") and not l.startswith("---"))
 
    diff_text = "".join(unified)
 
    return {
        "celex":         celex,
        "title":         title,
        "added_lines":   added,
        "removed_lines": removed,
        "diff_snippet":  diff_text[:3000],    # keep email bodies manageable
        "full_diff_len": len(diff_text),
    }
 
 
# ── Download with source fallback ─────────────────────────────────────────────
 
def _download_directive(
    session: requests.Session,
    celex: str,
    title: str,
    eurlex_primed: bool,
) -> str | None:
    """
    Try CELLAR first (CI-friendly), fall back to EUR-Lex HTML endpoint.
    Returns clean plain text or None.
    """
    # ── Primary: CELLAR API ──────────────────────────────────────────────────
    log.info("[%s] Trying CELLAR …", celex)
    cellar_url = CELLAR_URL.format(celex=celex)
    html = _fetch_url(session, cellar_url, label=f"CELLAR/{celex}")
 
    if html:
        return _html_to_text(html)
 
    # ── Fallback: EUR-Lex HTML endpoint ──────────────────────────────────────
    log.info("[%s] CELLAR failed — falling back to EUR-Lex …", celex)
    if not eurlex_primed:
        _prime_eurlex_session(session)
 
    eurlex_url = EURLEX_URL.format(celex=celex)
    html = _fetch_url(session, eurlex_url, label=f"EUR-Lex/{celex}")
 
    if html:
        return _html_to_text(html)
 
    log.error("[%s] %s — could not download from any source", celex, title)
    return None
 
 
# ── Main ──────────────────────────────────────────────────────────────────────
 
def run() -> None:
    run_ts = datetime.now(timezone.utc).isoformat()
    log.info("ETS Legal Monitor — run started at %s", run_ts)
 
    # Write empty output immediately so a mid-run failure never leaves
    # the file missing for downstream workflow steps.
    DIFFS_PATH.write_text(json.dumps({
        "run_at":            run_ts,
        "amendments_found":  False,
        "summary":           "",
        "diffs":             [],
        "errors":            [],
    }, indent=2))
 
    session        = _make_session()
    eurlex_primed  = False
    diffs: list[dict]  = []
    errors: list[str]  = []
    first_run_celex: list[str] = []
 
    for directive in DIRECTIVES:
        celex = directive["celex"]
        title = directive["title"]
        log.info("── %s  %s", celex, title)
 
        text = _download_directive(session, celex, title, eurlex_primed)
        eurlex_primed = True   # only need to prime once
 
        if text is None:
            errors.append(f"{celex} ({title}): download failed from all sources")
            continue
 
        baseline = _load_baseline(celex)
 
        if baseline is None:
            # First time we've seen this directive — save baseline, no diff yet
            _save_baseline(celex, text)
            first_run_celex.append(celex)
            log.info("[%s] First run — baseline saved, no diff produced", celex)
            continue
 
        diff = _compute_diff(celex, title, baseline, text)
 
        if diff:
            log.info(
                "[%s] AMENDMENT DETECTED  +%d/-%d lines",
                celex, diff["added_lines"], diff["removed_lines"]
            )
            diffs.append(diff)
            # Update baseline to current text so the next run diffs against the
            # new version, not the original.
            _save_baseline(celex, text)
        else:
            log.info("[%s] No change", celex)
 
        # Be polite — small delay between downloads
        time.sleep(2)
 
    # ── Build human-readable summary ─────────────────────────────────────────
    amendments_found = len(diffs) > 0
 
    summary_lines: list[str] = []
 
    if first_run_celex:
        summary_lines.append(
            f"First-run baselines saved for: {', '.join(first_run_celex)}\n"
            "No diffs produced for these — next run will compare against today's text.\n"
        )
 
    if amendments_found:
        summary_lines.append(f"{len(diffs)} amendment(s) detected:\n")
        for d in diffs:
            summary_lines.append(
                f"  • {d['title']} ({d['celex']})\n"
                f"    +{d['added_lines']} lines added, -{d['removed_lines']} lines removed\n"
                f"    (diff is {d['full_diff_len']:,} chars total; "
                f"first 3 000 chars saved in diffs.json)\n"
            )
        summary_lines.append(
            "\nFull diffs are saved as a workflow artifact (retained 90 days)."
        )
    elif not first_run_celex:
        summary_lines.append("No amendments detected this run.")
 
    if errors:
        summary_lines.append(f"\n{len(errors)} download error(s):")
        for e in errors:
            summary_lines.append(f"  ✗ {e}")
 
    summary = "\n".join(summary_lines).strip()
    log.info("\n%s", summary)
 
    # ── Write final diffs.json ────────────────────────────────────────────────
    DIFFS_PATH.write_text(json.dumps({
        "run_at":           run_ts,
        "amendments_found": amendments_found,
        "summary":          summary,
        "diffs":            diffs,
        "errors":           errors,
    }, indent=2))
 
    log.info("diffs.json written — amendments_found=%s", amendments_found)
 
    # Exit non-zero if there were download errors so the workflow step turns
    # yellow (warning) rather than silently green.
    if errors and not amendments_found:
        sys.exit(1)
 
 
if __name__ == "__main__":
    run()
