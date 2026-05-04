"""
EU Legislative Act Version Tracker
===================================
Tracks changes to EU legislative acts in Supabase.

Two entry points:

- `process_act`       – single text; compares against DB latest, inserts if changed.
- `process_act_bulk`  – list of texts (oldest → newest); used on first run to
                        seed all known historical versions into the DB.

Environment variables:
    SUPABASE_URL  – Your Supabase project URL
    SUPABASE_KEY  – Your Supabase service-role or anon key

Tables required:
    act-versions         – stores full text of each version
    act_versions_diffs   – stores line-level diffs between consecutive versions
"""

import hashlib
import logging
import os
import re
import sys
from typing import Optional

from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")

TABLE_VERSIONS = "act_versions"
TABLE_DIFFS = "act_versions_diffs"


def get_supabase_client() -> Client:
    """Create and return a Supabase client, validating credentials."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL and SUPABASE_KEY must be set as environment variables.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------
def normalize(text: str) -> str:
    """Normalize text for consistent hash comparison."""
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


def hash_text(text: str) -> str:
    """Return a SHA-256 hex digest of the given text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def generate_diff(old_text: str, new_text: str) -> str:
    """Generate a simple line-level diff between two texts."""
    old_lines = old_text.splitlines()
    new_lines = new_text.splitlines()

    additions = [f"+ {line}" for line in new_lines if line not in old_lines]
    removals = [f"- {line}" for line in old_lines if line not in new_lines]

    return "\n".join(additions + removals)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_latest_version(client: Client, act_id: str) -> Optional[dict]:
    """Fetch the latest version of an act from the database."""
    res = (
        client.table(TABLE_VERSIONS)
        .select("*")
        .eq("act_id", act_id)
        .eq("is_latest", True)
        .order("version_number", desc=True)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def insert_version(
    client: Client,
    act_id: str,
    celex: str,
    text: str,
    content_hash: str,
    version_number: int,
    is_latest: bool = True,
) -> Optional[dict]:
    """Insert a new version row."""
    res = (
        client.table(TABLE_VERSIONS)
        .insert(
            {
                "act_id": act_id,
                "celex": celex,
                "content": text,
                "content_hash": content_hash,
                "version_number": version_number,
                "is_latest": is_latest,
            }
        )
        .execute()
    )

    if not res.data:
        logger.error("Failed to insert version %d for act %s", version_number, act_id)
        return None

    logger.info("Inserted v%d for act %s", version_number, act_id)
    return res.data[0]


def unmark_latest(client: Client, version_id: str) -> None:
    """Set is_latest=False on a previously-latest version row."""
    client.table(TABLE_VERSIONS).update({"is_latest": False}).eq("id", version_id).execute()


def insert_diff(
    client: Client,
    act_id: str,
    celex: str,
    from_version: int,
    to_version: int,
    diff_text: str,
) -> None:
    """Store a diff between two consecutive versions."""
    client.table(TABLE_DIFFS).insert(
        {
            "act_id": act_id,
            "celex": celex,
            "from_version": from_version,
            "to_version": to_version,
            "diff": diff_text,
        }
    ).execute()
    logger.info("Stored diff v%d → v%d for act %s", from_version, to_version, act_id)


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------
def process_act(
    client: Client,
    act_id: str,
    celex: str,
    current_text: str,
) -> None:
    """
    Process a single act version.

    Parameters
    ----------
    client : Client
        Authenticated Supabase client.
    act_id : str
        Unique identifier for the act.
    celex : str
        The CELEX number of the act.
    current_text : str
        The full text of the act as it exists right now.
    """
    if not current_text.strip():
        logger.warning("Empty text provided for %s – skipping.", celex)
        return

    logger.info("Processing CELEX %s (act_id=%s)", celex, act_id)

    current_hash = hash_text(normalize(current_text))
    latest = get_latest_version(client, act_id)

    # ----- First version ever -----
    if latest is None:
        logger.info("No existing versions – storing as v1.")
        insert_version(client, act_id, celex, current_text, current_hash, version_number=1)
        return

    # ----- No change -----
    logger.info(
        "Latest in DB: v%d | DB hash: %s | New hash: %s",
        latest["version_number"],
        latest["content_hash"],
        current_hash,
    )

    if latest["content_hash"] == current_hash:
        logger.info("No change detected – skipping.")
        return

    # ----- Change detected -----
    next_version = int(latest["version_number"]) + 1

    inserted = insert_version(client, act_id, celex, current_text, current_hash, next_version)
    if not inserted:
        return

    unmark_latest(client, latest["id"])

    diff_text = generate_diff(latest["content"], current_text)
    insert_diff(client, act_id, celex, latest["version_number"], next_version, diff_text)

    logger.info("Update complete – new version: v%d", next_version)


def process_act_bulk(
    client: Client,
    act_id: str,
    celex: str,
    versions: list[str],
) -> None:
    """
    Seed all known historical versions of an act into the DB.

    If versions already exist in the DB, only the last element in the list
    is compared against the latest stored version (same as process_act).

    Parameters
    ----------
    client : Client
        Authenticated Supabase client.
    act_id : str
        Unique identifier for the act.
    celex : str
        The CELEX number of the act.
    versions : list[str]
        Ordered list of full-text versions (oldest → newest).
    """
    if not versions:
        logger.warning("No versions provided for %s – skipping.", celex)
        return

    logger.info("Processing CELEX %s (act_id=%s) – %d version(s) provided", celex, act_id, len(versions))

    latest = get_latest_version(client, act_id)

    # ----- Versions already in DB → incremental update only -----
    if latest is not None:
        logger.info("Versions already exist in DB – falling back to incremental update.")
        process_act(client, act_id, celex, versions[-1])
        return

    # ----- Empty DB → bulk insert all versions -----
    logger.info("No existing versions – bulk inserting %d version(s).", len(versions))

    for i, text in enumerate(versions):
        if not text.strip():
            logger.warning("Empty text at position %d – skipping.", i)
            continue

        version_number = i + 1
        is_last = i == len(versions) - 1
        content_hash = hash_text(normalize(text))

        inserted = insert_version(
            client, act_id, celex, text, content_hash, version_number, is_latest=is_last
        )
        if not inserted:
            logger.error("Bulk insert aborted at v%d.", version_number)
            return

        # Store diff between consecutive versions
        if i > 0:
            diff_text = generate_diff(versions[i - 1], text)
            insert_diff(client, act_id, celex, version_number - 1, version_number, diff_text)

    logger.info("Bulk insert complete – %d version(s) stored.", len(versions))
