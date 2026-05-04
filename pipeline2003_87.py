import time
import hashlib
import difflib
import random

# -------------------------
# MEMORY STORE (SIMULATES DB)
# -------------------------
store = {}

CELEX_LIST = [
    "32003L0087",
    "32018R2066",
    "32018R2067"
]


# -------------------------
# SIMULATED "EU LAW VERSIONS"
# -------------------------
def fetch_legislation_text(celex):
    """
    Simulates EU consolidated law changes over time.
    Each run randomly returns a different version.
    """

    base_versions = [
        f"{celex} | Article 1 defines scope.",
        f"{celex} | Article 1 defines scope. Article 5 added compliance rules.",
        f"{celex} | Article 1 updated. Article 5 expanded. Annex added.",
        f"{celex} | Full consolidation with penalty regime introduced."
    ]

    # FORCE variation (this is key for debugging)
    return random.choice(base_versions) + f" | ts={int(time.time())}"


# -------------------------
# HASH FUNCTION
# -------------------------
def hash_text(text):
    return hashlib.sha256(text.encode()).hexdigest()


# -------------------------
# DIFF FUNCTION
# -------------------------
def diff(old, new):
    return "\n".join(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            lineterm=""
        )
    )


# -------------------------
# CORE LOGIC
# -------------------------
def process_celex(celex):
    print("\n" + "="*80)
    print(f"📘 PROCESSING CELEX: {celex}")
    print("="*80)

    new_text = fetch_legislation_text(celex)
    new_hash = hash_text(new_text)

    print("\n🧾 FETCHED TEXT:")
    print(new_text)

    print("\n🔐 NEW HASH:")
    print(new_hash)

    previous = store.get(celex)

    # -------------------------
    # FIRST VERSION CASE
    # -------------------------
    if not previous:
        print("\n🆕 NO PREVIOUS VERSION FOUND → creating FIRST version")
        store[celex] = {
            "text": new_text,
            "hash": new_hash,
            "version": 1
        }
        return

    print("\n📄 PREVIOUS VERSION EXISTS")
    print("OLD HASH:", previous["hash"])

    # -------------------------
    # SAME VERSION CHECK
    # -------------------------
    if previous["hash"] == new_hash:
        print("\n⏭ NO CHANGE DETECTED → skipping version creation")
        return

    # -------------------------
    # NEW VERSION FOUND
    # -------------------------
    print("\n🔥 CHANGE DETECTED → NEW VERSION CREATED")

    print("\n📊 DIFF:")
    print(diff(previous["text"], new_text))

    store[celex] = {
        "text": new_text,
        "hash": new_hash,
        "version": previous["version"] + 1
    }

    print(f"\n✅ VERSION UPDATED → v{store[celex]['version']}")


# -------------------------
# RUN LOOP (SIMULATES PIPELINE)
# -------------------------
def run():
    print("\n🚀 STARTING CELEX DEBUG PIPELINE\n")

    for i in range(5):  # multiple runs to simulate evolution
        print(f"\n\n🔁 ITERATION {i+1}")
        for celex in CELEX_LIST:
            process_celex(celex)

        time.sleep(1)

    print("\n\n🎯 FINAL STATE:")
    for k, v in store.items():
        print(f"{k} → version {v['version']}")


if __name__ == "__main__":
    run()
