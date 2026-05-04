import time
import hashlib
import difflib

# -------------------------
# MEMORY STORE (SIMULATES DB)
# -------------------------
store = {}

# -------------------------
# PER-CELEX VERSION STATE
# -------------------------
state = {}

CELEX_LIST = [
    "32003L0087",
    "32018R2066",
    "32018R2067"
]


# -------------------------
# DETERMINISTIC VERSION GENERATOR
# (NO RANDOMNESS = NO "STOP AT VERSION 2")
# -------------------------
def fetch_legislation_text(celex):
    """
    Each CELEX evolves independently and deterministically.
    This guarantees infinite version progression.
    """

    s = state.setdefault(celex, {"i": 0})

    step = s["i"]

    evolution_map = [
        "Article 1 defines scope.",
        "Article 1 defines scope. Article 5 introduces compliance rules.",
        "Article 1 revised. Article 5 expanded enforcement obligations.",
        "Full consolidation includes penalty regime and reporting duties.",
        "Major amendment introduces EU-wide harmonisation framework.",
        "Complete recast with digital reporting and enforcement layer."
    ]

    # deterministic progression (NO RANDOMNESS)
    index = min(step, len(evolution_map) - 1)

    text = evolution_map[index]

    # increment state
    state[celex]["i"] += 1

    return f"{celex} | {text} | ts={int(time.time())}"


# -------------------------
# HASH FUNCTION
# -------------------------
def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# -------------------------
# DIFF FUNCTION (GIT STYLE)
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
# MAIN PROCESSOR
# -------------------------
def process_celex(celex):
    print("\n" + "="*90)
    print(f"📘 CELEX: {celex}")
    print("="*90)

    new_text = fetch_legislation_text(celex)
    new_hash = hash_text(new_text)

    print("\n🧾 FETCHED TEXT:")
    print(new_text)

    print("\n🔐 HASH:")
    print(new_hash)

    previous = store.get(celex)

    # -------------------------
    # FIRST VERSION
    # -------------------------
    if not previous:
        print("\n🆕 FIRST VERSION CREATED")
        store[celex] = {
            "text": new_text,
            "hash": new_hash,
            "version": 1
        }
        print("VERSION = 1")
        return

    print("\n📄 PREVIOUS VERSION FOUND")
    print("OLD HASH:", previous["hash"])

    # -------------------------
    # SAME VERSION CHECK
    # -------------------------
    if previous["hash"] == new_hash:
        print("\n⏭ NO CHANGE DETECTED → SKIP VERSION")
        return

    # -------------------------
    # NEW VERSION
    # -------------------------
    print("\n🔥 CHANGE DETECTED → NEW VERSION CREATED")

    print("\n📊 DIFF:")
    print(diff(previous["text"], new_text))

    new_version = previous["version"] + 1

    store[celex] = {
        "text": new_text,
        "hash": new_hash,
        "version": new_version
    }

    print(f"\n✅ VERSION UPDATED → v{new_version}")


# -------------------------
# RUN SIMULATION
# -------------------------
def run():
    print("\n🚀 STARTING CELEX DEBUG VERSION ENGINE\n")

    # multiple iterations to simulate real pipeline over time
    for i in range(6):
        print("\n" + "#"*30)
        print(f"🔁 GLOBAL ITERATION {i+1}")
        print("#"*30)

        for celex in CELEX_LIST:
            process_celex(celex)

        time.sleep(1)

    # -------------------------
    # FINAL STATE
    # -------------------------
    print("\n\n🎯 FINAL VERSION STATE:\n")

    for celex, data in store.items():
        print(f"{celex} → version {data['version']}")


if __name__ == "__main__":
    run()
