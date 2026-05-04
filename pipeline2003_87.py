import hashlib
import difflib
import re

# -------------------------
# MEMORY STORE (SIMULATED DB)
# -------------------------
store = {}

# -------------------------
# INTERNAL STATE (SIMULATES EUR-Lex snapshots over time)
# -------------------------
state = {}

CELEX_LIST = [
    "32003L0087",
    "32018R2066",
    "32018R2067"
]


# -------------------------
# 1. STABLE LEGAL TEXT SOURCE
#    (NO timestamps, NO randomness)
# -------------------------
def fetch_legislation_text(celex):
    """
    Deterministic legal evolution per CELEX.
    Each CELEX progresses only when state increases.
    """

    versions = {
        "32003L0087": [
            "Article 1 defines scope of application.",
            "Article 1 defines scope. Article 5 introduces compliance obligations.",
            "Article 1 revised. Article 5 expanded enforcement rules.",
        ],
        "32018R2066": [
            "Monitoring and reporting of greenhouse gas emissions.",
            "Monitoring, reporting and verification framework established.",
            "Expanded MRV system with digital reporting obligations.",
        ],
        "32018R2067": [
            "Rules on verification of emissions data.",
            "Verification procedures updated and accredited verifiers introduced.",
            "Full verification regime aligned with EU ETS Phase IV.",
        ]
    }

    s = state.setdefault(celex, {"i": 0})
    i = s["i"]

    # IMPORTANT: clamp index (no artificial extension)
    text = versions[celex][min(i, len(versions[celex]) - 1)]

    return text


# -------------------------
# 2. NORMALIZATION (CRITICAL FIX)
# -------------------------
def normalize(text):
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


# -------------------------
# 3. HASH FUNCTION (ONLY NORMALIZED LEGAL CONTENT)
# -------------------------
def hash_text(text):
    return hashlib.sha256(normalize(text).encode("utf-8")).hexdigest()


# -------------------------
# 4. DIFF ENGINE
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
# 5. CORE PROCESSOR
# -------------------------
def process_celex(celex):
    print("\n" + "=" * 80)
    print(f"📘 CELEX: {celex}")
    print("=" * 80)

    raw_text = fetch_legislation_text(celex)

    print("\n📄 RAW TEXT:")
    print(raw_text)

    normalized = normalize(raw_text)
    new_hash = hash_text(raw_text)

    print("\n🧼 NORMALIZED:")
    print(normalized)

    print("\n🔐 HASH:")
    print(new_hash)

    previous = store.get(celex)

    # -------------------------
    # FIRST VERSION
    # -------------------------
    if not previous:
        print("\n🆕 FIRST VERSION CREATED")
        store[celex] = {
            "text": raw_text,
            "hash": new_hash,
            "version": 1
        }
        state[celex]["i"] += 1
        return

    print("\n📄 PREVIOUS VERSION FOUND")
    print("OLD HASH:", previous["hash"])

    # -------------------------
    # NO CHANGE DETECTED
    # -------------------------
    if previous["hash"] == new_hash:
        print("\n⏭ NO LEGAL CHANGE DETECTED → NO NEW VERSION")
        return

    # -------------------------
    # CHANGE DETECTED
    # -------------------------
    print("\n🔥 LEGAL CHANGE DETECTED → CREATING NEW VERSION")

    print("\n📊 DIFF:")
    print(diff(previous["text"], raw_text))

    new_version = previous["version"] + 1

    store[celex] = {
        "text": raw_text,
        "hash": new_hash,
        "version": new_version
    }

    print(f"\n✅ VERSION UPDATED → v{new_version}")

    # IMPORTANT: only advance state AFTER confirmed change
    state[celex]["i"] += 1


# -------------------------
# 6. RUN LOOP (REALISTIC BEHAVIOR TEST)
# -------------------------
def run():
    print("\n🚀 CELEX VERSION ENGINE v3 (STABLE DEBUG)\n")

    for i in range(6):
        print("\n" + "#" * 50)
        print(f"🔁 ITERATION {i+1}")
        print("#" * 50)

        for celex in CELEX_LIST:
            process_celex(celex)

    print("\n\n🎯 FINAL STATE:\n")

    for celex, data in store.items():
        print(f"{celex} → version {data['version']}")


if __name__ == "__main__":
    run()
