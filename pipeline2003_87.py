import hashlib
import difflib
import re
import time

# -------------------------
# IN-MEMORY VERSION STORE
# -------------------------
store = {}

# -------------------------
# SIMULATED CELEX STATE
# -------------------------
state = {}

CELEX_LIST = [
    "32003L0087",
    "32018R2066",
    "32018R2067"
]


# -------------------------
# 1. SIMULATED LEGAL DATA SOURCE
# (NO timestamps, only real content changes)
# -------------------------
def fetch_legislation_text(celex):
    """
    Simulates how EUR-Lex would evolve consolidated texts.
    Each CELEX evolves independently.
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

    text = versions[celex][min(i, len(versions[celex]) - 1)]

    state[celex]["i"] += 1

    return text


# -------------------------
# 2. NORMALIZATION (CRITICAL FIX)
# removes noise so false versions are not created
# -------------------------
def normalize(text):
    text = text.lower()
    text = re.sub(r"\s+", " ", text)      # collapse whitespace
    text = re.sub(r"[^\w\s]", "", text)   # remove punctuation
    return text.strip()


# -------------------------
# 3. HASH FUNCTION (BASED ON NORMALIZED TEXT)
# -------------------------
def hash_text(text):
    return hashlib.sha256(normalize(text).encode("utf-8")).hexdigest()


# -------------------------
# 4. DIFF ENGINE (LEGAL VIEW)
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
# 5. CORE VERSION PROCESSOR
# -------------------------
def process_celex(celex):
    print("\n" + "=" * 90)
    print(f"📘 CELEX: {celex}")
    print("=" * 90)

    raw_text = fetch_legislation_text(celex)

    print("\n📄 RAW TEXT:")
    print(raw_text)

    normalized = normalize(raw_text)
    new_hash = hash_text(raw_text)

    print("\n🧼 NORMALIZED TEXT:")
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
            "normalized": normalized,
            "hash": new_hash,
            "version": 1
        }
        return

    print("\n📄 PREVIOUS VERSION FOUND")
    print("OLD HASH:", previous["hash"])

    # -------------------------
    # NO CHANGE DETECTED
    # -------------------------
    if previous["hash"] == new_hash:
        print("\n⏭ NO LEGAL CHANGE DETECTED → SKIP VERSION")
        return

    # -------------------------
    # NEW VERSION DETECTED
    # -------------------------
    print("\n🔥 LEGAL CHANGE DETECTED → NEW VERSION")

    print("\n📊 DIFF:")
    print(diff(previous["text"], raw_text))

    new_version = previous["version"] + 1

    store[celex] = {
        "text": raw_text,
        "normalized": normalized,
        "hash": new_hash,
        "version": new_version
    }

    print(f"\n✅ VERSION UPDATED → v{new_version}")


# -------------------------
# 6. SIMULATION LOOP
# -------------------------
def run():
    print("\n🚀 EU LEGAL VERSION ENGINE v2 STARTED\n")

    for i in range(6):
        print("\n" + "#" * 50)
        print(f"🔁 ITERATION {i+1}")
        print("#" * 50)

        for celex in CELEX_LIST:
            process_celex(celex)

        time.sleep(1)

    print("\n\n🎯 FINAL STATE:\n")

    for celex, data in store.items():
        print(f"{celex} → version {data['version']}")


if __name__ == "__main__":
    run()
