import requests
import time
from datetime import datetime

# =========================
# CONFIG
# =========================

OPEN_SKY_URL = "https://opensky-network.org/api/states/all"

# Optional Supabase (leave empty if not using storage)
SUPABASE_URL = "https://eeuedfwjnaupakapmvwi.supabase.co"
SUPABASE_KEY = "sb_publishable_A-2hEZ3DJCRZqL7rOxi7Gw_oN2lK4Cn"

USE_DB = bool(SUPABASE_URL and SUPABASE_KEY)

if USE_DB:
    from supabase import create_client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# SIMPLE AIRPORT MAP
# =========================

AIRPORTS = {
    "LCA": "Larnaca",
    "PFO": "Paphos",
    "HFA": "Haifa",
    "ETM": "Ramon",
    "ATH": "Athens",
    "RHO": "Rhodes",
    "HER": "Heraklion"
}

# =========================
# ICAO24 → REGISTRATION (fallback map)
# You can extend this over time
# =========================

REG_DB = {}

def resolve_registration(icao24):
    return REG_DB.get(icao24, "UNKNOWN")

# =========================
# ROUTE INFERENCE (heuristic)
# =========================

def guess_route(callsign):
    c = callsign.upper()

    if "LCA" in c:
        return "Haifa → Larnaca"
    if "PFO" in c:
        return "Haifa → Paphos"
    if "ETM" in c:
        return "Haifa → Eilat"
    if "ATH" in c:
        return "Haifa → Athens"
    if "RHO" in c:
        return "Haifa → Rhodes"
    if "HER" in c:
        return "Haifa → Heraklion"

    return "Haifa ↔ Unknown"

# =========================
# FETCH LIVE DATA
# =========================

def fetch_air_haifa_flights():
    try:
        r = requests.get(OPEN_SKY_URL, timeout=20)
        data = r.json()
    except Exception as e:
        print("Fetch error:", e)
        return []

    results = []

    for s in data.get("states", []):
        icao24 = s[0]
        callsign = (s[1] or "").strip()

        # AIR HAIFA FILTER (E2 prefix)
        if not callsign.startswith("E2"):
            continue

        results.append({
            "time": datetime.utcnow().isoformat(),
            "flight": callsign,
            "route": guess_route(callsign),
            "registration": resolve_registration(icao24),
            "icao24": icao24
        })

    return results

# =========================
# STORE (optional)
# =========================

def store(records):
    if not USE_DB:
        return

    for r in records:
        supabase.table("air_haifa_flights").insert(r).execute()

# =========================
# RUN LOOP
# =========================

def run():
    print("✈️ Air Haifa Tracker Started")

    while True:
        flights = fetch_air_haifa_flights()

        if flights:
            print(f"\n{len(flights)} flights found:")

            for f in flights:
                print(f"{f['time']} | {f['flight']} | {f['route']} | {f['registration']}")

            store(flights)
        else:
            print("No Air Haifa flights detected")

        time.sleep(60)

# =========================
# START
# =========================

if __name__ == "__main__":
    run()
