import os, sys, json, time, requests, pandas as pd
from datetime import datetime

FRED = "https://api.stlouisfed.org/fred/series/observations"
API = os.getenv("FRED_API_KEY") or ""
OUT = sys.argv[1] if len(sys.argv)>1 else "data/processed/fred_oas.csv"

# Auswahl beliebter OAS-Serien (US & Euro) – frei erweiterbar
SERIES = {
    # US Investment Grade / High Yield (gesamt)
    "BAMLC0A0CM": "US_IG_OAS",      # ICE BofA US Corp OAS
    "BAMLH0A0HYM2": "US_HY_OAS",    # ICE BofA US HY OAS
    # Euro IG/HY
    "BEMLEIG": "EU_IG_OAS",         # ICE BofA Euro Corp OAS (FRED Code kann variieren)
    "BEMLEHY": "EU_HY_OAS",         # ICE BofA Euro HY OAS
}

def pull(series_id):
    p = {"series_id": series_id, "api_key": API, "file_type": "json", "observation_start": "1990-01-01"}
    r = requests.get(FRED, params=p, timeout=30); r.raise_for_status()
    j = r.json()
    rows = []
    for obs in j.get("observations", []):
        v = obs.get("value")
        try:
            val = float(v)
        except Exception:
            val = None
        rows.append({"date": obs["date"], "series": series_id, "value": val})
    return pd.DataFrame(rows)

def main():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    frames = []
    for sid in SERIES:
        try:
            frames.append(pull(sid))
            time.sleep(0.2)
        except Exception as e:
            print("FRED OAS fail", sid, e)
    if not frames:
        print("no OAS data"); return 0
    df = pd.concat(frames, ignore_index=True)
    # optional: breite Tabelle
    wide = df.pivot(index="date", columns="series", values="value").reset_index()
    wide.rename(columns=SERIES, inplace=True)
    wide.to_csv(OUT, index=False)
    print("wrote", OUT, len(wide))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
