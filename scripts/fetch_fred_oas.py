# scripts/fetch_fred_oas.py
import os, time, json, requests, pandas as pd

FRED = "https://api.stlouisfed.org/fred/series/observations"
API  = os.getenv("FRED_API_KEY") or ""
OUT  = "data/processed/fred_oas.csv"

# Beliebig erweiterbar – das sind solide Defaults (US & Euro)
SERIES = {
  "BAMLC0A0CM":   "US_IG_OAS",   # ICE BofA US Corporate OAS
  "BAMLH0A0HYM2": "US_HY_OAS",   # ICE BofA US High Yield OAS
  # Euro (falls ein Code leer zurückkommt, passt er die Tage nicht—einfach ergänzen/ändern)
  "BEMLEIG":      "EU_IG_OAS",   # ICE BofA Euro Corporate OAS
  "BEMLEHY":      "EU_HY_OAS",   # ICE BofA Euro High Yield OAS
}

def pull(series_id):
    p = {"series_id": series_id, "api_key": API, "file_type": "json", "observation_start": "1999-01-01"}
    r = requests.get(FRED, params=p, timeout=30)
    r.raise_for_status()
    j = r.json()
    rows = []
    for obs in j.get("observations", []):
        v = obs.get("value")
        try:  rows.append({"date": obs["date"], "series": series_id, "value": float(v)})
        except: rows.append({"date": obs["date"], "series": series_id, "value": None})
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
    wide = df.pivot(index="date", columns="series", values="value").reset_index().rename(columns=SERIES)
    wide.to_csv(OUT, index=False)
    print("wrote", OUT, len(wide))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
