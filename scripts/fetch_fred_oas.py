#!/usr/bin/env python3
import os, csv, json, requests

API = "https://api.stlouisfed.org/fred/series/observations"
KEY = os.getenv("FRED_API_KEY", "").strip()

# ICE BofA OAS – US & Euro (IG/HY)
SERIES = [
    ("BAMLCC0A0CM",  "US_IG"),   # US IG OAS (alias ok)
    ("BAMLH0A0HYM2","US_HY"),   # US HY OAS
    ("BEMLCC0A0CM",  "EU_IG"),   # Euro IG OAS
    ("BEMLH0A0HYM2","EU_HY"),   # Euro HY OAS
]

OUT = "data/processed/fred_oas.csv"
ERR = "data/reports/fred_errors.json"
os.makedirs(os.path.dirname(OUT), exist_ok=True)
os.makedirs(os.path.dirname(ERR), exist_ok=True)

def pull(series_id):
    params = {
        "series_id": series_id,
        "api_key": KEY,
        "file_type": "json",
        "observation_start": "1998-01-01",
    }
    r = requests.get(API, params=params, timeout=60)
    if r.status_code != 200:
        try:
            body = r.json()
        except Exception:
            body = {"text": r.text[:600]}
        return [], {"series_id": series_id, "status": r.status_code, "body": body}
    data = r.json().get("observations", [])
    rows = [(o["date"], o["value"]) for o in data if o.get("value") not in ("", ".")]
    return rows, None

def main():
    errors = []
    rows_all = []
    if not KEY:
        errors.append({"msg": "missing FRED_API_KEY"})
    else:
        for sid, bucket in SERIES:
            rows, err = pull(sid)
            if err:
                errors.append(err)
            else:
                region = "US" if bucket.startswith("US_") else "EU"
                for dt, val in rows:
                    rows_all.append([dt, sid, val, bucket, region])

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","series_id","value","bucket","region"])
        w.writerows(rows_all)

    with open(ERR, "w", encoding="utf-8") as f:
        json.dump({"file": OUT, "errors": errors}, f, indent=2)

    print(f"FRED OAS rows written: {len(rows_all)}")
    if errors:
        print("FRED errors:", json.dumps(errors, indent=2))

if __name__ == "__main__":
    main()
