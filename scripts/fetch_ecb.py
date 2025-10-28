# scripts/fetch_ecb.py (Ausschnitt)
CISS_SERIES = {
    "ciss_ea": "CISS.D.U2.Z0Z.4F.EC.SS.CI.IDX.3",  # Euro Area
    "ciss_us": "CISS.D.US.Z0Z.4F.EC.SS.CI.IDX.3",  # United States
}

def ecb_url(key: str, last_n=720):
    base = "https://data-api.ecb.europa.eu/service/data/CISS/"
    return f"{base}{key}?lastNObservations={last_n}&format=csvdata"

def fetch_series(alias, key, out_path, errors):
    import requests, csv, io
    url = ecb_url(key)
    r = requests.get(url, timeout=30)
    if r.status_code != 200 or not r.text or "<html" in r.text.lower():
        errors.append({"alias": alias, "err": f"HTTP {r.status_code}: {r.text[:200]}"})
        return False
    # Schreibe CSV RAW
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(r.text)
    return True

def main():
    os.makedirs("data/macro/ecb", exist_ok=True)
    errors, ok = [], []
    targets = [
        ("ciss_ea", "data/macro/ecb/ciss_ea_new.csv"),
        ("ciss_us", "data/macro/ecb/ciss_us_new.csv"),
    ]
    for alias, path in targets:
        key = CISS_SERIES[alias]
        if fetch_series(alias, key, path, errors):
            ok.append(alias)
    # Report
    rep = {"ok": ok, "errors": errors}
    os.makedirs("data/reports", exist_ok=True)
    with open("data/reports/ecb_errors.json", "w", encoding="utf-8") as f:
        import json; json.dump(rep, f, indent=2)
    # kleines Preview
    with open("data/reports/eu_checks/ecb_preview.txt", "w", encoding="utf-8") as f:
        f.write("file,last_date,last_value\n")
        for alias, path in targets:
            if os.path.exists(path):
                import pandas as pd
                df = pd.read_csv(path)
                if not df.empty:
                    f.write(f"{os.path.basename(path)},{df.iloc[-1]['TIME_PERIOD']},{df.iloc[-1]['OBS_VALUE']}\n")

if __name__ == "__main__":
    main()
