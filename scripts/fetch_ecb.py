# scripts/fetch_ecb.py
import os, csv, io, sys, json, datetime as dt
import requests
import pandas as pd

BASE = "https://data-api.ecb.europa.eu/service/data"
OUTDIR = "data/macro/ecb"
REPORT_DIR = "data/reports"
EU_CHECKS_DIR = os.path.join(REPORT_DIR, "eu_checks")
os.makedirs(OUTDIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(EU_CHECKS_DIR, exist_ok=True)

# --- Serien-Keys (korrekt)
SERIES = [
    # alias, dataset, series_key, outfile
    ("ciss_ea", "CISS", "CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX", "ciss_ea.csv"),
    ("ciss_us", "CISS", "CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX", "ciss_us.csv"),
    ("exr_usd_eur", "EXR", "EXR.D.USD.EUR.SP00.A", "exr_usd_eur.csv"),
]

PARAMS = {
    "lastNObservations": "720",   # ~3 Jahre daily
    "format": "csvdata"
}

def fetch_to_csv(dataset: str, series: str, outpath: str):
    url = f"{BASE}/{dataset}/{series}"
    r = requests.get(url, params=PARAMS, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:180]}")
    # Direkt speichern
    with open(outpath, "w", encoding="utf-8", newline="") as f:
        f.write(r.text)

def last_date_value(path: str):
    try:
        df = pd.read_csv(path)
        # Spaltennamen sind bei ECB csvdata meist: TIME_PERIOD, OBS_VALUE
        # Fallbacks absichern:
        time_col = "TIME_PERIOD" if "TIME_PERIOD" in df.columns else df.columns[0]
        val_col  = "OBS_VALUE"   if "OBS_VALUE"   in df.columns else df.columns[-1]
        if len(df) == 0:
            return "", ""
        return str(df[time_col].iloc[-1]), str(df[val_col].iloc[-1])
    except Exception:
        return "", ""

def main():
    ok = []
    errors = []
    for alias, dataset, key, fname in SERIES:
        outpath = os.path.join(OUTDIR, fname)
        try:
            fetch_to_csv(dataset, key, outpath)
            ok.append(alias)
        except Exception as e:
            errors.append({"alias": alias, "err": str(e)})

    print(f"ECB OK: {ok}")
    print("ECB ERRORS:", json.dumps(errors, indent=2) if errors else "[]")

    # Preview für EU-Checks
    preview_path = os.path.join(EU_CHECKS_DIR, "ecb_preview.txt")
    with open(preview_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file","last_date","last_value"])
        for _, _, _, fname in SERIES:
            p = os.path.join(OUTDIR, fname)
            d, v = last_date_value(p)
            w.writerow([f"data/macro/ecb/{fname}", d, v])

    # Fehlerreport (wie bei dir üblich)
    err_path = os.path.join(REPORT_DIR, "ecb_errors.json")
    with open(err_path, "w", encoding="utf-8") as f:
        json.dump({"errors": errors}, f, indent=2)

if __name__ == "__main__":
    sys.exit(main())
