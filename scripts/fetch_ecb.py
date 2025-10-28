# scripts/fetch_ecb.py
import os, sys, csv, time, json
import urllib.request as ureq

BASE = "https://data-api.ecb.europa.eu/service/data"
OUTDIR = "data/macro/ecb"
os.makedirs(OUTDIR, exist_ok=True)

def fetch(url, out_path):
    try:
        with ureq.urlopen(url, timeout=40) as r, open(out_path, "wb") as f:
            f.write(r.read())
        return True, None
    except Exception as e:
        return False, str(e)

errors = []

# 1) USD/EUR reference rate (works)
exr_url = f"{BASE}/EXR/D.USD.EUR.SP00.A?lastNObservations=720&format=csvdata"
ok, err = fetch(exr_url, os.path.join(OUTDIR, "exr_usd_eur.csv"))
if not ok:
    errors.append({"alias": "exr_usd_eur", "err": err})

# 2) CISS Euro Area (★ fixed URL: remove duplicate 'CISS.' in the key path)
ciss_ea_url = f"{BASE}/CISS/D.U2.Z0Z.4F.EC.SS.CI.IDX?lastNObservations=720&format=csvdata"
ok, err = fetch(ciss_ea_url, os.path.join(OUTDIR, "ciss_ea.csv"))
if not ok:
    errors.append({"alias": "ciss_ea", "err": err})

# (optional) If you want the U.S. CISS from ECB (if available in this dataflow),
# try US instead of U2. If not available, skip gracefully.
ciss_us_url = f"{BASE}/CISS/D.US.Z0Z.4F.EC.SS.CI.IDX?lastNObservations=720&format=csvdata"
ok, err = fetch(ciss_us_url, os.path.join(OUTDIR, "ciss_us.csv"))
if not ok:
    # Don’t fail the batch if US is not provided in CISS
    errors.append({"alias": "ciss_us", "err": err})

# Write a tiny report so the workflow can show details
rep = {"errors": errors}
os.makedirs("data/reports", exist_ok=True)
with open("data/reports/ecb_errors.json", "w", encoding="utf-8") as f:
    json.dump(rep, f, indent=2)

print("ECB OK:", ["exr_usd_eur"])
print("ECB ERRORS:", json.dumps(errors, indent=2))
