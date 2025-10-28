#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time
from io import StringIO
import requests
import pandas as pd

BASE = "https://data-api.ecb.europa.eu/service/data"
TARGETS = {
    # USD/EUR Spot, daily, „lastNObservations“ funktioniert mit csvdata gut
    "exr_usd_eur": "EXR/D.USD.EUR.SP00.A",
    # CISS Euro Area (EA = U2)
    "ciss_ea":     "CISS/M.U2.Z0Z.F.W0.SS_CI.4F.B.F",
}

OUT_DIR = "data/macro/ecb"
ERR_JSON = "data/reports/ecb_errors.json"

def fetch_csv(path: str, last_n: int = 365) -> pd.DataFrame:
    url = f"{BASE}/{path}"
    params = {"lastNObservations": str(last_n), "format": "csvdata"}
    r = requests.get(url, params=params, timeout=40, headers={"User-Agent":"ecb-fetch/1.0"})
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text))

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(ERR_JSON), exist_ok=True)
    errors = []
    files = {}

    for alias, path in TARGETS.items():
        try:
            df = fetch_csv(path, 720)  # 2 Jahre
            outp = os.path.join(OUT_DIR, f"{alias}.csv")
            df.to_csv(outp, index=False)
            files[alias] = outp
            print(f"✅ ECB {alias}: {len(df)} rows -> {outp}")
        except Exception as e:
            errors.append({"alias": alias, "msg": f"ECB fetch failed for {path}", "err": str(e)})
            print(f"❌ ECB {alias} failed: {e}")

    rep = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "files": files, "errors": errors}
    with open(ERR_JSON, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)

if __name__ == "__main__":
    main()
