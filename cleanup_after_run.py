#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Aufräumen & Komprimieren
- Komprimiert große CSVs zu .csv.gz
- Entfernt unnötige Options-Details (wir behalten nur Summary/Totals)
- Lässt kleine Previews/QA-Dateien unberührt
"""

import os, gzip, shutil, json

DATA = "data/processed"
REP  = "data/reports"
os.makedirs(REP, exist_ok=True)

TO_COMPRESS = [
    "cot_10y.csv",
    "cot_latest_raw.csv",
    "cot.csv",
    "fundamentals_core.csv",
    "earnings_results.csv",
    "fred_oas.csv",
]
REMOVE_IF_EXISTS = [
    "options_oi_by_expiry.csv",
    # Falls du die Einzel-Chain-Dateien speicherst, auch löschen:
    # "options_chains_raw.csv"
]

def compress(path):
    gz = path + ".gz"
    with open(path, "rb") as f_in, gzip.open(gz, "wb", compresslevel=5) as f_out:
        shutil.copyfileobj(f_in, f_out)
    return gz

def main():
    rep = {"compressed":[], "removed":[]}
    for name in TO_COMPRESS:
        p = os.path.join(DATA, name)
        if os.path.exists(p) and os.path.getsize(p) > 0:
            gz = compress(p)
            rep["compressed"].append({"src":p, "dst":gz})
    for name in REMOVE_IF_EXISTS:
        p = os.path.join(DATA, name)
        if os.path.exists(p):
            try:
                os.remove(p)
                rep["removed"].append(p)
            except Exception:
                pass
    with open(os.path.join(REP,"cleanup_report.json"),"w",encoding="utf-8") as f:
        json.dump(rep, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()
