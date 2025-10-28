#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_cds_proxy.py  (ERSATZ)
Erzeugt einen CDS-Proxy je Symbol anhand von Region → (IG/HY)-OAS aus FRED.
Input:  data/processed/fred_oas.csv (mit Region/Bucket) + Watchlist
Output: data/processed/cds_proxy.csv
Report: data/reports/cds_proxy_report.json
"""

import os, csv, json, datetime as dt
from typing import Dict, Tuple

OUT_CSV = "data/processed/cds_proxy.csv"
REP_JSON = "data/reports/cds_proxy_report.json"
FRED_OAS_CSV = "data/processed/fred_oas.csv"

WATCHLIST = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
STRICT = os.getenv("CDS_STRICT", "true").lower() == "true"

os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(REP_JSON), exist_ok=True)

EU_SUFFIX = (
    ".DE",".PA",".AS",".MC",".BR",".MI",".ST",".CO",".SE",".FI",".DK",".IE",".AT",".BE",".PT",
    ".PL",".CZ",".HU",".NO",".CH",".GB",".NL",".HE",".OL",".WA",".LS",".VI",".BR",".BRX"
)

def detect_region(symbol: str) -> str:
    s = symbol.upper()
    return "EU" if s.endswith(EU_SUFFIX) else "US"

def read_watchlist(path: str):
    syms = []
    if not os.path.exists(path):
        return syms
    with open(path, encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            # CSV oder txt tolerant
            t = t.split(",")[0].strip()
            if t:
                syms.append(t)
    return syms

def latest_oas_by_region_bucket() -> Dict[Tuple[str,str], float]:
    """liest fred_oas.csv und nimmt den jüngsten Wert pro (region,bucket)"""
    latest = {}
    if not os.path.exists(FRED_OAS_CSV):
        return latest
    with open(FRED_OAS_CSV, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            region = (row.get("region") or "").upper()
            bucket = (row.get("bucket") or "").upper()
            try:
                val = float(row.get("value", ""))
            except Exception:
                continue
            key = (region, bucket)
            # Dateikomparator – wir überschreiben einfach, weil Datei schon sortiert war;
            # alternativ: man könnte per Datum vergleichen.
            latest[key] = val
    return latest

def main():
    report = {
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fred_oas_used": {},
        "symbols": [],
        "errors": []
    }

    symbols = read_watchlist(WATCHLIST)
    oas = latest_oas_by_region_bucket()

    # sichtbares Summary der verwendeten OAS
    for (reg,bkt), v in sorted(oas.items()):
        report["fred_oas_used"].setdefault(reg, {})[bkt] = v

    rows = []
    for sym in symbols:
        reg = detect_region(sym)
        # Strategie: IG bevorzugt; optionaler HY-Blend möglich
        ig = oas.get((reg, "IG"))
        hy = oas.get((reg, "HY"))

        if ig is None and hy is None:
            msg = f"no OAS for region={reg}"
            report["errors"].append({"symbol": sym, "msg": msg})
            if STRICT:
                proxy = None
            else:
                # weicher Fallback: nimm US.IG falls EU fehlt
                proxy = oas.get(("US", "IG"))
        else:
            # Einfach: IG als Proxy; optional (0.8*IG + 0.2*HY) möglich
            proxy = ig if ig is not None else hy

        rows.append({
            "symbol": sym,
            "region": reg+"_IG" if proxy is not None else reg+"_NA",
            "asof": dt.date.today().isoformat(),
            "proxy_spread": f"{proxy:.2f}" if proxy is not None else ""
        })
        report["symbols"].append({"symbol": sym, "proxy": f"{reg}_IG" if proxy is not None else "NA",
                                  "asof": dt.date.today().isoformat(),
                                  "proxy_spread": proxy})

    # schreiben
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol","region","asof","proxy_spread"])
        w.writeheader()
        w.writerows(rows)

    json.dump(report, open(REP_JSON, "w"), indent=2)

if __name__ == "__main__":
    main()
