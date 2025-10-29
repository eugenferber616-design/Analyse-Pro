# scripts/build_cds_proxy_v2.py
import os, csv, json
from collections import defaultdict

FRED_OAS = "data/processed/fred_oas.csv"
FUND_CSV = "data/processed/fundamentals_core.csv"
OUT_CSV  = "data/processed/cds_proxy.csv"
REPORT   = "data/reports/cds_proxy_report.json"

def load_fred_oas(path):
    # date,series_id,value,bucket,region
    oas = defaultdict(list)
    if not os.path.exists(path): return {}
    with open(path, encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            try:
                val = float(r["value"])
            except:
                continue
            oas[(r["bucket"], r["region"])].append(val)
    # take latest value per bucket/region
    last = {}
    for k, arr in oas.items():
        if arr: last[k] = arr[-1]
    return last

def region_for_symbol(sym:str)->str:
    return "EU" if sym.endswith(".DE") else "US"

def load_symbols():
    # wir nehmen die Symbole aus FUND, weil dort alle normalisiert sind
    syms=[]
    if os.path.exists(FUND_CSV):
        with open(FUND_CSV, encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for r in rd:
                s=r["symbol"].strip()
                if s and s!="symbol_proxy": syms.append(s)
    return list(dict.fromkeys(syms))

def main():
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    fred = load_fred_oas(FRED_OAS)

    us_ig = fred.get(("IG","US"))
    us_hy = fred.get(("HY","US"))
    eu_ig = fred.get(("IG","EU"))  # meist None (FRED hat keine EU IG Serie)
    eu_hy = fred.get(("HY","EU"))  # vorhanden: BAMLHE00EHYIOAS

    rows=[]
    for sym in load_symbols():
        reg = region_for_symbol(sym)

        if reg=="US":
            base = us_ig or us_hy or 1.0
        else:  # EU
            # 1) echte EU HY wenn vorhanden (bessere Risikonähe als IG-Proxy)
            if eu_hy:
                base = eu_hy
            # 2) falls irgendwann EU-IG verfügbar wäre: nimm die
            elif eu_ig:
                base = eu_ig
            # 3) sonst Proxy: US-IG * 0.9 (leichte Absenkung ggü. US)
            elif us_ig:
                base = round(us_ig * 0.90, 2)
            else:
                base = 1.0

        rows.append({"symbol": sym, "region": reg+"_IG", "proxy_spread": base})

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=["symbol","region","proxy_spread"])
        w.writeheader()
        for r in rows: w.writerow(r)

    rep = {
        "ts": __import__("datetime").datetime.utcnow().isoformat()+"Z",
        "rows": len(rows),
        "fred_oas_used": {
            "US_IG": us_ig, "US_HY": us_hy, "EU_IG": eu_ig, "EU_HY": eu_hy
        },
        "errors": [],
        "preview": OUT_CSV
    }
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    json.dump(rep, open(REPORT,"w"), indent=2)
    print(json.dumps(rep, indent=2))

if __name__ == "__main__":
    main()
