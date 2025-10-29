# scripts/build_cds_proxy_v2.py
import csv, json, pandas as pd, pathlib as pl

IN_FRED = pl.Path("data/processed/fred_oas.csv")
WATCH   = pl.Path("watchlists/mylist.txt")
OUT     = pl.Path("data/processed/cds_proxy.csv")
REPORT  = pl.Path("data/reports/cds_proxy_report.json")
OUT.parent.mkdir(parents=True, exist_ok=True); REPORT.parent.mkdir(parents=True, exist_ok=True)

# Buckets: series_id -> bucket
BUCKETS = {
    "BAMLC0A0CM":   "US_IG",
    "BAMLH0A0HYM2":"US_HY",
    "BAMLHE00EHYIOAS":"EU_HY",   # Euro HY
    # optional: falls du ein EU_IG OAS findest, hier ergänzen:
    # "XXXXXX":"EU_IG",
}

def read_watchlist(p):
    syms=[]
    for line in p.read_text(encoding="utf-8").splitlines():
        s=line.strip()
        if not s or s.startswith("#"): continue
        syms.append(s)
    return syms

def last_oas_by_bucket(df):
    df=df.sort_values("date")
    last = {}
    for sid,grp in df.groupby("series_id"):
        b=BUCKETS.get(sid)
        if not b: continue
        val = pd.to_numeric(grp["value"], errors="coerce").dropna().iloc[-1]
        last[b]=float(val)
    return last

def region_for_symbol(s):
    # .DE → EU; sonst US (einfach/robust)
    return "EU" if s.endswith(".DE") else "US"

def proxy_bucket(region):
    return f"{region}_IG"  # konservativ IG; du kannst hier Regeln verfeinern

def main():
    w = read_watchlist(WATCH)
    df = pd.read_csv(IN_FRED)
    buckets = last_oas_by_bucket(df)

    rows=[]
    for s in w:
        reg = region_for_symbol(s)
        bkt = proxy_bucket(reg)
        val = buckets.get(bkt)
        # Fallbacks
        if val is None and reg=="EU": val = buckets.get("US_IG")
        if val is None: val = buckets.get("US_IG")
        rows.append((s,f"{reg}_IG", val))

    pd.DataFrame(rows, columns=["symbol","region","proxy_spread"]).to_csv(OUT, index=False)
    rep = {"ts": pd.Timestamp.utcnow().isoformat()+"Z", "rows": len(rows), "fred_oas_used": buckets, "errors": []}
    REPORT.write_text(json.dumps(rep, indent=2))
    print(json.dumps(rep, indent=2))

if __name__=="__main__": main()
