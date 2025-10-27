# scripts/build_cds_proxy.py
import os, re, json, pandas as pd
from datetime import datetime

WL             = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OAS_PATH       = "data/processed/fred_oas.csv"
ICE_PATH       = "data/processed/cds_eod.csv"          # optional
MAP_PATH       = "config/cds_bucket_map.csv"           # optional
OUT_WIDE       = "data/processed/cds_proxy.csv"
REPORT_PATH    = "data/reports/cds_proxy_report.json"

BUCKETS = {"US_IG_OAS","US_HY_OAS","EU_IG_OAS","EU_HY_OAS"}

EU_SUFFIX = (".DE",".PA",".AS",".MI",".BR",".VX",".MC",".BR"," .L",".IR",".PL",".NL",".BE",".F",".HM",".DU",".SG")  # heuristic

def read_watchlist(path):
    if not os.path.exists(path): return []
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.lower().startswith("symbol"): continue
            syms.append(s)
    return syms

def auto_bucket(sym):
    up = sym.upper()
    if up.endswith(EU_SUFFIX):   # grobe EU-Heuristik
        return "EU_IG_OAS"
    return "US_IG_OAS"

def load_map():
    if os.path.exists(MAP_PATH):
        df = pd.read_csv(MAP_PATH)
        m = {str(r["symbol"]).strip(): str(r["bucket"]).strip() for _,r in df.iterrows()}
        return {k:v for k,v in m.items() if v in BUCKETS}
    return {}

def latest_ice_anchor(sym, ice_df):
    """Greife den jüngsten 5Y-Single-Name-Wert, ticker==sym (falls vorhanden)."""
    if ice_df is None or ice_df.empty: return None, None
    sub = ice_df[(ice_df["type"]=="single_name") & (ice_df["ticker"].astype(str).str.upper()==sym.upper())]
    if sub.empty: return None, None
    sub = sub.dropna(subset=["spread_bps"])
    if sub.empty: return None, None
    sub = sub.sort_values("date")
    d = sub["date"].iloc[-1]
    v = float(sub["spread_bps"].iloc[-1])
    return d, v

def main():
    os.makedirs(os.path.dirname(OUT_WIDE), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    if not os.path.exists(OAS_PATH):
        print("missing", OAS_PATH); return 0
    oas = pd.read_csv(OAS_PATH)
    oas["date"] = pd.to_datetime(oas["date"]).dt.date

    wl  = read_watchlist(WL)
    bucket_override = load_map()
    ice = None
    if os.path.exists(ICE_PATH):
        ice = pd.read_csv(ICE_PATH)
        if not ice.empty:
            ice["date"] = pd.to_datetime(ice["date"]).dt.date

    # bauen: pro Symbol eine Serie (einfach der gewählte Bucket)
    cols = [c for c in oas.columns if c!="date"]
    if not cols:
        print("no OAS columns"); return 0

    wide = pd.DataFrame({"date": oas["date"]})
    report = {"symbols": len(wl), "mapped": 0, "anchored": 0, "unmapped": []}

    for sym in wl:
        bucket = bucket_override.get(sym) or auto_bucket(sym)
        if bucket not in oas.columns:
            report["unmapped"].append({"symbol": sym, "bucket": bucket})
            continue

        series = oas[["date", bucket]].rename(columns={bucket: sym}).copy()

        # optional: ICE-Anker – skaliere Level an letzterem Tag
        if ice is not None and not ice.empty:
            d, v = latest_ice_anchor(sym, ice)
            if v is not None:
                ref = series.loc[series["date"]==d, sym]
                if not ref.empty and ref.iloc[0] and pd.notna(ref.iloc[0]):
                    scale = v / float(ref.iloc[0])
                    series[sym] = series[sym] * scale
                    report["anchored"] += 1

        if wide.shape[0] != series.shape[0]:
            # sichere Merge auf Datum
            wide = wide.merge(series, on="date", how="outer")
        else:
            wide = pd.merge(wide, series, on="date", how="left")

        report["mapped"] += 1

    # Aufräumen
    wide.sort_values("date", inplace=True)
    wide.to_csv(OUT_WIDE, index=False)

    report["output_rows"] = int(wide.shape[0])
    report["output_cols"] = int(wide.shape[1]-1)
    with open(REPORT_PATH,"w",encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"wrote {OUT_WIDE} rows={wide.shape[0]} cols={wide.shape[1]-1}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
