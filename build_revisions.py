# scripts/build_revisions.py
import os, time, json, csv, requests
import pandas as pd, numpy as np

FINNHUB = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
WL = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OUT = "data/processed/revisions.csv.gz"
RPT = "data/reports/rev_errors.json"
os.makedirs("data/processed", exist_ok=True); os.makedirs("data/reports", exist_ok=True)

def get_estimates(sym):
    # Quarterly & yearly Estimates – wir brauchen Verlauf für Revisions-% (letzte 90 Tage)
    url = "https://finnhub.io/api/v1/stock/earnings-estimate"
    r = requests.get(url, params={"symbol": sym, "freq":"quarterly", "token": FINNHUB}, timeout=20)
    if not r.ok: return []
    return r.json() or []

def get_targets(sym):
    url = "https://finnhub.io/api/v1/stock/price-target"
    r = requests.get(url, params={"symbol": sym, "token": FINNHUB}, timeout=20)
    return r.json() if r.ok else {}

def rev_3m(estrows, field):
    # field in {"epsAvg","revenueAvg"} je nach API – wir bilden ∆ in %
    if not estrows: return np.nan
    df = pd.DataFrame(estrows)
    if field not in df.columns: return np.nan
    df["t"] = pd.to_datetime(df.get("period") or df.get("reportDate"), errors="coerce")
    df = df.sort_values("t").dropna(subset=[field])
    if len(df) < 2: return np.nan
    # 3M-Fenster: letzte vs. vor 90 Tagen (nächste verfügbare)
    latest = df.iloc[-1][field]
    past = df[df["t"] <= (pd.Timestamp.today() - pd.Timedelta(days=90))][field]
    if past.empty: return np.nan
    p = past.iloc[-1]
    if p in (0, None) or pd.isna(p): return np.nan
    return (latest - p) / abs(p)

def main():
    errs = []
    syms = [s.strip().split(",")[0] for s in open(WL) if s.strip() and not s.startswith("#")]
    rows=[]
    for s in syms:
        try:
            est = get_estimates(s); time.sleep(0.35)
            tgt = get_targets(s);  time.sleep(0.25)
            rows.append(dict(
                symbol=s,
                eps_rev_3m = rev_3m(est, "epsAvg"),
                sales_rev_3m = rev_3m(est, "revenueAvg"),
                pt_mean = tgt.get("targetMean"),
                pt_upside = (tgt.get("targetMean") or np.nan)  # Preis nicht verfügbar -> nur Rohwert
            ))
        except Exception as e:
            errs.append({"symbol":s, "err":str(e)})
    pd.DataFrame(rows).to_csv(OUT, index=False, compression="gzip")
    with open(RPT, "w", encoding="utf-8") as f: json.dump({"errors":errs, "rows":len(rows)}, f, indent=2)
    print(f"revisions.csv.gz rows: {len(rows)}  errors: {len(errs)}")

if __name__ == "__main__":
    main()
