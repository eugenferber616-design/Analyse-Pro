# scripts/fetch_stooq.py
import argparse, os, sys, csv, time
import pandas as pd
import yfinance as yf  # nutzen wir als Fallback-Downloader, aber nur für .DE

parser = argparse.ArgumentParser()
parser.add_argument("--watchlist", required=True)
parser.add_argument("--days", type=int, default=365)
args = parser.parse_args()

OUTDIR = "data/market/stooq"
PROCESSED = "data/processed/fx_quotes.csv"  # kleine Vorschau/Check
os.makedirs(OUTDIR, exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# XETRA only
def read_watchlist(p):
    syms = []
    with open(p, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): 
                continue
            if s.endswith(".DE"):  # NUR Xetra
                syms.append(s)
    return sorted(set(syms))

symbols = read_watchlist(args.watchlist)
print(f"Stooq/Xetra: {len(symbols)} .DE Symbole")

# Laden via yfinance (Stooq-Mirror) – für .DE funktioniert das stabil
ok = 0
for s in symbols:
    try:
        df = yf.download(s, period=f"{args.days}d", interval="1d", auto_adjust=False, progress=False)
        if df is None or df.empty:
            print(f"ERR {s}: no data")
            continue
        out_p = os.path.join(OUTDIR, f"{s.replace('.','_')}.csv")
        df.to_csv(out_p)
        ok += 1
    except Exception as e:
        print(f"ERR {s}: {e}")
    time.sleep(0.2)

# winzige Zusammenfassung (nur Close)
rows = []
for s in symbols:
    p = os.path.join(OUTDIR, f"{s.replace('.','_')}.csv")
    if not os.path.exists(p):
        continue
    try:
        df = pd.read_csv(p)
        if not df.empty:
            rows.append([s, df["Date"].iloc[-1], float(df["Close"].iloc[-1])])
    except Exception:
        pass

with open(PROCESSED, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["symbol","last_date","last_close"])
    w.writerows(rows)

print(f"Downloaded .DE files: {ok} / {len(symbols)}")
