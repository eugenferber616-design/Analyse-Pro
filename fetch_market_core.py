# scripts/fetch_market_core.py
import argparse, sys, os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

# Yahoo-Ticker Mapping
TICKERS = {
    "VIX":    "^VIX",
    "VIX3M":  "^VIX3M",
    "DXY":    "DX-Y.NYB",   # Alternativen: "DX=F" (Futures), falls DX-Y nicht verfügbar
    "USDJPY": "JPY=X",      # USD/JPY (invertiert), später in USDJPY umgerechnet
    "HYG":    "HYG",
    "LQD":    "LQD",
    "XLF":    "XLF",
    "SPY":    "SPY"
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2000)
    ap.add_argument("--out",  type=str, default="data/processed/market_core.csv")
    args = ap.parse_args()

    end   = datetime.utcnow()
    start = end - timedelta(days=args.days+30)

    out = {}
    for key, ysym in TICKERS.items():
        try:
            df = yf.download(ysym, start=start, end=end, progress=False, auto_adjust=False)
            if df.empty: 
                print(f"[mkt] warn: {key} ({ysym}) empty"); continue
            s = df["Adj Close"] if "Adj Close" in df.columns else df["Close"]
            s.name = key
            out[key] = s
            print(f"[mkt] ok: {key} rows={len(s)}")
        except Exception as e:
            print(f"[mkt] warn: {key} skipped ({e})")

    if not out:
        print("no market series", file=sys.stderr); return 1

    df = pd.concat(out.values(), axis=1).sort_index().ffill()

    # USDJPY: Yahoo liefert JPY pro USD -> wir wollen USDJPY (Preis in JPY je USD)
    if "USDJPY" in df.columns:
        # JPY=X ist USD/JPY (1 USD in JPY), passt bereits → nur sicherstellen, dass numeric ist
        df["USDJPY"] = pd.to_numeric(df["USDJPY"], errors="coerce")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index_label="date")
    print("wrote", args.out, "cols=", df.shape[1], "rows=", len(df))
    return 0

if __name__ == "__main__":
    sys.exit(main())
