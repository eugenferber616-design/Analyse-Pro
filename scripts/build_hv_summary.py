# scripts/build_hv_summary.py
# Berechnet HV20/HV60 aus LOKALEN Preisdaten (data/prices/{SUBDIR}/{SYM}.csv).
# Output: data/processed/hv_summary.csv.gz

import os
import sys
import csv
import gzip
import time
import math
import json
import argparse
import concurrent.futures
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict

# ──────────────────────────────────────────────────────────────────────────────
# Konfiguration & Pfade
# ──────────────────────────────────────────────────────────────────────────────

BASE_PRICES = os.path.join("data", "prices")
BASE_PROCESSED = os.path.join("data", "processed")
BASE_REPORTS = os.path.join("data", "reports")

# ──────────────────────────────────────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────────────────────────────────────

def clean_symbol(s: str) -> str:
    """Bereinigt Symbol von Kommentaren etc."""
    s = str(s).strip().upper()
    if "#" in s: s = s.split("#", 1)[0].strip()
    if "//" in s: s = s.split("//", 1)[0].strip()
    if "," in s: s = s.split(",", 1)[0].strip()
    return s

def read_watchlist(path: str) -> List[str]:
    """Liest Watchlist (TXT oder CSV) ein."""
    if not path or not os.path.exists(path):
        return []
    
    out = []
    # CSV-Support
    if path.endswith(".csv"):
        try:
            df = pd.read_csv(path)
            col = "symbol" if "symbol" in df.columns else df.columns[0]
            out = df[col].dropna().apply(clean_symbol).tolist()
        except:
            pass
    else:
        # TXT-Support
        with open(path, encoding="utf-8") as f:
            for line in f:
                t = clean_symbol(line)
                if t and not t.lower().startswith("symbol"):
                    out.append(t)
                    
    return sorted(list(set(out)))  # Unique & Sorted

def get_local_price_file(symbol: str) -> Optional[str]:
    """
    Findet den Pfad zur CSV Datei in der neuen Ordnerstruktur.
    data/prices/A/AAPL.csv
    data/prices/#/1COV.csv
    """
    if not symbol: return None
    
    first_char = symbol[0].upper()
    if not first_char.isalpha():
        first_char = "#"
        
    path = os.path.join(BASE_PRICES, first_char, f"{symbol}.csv")
    if os.path.exists(path):
        return path
    return None

def calc_volatility(prices: pd.Series, window: int) -> Optional[float]:
    """Berechnet HV (annualisiert) für das gegebene Fenster."""
    if len(prices) < window + 1:
        return None
    
    # Log Returns sind präziser für Volatilität
    import numpy as np
    log_rets = np.log(prices / prices.shift(1)).dropna()
    
    if len(log_rets) < window:
        return None
        
    # Standardabweichung der letzten N Tage * Wurzel(252) für p.a.
    std_dev = log_rets.tail(window).std(ddof=1)
    return float(std_dev * math.sqrt(252))

# ──────────────────────────────────────────────────────────────────────────────
# Core Logic
# ──────────────────────────────────────────────────────────────────────────────

def process_symbol(symbol: str) -> Dict:
    res = {
        "symbol": symbol,
        "hv20": None,
        "hv60": None,
        "asof": None,
        "ok": False,
        "err": None
    }

    path = get_local_price_file(symbol)
    if not path:
        res["err"] = "File not found"
        return res

    try:
        # Lade nur Date und Close für Performance
        df = pd.read_csv(path, usecols=lambda c: c.lower() in ["date", "close", "adj close"])
        
        # Spalten normalisieren
        df.columns = [c.lower().replace("adj close", "close") for c in df.columns]
        
        if "date" not in df.columns or "close" not in df.columns:
            res["err"] = "Missing columns"
            return res

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date").dropna()
        
        if df.empty:
            res["err"] = "Empty data"
            return res

        # Berechne HV
        res["hv20"] = calc_volatility(df["close"], 20)
        res["hv60"] = calc_volatility(df["close"], 60)
        res["asof"] = df["date"].iloc[-1].strftime("%Y-%m-%d")
        res["ok"] = True

    except Exception as e:
        res["err"] = str(e)

    return res

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="Pfad zur Watchlist")
    ap.add_argument("--out", default="data/processed/hv_summary.csv.gz")
    ap.add_argument("--max_workers", type=int, default=4)
    # Diese Argumente sind für Kompatibilität mit dem alten Aufruf noch da, aber inaktiv:
    ap.add_argument("--days", type=int, default=252) 
    ap.add_argument("--yf-fallback", action="store_true") 
    
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(BASE_REPORTS, exist_ok=True)

    symbols = read_watchlist(args.watchlist)
    print(f"Calculating HV for {len(symbols)} symbols from local files...")

    rows = []
    errors = []
    t0 = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        future_to_sym = {pool.submit(process_symbol, s): s for s in symbols}
        
        for future in concurrent.futures.as_completed(future_to_sym):
            r = future.result()
            if r["ok"]:
                rows.append(r)
            else:
                errors.append(r)

    # Schreiben
    tmp_out = args.out + ".tmp"
    with gzip.open(tmp_out, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "hv20", "hv60", "asof"])
        for r in rows:
            h20 = f"{r['hv20']:.6f}" if r["hv20"] is not None else ""
            h60 = f"{r['hv60']:.6f}" if r["hv60"] is not None else ""
            writer.writerow([r["symbol"], h20, h60, r["asof"]])

    os.replace(tmp_out, args.out)

    # Statistik Report
    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "total_symbols": len(symbols),
        "success": len(rows),
        "failed": len(errors),
        "duration_sec": round(time.time() - t0, 2),
        "output": args.out,
        "sample_errors": [e["symbol"] + ": " + str(e["err"]) for e in errors[:10]]
    }
    
    rep_path = os.path.join(BASE_REPORTS, "hv_report.json")
    with open(rep_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Done. Wrote {len(rows)} rows to {args.out}")
    print(f"Errors: {len(errors)} (See {rep_path})")

if __name__ == "__main__":
    sys.exit(main())
