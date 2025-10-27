#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a per-symbol CDS proxy spread using public OAS indices (FRED),
plus simple symbol-specific scaling (leverage + historical volatility).

Inputs (if present)
- data/processed/fred_oas.csv           # columns: series_id,date,value
- data/processed/fundamentals_core.csv  # should include debt_to_equity (or similar)
- data/processed/options_oi_summary.csv # includes hv20/hv60 from yfinance

Watchlist
- env WATCHLIST_STOCKS  (txt or csv with 'symbol' column), default: watchlists/mylist.txt

Output
- data/processed/cds_proxy.csv          # symbol, proxy, asof, proxy_spread   (percent units, e.g. 0.89)
- data/reports/cds_proxy_report.json
"""

import os
import json
from datetime import date, datetime
from typing import List, Dict, Optional

import math
import numpy as np
import pandas as pd

# ---------- small utils ----------

def ensure_dirs() -> None:
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

def read_watchlist(path: str) -> List[str]:
    """Reads txt (one symbol per line) or csv with 'symbol' column."""
    if not os.path.exists(path):
        return []
    if path.lower().endswith(".csv"):
        try:
            df = pd.read_csv(path)
            if "symbol" in df.columns:
                syms = [str(s).strip() for s in df["symbol"].dropna().tolist()]
                return [s for s in syms if s]
        except Exception:
            pass
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and s.lower() != "symbol":
                out.append(s)
    return out

def _to_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None

# ---------- load inputs ----------

FRED_CSV = "data/processed/fred_oas.csv"
FUN_CSV  = "data/processed/fundamentals_core.csv"
OI_SUM   = "data/processed/options_oi_summary.csv"

def load_latest_oas() -> Dict[str, float]:
    """
    Return latest OAS levels for the key buckets as *percent* values
    (e.g. 0.77 = 77bp). If your fred_oas.csv stores decimals (0.0077),
    simply multiply by 100 below.
    """
    ids = {
        "US_IG":  "BAMLC0A0CM",     # ICE BofA US Corporate IG OAS
        "US_HY":  "BAMLH0A0HYM2",   # ICE BofA US High Yield OAS
        "EU_IG":  "BEMLEIG",        # ICE BofA Euro IG OAS
        "EU_HY":  "BEMLEHY",        # ICE BofA Euro HY OAS
    }
    out = {k: np.nan for k in ids}
    if not os.path.exists(FRED_CSV):
        return out

    df = pd.read_csv(FRED_CSV)
    # normalize column names
    lc = {c.lower(): c for c in df.columns}
    sid = lc.get("series_id", "series_id")
    dat = lc.get("date", "date")
    val = lc.get("value", "value")

    for k, series in ids.items():
        s = df[df[sid] == series].copy()
        if s.empty:
            continue
        s[dat] = pd.to_datetime(s[dat], errors="coerce")
        s.sort_values(dat, inplace=True)
        v = _to_float(s[val].dropna().iloc[-1]) if not s[val].dropna().empty else None
        if v is None:
            continue
        # If the file stores decimals (0.0077), convert to percent:
        # Heuristic: treat anything < 0.2 as decimal and multiply by 100.
        out[k] = float(v * 100.0) if v < 0.2 else float(v)
    return out

def load_fundamentals() -> pd.DataFrame:
    if not os.path.exists(FUN_CSV):
        return pd.DataFrame()
    df = pd.read_csv(FUN_CSV)
    # unify symbol column
    if "symbol" not in df.columns:
        # try a likely alternative
        for c in df.columns:
            if c.lower() == "ticker":
                df = df.rename(columns={c: "symbol"})
                break
    return df

def load_options_summary() -> pd.DataFrame:
    if not os.path.exists(OI_SUM):
        return pd.DataFrame()
    df = pd.read_csv(OI_SUM)
    return df

# ---------- risk features per symbol ----------

def hv60_for(symbol: str, oi_df: pd.DataFrame) -> Optional[float]:
    if oi_df is None or oi_df.empty:
        return None
    d = oi_df[oi_df["symbol"] == symbol]
    if d.empty or "hv60" not in d.columns:
        return None
    v = _to_float(d["hv60"].dropna().iloc[0]) if not d["hv60"].dropna().empty else None
    return v

def leverage_for(symbol: str, fun_df: pd.DataFrame) -> Optional[float]:
    if fun_df is None or fun_df.empty:
        return None
    d = fun_df[fun_df["symbol"] == symbol]
    if d.empty:
        return None
    for c in ["debt_to_equity", "totalDebt_to_marketCap", "debt_to_assets", "de_ratio", "debt_to_equity_ttm"]:
        if c in d.columns and d[c].notna().any():
            return _to_float(d[c].dropna().iloc[0])
    return None

def region_for(symbol: str) -> str:
    """
    Simple heuristic: US for common US tickers; for anything else, let it be US as default.
    If du später ein Mapping pflegen willst, lege eine config/cds_proxy_map.csv an.
    """
    return "US"

def credit_bucket(leverage: Optional[float], hv60: Optional[float]) -> str:
    """
    Coarse credit bucket from leverage & vol.
    We compute a small risk score; threshold determines IG vs HY.
    """
    L = 0.0 if leverage is None or np.isnan(leverage) else float(leverage)
    H = 0.0 if hv60     is None or np.isnan(hv60)     else float(hv60)
    # clip extreme outliers
    L = float(np.clip(L, 0.0, 5.0))
    H = float(np.clip(H, 0.0, 1.0))   # hv60 in ~0..1 (100% annualized)
    score = L + 8.0 * H               # HV stärker gewichten
    return "HY" if score >= 3.0 else "IG"

def scale_multiplier(leverage: Optional[float], hv60: Optional[float]) -> float:
    """
    Smooth multiplicative scaling: 1.0 +/- something bounded by tanh.
    Keeps values stable und robust.
    """
    L = 0.0 if leverage is None or np.isnan(leverage) else float(leverage)
    H = 0.0 if hv60     is None or np.isnan(hv60)     else float(hv60)

    # Normalize ranges: leverage ~[0..2+] -> tanh(L) ∈ [0..~0.96]
    # HV ~[0..0.6] typical -> tanh(H*2) for a bit more sensitivity
    mul_L = 1.0 + 0.25 * math.tanh(L)
    mul_H = 1.0 + 0.35 * math.tanh(2.0 * H)
    return float(mul_L * mul_H)

# ---------- proxy assembly ----------

def proxy_for_symbol(symbol: str,
                     fred_oas: Dict[str, float],
                     fun_df: pd.DataFrame,
                     oi_df: pd.DataFrame) -> Dict[str, object]:
    region = region_for(symbol)
    L = leverage_for(symbol, fun_df)
    H = hv60_for(symbol, oi_df)
    bucket = credit_bucket(L, H)

    # base OAS (percent units)
    key = f"{region}_{bucket}"
    base = fred_oas.get(key, np.nan)

    # scale for symbol-risk
    mul = scale_multiplier(L, H)
    spread = base * mul if not (pd.isna(base) or pd.isna(mul)) else np.nan

    return {
        "symbol": symbol,
        "proxy": key,
        "asof": str(date.today()),
        # keep 2 decimals like 0.77 (77bp). If you prefer more precision, reduce rounding.
        "proxy_spread": None if pd.isna(spread) else round(float(spread), 2)
    }

# ---------- main ----------

def main() -> int:
    ensure_dirs()

    wl = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    symbols = read_watchlist(wl)
    if not symbols:
        # Safe default
        symbols = ["AAPL", "MSFT", "NVDA", "SPY"]

    fred = load_latest_oas()
    fun  = load_fundamentals()
    ois  = load_options_summary()

    rows = []
    for s in symbols:
        try:
            rows.append(proxy_for_symbol(s, fred, fun, ois))
        except Exception as e:
            # still emit row with NaNs so we see it in output
            rows.append({"symbol": s, "proxy": "US_IG", "asof": str(date.today()), "proxy_spread": None})
            print(f"[warn] proxy failed for {s}: {e}")

    out = pd.DataFrame(rows)
    out.to_csv("data/processed/cds_proxy.csv", index=False)
    print("wrote data/processed/cds_proxy.csv rows=", len(out))
    # small text echo for logs
    for _, r in out.iterrows():
        print(f"{r['symbol']},{r['proxy']},{r['asof']},{r['proxy_spread']}")

    # JSON report (avoid Timestamp objects)
    report = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "asof": str(date.today()),
        "rows": int(len(out)),
        "missing": int(out["proxy_spread"].isna().sum()),
        "sample": out.head(10).to_dict(orient="records"),
        "fred_oas_used": fred
    }
    with open("data/reports/cds_proxy_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
