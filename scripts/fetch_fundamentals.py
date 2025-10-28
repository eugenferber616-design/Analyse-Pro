#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, argparse
import requests
import pandas as pd
import yfinance as yf

OUT_DIR = "data/fundamentals"
OUT_CORE = "data/processed/fundamentals_core.csv"
ERR_JSON = "data/reports/fund_errors.json"

FINNHUB_BASE = "https://finnhub.io/api/v1"

FIELDS = [
    "market_cap","beta","shares_out","pe_ttm","ps_ttm","pb_ttm",
    "roe_ttm","gross_margin","oper_margin","net_margin","debt_to_equity",
]

def finnhub_fetch(symbol: str, token: str) -> dict:
    headers = {"Accept":"application/json"}
    # profile2
    p = requests.get(f"{FINNHUB_BASE}/stock/profile2", params={"symbol": symbol, "token": token}, timeout=20, headers=headers)
    if p.status_code == 403:
        raise PermissionError("403 profile2")
    p.raise_for_status()
    prof = p.json() or {}

    # metrics (TTM)
    m = requests.get(f"{FINNHUB_BASE}/stock/metric", params={"symbol": symbol, "metric":"all","token": token}, timeout=25, headers=headers)
    if m.status_code == 403:
        raise PermissionError("403 metric")
    m.raise_for_status()
    met = m.json().get("metric", {}) if m.content else {}

    out = {
        "symbol": symbol,
        "market_cap": prof.get("marketCapitalization"),
        "beta": prof.get("beta"),
        "shares_out": prof.get("shareOutstanding"),
        "pe_ttm": met.get("peInclExtraTTM"),
        "ps_ttm": met.get("psTTM"),
        "pb_ttm": met.get("pbAnnual"),
        "roe_ttm": met.get("roeTTM"),
        "gross_margin": met.get("grossMarginTTM"),
        "oper_margin": met.get("operatingMarginTTM"),
        "net_margin": met.get("netProfitMarginTTM"),
        "debt_to_equity": met.get("ltD2EquityAnnual"),
    }
    return out

def yahoo_fallback(symbol: str) -> dict:
    t = yf.Ticker(symbol.replace(".", "-"))
    info = t.fast_info or {}
    # grobe Annäherung / subset
    return {
        "symbol": symbol,
        "market_cap": info.get("market_cap"),
        "beta": None,  # Yahoo liefert Beta in .info, ist teilweise gesperrt – lassen wir NA
        "shares_out": None,
        "pe_ttm": None,
        "ps_ttm": None,
        "pb_ttm": None,
        "roe_ttm": None,
        "gross_margin": None,
        "oper_margin": None,
        "net_margin": None,
        "debt_to_equity": None,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default=OUT_DIR)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_CORE), exist_ok=True)
    os.makedirs(os.path.dirname(ERR_JSON), exist_ok=True)

    token = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY")
    if not token:
        print("❌ FINNHUB Token fehlt")
        return

    with open(args.watchlist, "r", encoding="utf-8") as f:
        syms = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]

    rows, errors = [], []
    for sym in syms:
        try:
            rows.append(finnhub_fetch(sym, token))
        except PermissionError as e:
            # EU-Symbole -> Fallback Yahoo (teilweise nur market_cap)
            try:
                fb = yahoo_fallback(sym)
                rows.append(fb)
                errors.append({"symbol": sym, "reason": "403 -> yahoo_fallback"})
            except Exception as ee:
                errors.append({"symbol": sym, "reason": f"fallback_failed: {ee}"})
        except Exception as e:
            errors.append({"symbol": sym, "reason": str(e)})

    if rows:
        df = pd.DataFrame(rows)
        df = df[["symbol"] + FIELDS]  # konsistente Reihenfolge
        df.to_csv(OUT_CORE, index=False)
        print(f"✅ fundamentals_core.csv rows: {len(df)}")
    else:
        pd.DataFrame(columns=["symbol"] + FIELDS).to_csv(OUT_CORE, index=False)
        print("⚠️ fundamentals_core.csv leer")

    rep = {
        "total": len(syms),
        "ok": sum(1 for r in rows if r),
        "failed": len(errors),
        "errors": errors,
    }
    with open(ERR_JSON, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)

if __name__ == "__main__":
    main()
