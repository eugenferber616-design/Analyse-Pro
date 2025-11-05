#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_fundamentals_v2.py – Globale Fundamentals (US + EU) mit Multi-Provider-Fallback

Ziele:
- **Primär:** Finnhub (profile2 + metric?metric=all)
- **Fallbacks:**
  1) yfinance (Ticker.info + quarterly_* / financials)
  2) Merge mit bestehender Datei (CSV/CSV.GZ) – behält beste/neueste Werte
- **ADR/EU-Mapping:** via CSV overrides (symbol,api_symbol)
- **Robustheit:** Retry/Backoff, Typisierung, Null-/NaN-Cleaning
- **Extras:** abgeleitete Kennzahlen (EV, EV/EBITDA, EV/Sales, FCF-Yield, Earnings-Yield, Piotroski light, Accruals)
- **Outputs:**
    data/processed/fundamentals_core.csv
    data/processed/fundamentals_core.csv.gz
    data/reports/fundamentals_report.json
    data/reports/fundamentals_preview.txt

CLI-Beispiele:
  python scripts/fetch_fundamentals_v2.py \
    --watchlist watchlists/mylist.txt \
    --overrides watchlists/fund_overrides.csv \
    --merge-existing data/processed/fundamentals_core.csv.gz

Abhängigkeiten: pandas, requests, (optional) yfinance
"""
from __future__ import annotations
import os, csv, time, json, gzip, io, math
from typing import Dict, List, Tuple
from pathlib import Path

import requests
import pandas as pd

# ------------------------------------------------------------
# ENV / Pfade
# ------------------------------------------------------------
OUT_DIR = Path("data/processed")
REP_DIR = Path("data/reports")
for p in (OUT_DIR, REP_DIR):
    p.mkdir(parents=True, exist_ok=True)

FINNHUB_TOKEN = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN") or ""
FINNHUB_SLEEP_MS = int(os.getenv("FINNHUB_SLEEP_MS", "200"))
DEFAULT_OVERRIDES = os.getenv("FUND_OVERRIDES", "watchlists/fund_overrides.csv")
DEFAULT_WATCHLIST = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")

# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------

def sleep_ms(ms: int):
    time.sleep(max(0, ms) / 1000.0)


def read_watchlist(path: str | Path) -> List[str]:
    path = Path(path)
    if not path.exists():
        return []
    txt = path.read_text(encoding="utf-8", errors="ignore")
    lines = []
    if "," in txt or "symbol" in txt.lower():
        # CSV
        rows = list(csv.DictReader(io.StringIO(txt)))
        for r in rows:
            s = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if s and not s.startswith("#"):
                lines.append(s)
    else:
        for ln in txt.splitlines():
            s = ln.strip().upper()
            if s and not s.startswith("#") and s.lower() != "symbol":
                lines.append(s)
    # order-preserving dedup
    seen=set(); out=[]
    for s in lines:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out


def read_overrides(path: str | Path) -> Dict[str,str]:
    path = Path(path)
    if not path.exists():
        return {}
    out: Dict[str,str] = {}
    rows = list(csv.DictReader(path.read_text(encoding="utf-8", errors="ignore").splitlines()))
    for r in rows:
        sym = (r.get("symbol") or "").strip().upper()
        api = (r.get("api_symbol") or "").strip().upper()
        if sym and api:
            out[sym]=api
    return out


def api_symbol_for(sym: str, overrides: Dict[str,str]) -> str:
    if sym in overrides:
        return overrides[sym]
    # einfache Heuristik: Suffix abschneiden (SAP.DE -> SAP)
    if "." in sym:
        return sym.split(".",1)[0]
    return sym


def to_float(x):
    try:
        if x is None or (isinstance(x,float) and math.isnan(x)):
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


# ------------------------------------------------------------
# Provider: Finnhub
# ------------------------------------------------------------
FINN_BASE = "https://finnhub.io/api/v1"

def _fh_get(path: str, params: Dict[str,object], retries: int = 3, base_sleep: int = None):
    base_sleep = base_sleep or FINNHUB_SLEEP_MS
    params = dict(params or {})
    params["token"] = FINNHUB_TOKEN
    for k in range(retries):
        try:
            r = requests.get(f"{FINN_BASE}/{path}", params=params, timeout=30)
            if r.status_code == 429 and k+1 < retries:
                sleep_ms(base_sleep * (2**k));
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if k+1 == retries:
                raise
            sleep_ms(base_sleep * (2**k))
    return None


def finnhub_pull(sym_api: str) -> Tuple[dict, dict]:
    prof = _fh_get("stock/profile2", {"symbol": sym_api}) or {}
    met  = _fh_get("stock/metric", {"symbol": sym_api, "metric": "all"}) or {}
    sleep_ms(FINNHUB_SLEEP_MS)
    return prof, met


def normalize_finnhub(symbol: str, prof: dict, met: dict) -> dict:
    m = met.get("metric", {}) if isinstance(met, dict) else {}
    def g(*keys, default=None):
        for k in keys:
            v = m.get(k)
            if v not in (None, "", "NaN"):
                return v
        return default

    row = {
        "symbol": symbol,
        "name": prof.get("name") or prof.get("ticker") or "",
        "exchange": prof.get("exchange") or prof.get("exchangeShortName") or "",
        "country": prof.get("country") or "",
        "industry": prof.get("finnhubIndustry") or prof.get("industry") or "",
        "currency": prof.get("currency") or "",
        # Kern-KPIs
        "market_cap": g("marketCapitalization"),
        "pe": g("peInclExtraTTM", "peBasicExclExtraTTM", "peNormalizedAnnual"),
        "ps": g("psTTM", "priceToSalesTTM"),
        "pb": g("pbAnnual", "pbQuarterly"),
        "ev_to_ebitda": g("evToEbitdaAnnual", "evToEbitdaTTM"),
        "gross_margin": g("grossMarginTTM", "grossMarginAnnual"),
        "op_margin": g("operatingMarginTTM", "operatingMarginAnnual"),
        "net_margin": g("netProfitMarginTTM", "netProfitMarginAnnual"),
        "roic": g("roicTTM", "roicAnnual"),
        "roe": g("roeTTM", "roeAnnual"),
        "debt_to_equity": g("totalDebt/totalEquityAnnual"),
        "total_debt": g("totalDebt"),
        "total_cash": g("totalCash"),
        "net_debt": g("netDebtAnnual"),
        "fcf_margin": g("fcfMarginTTM"),
        "div_yield": g("currentDividendYieldTTM", "dividendYieldIndicatedAnnual"),
        "eps_ttm": g("epsInclExtraItemsTTM", "epsExclExtraItemsTTM"),
        "revenue_ttm": g("revenueTTM"),
        "revenue_growth_yoy": g("revenueGrowthTTMYoy", "revenueGrowthAnnualYoy"),
        "eps_growth_yoy": g("epsGrowthTTMYoy", "epsGrowthAnnualYoy"),
        "shares_out": g("shareIssued"),
        "beta": g("beta"),
        "asof": m.get("lastUpdatedTime") or "",
        "source": "finnhub",
    }
    # Ableitungen
    mc = to_float(row.get("market_cap"))
    debt = to_float(row.get("total_debt"))
    cash = to_float(row.get("total_cash"))
    ev = (mc if math.isfinite(mc) else float("nan"))
    if math.isfinite(debt): ev = ev + debt if math.isfinite(ev) else debt
    if math.isfinite(cash): ev = ev - cash if math.isfinite(ev) else float("nan")
    ebitda = to_float(m.get("ebitda"))
    sales  = to_float(row.get("revenue_ttm"))
    fcf    = to_float(m.get("freeCashFlowTTM"))
    pe_ttm = to_float(m.get("peTTM"))

    row["enterprise_value"] = ev
    row["ev_ebitda"] = (ev/ebitda) if (math.isfinite(ev) and math.isfinite(ebitda) and ebitda>0) else float("nan")
    row["ev_sales"]  = (ev/sales)  if (math.isfinite(ev) and math.isfinite(sales)  and sales>0)  else float("nan")
    row["fcf_yield"] = (fcf/mc)    if (math.isfinite(fcf) and math.isfinite(mc)    and mc>0)    else float("nan")
    row["earnings_yield"] = (1.0/pe_ttm) if (math.isfinite(pe_ttm) and pe_ttm>0) else float("nan")

    # Accruals
    ni  = to_float(m.get("netIncomeTTM"))
    cfo = to_float(m.get("cashFlowFromOperationsTTM"))
    ta  = to_float(m.get("totalAssets"))
    row["accruals"] = ((ni - cfo)/ta) if (math.isfinite(ni) and math.isfinite(cfo) and math.isfinite(ta) and ta>0) else float("nan")

    # Piotroski light (2 Flags, erweiterbar)
    flags = [
        1 if to_float(m.get("netIncomeAnnual"))>0 else 0,
        1 if to_float(m.get("operatingCashFlowAnnual"))>0 else 0,
    ]
    row["piotroski_f"] = sum(int(x) for x in flags if x in (0,1))
    return row


# ------------------------------------------------------------
# Provider: yfinance (optional)
# ------------------------------------------------------------
_YF_AVAIL = None

def yf_available() -> bool:
    global _YF_AVAIL
    if _YF_AVAIL is None:
        try:
            import yfinance as yf  # noqa
            _YF_AVAIL = True
        except Exception:
            _YF_AVAIL = False
    return _YF_AVAIL


def yf_pull(symbol: str) -> dict:
    if not yf_available():
        return {}
    import yfinance as yf
    row = {"symbol": symbol, "source": "yfinance"}
    try:
        tk = yf.Ticker(symbol)
        info = tk.get_info() if hasattr(tk, "get_info") else getattr(tk, "info", {})
        def gi(k, default=None):
            v = info.get(k, default)
            return None if v in (None, "NaN", "nan", "") else v
        row.update({
            "name": gi("longName") or gi("shortName") or "",
            "exchange": gi("exchange") or "",
            "country": gi("country") or "",
            "industry": gi("industry") or "",
            "currency": gi("currency") or "",
            "market_cap": gi("marketCap"),
            "pe": gi("trailingPE"),
            "ps": gi("priceToSalesTrailing12Months"),
            "pb": gi("priceToBook"),
            "ev_to_ebitda": gi("enterpriseToEbitda"),
            "gross_margin": gi("grossMargins"),
            "op_margin": gi("operatingMargins"),
            "net_margin": gi("profitMargins"),
            "roe": gi("returnOnEquity"),
            "total_debt": gi("totalDebt"),
            "total_cash": gi("totalCash"),
            "div_yield": gi("dividendYield"),
            "eps_ttm": gi("trailingEps"),
            "revenue_ttm": gi("totalRevenue"),
            "shares_out": gi("sharesOutstanding"),
            "beta": gi("beta"),
            "asof": "",
        })
        # Ableitungen (falls möglich)
        mc = to_float(row.get("market_cap"))
        debt = to_float(row.get("total_debt"))
        cash = to_float(row.get("total_cash"))
        ev = (mc if math.isfinite(mc) else float("nan"))
        if math.isfinite(debt): ev = ev + debt if math.isfinite(ev) else debt
        if math.isfinite(cash): ev = ev - cash if math.isfinite(ev) else float("nan")
        sales = to_float(row.get("revenue_ttm"))
        # yfinance liefert oft kein EBITDA direkt im info (enterpriseToEbitda existiert)
        e2e = to_float(row.get("ev_to_ebitda"))
        ebitda = ev / e2e if (math.isfinite(ev) and math.isfinite(e2e) and e2e>0) else float("nan")
        row["enterprise_value"] = ev
        row["ev_ebitda"] = (ev/ebitda) if (math.isfinite(ev) and math.isfinite(ebitda) and ebitda>0) else float("nan")
        row["ev_sales"]  = (ev/sales)  if (math.isfinite(ev) and math.isfinite(sales) and sales>0)  else float("nan")
        # FCF-Yield/Earnings-Yield fehlen meist → bleiben NaN
        row.setdefault("fcf_yield", float("nan"))
        row.setdefault("earnings_yield", (1.0/to_float(row.get("pe"))) if (math.isfinite(to_float(row.get("pe"))) and to_float(row.get("pe"))>0) else float("nan"))
        row.setdefault("accruals", float("nan"))
        row.setdefault("piotroski_f", float("nan"))
        return row
    except Exception:
        return {}


# ------------------------------------------------------------
# IO helpers
# ------------------------------------------------------------

def read_existing(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            raw = f.read()
        bio = io.BytesIO(raw)
        try:
            df = pd.read_csv(bio, sep=";", low_memory=False)
            if df.shape[1] == 1:
                bio.seek(0)
                df = pd.read_csv(bio, sep=",")
        except Exception:
            bio.seek(0)
            df = pd.read_csv(bio)
        return df
    else:
        try:
            df = pd.read_csv(path, sep=";", low_memory=False)
            if df.shape[1] == 1:
                df = pd.read_csv(path, sep=",")
            return df
        except Exception:
            return pd.read_csv(path)


def df_safe_head_csv(df: pd.DataFrame, path: Path, n: int = 40):
    with open(path, "w", encoding="utf-8") as f:
        if df.empty:
            f.write("empty\n")
        else:
            f.write(df.head(n).to_csv(index=False))


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default=DEFAULT_WATCHLIST)
    ap.add_argument("--overrides", default=DEFAULT_OVERRIDES)
    ap.add_argument("--merge-existing", default="", help="Bestehende fundamentals_core.csv[.gz] zum Mergen")
    ap.add_argument("--prefer", default="finnhub,yfinance", help="Provider-Reihenfolge (Komma)")
    args = ap.parse_args()

    report = {"ok":[], "fallback":[], "miss":[], "errors":[], "provider_order": args.prefer}

    symbols = read_watchlist(args.watchlist)
    overrides = read_overrides(args.overrides)

    rows: List[dict] = []
    for sym in symbols:
        api_sym = api_symbol_for(sym, overrides)
        got = False
        # Provider-Reihenfolge respektieren
        for prov in [p.strip().lower() for p in args.prefer.split(",") if p.strip()]:
            try:
                if prov == "finnhub" and FINNHUB_TOKEN:
                    prof, met = finnhub_pull(api_sym)
                    row = normalize_finnhub(sym, prof, met)
                    rows.append(row)
                    (report["ok"] if row.get("source")=="finnhub" else report["fallback"]).append(sym)
                    got = True
                    break
                if prov == "yfinance":
                    yrow = yf_pull(api_sym)
                    if yrow and any((k not in ("symbol","name","exchange","country","industry","currency","source") and pd.notna(yrow.get(k))) for k in yrow.keys()):
                        yrow["symbol"] = sym  # Rückmap auf Originalsymbol
                        rows.append(yrow)
                        report["fallback"].append(sym)
                        got = True
                        break
            except Exception as e:
                report["errors"].append({"symbol": sym, "provider": prov, "err": str(e)[:240]})
        if not got:
            report["miss"].append(sym)

    cols = [
        "symbol","name","exchange","country","industry","currency",
        "market_cap","enterprise_value","pe","ps","pb",
        "ev_to_ebitda","ev_ebitda","ev_sales",
        "gross_margin","op_margin","net_margin",
        "roic","roe","beta",
        "total_debt","total_cash","net_debt",
        "debt_to_equity","fcf_margin","fcf_yield","div_yield",
        "eps_ttm","revenue_ttm","revenue_growth_yoy","eps_growth_yoy","shares_out",
        "earnings_yield","accruals","piotroski_f",
        "asof","source"
    ]
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols]

    # numerische Typisierung
    num_cols = [c for c in cols if c not in ("symbol","name","exchange","country","industry","currency","asof","source")]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Merge mit bestehender Datei
    if args.merge_existing:
        old = read_existing(args.merge_existing)
        if not old.empty:
            for c in cols:
                if c not in old.columns:
                    old[c] = pd.NA
            old = old[cols]
            # bevorzugt neuere Quelle (finnhub > yfinance) und nicht-NaN
            merged = pd.concat([old, df], ignore_index=True)
            merged.sort_values(["symbol"], inplace=True)
            # last-win per symbol, aber wenn neuere Quelle finnhub vorhanden, die behalten
            merged["_src_rank"] = merged["source"].map({"finnhub":2, "yfinance":1}).fillna(0)
            merged = (merged
                      .sort_values(["symbol","_src_rank"], ascending=[True, False])
                      .drop_duplicates(subset=["symbol"], keep="first")
                      .drop(columns=["_src_rank"]))
            df = merged

    # Schreiben
    out_csv = OUT_DIR / "fundamentals_core.csv"
    out_gz  = OUT_DIR / "fundamentals_core.csv.gz"
    df.to_csv(out_csv, index=False)
    df.to_csv(out_gz, index=False, compression="gzip")

    # Reports
    preview = REP_DIR / "fundamentals_preview.txt"
    with open(preview, "w", encoding="utf-8") as f:
        if df.empty:
            f.write("empty\n")
        else:
            f.write(df.head(50).to_csv(index=False))

    report["rows"] = int(len(df))
    report["symbols"] = int(df["symbol"].nunique())
    with open(REP_DIR / "fundamentals_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"[summary] rows={len(df)} symbols={df['symbol'].nunique()} out={out_csv}")


if __name__ == "__main__":
    main()
