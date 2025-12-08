#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_fundamentals_pro.py

Holt für alle Symbole in WATCHLIST_STOCKS:
- Profil (profile2): name, exchange, country, sector, industry, currency
- Kennzahlen (metric=all):
    marketcap, beta, shares_out
    pe, ps, pb, ev, ev/ebitda, ev/sales
    gross_margin, oper_margin, net_margin
    rev_ttm, eps_ttm
    rev_yoy, eps_yoy
    fcf_yield, earnings_yield, buyback_yield
    roe, roic, roa
    debt_to_equity, div_yield
    total_debt, cash, net_debt, current_ratio
    accruals, piotroski_f (vereinfachte Näherung)

Outputs:
  data/processed/fundamentals_pro.csv   (volle Version)
  data/processed/fundamentals_core.csv  (gleiche Tabelle, wird von factor_scores/equity_master genutzt)
"""

import os
import json
import time
import math
import csv
import requests
from pathlib import Path

import pandas as pd
import numpy as np

FINNHUB = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
WL = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")

OUT_PRO = "data/processed/fundamentals_pro.csv"
OUT_CORE = "data/processed/fundamentals_core.csv"

os.makedirs("data/processed", exist_ok=True)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def read_symbols(path: str):
    p = Path(path)
    if not p.exists():
        raise SystemExit("Watchlist nicht gefunden: %s" % path)

    if p.suffix.lower() == ".csv":
        with p.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            col = None
            for c in (r.fieldnames or []):
                if c and c.lower() in ("symbol", "ticker", "ric"):
                    col = c
                    break
            if col is None:
                col = (r.fieldnames or ["symbol"])[0]
            out = []
            for row in r:
                v = (row.get(col) or "").strip()
                if v:
                    out.append(v)
            return out
    else:
        return [
            ln.strip().split(",")[0]
            for ln in p.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")
        ]


def q(url, params, token, retries=2, sleep_sec=2.0):
    """Request mit einfachem Retry + 429-Handling."""
    p = dict(params or {})
    p["token"] = token
    last_exc = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, params=p, timeout=30)
            if r.status_code == 429:
                time.sleep(sleep_sec * (i + 1))
                continue
            r.raise_for_status()
            return r.json() or {}
        except Exception as e:
            last_exc = e
            time.sleep(sleep_sec * (i + 1))
    if last_exc:
        raise last_exc
    return {}


def get_profile(sym):
    return q(
        "https://finnhub.io/api/v1/stock/profile2",
        {"symbol": sym},
        FINNHUB,
        retries=1,
        sleep_sec=1.5
    )


def get_metrics(sym):
    j = q(
        "https://finnhub.io/api/v1/stock/metric",
        {"symbol": sym, "metric": "all"},
        FINNHUB,
        retries=1,
        sleep_sec=2.0
    )
    return (j or {}).get("metric", {}) or {}


def safe(v):
    """Robuste Float-Konvertierung → np.nan bei allem, was nicht sauber ist."""
    try:
        if v is None:
            return np.nan
        if isinstance(v, str):
            s = v.strip()
            if not s or s.lower() in ("nan", "na", "none"):
                return np.nan
            return float(s)
        return float(v)
    except Exception:
        return np.nan


def pick(m, *keys):
    for k in keys:
        if k and k in m and m[k] is not None:
            return m[k]
    return None


def derived_row(sym, prof, m):
    # Rohwerte
    mc   = safe(m.get("marketCapitalization"))
    debt = safe(m.get("totalDebt"))
    cash = safe(m.get("totalCash"))
    ev   = np.nan
    if not math.isnan(mc):
        ev = mc
        if not math.isnan(debt):
            ev += debt
        if not math.isnan(cash):
            ev -= cash
    ebitda = safe(m.get("ebitda"))
    sales  = safe(m.get("revenueTTM"))
    fcf    = safe(m.get("freeCashFlowTTM"))
    ni     = safe(m.get("netIncomeTTM"))

    ev_ebitda = np.nan
    if not math.isnan(ev) and not math.isnan(ebitda) and ebitda > 0:
        ev_ebitda = ev / ebitda

    ev_sales = np.nan
    if not math.isnan(ev) and not math.isnan(sales) and sales > 0:
        ev_sales = ev / sales

    fcf_yld = np.nan
    if not math.isnan(fcf) and not math.isnan(mc) and mc > 0:
        fcf_yld = fcf / mc

    pe_ttm = safe(m.get("peTTM") or m.get("peNormalizedAnnual"))
    ey = np.nan
    if not math.isnan(pe_ttm) and pe_ttm != 0:
        ey = 1.0 / pe_ttm

    buyback_yld = safe(m.get("buyBackYieldTTM"))  # % von Finnhub
    roe   = safe(m.get("roeTTM"))
    roic  = safe(m.get("roicTTM"))
    roa   = safe(m.get("roaTTM"))

    # Accruals
    accruals = np.nan
    cfo = safe(m.get("cashFlowFromOperationsTTM"))
    ta  = safe(m.get("totalAssets"))
    if not math.isnan(ni) and not math.isnan(cfo) and not math.isnan(ta) and ta > 0:
        accruals = (ni - cfo) / ta

    # Piotroski-F (sehr grobe Näherung mit wenigen Komponenten)
    pio = np.nan
    try:
        flags = []
        flags += [1 if safe(m.get("netIncomeAnnual")) > 0 else 0]
        flags += [1 if safe(m.get("operatingCashFlowAnnual")) > 0 else 0]
        pio = sum(int(x) for x in flags if x in (0, 1))
    except Exception:
        pass

    # Debt / Net Debt / Current Ratio
    total_debt = debt
    net_debt = np.nan
    if not math.isnan(debt) and not math.isnan(cash):
        net_debt = debt - cash
    current_ratio = safe(m.get("currentRatioAnnual") or m.get("currentRatio"))

    row = dict(
        symbol=sym,
        # Profil
        name=prof.get("name") or prof.get("ticker") or "",
        exchange=prof.get("exchange") or prof.get("mic") or "",
        country=prof.get("country") or "",
        sector=prof.get("finnhubIndustry") or prof.get("sector") or "",
        industry=prof.get("gind") or prof.get("ggroup") or "",
        currency=prof.get("currency") or "",
        # Größe / Risiko / Bewertung (Basis)
        marketcap=mc,
        market_cap=mc,  # alias
        beta=safe(m.get("beta")),
        shares_out=safe(m.get("shareOutstanding")),
        pe=pe_ttm,
        ps=safe(m.get("psRatioTTM") or m.get("priceToSalesTTM")),
        pb=safe(m.get("pbAnnual") or m.get("priceToBookAnnual") or m.get("pbTTM")),
        # EV / FCF / Spreads
        ev=ev,
        ev_ebitda=ev_ebitda,
        ev_sales=ev_sales,
        fcf_yield=fcf_yld,
        earnings_yield=ey,
        buyback_yield=buyback_yld,
        # Margen + Profitabilität
        gross_margin=safe(m.get("grossMarginTTM")),
        oper_margin=safe(m.get("operatingMarginTTM")),
        net_margin=safe(m.get("netProfitMarginTTM") or m.get("netMargin")),
        roe=roe,
        roe_ttm=roe,
        roic=roic,
        roic_ttm=roic,
        roa=roa,
        roa_ttm=roa,
        # TTM-Level
        rev_ttm=sales,
        eps_ttm=safe(m.get("epsTTM")),
        # Wachstum (YoY)
        rev_yoy=safe(m.get("revenueGrowthTTMYoy") or m.get("revenueGrowthTTM")),
        eps_yoy=safe(m.get("epsGrowthTTMYoy") or m.get("epsGrowthTTM")),
        revenue_growth=safe(m.get("revenueGrowthTTMYoy") or m.get("revenueGrowthTTM")),
        eps_growth=safe(m.get("epsGrowthTTMYoy") or m.get("epsGrowthTTM")),
        # Verschuldung / Dividende
        debt_to_equity=safe(m.get("totalDebt/totalEquityAnnual") or m.get("debtToEquity")),
        total_debt=total_debt,
        cash=cash,
        net_debt=net_debt,
        current_ratio=current_ratio,
        div_yield=safe(m.get("dividendYieldIndicatedAnnual") or m.get("dividendYieldTTM")),
        # Qualität extra
        accruals=accruals,
        piotroski_f=pio,
    )
    return row


def main():
    if not FINNHUB:
        raise SystemExit("Kein FINNHUB_API_KEY/FINNHUB_TOKEN im ENV gesetzt.")

    syms = [s.strip().upper() for s in read_symbols(WL) if s.strip()]
    if not syms:
        raise SystemExit("WATCHLIST_STOCKS leer oder keine Symbole gefunden.")

    rows = []
    for i, s in enumerate(syms, 1):
        print(f"[{i}/{len(syms)}] {s} …")
        try:
            prof = get_profile(s)
        except Exception as e:
            print(f"  profile skip {s}: {e}")
            prof = {}

        try:
            met = get_metrics(s)
        except Exception as e:
            print(f"  metric skip {s}: {e}")
            met = {}

        row = derived_row(s, prof, met)
        # wenn fast alles NaN/leer -> sehr wahrscheinlich kein Equity
        non_empty = sum(
            v not in (None, "", np.nan) for v in row.values()
        )
        if non_empty <= 5:
            print(f"  skip {s}: zu wenig Fundamentals ({non_empty} Felder)")
            time.sleep(0.35)
            continue

        rows.append(row)
        time.sleep(0.35)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_PRO, index=False)
    df.to_csv(OUT_CORE, index=False)
    print(f"fundamentals_pro/core rows: {len(df)} -> {OUT_PRO}, {OUT_CORE}")


if __name__ == "__main__":
    main()
