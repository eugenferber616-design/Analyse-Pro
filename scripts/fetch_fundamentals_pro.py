# scripts/fetch_fundamentals_pro.py
import os, json, time, math, csv, requests
import pandas as pd, numpy as np

FINNHUB = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
WL = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OUT = "data/processed/fundamentals_pro.csv"
os.makedirs("data/processed", exist_ok=True)

def get_profile(sym):
    r = requests.get("https://finnhub.io/api/v1/stock/profile2",
        params={"symbol": sym, "token": FINNHUB}, timeout=20)
    return r.json() if r.ok else {}

def get_metrics(sym):
    r = requests.get("https://finnhub.io/api/v1/stock/metric",
        params={"symbol": sym, "metric":"all", "token": FINNHUB}, timeout=25)
    return (r.json() or {}).get("metric", {})

def safe(v): 
    return np.nan if v in (None, "", "NaN") else float(v)

def derived_row(sym, prof, m):
    # Grundwerte
    mc   = safe(m.get("marketCapitalization"))
    debt = safe(m.get("totalDebt"))
    cash = safe(m.get("totalCash"))
    ev   = mc + (debt or 0) - (cash or 0) if mc is not np.nan else np.nan
    ebitda = safe(m.get("ebitda"))
    sales  = safe(m.get("revenueTTM"))
    fcf    = safe(m.get("freeCashFlowTTM"))
    ni     = safe(m.get("netIncomeTTM"))

    ev_ebitda = ev/ebitda if ev and ebitda and ebitda>0 else np.nan
    ev_sales  = ev/sales  if ev and sales and sales>0   else np.nan
    fcf_yld   = fcf/mc    if fcf and mc and mc>0       else np.nan
    ey        = 1.0/safe(m.get("peTTM")) if safe(m.get("peTTM")) not in (0,np.nan) else np.nan
    buyback_yld = safe(m.get("buyBackYieldTTM"))  # Finnhub liefert %
    roe   = safe(m.get("roeTTM"))
    roic  = safe(m.get("roicTTM")) or np.nan
    roa   = safe(m.get("roaTTM"))

    accruals = np.nan
    cfo = safe(m.get("cashFlowFromOperationsTTM"))
    ta  = safe(m.get("totalAssets"))
    if ni is not np.nan and cfo is not np.nan and ta and ta>0: 
        accruals = (ni - cfo)/ta

    # Piotroski (vereinfachte Näherung aus Metrics, falls Felder fehlen -> NaN)
    pio = np.nan
    try:
        flags = []
        flags += [1 if safe(m.get("netIncomeAnnual"))>0 else 0]
        flags += [1 if (safe(m.get("operatingCashFlowAnnual")) or 0) > 0 else 0]
        # mehr Komponenten möglich… (Leverage, Liquidität, Margenverbesserung)
        pio = sum(int(x) for x in flags if x in (0,1))
    except: pass

    row = dict(
        symbol=sym,
        sector=prof.get("finnhubIndustry") or prof.get("sector"),
        country=prof.get("country"),
        market_cap=mc, ev=ev, ev_ebitda=ev_ebitda, ev_sales=ev_sales,
        fcf_yield=fcf_yld, earnings_yield=ey, buyback_yield=buyback_yld,
        roe_ttm=roe, roic_ttm=roic, roa_ttm=roa, accruals=accruals,
        piotroski_f=pio,
        gross_margin=safe(m.get("grossMarginTTM")),
        revenue_growth=safe(m.get("revenueGrowthTTMYoy")),
        eps_growth=safe(m.get("epsGrowthTTMYoy")),
        debt_to_equity=safe(m.get("totalDebt/totalEquityAnnual"))
    )
    return row

def main():
    syms = [s.strip().split(",")[0] for s in open(WL) if s.strip() and not s.startswith("#")]
    rows = []
    for s in syms:
        prof = get_profile(s); time.sleep(0.35)
        met  = get_metrics(s); time.sleep(0.35)
        rows.append(derived_row(s, prof, met))
    df = pd.DataFrame(rows)
    df.to_csv(OUT, index=False)
    print(f"fundamentals_pro.csv rows: {len(df)} -> {OUT}")

if __name__ == "__main__":
    main()
