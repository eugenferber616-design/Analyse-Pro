#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_equity_template.py  (RICH PAYLOAD EDITION)
Baut eine quellengebundene, LLM-freundliche JSON pro Ticker aus den bereits
vorhandenen Pipeline-Dateien und rendert eine kleine HTML-Vorschau.

Nutzt u.a.:
  - data/processed/fundamentals_core.csv
  - data/processed/earnings_results.csv
  - docs/earnings_next.json
  - data/processed/options_oi_by_expiry.csv
  - data/processed/options_oi_by_strike.csv
  - data/processed/options_oi_strike_max.csv
  - data/processed/hv_summary.csv.gz
  - data/processed/cds_proxy.csv
  - data/processed/market_core.csv.gz  (optional)

CLI bleibt kompatibel:
  --symbol        Ticker
  --out-json      Pfad zur JSON-Ausgabe
  --out-html      Pfad zur HTML-Ausgabe
  --public-base   Öffentliche Basis-URL (R2), z.B. https://pub-...r2.dev
"""

from __future__ import annotations
import os, json, math, argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd

# ----------------------------- Utils -----------------------------------------
def _read_csv(path: str | Path) -> pd.DataFrame:
    """Liest CSV oder CSV.GZ robust; existiert Datei nicht, -> leeres DF."""
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        if p.suffix == ".gz":
            return pd.read_csv(p, compression="gzip")
        return pd.read_csv(p)
    except Exception:
        try:
            return pd.read_csv(p, low_memory=False)
        except Exception:
            return pd.DataFrame()

def _read_json(path: str | Path):
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None

def _num(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None

def _first(series, default=None):
    try:
        v = series.iloc[0]
        return v if pd.notna(v) else default
    except Exception:
        return default

# ----------------------------- Laden der Quellen ------------------------------
P = {
    "fundamentals": "data/processed/fundamentals_core.csv",
    "earnings_results": "data/processed/earnings_results.csv",
    "earnings_next": "docs/earnings_next.json",
    "options_by_expiry": "data/processed/options_oi_by_expiry.csv",
    "options_by_strike": "data/processed/options_oi_by_strike.csv",
    "strike_max": "data/processed/options_oi_strike_max.csv",
    "hv_summary": "data/processed/hv_summary.csv.gz",
    "cds_proxy": "data/processed/cds_proxy.csv",
    "market_core": "data/processed/market_core.csv.gz",  # optional
}

DF_fund = _read_csv(P["fundamentals"])
DF_earn = _read_csv(P["earnings_results"])
DF_exp  = _read_csv(P["options_by_expiry"])
DF_strk = _read_csv(P["options_by_strike"])
DF_smax = _read_csv(P["strike_max"])
DF_hv   = _read_csv(P["hv_summary"])
DF_cds  = _read_csv(P["cds_proxy"])
DF_mkt  = _read_csv(P["market_core"])
EARN_NEXT = _read_json(P["earnings_next"]) or []

# Vorindizierung
if not DF_fund.empty and "symbol" in DF_fund.columns:
    DF_fund["symbol"] = DF_fund["symbol"].astype(str).str.upper()
    DF_fund.set_index("symbol", inplace=True, drop=False)

if not DF_hv.empty and "symbol" in DF_hv.columns:
    DF_hv["symbol"] = DF_hv["symbol"].astype(str).str.upper()
    DF_hv.set_index("symbol", inplace=True, drop=False)

if not DF_cds.empty and "symbol" in DF_cds.columns:
    DF_cds["symbol"] = DF_cds["symbol"].astype(str).str.upper()
    DF_cds.set_index("symbol", inplace=True, drop=False)

if not DF_smax.empty and "symbol" in DF_smax.columns:
    DF_smax["symbol"] = DF_smax["symbol"].astype(str).str.upper()
    DF_smax.set_index("symbol", inplace=True, drop=False)

# Map: nächster Earnings-Termin
EARN_NEXT_MAP: Dict[str,str] = {}
for row in EARN_NEXT if isinstance(EARN_NEXT, list) else []:
    s = str(row.get("symbol","")).upper()
    d = row.get("next_date")
    if s and d:
        EARN_NEXT_MAP[s] = d

# ----------------------------- Kernlogik -------------------------------------
def build_payload(symbol: str, public_base: str) -> Dict[str, Any]:
    s = symbol.strip().upper()

    # Fundamentals
    f = DF_fund.loc[s] if (not DF_fund.empty and s in DF_fund.index) else {}

    profile = {
        "name": f.get("name") if isinstance(f, pd.Series) else None,
        "ticker": s,
        "exchange": f.get("exchange") if isinstance(f, pd.Series) else None,
        "country": f.get("country") if isinstance(f, pd.Series) else None,
        "currency": f.get("currency") if isinstance(f, pd.Series) else None,
        "sector": f.get("sector") if isinstance(f, pd.Series) else None,
        "industry": f.get("industry") if isinstance(f, pd.Series) else None,
        "isin": f.get("isin") if isinstance(f, pd.Series) else None,
        "market_cap": _num(f.get("market_cap")) if isinstance(f, pd.Series) else None,
        "source": "fundamentals_core.csv"
    }

    financials = {
        "revenue_ttm": _num(f.get("revenue_ttm")) if isinstance(f, pd.Series) else None,
        "gross_margin": _num(f.get("gross_margin")) if isinstance(f, pd.Series) else None,
        "op_margin": _num(f.get("oper_margin")) if isinstance(f, pd.Series) else None,
        "fcf_ttm": _num(f.get("fcf_ttm")) if isinstance(f, pd.Series) else None,
        "net_debt": _num(f.get("net_debt")) if isinstance(f, pd.Series) else None,
        "liquidity_note": f.get("liquidity_note") if isinstance(f, pd.Series) else "",
    }

    valuation = {
        "pe": _num(f.get("pe")) if isinstance(f, pd.Series) else None,
        "ps": _num(f.get("ps")) if isinstance(f, pd.Series) else None,
        "pb": _num(f.get("pb")) if isinstance(f, pd.Series) else None,
        "ev_ebitda": _num(f.get("ev_ebitda")) if isinstance(f, pd.Series) else None,
        "note": ""
    }

    # HV / Volatilität
    hv = DF_hv.loc[s] if (not DF_hv.empty and s in DF_hv.index) else {}
    volatility = {
        "hv20": _num(hv.get("hv20")) if isinstance(hv, pd.Series) else None,
        "hv60": _num(hv.get("hv60")) if isinstance(hv, pd.Series) else None
    }

    # Credit-Proxy
    cd = DF_cds.loc[s] if (not DF_cds.empty and s in DF_cds.index) else {}
    credit_proxy = {
        "proxy": cd.get("proxy") if isinstance(cd, pd.Series) else None,
        "proxy_spread": _num(cd.get("proxy_spread")) if isinstance(cd, pd.Series) else None
    }

    # Options: Put/Call + Focus-Strike
    pcr, peak_expiry = None, None
    if not DF_exp.empty:
        sub = DF_exp[DF_exp["symbol"].astype(str).str.upper() == s]
        if not sub.empty:
            last = sub.sort_values("expiry").iloc[-1]
            c = _num(last.get("total_call_oi"))
            p = _num(last.get("total_put_oi"))
            if c and c > 0 and p is not None:
                pcr = round(p / c, 3)
            peak_expiry = str(last.get("expiry"))

    smax = DF_smax.loc[s] if (not DF_smax.empty and s in DF_smax.index) else {}
    options = {
        "expiry": smax.get("expiry") if isinstance(smax, pd.Series) else peak_expiry,
        "focus_strike": _num(smax.get("focus_strike")) if isinstance(smax, pd.Series) else None,
        "focus_side": smax.get("focus_side") if isinstance(smax, pd.Series) else None,
        "put_call_ratio": pcr
    }

    # Earnings: Historie (letzte 6–8) + nächster Termin
    earnings_history: List[Dict[str, Any]] = []
    if not DF_earn.empty:
        er = DF_earn[DF_earn["symbol"].astype(str).str.upper() == s].copy()
        if not er.empty:
            er = er.sort_values("date").tail(8)
            for _, r in er.iterrows():
                earnings_history.append({
                    "date": str(r.get("date")),
                    "eps": _num(r.get("eps")),
                    "surprise_pct": _num(r.get("surprise_percent")),
                    "revenue": _num(r.get("revenue"))
                })

    earnings = {
        "next_date": EARN_NEXT_MAP.get(s),
        "history": earnings_history
    }

    # Summary bullets (rein faktenbasiert)
    bullets = []
    if profile.get("market_cap"):
        bullets.append(f"Marktkapitalisierung: ~{int(profile['market_cap']):,}".replace(",", "."))
    if valuation.get("pe"):
        bullets.append(f"P/E (ttm): {valuation['pe']:.1f}")
    if volatility.get("hv20") is not None:
        bullets.append(f"HV20: {volatility['hv20']:.2f}")
    if credit_proxy.get("proxy_spread") is not None:
        bullets.append(f"Credit-Proxy Spread: {credit_proxy['proxy_spread']:.1f} bp")
    if options.get("put_call_ratio") is not None:
        bullets.append(f"Put/Call OI: {options['put_call_ratio']:.2f}")

    # R2-Links (damit externe Agenten Quellen haben)
    base = (public_base or "https://pub-CHANGE-ME.r2.dev").rstrip("/")
    r2_links = {
        "eq_payload_gz": f"{base}/data/processed/eq_template/{s}.json.gz",
        "fundamentals": f"{base}/data/processed/fundamentals_core.csv.gz",
        "earnings_results": f"{base}/data/processed/earnings_results.csv.gz",
        "earnings_next": f"{base}/docs/earnings_next.json.gz",
        "options_by_expiry": f"{base}/data/processed/options_oi_by_expiry.csv.gz",
        "options_by_strike": f"{base}/data/processed/options_oi_by_strike.csv.gz",
        "hv_summary": f"{base}/data/processed/hv_summary.csv.gz",
        "cds_proxy": f"{base}/data/processed/cds_proxy.csv.gz"
    }

    # Externe, generische Quellen (zur Web-Ergänzung)
    external = [
        {"name": "Company IR", "url": f"https://www.google.com/search?q={s}+investor+relations"},
        {"name": "SEC Search", "url": f"https://www.sec.gov/edgar/search/#/q={s}"},
        {"name": "Yahoo Finance", "url": f"https://finance.yahoo.com/quote/{s}"}
    ]

    # Quellenliste für die JSON-Struktur
    sources = [
        {"name":"fundamentals_core.csv","type":"fundamentals","snippet":""},
        {"name":"earnings_results.csv","type":"earnings","snippet":"Q-Ergebnisse & Surprise%"},
        {"name":"earnings_next.json","type":"calendar","snippet":"nächster Termin"},
        {"name":"options_oi_*","type":"options","snippet":"Put/Call & Fokus-Strike"},
        {"name":"hv_summary.csv.gz","type":"volatility","snippet":"HV20/HV60"},
        {"name":"cds_proxy.csv","type":"credit","snippet":"Proxy/OAS"}
    ]

    # Output (kompatibel mit deinem Prompt – plus Zusatzfelder 'profile', 'sources.r2')
    out: Dict[str, Any] = {
        "ticker": s,
        "summary_bullets": bullets,
        "profile": profile,
        "financials": {
            "revenue_yoy": None,             # unbekannt (keine saubere Quelle lokal)
            "gross_margin": financials["gross_margin"],
            "op_margin": financials["op_margin"],
            "fcf": financials["fcf_ttm"],
            "net_debt": financials["net_debt"],
            "liquidity_note": financials["liquidity_note"] or ""
        },
        "outlook": [],                       # absichtlich leer (keine Spekulation)
        "risks": [],                         # absichtlich leer (nur harte Daten)
        "competition": [],
        "valuation": {"pe": valuation["pe"], "ev_ebitda": valuation["ev_ebitda"], "note": valuation["note"]},
        "earnings_dynamics": {
            "revisions": "",                 # falls du revisions.csv.gz befüllst, kann man das ergänzen
            "surprises": "",                 # Details stecken in earnings.history
            "tone": ""
        },
        "catalysts": [],
        "stance": "neutral",
        "options": options,
        "volatility": volatility,
        "credit_proxy": credit_proxy,
        "earnings": earnings,
        "sources": sources,
        "links": {
            "html": f"{base}/site/eq/{s}.html",
            "json_gz": f"{base}/data/processed/eq_template/{s}.json.gz"
        },
        "sources_r2": r2_links,
        "sources_external": external,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds")+"Z"
    }
    return out

# ----------------------------- HTML ------------------------------------------
def fmt_pct(x):
    try:
        return f"{float(x)*100:.1f}%"
    except Exception:
        return "unbekannt"

def fmt_num(x):
    try:
        return f"{float(x):,.0f}".replace(",", ".")
    except Exception:
        return "unbekannt"

def render_html(j: Dict[str,Any]) -> str:
    bullets = "".join(f"<li>{b}</li>" for b in j.get("summary_bullets",[]))
    hv = j.get("volatility", {})
    cp = j.get("credit_proxy", {})
    opt = j.get("options", {})
    fin = j.get("financials", {})
    val = j.get("valuation", {})

    return f"""<!doctype html><html lang="de"><meta charset="utf-8">
<title>{j['ticker']} – Equity Payload</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{{margin:0;background:#0f1116;color:#e6e6e6;font:15px/1.5 system-ui,Segoe UI,Roboto,Arial}}
.wrap{{max-width:980px;margin:28px auto;padding:0 16px}}
.card{{background:#121c28;border:1px solid #273041;border-radius:14px;box-shadow:0 8px 22px rgba(0,0,0,.35);padding:18px;margin:0 0 20px}}
h1,h2{{margin:0 0 12px}} .muted{{opacity:.8}} a{{color:#8ab4ff;text-decoration:none}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:900px){{.grid{{grid-template-columns:1fr}}}}
.badge{{display:inline-block;padding:2px 8px;border:1px solid #2a3b50;border-radius:999px;background:#0c1420;color:#cdd9e5;font-size:12px}}
.kv td{{padding:4px 8px;border-bottom:1px solid #243042}}
</style>
<div class="wrap">
  <div class="card">
    <h1>Equity Payload – <span>{j['ticker']}</span></h1>
    <div class="badge">{j.get('stance','neutral').upper()}</div>
    <p class="muted">erstellt: {j.get('generated_at','')}</p>
    <ul>{bullets or "<li>–</li>"}</ul>
  </div>

  <div class="grid">
    <div class="card">
      <h2>Finanzlage</h2>
      <table class="kv">
        <tr><td>Gross Margin</td><td>{fmt_pct(fin.get('gross_margin'))}</td></tr>
        <tr><td>Operating Margin</td><td>{fmt_pct(fin.get('op_margin'))}</td></tr>
        <tr><td>FCF (ttm)</td><td>{fmt_num(fin.get('fcf'))}</td></tr>
        <tr><td>Net Debt</td><td>{fmt_num(fin.get('net_debt'))}</td></tr>
      </table>
    </div>
    <div class="card">
      <h2>Bewertung & Risiko</h2>
      <table class="kv">
        <tr><td>P/E</td><td>{val.get('pe') if val.get('pe') is not None else "unbekannt"}</td></tr>
        <tr><td>EV/EBITDA</td><td>{val.get('ev_ebitda') if val.get('ev_ebitda') is not None else "unbekannt"}</td></tr>
        <tr><td>HV20 / HV60</td><td>{hv.get('hv20') if hv.get('hv20') is not None else "–"} / {hv.get('hv60') if hv.get('hv60') is not None else "–"}</td></tr>
        <tr><td>Credit-Proxy</td><td>{cp.get('proxy') or "–"} | {cp.get('proxy_spread') if cp.get('proxy_spread') is not None else "–"} bp</td></tr>
      </table>
    </div>
    <div class="card">
      <h2>Optionen</h2>
      <table class="kv">
        <tr><td>Focus-Expiry</td><td>{opt.get('expiry') or "unbekannt"}</td></tr>
        <tr><td>Focus-Strike</td><td>{fmt_num(opt.get('focus_strike'))}</td></tr>
        <tr><td>Put/Call OI</td><td>{opt.get('put_call_ratio') if opt.get('put_call_ratio') is not None else "unbekannt"}</td></tr>
      </table>
    </div>
    <div class="card">
      <h2>Earnings</h2>
      <p>Nächster Termin: {j.get('earnings',{}).get('next_date') or "unbekannt"}</p>
      <p class="muted">Details (history) nur in JSON.</p>
    </div>
  </div>

  <div class="card">
    <h2>Quellen</h2>
    <ul>
      <li>R2 JSON: <a href="{j['links']['json_gz']}">{j['links']['json_gz']}</a></li>
      <li>Fundamentals: <a href="{j.get('sources_r2',{}).get('fundamentals','')}">fundamentals_core.csv.gz</a></li>
      <li>Options: <a href="{j.get('sources_r2',{}).get('options_by_expiry','')}">options_by_expiry.csv.gz</a></li>
      <li>HV: <a href="{j.get('sources_r2',{}).get('hv_summary','')}">hv_summary.csv.gz</a></li>
      <li>Credit: <a href="{j.get('sources_r2',{}).get('cds_proxy','')}">cds_proxy.csv.gz</a></li>
    </ul>
  </div>
</div>
</html>"""

# ----------------------------- CLI -------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-html", required=True)
    ap.add_argument("--public-base", required=True)
    args = ap.parse_args()

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_html).parent.mkdir(parents=True, exist_ok=True)

    payload = build_payload(args.symbol, args.public_base)

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    html = render_html(payload)
    with open(args.out_html, "w", encoding="utf-8") as f:
        f.write(html)

    print("Wrote:", args.out_json, "and", args.out_html)

if __name__ == "__main__":
    main()
