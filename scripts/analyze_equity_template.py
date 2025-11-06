#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_equity_template.py
Baut eine kompakte, quellengebundene Analyse (OHNE LLM) für einen Ticker:
- Holt Basisdaten (Name, Sector/Industry, MCap) via Finnhub oder yfinance
- Holt letzte Earnings-Überraschungen (falls frühere Pipeline-Datei existiert)
- Schreibt JSON im geforderten Schema + HTML-Report
"""

from __future__ import annotations
import os, sys, json, math, argparse, gzip
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import requests

try:
    import yfinance as yf
except Exception:
    yf = None

def _env(key: str) -> str:
    return os.getenv(key, "").strip()

def fetch_profile(symbol: str) -> Dict[str, Any]:
    """Versucht Finnhub profile2, sonst yfinance."""
    token = _env("FINNHUB_TOKEN") or _env("FINNHUB_API_KEY")
    if token:
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/stock/profile2",
                params={"symbol": symbol, "token": token},
                timeout=15,
            )
            if r.ok:
                d = r.json() or {}
                if d.get("name"):
                    return {
                        "name": d.get("name"),
                        "ticker": symbol,
                        "exchange": d.get("exchange") or "",
                        "country": d.get("country") or "",
                        "currency": d.get("currency") or "",
                        "sector": d.get("finnhubIndustry") or "",
                        "ipo": d.get("ipo") or "",
                        "market_cap": d.get("marketCapitalization"),
                        "weburl": d.get("weburl") or "",
                        "source": "finnhub:profile2"
                    }
        except Exception: 
            pass

    # yfinance fallback
    if yf:
        try:
            t = yf.Ticker(symbol)
            info = t.fast_info if hasattr(t, "fast_info") else {}
            basics = {
                "name": getattr(t, "info", {}).get("longName") if hasattr(t, "info") else "",
                "ticker": symbol,
                "exchange": "",
                "country": "",
                "currency": info.get("currency") or "",
                "sector": getattr(t, "info", {}).get("sector", "") if hasattr(t, "info") else "",
                "ipo": "",
                "market_cap": info.get("market_cap") or getattr(t, "info", {}).get("marketCap"),
                "weburl": "",
                "source": "yfinance"
            }
            return basics
        except Exception:
            pass

    return {"ticker": symbol, "source": "unknown"}

def fetch_fundamentals_light(symbol: str) -> Dict[str, Any]:
    """Sehr kleine Heuristik: hol Preis & einfache Margen, wenn greifbar."""
    out = {}
    if yf:
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="1y")
            out["price"] = float(hist["Close"].dropna().iloc[-1]) if not hist.empty else None
            info = getattr(t, "info", {})
            if info:
                out["trailing_pe"] = info.get("trailingPE")
                out["forward_pe"] = info.get("forwardPE")
                out["gross_margins"] = info.get("grossMargins")
                out["operating_margins"] = info.get("operatingMargins")
                out["free_cashflow"] = info.get("freeCashflow")
                out["total_debt"] = info.get("totalDebt")
                out["total_cash"] = info.get("totalCash")
        except Exception:
            pass
    return out

def load_earnings_results_local(symbol: str) -> Dict[str, Any]:
    """Falls deine Pipeline `data/processed/earnings_results.csv` erzeugt hat:
       zieh letzte Surprise% und Trend-Hinweis. Sonst leer."""
    p = Path("data/processed/earnings_results.csv")
    if not p.exists():
        return {}
    try:
        df = pd.read_csv(p)
        df = df[df["symbol"].str.upper() == symbol.upper()]
        if df.empty:
            return {}
        df = df.sort_values("date", ascending=False)
        last = df.iloc[0].to_dict()
        surprise = last.get("surprise_percent")
        note = f"Letzte Überraschung: {surprise:.1f}%" if pd.notna(surprise) else ""
        return {
            "last_surprise_pct": float(surprise) if pd.notna(surprise) else None,
            "note": note,
            "source": "pipeline:earnings_results"
        }
    except Exception:
        return {}

def make_json(symbol: str, public_base: str) -> Dict[str, Any]:
    prof = fetch_profile(symbol)
    fin  = fetch_fundamentals_light(symbol)
    ear  = load_earnings_results_local(symbol)

    name = prof.get("name") or prof.get("ticker") or symbol

    # Summary bullets (nur harte Fakten die wir haben)
    bullets = []
    if prof.get("market_cap"):
        bullets.append(f"{name}: Marktkap. ~{int(prof['market_cap']):,} Mio (Quelle: {prof.get('source')})".replace(",", "."))
    if fin.get("price"):
        bullets.append(f"Aktienkurs ~{fin['price']:.2f} {prof.get('currency','')}".strip())
    if ear.get("last_surprise_pct") is not None:
        bullets.append(f"Earnings-Überraschung zuletzt {ear['last_surprise_pct']:.1f}%")

    # Financials Block
    financials = {
        "revenue_yoy": None,
        "gross_margin": fin.get("gross_margins"),
        "op_margin": fin.get("operating_margins"),
        "fcf": fin.get("free_cashflow"),
        "net_debt": None if fin.get("total_debt") is None else float(fin.get("total_debt") or 0) - float(fin.get("total_cash") or 0),
        "liquidity_note": ""
    }

    # Valuation
    val_note = ""
    if fin.get("trailing_pe"): val_note += f"Trailing P/E ~{fin['trailing_pe']:.1f}. "
    if fin.get("forward_pe"):  val_note += f"Forward P/E ~{fin['forward_pe']:.1f}."
    valuation = {"pe": fin.get("trailing_pe"), "ev_ebitda": None, "note": val_note.strip()}

    # Outlook/Risks/Konkurrenz: hier Platzhalter ohne Spekulation
    outlook = []
    risks = []
    competition = []

    earnings_dyn = {"revisions": "", "surprises": ear.get("note",""), "tone": ""}

    # Stance nur "neutral", da wir ohne LLM/echte Textquellen keine Wertung ableiten
    stance = "neutral"

    # Quellenliste
    sources = []
    if prof.get("source"): sources.append({"name": prof["source"], "type": "profile", "snippet": ""})
    if valuation["note"]:  sources.append({"name": "yfinance", "type": "valuation", "snippet": valuation["note"]})
    if ear.get("source"):  sources.append({"name": ear["source"], "type": "earnings", "snippet": ear.get("note","")})

    # fertiges Objekt
    j = {
      "ticker": symbol.upper(),
      "summary_bullets": bullets,
      "financials": financials,
      "outlook": outlook,
      "risks": risks,
      "competition": competition,
      "valuation": valuation,
      "earnings_dynamics": earnings_dyn,
      "catalysts": [],
      "stance": stance,
      "sources": sources,
      "links": {
        "html": f"{public_base}/site/eq/{symbol.upper()}.html",
        "json_gz": f"{public_base}/analysis/eq_template/{symbol.upper()}.json.gz"
      },
      "generated_at": datetime.utcnow().isoformat()+"Z"
    }
    return j

def render_html(j: Dict[str,Any]) -> str:
    """Kompakte Dark-Card HTML-Seite, selbsterklärend."""
    bullets = "".join(f"<li>{b}</li>" for b in j.get("summary_bullets",[]))
    risks   = "".join(f"<li>{r.get('risk','')}</li>" for r in j.get("risks",[]))
    comp    = "".join(f"<li>{c}</li>" for c in j.get("competition",[]))
    outs    = "".join(f"<li>{o}</li>" for o in j.get("outlook",[]))
    cats    = "".join(f"<li>{c}</li>" for c in j.get("catalysts",[]))
    srcs    = "".join(f"<li>{s.get('name','')} – {s.get('type','')}</li>" for s in j.get("sources",[]))

    return f"""<!doctype html><html lang="de"><meta charset="utf-8">
<title>{j['ticker']} – Template Analyse</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{{margin:0;background:#0f1116;color:#e6e6e6;font:15px/1.5 system-ui,Segoe UI,Roboto,Arial}}
.wrap{{max-width:960px;margin:28px auto;padding:0 16px}}
.card{{background:#121c28;border:1px solid #273041;border-radius:14px;box-shadow:0 8px 22px rgba(0,0,0,.35);padding:18px;margin:0 0 20px}}
h1,h2{{margin:0 0 12px}}
a{{color:#8ab4ff;text-decoration:none}}
.mono{{font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, monospace}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:800px){{.grid{{grid-template-columns:1fr}}}}
.badge{{display:inline-block;padding:2px 8px;border:1px solid #2a3b50;border-radius:999px;background:#0c1420;color:#cdd9e5;font-size:12px}}
</style>
<div class="wrap">
  <div class="card">
    <h1>Template Analyse – <span class="mono">{j['ticker']}</span></h1>
    <div class="badge">{j.get('stance','neutral').upper()}</div>
    <p class="mono" style="opacity:.7">erstellt: {j.get('generated_at','')}</p>
    <ul>{bullets}</ul>
  </div>

  <div class="grid">
    <div class="card"><h2>Unternehmensprofil</h2><p>Quelle: Profil/Finanzen (ohne Spekulation)</p></div>
    <div class="card"><h2>Finanzlage</h2>
      <p>Gross Margin: {fmt_pct(j['financials'].get('gross_margin'))}, Op Margin: {fmt_pct(j['financials'].get('op_margin'))}</p>
      <p>FCF: {fmt_num(j['financials'].get('fcf'))} | Net Debt: {fmt_num(j['financials'].get('net_debt'))}</p>
    </div>
    <div class="card"><h2>Ausblick</h2><ul>{outs or "<li>unbekannt</li>"}</ul></div>
    <div class="card"><h2>Risiken</h2><ul>{risks or "<li>unbekannt</li>"}</ul></div>
    <div class="card"><h2>Wettbewerb</h2><ul>{comp or "<li>unbekannt</li>"}</ul></div>
    <div class="card"><h2>Bewertung</h2><p>{j['valuation'].get('note') or "unbekannt"}</p></div>
    <div class="card"><h2>Earnings-Dynamik</h2><p>{j['earnings_dynamics'].get('surprises') or "unbekannt"}</p></div>
    <div class="card"><h2>Katalysatoren</h2><ul>{cats or "<li>unbekannt</li>"}</ul></div>
  </div>

  <div class="card"><h2>Quellen</h2><ul>{srcs or "<li>unbekannt</li>"}</ul>
    <p>JSON: <a href="{j['links']['json_gz']}">{j['links']['json_gz']}</a></p>
  </div>
</div>
<script>
function fmtNum(x){{return x===null||x===undefined? "": new Intl.NumberFormat('de-DE').format(x)}}
</script>
</html>"""

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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-html", required=True)
    ap.add_argument("--public-base", required=True)
    args = ap.parse_args()

    j = make_json(args.symbol, args.public_base)

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_html).parent.mkdir(parents=True, exist_ok=True)

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(j, f, ensure_ascii=False, indent=2)

    html = render_html(j)
    with open(args.out_html, "w", encoding="utf-8") as f:
        f.write(html)

    print("Wrote:", args.out_json, "and", args.out_html)

if __name__ == "__main__":
    main()
