#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_equity_template.py  (AI-READY PAYLOAD)
Baut eine kompakte, quellengebundene Analyse-Payload (OHNE LLM) für einen Ticker.
- Liest lokale Pipeline-Outputs: fundamentals_core, hv_summary, cds_proxy,
  options_oi_* (expiry/strike/signals), earnings_next, earnings_results, market_core, riskindex_snapshot
- Optional: Finnhub/yfinance Fallbacks für Basisprofil
- Schreibt JSON (reich an Feldern + Quellenpfaden) und ein schlankes HTML zur Sichtprüfung
CLI bleibt identisch zum Vorgänger:
  --symbol SYMBOL --out-json PATH.json --out-html PATH.html --public-base https://pub-...r2.dev
"""

from __future__ import annotations
import os, sys, json, math, argparse, gzip
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import requests

try:
    import yfinance as yf
except Exception:
    yf = None

# ----------------------------- Helpers -----------------------------

def _env(k: str, default: str = "") -> str:
    v = os.getenv(k)
    return v.strip() if v else default

def _read_csv_any(path: str) -> Optional[pd.DataFrame]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        if str(p).endswith(".gz"):
            return pd.read_csv(p, compression="gzip")
        return pd.read_csv(p)
    except Exception:
        return None

def _read_json_any(path: str) -> Optional[Any]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        if str(p).endswith(".gz"):
            with gzip.open(p, "rt", encoding="utf-8") as f:
                return json.load(f)
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _fmt_pct(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _upper(x: str) -> str:
    return (x or "").upper()

# ------------------------ Online profile (fallback) ------------------------

def fetch_profile(symbol: str) -> Dict[str, Any]:
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
                        "source": "finnhub:profile2",
                    }
        except Exception:
            pass

    if yf:
        try:
            t = yf.Ticker(symbol)
            info = getattr(t, "info", {})
            finfo = getattr(t, "fast_info", {})
            return {
                "name": info.get("longName") or info.get("shortName") or "",
                "ticker": symbol,
                "exchange": info.get("exchange") or "",
                "country": info.get("country") or "",
                "currency": finfo.get("currency") or info.get("currency") or "",
                "sector": info.get("sector") or "",
                "ipo": "",
                "market_cap": finfo.get("market_cap") or info.get("marketCap"),
                "weburl": info.get("website") or "",
                "source": "yfinance",
            }
        except Exception:
            pass

    return {"ticker": symbol, "source": "unknown"}

# ------------------------ Local pipeline readers ------------------------

def pick_row(df: Optional[pd.DataFrame], symbol_col: str, symbol: str) -> Optional[pd.Series]:
    if df is None or df.empty: return None
    colz = {c.lower(): c for c in df.columns}
    sc = colz.get(symbol_col.lower())
    if sc is None: return None
    sub = df[df[sc].astype(str).str.upper() == _upper(symbol)]
    if sub.empty: return None
    return sub.iloc[0]

def load_fundamentals_core(symbol: str) -> Dict[str, Any]:
    df = _read_csv_any("data/processed/fundamentals_core.csv")
    r = pick_row(df, "symbol", symbol)
    if r is None: return {}
    # tolerante Spaltennamen
    def g(*cand):
        for c in cand:
            if c in r: return r[c]
        return None
    return {
        "revenue_ttm": _float(g("revenue_ttm", "revenue")),
        "gross_margin": _float(g("gross_margin", "grossMargins")),
        "operating_margin": _float(g("operating_margin", "operatingMargins")),
        "free_cashflow": _float(g("free_cashflow", "freeCashflow")),
        "total_debt": _float(g("total_debt")),
        "total_cash": _float(g("total_cash")),
        "pe_trailing": _float(g("pe", "trailingPE")),
        "pe_forward": _float(g("forward_pe", "forwardPE")),
        "ev_ebitda": _float(g("ev_ebitda")),
        "currency": g("currency") or "",
        "source": "fundamentals_core.csv",
    }

def load_hv(symbol: str) -> Dict[str, Any]:
    df = _read_csv_any("data/processed/hv_summary.csv.gz")
    r = pick_row(df, "symbol", symbol)
    if r is None: return {}
    return {
        "hv20": _float(r.get("hv20")),
        "hv60": _float(r.get("hv60")),
        "source": "hv_summary.csv.gz",
    }

def load_cds_proxy(symbol: str) -> Dict[str, Any]:
    df = _read_csv_any("data/processed/cds_proxy.csv")
    r = pick_row(df, "symbol", symbol)
    if r is None: return {}
    return {
        "proxy_spread": _float(r.get("proxy_spread")),
        "asof": str(r.get("asof")) if r.get("asof") is not None else "",
        "source": "cds_proxy.csv",
    }

def load_options(symbol: str) -> Dict[str, Any]:
    # by_expiry (finde max total_oi)
    dfe = _read_csv_any("data/processed/options_oi_by_expiry.csv.gz")
    exp = None
    put_oi = call_oi = None
    if dfe is not None and not dfe.empty:
        sub = dfe[dfe["symbol"].astype(str).str.upper() == _upper(symbol)].copy()
        if not sub.empty:
            # flexible Spaltennamen
            def g(row, name, alt):
                return row[name] if name in row else row.get(alt)
            if "total_call_oi" in sub.columns and "total_put_oi" in sub.columns:
                sub["total_oi"] = sub["total_call_oi"].fillna(0) + sub["total_put_oi"].fillna(0)
                top = sub.sort_values("total_oi", ascending=False).head(1)
                if not top.empty:
                    exp = str(top.iloc[0].get("expiry"))
                    call_oi = _float(top.iloc[0].get("total_call_oi"))
                    put_oi  = _float(top.iloc[0].get("total_put_oi"))

    # by_strike (focus)
    dfs = _read_csv_any("data/processed/options_oi_by_strike.csv")
    focus_strike = focus_side = None
    if dfs is not None and not dfs.empty:
        sub = dfs[dfs["symbol"].astype(str).str.upper() == _upper(symbol)]
        if not sub.empty:
            # nimm erste Zeile (bereits sortiert in Pipeline)
            focus_strike = _float(sub.iloc[0].get("focus_strike"))
            focus_side   = str(sub.iloc[0].get("focus_side") or "")

    return {
        "focus_expiry": exp,
        "focus_strike": focus_strike,
        "focus_side": focus_side,
        "put_oi": put_oi,
        "call_oi": call_oi,
        "source": "options_oi_by_expiry.csv.gz + options_oi_by_strike.csv",
    }

def load_earnings(symbol: str) -> Dict[str, Any]:
    nxt = _read_json_any("docs/earnings_next.json")
    next_date = ""
    if isinstance(nxt, list):
        for row in nxt:
            if _upper(row.get("symbol","")) == _upper(symbol):
                next_date = row.get("next_date","") or ""
                break

    df = _read_csv_any("data/processed/earnings_results.csv")
    last_surprise = None
    if df is not None and not df.empty:
        sub = df[df["symbol"].astype(str).str.upper() == _upper(symbol)]
        if not sub.empty:
            sub = sub.sort_values("date", ascending=False)
            v = sub.iloc[0].get("surprise_percent")
            last_surprise = _float(v)

    return {
        "next_date": next_date,
        "last_surprise_pct": last_surprise,
        "source": "earnings_next.json + earnings_results.csv",
    }

def load_riskindex_snapshot() -> Dict[str, Any]:
    j = _read_json_any("data/processed/riskindex_snapshot.json") or \
        _read_json_any("data/processed/riskindex_snapshot.json.gz")
    if not isinstance(j, dict): return {}
    return {
        "regime": j.get("regime"),
        "score": _float(j.get("score")),
        "source": "riskindex_snapshot.json(.gz)",
    }

# ------------------------ JSON builder ------------------------

def build_payload(symbol: str, public_base: str) -> Dict[str, Any]:
    prof = fetch_profile(symbol)                     # Fallbacks
    fcs  = load_fundamentals_core(symbol)
    hv   = load_hv(symbol)
    cds  = load_cds_proxy(symbol)
    opt  = load_options(symbol)
    ern  = load_earnings(symbol)
    rix  = load_riskindex_snapshot()

    currency = fcs.get("currency") or prof.get("currency") or ""

    financials = {
        "revenue_yoy": None,  # nicht robust lokal vorhanden → leer
        "gross_margin": fcs.get("gross_margin"),
        "op_margin":   fcs.get("operating_margin"),
        "fcf":         fcs.get("free_cashflow"),
        "net_debt":    (fcs.get("total_debt") or 0.0) - (fcs.get("total_cash") or 0.0) \
                        if fcs.get("total_debt") is not None or fcs.get("total_cash") is not None else None,
        "liquidity_note": "",
        "currency": currency,
    }

    valuation = {
        "pe": fcs.get("pe_trailing"),
        "ev_ebitda": fcs.get("ev_ebitda"),
        "note": " ".join([
            f"Trailing P/E ~{fcs['pe_trailing']:.1f}" if fcs.get("pe_trailing") else "",
            f"Forward P/E ~{fcs['pe_forward']:.1f}" if fcs.get("pe_forward") else "",
            f"EV/EBITDA ~{fcs['ev_ebitda']:.1f}" if fcs.get("ev_ebitda") else "",
        ]).strip(),
    }

    sources = []
    def add_src(name, typ, path_note=""):
        if name:
            sources.append({"name": name, "type": typ, "snippet": path_note})

    add_src(prof.get("source"), "profile")
    add_src(fcs.get("source"),  "fundamentals",  f"{public_base}/data/processed/fundamentals_core.csv.gz")
    add_src(hv.get("source"),   "hv",            f"{public_base}/data/processed/hv_summary.csv.gz")
    add_src(cds.get("source"),  "credit",        f"{public_base}/data/processed/cds_proxy.csv.gz")
    add_src(opt.get("source"),  "options",
            f"{public_base}/data/processed/options_oi_by_expiry.csv.gz; "
            f"{public_base}/data/processed/options_oi_by_strike.csv.gz")
    add_src(ern.get("source"),  "earnings",
            f"{public_base}/docs/earnings_next.json.gz; {public_base}/data/processed/earnings_results.csv.gz")
    add_src(rix.get("source"),  "riskindex",     f"{public_base}/data/processed/riskindex_snapshot.json.gz")

    bullets = []
    if prof.get("market_cap"):
        bullets.append(f"Marktkap.: ~{int(prof['market_cap']):,} (Quelle: {prof.get('source')})".replace(",", "."))
    if cds.get("proxy_spread") is not None:
        bullets.append(f"Credit-Proxy Spread: {cds['proxy_spread']:.2f} bp")
    if hv.get("hv20") is not None or hv.get("hv60") is not None:
        bullets.append(f"HV20/HV60: {hv.get('hv20','–')} / {hv.get('hv60','–')}")

    payload = {
        "ticker": _upper(symbol),
        "name": prof.get("name") or "",
        "exchange": prof.get("exchange") or "",
        "country": prof.get("country") or "",
        "currency": currency,
        "web": prof.get("weburl") or "",
        "summary_bullets": bullets,

        "financials": financials,
        "outlook": [],                 # absichtlich leer (keine Spekulation)
        "risks": [],                   # dito
        "competition": [],             # dito
        "valuation": valuation,

        "earnings_dynamics": {
            "revisions": "",
            "surprises": (f"Letzte Überraschung: {ern['last_surprise_pct']:.1f}%"
                          if ern.get("last_surprise_pct") is not None else ""),
            "tone": "",
        },
        "catalysts": ([f"Nächster Earnings-Termin: {ern['next_date']}"] if ern.get("next_date") else []),

        # Optionen/HV/CDS/RiskIndex als technische Inputs
        "options": {
            "focus_expiry": opt.get("focus_expiry"),
            "focus_strike": opt.get("focus_strike"),
            "focus_side":   opt.get("focus_side"),
            "put_oi":       opt.get("put_oi"),
            "call_oi":      opt.get("call_oi"),
        },
        "volatility": {
            "hv20": hv.get("hv20"),
            "hv60": hv.get("hv60"),
        },
        "credit_proxy": {
            "spread_bp": cds.get("proxy_spread"),
            "asof": cds.get("asof"),
        },
        "riskindex": {
            "regime": rix.get("regime"),
            "score":  rix.get("score"),
        },

        # AI-Prompt-ähnliche Felder (falls du direkt in Google AI schicken willst)
        "stance": "neutral",
        "sources": sources,

        "links": {
            "json_gz": f"{public_base}/data/processed/eq_template/{_upper(symbol)}.json.gz",
            "html":    f"{public_base}/site/eq/{_upper(symbol)}.html",
        },
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
    return payload

# ------------------------ HTML (compact check) ------------------------

def render_html(j: Dict[str, Any]) -> str:
    kv_fin = [
        ("Gross Margin",  pct(j["financials"].get("gross_margin"))),
        ("Operating Margin", pct(j["financials"].get("op_margin"))),
        ("FCF (ttm)",  num(j["financials"].get("fcf"))),
        ("Net Debt",   num(j["financials"].get("net_debt"))),
    ]
    kv_val = [
        ("P/E", num(j["valuation"].get("pe"))),
        ("EV/EBITDA", num(j["valuation"].get("ev_ebitda"))),
        ("HV20 / HV60", f"{num(j['volatility'].get('hv20'))} / {num(j['volatility'].get('hv60'))}"),
        ("Credit-Proxy", f"{num(j['credit_proxy'].get('spread_bp'))} bp"),
    ]
    bullets = "".join(f"<li>{b}</li>" for b in j.get("summary_bullets", []))
    srcs = "".join(f"<li>{s.get('name','')} – {s.get('type','')}</li>" for s in j.get("sources", []))

    def tbl(rows):  # simple key/value table
        out = ['<table class="kv">']
        for k,v in rows:
            out.append(f"<tr><td>{k}</td><td>{v}</td></tr>")
        out.append("</table>")
        return "".join(out)

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
    <ul>{bullets}</ul>
  </div>

  <div class="grid">
    <div class="card"><h2>Finanzlage</h2>{tbl(kv_fin)}</div>
    <div class="card"><h2>Bewertung & Risiko</h2>{tbl(kv_val)}</div>
    <div class="card"><h2>Optionen</h2>{tbl([("Focus-Expiry",j['options'].get("focus_expiry") or "unbekannt"),
       ("Focus-Strike", j['options'].get("focus_strike") or "unbekannt"),
       ("Focus-Side",   j['options'].get("focus_side")   or "unbekannt"),
       ("Put/Call OI",  f"{num(j['options'].get('put_oi'))} / {num(j['options'].get('call_oi'))}")])}</div>
    <div class="card"><h2>Earnings</h2>
      <p>Nächster Termin: {j['catalysts'][0] if j['catalysts'] else "unbekannt"}</p>
      <p class="muted">{j['earnings_dynamics'].get('surprises') or ""}</p>
    </div>
  </div>

  <div class="card">
    <h2>Quellen</h2>
    <ul>{srcs or "<li>unbekannt</li>"}</ul>
    <p>JSON: <a href="{j['links']['json_gz']}">{j['links']['json_gz']}</a></p>
  </div>
</div>
</html>"""

def pct(x):
    try:
        return f"{float(x)*100:.1f}%"
    except Exception:
        return "unbekannt"

def num(x):
    try:
        v = float(x)
        if abs(v) >= 1000:  # Tausenderpunkte deutsch
            return f"{v:,.0f}".replace(",", ".")
        return f"{v:.2f}"
    except Exception:
        return "–"

# ------------------------ CLI ------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-html", required=True)
    ap.add_argument("--public-base", required=True)
    args = ap.parse_args()

    payload = build_payload(args.symbol, args.public_base)

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_html).parent.mkdir(parents=True, exist_ok=True)

    # JSON uncompressed (Workflow gzippt danach)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    html = render_html(payload)
    Path(args.out_html).write_text(html, encoding="utf-8")

    print("Wrote:", args.out_json, "and", args.out_html)

if __name__ == "__main__":
    main()
