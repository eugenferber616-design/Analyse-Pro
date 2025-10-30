#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Holt für viele Ticker den 'Earnings Summary' von Zacks,
übersetzt optional mit MarianMT (falls installiert), filtert Passagen,
schreibt JSONL + CSV und cached das HTML pro Symbol.

Ausgaben:
- data/processed/zacks_earnings_summary.jsonl
- data/processed/zacks_earnings_summary.csv
- data/reports/zacks_fetch_report.json
- Cache (HTML): data/cache/zacks/<SYMBOL>.html
"""

from __future__ import annotations

import asyncio
import aiohttp
import re
import json
import csv
import hashlib
import os
import time
from pathlib import Path
from typing import Optional
from bs4 import BeautifulSoup

# ---------- Konfig ----------
BASE = "https://www.zacks.com/stock/research/{sym}/earnings-calendar"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (research; +https://example.org)",
    "Accept-Language": "en-US,en;q=0.8",
}
RATE_DELAY_SEC = 1.2          # ~1 Request / 1.2s
HTTP_TIMEOUT = 30             # Sekunden
CONNECT_LIMIT = 4             # gleichzeitige Verbindungen

# Typische Floskeln/Blöcke entfernen (regex):
REMOVE_PATTERNS = [
    r"\bResearch for [A-Z.\-]+\b.*",         # CTA-Text unten
    r"\bView Zacks.*?Calendar\b.*",          # Navigationshinweise
]

# Kleine Ersetzungen nach Übersetzung (optional):
REPLACE_MAP = {
    r"\bearnings surprise\b": "Gewinnüberraschung",
}

# ---------- Utility ----------
def ensure_dirs() -> None:
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("data/reports").mkdir(parents=True, exist_ok=True)
    Path("data/cache/zacks").mkdir(parents=True, exist_ok=True)

def canon_symbol(s: str) -> str:
    s = (s or "").strip()
    s = s.split("#", 1)[0].split("//", 1)[0].strip()
    if "," in s:
        s = s.split(",", 1)[0].strip()
    return s

def read_watchlist(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    out, seen = [], set()
    if p.suffix.lower() == ".csv":
        with p.open("r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            col = None
            if rdr.fieldnames:
                for c in rdr.fieldnames:
                    if c and c.lower().strip() in ("symbol", "ticker"):
                        col = c
                        break
                if col is None:
                    col = rdr.fieldnames[0]
            for row in rdr:
                t = canon_symbol(row.get(col, ""))
                if t and t not in seen:
                    seen.add(t); out.append(t)
    else:
        for ln in p.read_text(encoding="utf-8").splitlines():
            t = canon_symbol(ln)
            if t and t.lower() not in ("symbol", "ticker") and t not in seen:
                seen.add(t); out.append(t)
    return out

def text_cleanup(txt: str) -> str:
    if not txt:
        return ""
    for pat in REMOVE_PATTERNS:
        txt = re.sub(pat, "", txt, flags=re.I)
    txt = re.sub(r"\s{2,}", " ", txt).strip()
    return txt

def compute_hash(doc: dict) -> str:
    m = hashlib.sha1(json.dumps(doc, sort_keys=True, ensure_ascii=False).encode("utf-8"))
    return m.hexdigest()[:16]

# ---------- Parser ----------
def parse_earnings_summary(html: str) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")

    # 1) H2/H3/H4 "Earnings Summary"
    hdr = None
    for tag in soup.find_all(["h2", "h3", "h4"]):
        if tag.get_text(strip=True).lower() == "earnings summary":
            hdr = tag
            break

    block_text = None
    if hdr:
        nxt = hdr.find_next(["p", "div"])
        if nxt:
            paras = []
            cur = nxt
            while cur and cur.name not in ("h2", "h3", "h4"):
                if cur.name == "p":
                    t = cur.get_text(" ", strip=True)
                    if t:
                        paras.append(t)
                cur = cur.find_next_sibling()
            if not paras and nxt.name == "div":
                paras = [p.get_text(" ", strip=True) for p in nxt.find_all("p")]
            block_text = " ".join(paras).strip()

    if not block_text:
        # 2) Fallback via Volltext
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"Earnings Summary\s*([^.].+?)(?:Earnings History|$)", txt, flags=re.I | re.S)
        if m:
            block_text = m.group(1).strip()

    return block_text or None

# ---------- Übersetzung (optional, lazy & robust) ----------
class MarianTranslator:
    """Wrapper um MarianMT; lädt nur, wenn verfügbar."""
    def __init__(self, model_name: str = "Helsinki-NLP/opus-mt-en-de"):
        try:
            from transformers import MarianMTModel, MarianTokenizer  # type: ignore
        except Exception as e:
            raise RuntimeError(f"transformers fehlt: {e}")
        self._tok = MarianTokenizer.from_pretrained(model_name)
        self._mdl = MarianMTModel.from_pretrained(model_name)

    def translate(self, text: str) -> str:
        if not text:
            return ""
        tok = self._tok([text], return_tensors="pt", truncation=True)
        gen = self._mdl.generate(**tok, max_new_tokens=512)
        out = self._tok.batch_decode(gen, skip_special_tokens=True)[0]
        return out

def make_translator(enabled: bool) -> Optional[MarianTranslator]:
    if not enabled:
        return None
    try:
        return MarianTranslator()
    except Exception as e:
        print(f"[translate] deaktiviert (Deps fehlen): {e}")
        return None

# ---------- Fetch ----------
async def fetch_one(
    session: aiohttp.ClientSession,
    symbol: str,
    translate: bool = False,
    translator: Optional[MarianTranslator] = None,
    cache_dir: Path = Path("data/cache/zacks"),
) -> dict:
    url = BASE.format(sym=symbol)
    cache_p = cache_dir / f"{symbol}.html"
    html = ""

    if cache_p.exists():
        html = cache_p.read_text(encoding="utf-8", errors="ignore")
    else:
        try:
            async with session.get(url, headers=HEADERS) as r:
                r.raise_for_status()
                html = await r.text()
            cache_p.write_text(html, encoding="utf-8")
            await asyncio.sleep(RATE_DELAY_SEC)
        except Exception as e:
            return {"symbol": symbol, "ok": False, "error": f"http: {e}", "source_url": url}

    summary_en = parse_earnings_summary(html)
    if not summary_en:
        return {"symbol": symbol, "ok": False, "error": "no_summary", "source_url": url}

    summary_en = text_cleanup(summary_en)

    summary_de = ""
    if translate and translator:
        try:
            summary_de = translator.translate(summary_en)
            for k, v in REPLACE_MAP.items():
                summary_de = re.sub(k, v, summary_de, flags=re.I)
            summary_de = text_cleanup(summary_de)
        except Exception as e:
            print(f"[translate:{symbol}] Fehler: {e}")
            summary_de = ""

    doc = {
        "symbol": symbol,
        "source_url": url,
        "opinion_en": summary_en,
        "opinion_de": summary_de or None,
        "asof": time.strftime("%Y-%m-%d"),
    }
    doc["hash"] = compute_hash(doc)
    return {"ok": True, **doc}

async def runner(watchlist: str, out_jsonl: str, out_csv: str, do_translate: bool) -> None:
    ensure_dirs()
    syms = read_watchlist(watchlist)
    if not syms:
        print("Keine Symbole gefunden.")
        return

    # Zacks mag i. d. R. Uppercase/Bindestrich in URLs (AAPL → aapl funktioniert aber auch)
    norm = [s.strip().upper().replace(".", "-") for s in syms]

    translator = make_translator(do_translate)

    results, errs = [], []
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    conn = aiohttp.TCPConnector(limit=CONNECT_LIMIT)

    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        for i, sym in enumerate(norm, 1):
            res = await fetch_one(session, sym, translate=bool(translator), translator=translator)
            if res.get("ok"):
                results.append(res)
                print(f"[{i}/{len(norm)}] ✔ {sym}")
            else:
                errs.append(res)
                print(f"[{i}/{len(norm)}] ✖ {sym} – {res.get('error')}")

    # JSONL
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # CSV
    cols = ["symbol", "asof", "source_url", "opinion_en", "opinion_de", "hash"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k) for k in cols})

    report = {
        "watchlist": watchlist,
        "total": len(norm),
        "ok": len(results),
        "failed": len(errs),
        "errors": errs[:50],
        "out_jsonl": out_jsonl,
        "out_csv": out_csv,
    }
    Path("data/reports/zacks_fetch_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nDone. OK={report['ok']} / {report['total']}  → {out_jsonl} ; {out_csv}")

# ---------- CLI ----------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="watchlists/mylist.txt", help="txt oder csv (Spalte 'symbol')")
    ap.add_argument("--out-jsonl", default="data/processed/zacks_earnings_summary.jsonl")
    ap.add_argument("--out-csv",   default="data/processed/zacks_earnings_summary.csv")
    ap.add_argument("--translate", action="store_true", help="MarianMT EN→DE aktivieren (falls installiert)")
    args = ap.parse_args()
    asyncio.run(runner(args.watchlist, args.out_jsonl, args.out_csv, args.translate))
