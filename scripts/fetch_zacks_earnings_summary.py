#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Holt für viele Ticker den 'Earnings Summary' von Zacks,
übersetzt optional mit MarianMT, filtert Passagen, speichert JSON/CSV.

Ausgaben:
- data/processed/zacks_earnings_summary.jsonl
- data/processed/zacks_earnings_summary.csv
- data/reports/zacks_fetch_report.json
- Cache (HTML): data/cache/zacks/<SYMBOL>.html
"""

import asyncio, aiohttp, async_timeout, re, json, csv, hashlib, os, time, random
from pathlib import Path
from bs4 import BeautifulSoup

# ------------------------------------------------------------
# Konfig
# ------------------------------------------------------------
BASES = [
    "https://www.zacks.com/stock/research/{sym}/earnings-calendar",
    "https://www.zacks.com/stock/quote/{sym}/earnings-calendar",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Connection": "keep-alive",
}

ROBOT_HINTS = ("are you a robot", "access denied", "request blocked", "forbidden")
RATE_LIMIT_SEC = 1.6  # langsamer + gleichmäßiger

def zacks_symbol(sym: str) -> str:
    """Konservativ auf US-Namensraum abbilden."""
    s = sym.upper().replace(".", "-")
    for suf in ("-DE","-F","-SW","-MI","-PA","-BR","-MC","-VI","-AS"):
        if s.endswith(suf): s = s[:-len(suf)]
    return s

async def fetch_one(session, symbol, translate=False, translator=None, cache_dir=Path("data/cache/zacks")):
    sym = zacks_symbol(symbol)
    last_err = None
    html = ""
    used_url = ""

    for base in BASES:
        url = base.format(sym=sym)
        used_url = url
        cache_p = cache_dir / f"{sym}.html"

        try:
            if cache_p.exists():
                html = cache_p.read_text(encoding="utf-8", errors="ignore")
            else:
                await asyncio.sleep(RATE_LIMIT_SEC + (os.urandom(1)[0] % 40)/100.0)  # 1.6–2.0s
                async with aiohttp.ClientSession(headers=HEADERS) as s2:
                    async with s2.get(url, headers={"Referer": f"https://www.zacks.com/stock/quote/{sym}"}) as r:
                        if r.status == 403:
                            last_err = "403_forbidden"
                            continue
                        r.raise_for_status()
                        html = await r.text()
                cache_p.write_text(html, encoding="utf-8")

            low = html.lower()
            if any(h in low for h in ROBOT_HINTS) or ("captcha" in low):
                last_err = "robot_block"
                html = ""
                continue

            break  # wir haben brauchbares HTML
        except Exception as e:
            last_err = f"http: {e}"
            html = ""
            continue

    if not html:
        return {"symbol": symbol, "url": used_url, "ok": False, "error": last_err or "no_html"}

    summary_en = parse_earnings_summary(html)
    if not summary_en:
        return {"symbol": symbol, "url": used_url, "ok": False, "error": "no_summary"}

    summary_en = text_cleanup(summary_en)

    summary_de = ""
    if translate and translator:
        try:
            summary_de = translator.translate(summary_en)
            summary_de = text_cleanup(summary_de)
        except Exception:
            summary_de = ""

    doc = {
        "symbol": symbol,
        "source_url": used_url,
        "opinion_en": summary_en,
        "opinion_de": summary_de or None,
        "asof": time.strftime("%Y-%m-%d"),
        "hash": compute_hash({
            "symbol": symbol, "source_url": used_url,
            "opinion_en": summary_en, "opinion_de": summary_de or None,
            "asof": time.strftime("%Y-%m-%d"),
        }),
    }
    return {"ok": True, **doc}

# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------
def ensure_dirs():
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
    if not p.exists(): return []
    out, seen = [], set()
    if p.suffix.lower() == ".csv":
        with p.open("r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            col = None
            if rdr.fieldnames:
                for c in rdr.fieldnames:
                    if c and c.strip().lower() in ("symbol", "ticker"):
                        col = c; break
                if col is None:
                    col = rdr.fieldnames[0]
            for row in rdr:
                t = canon_symbol(row.get(col, ""))
                if t and t not in seen:
                    seen.add(t); out.append(t)
    else:
        for ln in p.read_text(encoding="utf-8").splitlines():
            t = canon_symbol(ln)
            if t and t.lower() not in ("symbol","ticker") and t not in seen:
                seen.add(t); out.append(t)
    return out

def text_cleanup(txt: str) -> str:
    if not txt: return ""
    for pat in REMOVE_PATTERNS:
        txt = re.sub(pat, "", txt, flags=re.I)
    txt = re.sub(r"\s{2,}", " ", txt).strip()
    return txt

def compute_hash(doc: dict) -> str:
    m = hashlib.sha1(json.dumps(doc, sort_keys=True, ensure_ascii=False).encode("utf-8"))
    return m.hexdigest()[:16]

# ------------------------------------------------------------
# Parser
# ------------------------------------------------------------
def parse_earnings_summary(html: str) -> str | None:
    """Zuerst strukturiert (DOM), dann robuster Fallback per Regex (funktioniert
    auch auf r.jina.ai-Content)."""
    if not html:
        return None

    # DOM-Versuch
    try:
        soup = BeautifulSoup(html, "lxml")
        hdr = None
        for tag in soup.find_all(["h2","h3","h4"]):
            if tag.get_text(strip=True).lower() == "earnings summary":
                hdr = tag; break

        block_text = None
        if hdr:
            nxt = hdr.find_next(["p","div"])
            if nxt:
                paras = []
                cur = nxt
                while cur and cur.name not in ("h2","h3","h4"):
                    if cur.name == "p":
                        t = cur.get_text(" ", strip=True)
                        if t: paras.append(t)
                    cur = cur.find_next_sibling()
                if not paras and nxt.name == "div":
                    paras = [p.get_text(" ", strip=True) for p in nxt.find_all("p")]
                block_text = " ".join(paras).strip()
        if block_text:
            return block_text
    except Exception:
        pass

    # Text-Fallback (auch für r.jina.ai geeignet)
    txt = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m = re.search(r"Earnings Summary\s*([^.].+?)(?:Earnings History|$)", txt, flags=re.I|re.S)
    if m:
        return m.group(1).strip()
    return None

# ------------------------------------------------------------
# Optionale Übersetzung (lazy import)
# ------------------------------------------------------------
class MarianTranslator:
    def __init__(self, model_name: str = "Helsinki-NLP/opus-mt-en-de"):
        try:
            from transformers import MarianMTModel, MarianTokenizer  # type: ignore
        except Exception as e:
            raise RuntimeError("transformers/PyTorch fehlen. Lass --translate weg oder installiere die Backends.") from e
        self.tokenizer = MarianTokenizer.from_pretrained(model_name)
        self.model     = MarianMTModel.from_pretrained(model_name)

    def translate(self, text: str) -> str:
        if not text: return ""
        tok = self.tokenizer([text], return_tensors="pt", truncation=True)
        gen = self.model.generate(**tok, max_new_tokens=512)
        out = self.tokenizer.batch_decode(gen, skip_special_tokens=True)[0]
        return out

# ------------------------------------------------------------
# Fetch (mit 403-Fallback)
# ------------------------------------------------------------
async def fetch_one(session: aiohttp.ClientSession, symbol: str, translate=False, translator=None,
                    cache_dir: Path = Path("data/cache/zacks")) -> dict:
    url = BASE.format(sym=symbol)
    cache_p = cache_dir / f"{symbol}.html"
    html = ""

    # Höfliche Pause mit Jitter VOR dem Request
    await asyncio.sleep(RATE_DELAY_SEC * (0.8 + 0.4*random.random()))

    if cache_p.exists():
        html = cache_p.read_text(encoding="utf-8", errors="ignore")
    else:
        # 1) Normaler Versuch
        try:
            async with async_timeout.timeout(HTTP_TIMEOUT):
                async with session.get(url, headers=HEADERS) as r:
                    if r.status == 200:
                        html = await r.text()
                    else:
                        raise aiohttp.ClientResponseError(
                            request_info=r.request_info, history=r.history,
                            status=r.status, message=f"status={r.status}"
                        )
        except Exception as e1:
            # 2) Fallback über r.jina.ai (Text-Renderer)
            try:
                fb_url = FALLBACK_PREFIX + url.replace("https://", "").replace("http://", "")
                async with async_timeout.timeout(HTTP_TIMEOUT):
                    async with session.get(fb_url, headers={"User-Agent": HEADERS["User-Agent"]}) as r2:
                        r2.raise_for_status()
                        html = await r2.text()
            except Exception as e2:
                return {"symbol": symbol, "ok": False, "error": f"http: {e1} ; fb: {e2}", "source_url": url}

        # Cache schreiben
        cache_p.write_text(html, encoding="utf-8")

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
        except Exception:
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

# ------------------------------------------------------------
# Runner (sequentiell, bewusst ohne Concurrency wegen Anti-Bot)
# ------------------------------------------------------------
async def runner(watchlist: str, out_jsonl: str, out_csv: str, translate: bool):
    ensure_dirs()
    syms = read_watchlist(watchlist)
    if not syms:
        print("Keine Symbole gefunden."); return

    # Viele DE/EU-Symbole gibt es bei Zacks nicht → optional filtern
    # syms = [s for s in syms if "." not in s]  # nur US testen

    translator = None
    if translate:
        try:
            translator = MarianTranslator()
        except Exception as e:
            print(f"[warn] Übersetzung deaktiviert: {e}")
            translate = False

    results, errs = [], []
    async with aiohttp.ClientSession() as session:
        for i, sym in enumerate(syms, 1):
            sym = sym.strip().upper().replace(".", "-")  # Zacks nutzt oft Bindestrich
            res = await fetch_one(session, sym, translate=translate, translator=translator)
            if res.get("ok"):
                results.append(res)
                print(f"[{i}/{len(syms)}] ✔ {sym}")
            else:
                errs.append(res)
                print(f"[{i}/{len(syms)}] ✖ {sym} – {res.get('error')}")

    # JSONL
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # CSV
    cols = ["symbol","asof","source_url","opinion_en","opinion_de","hash"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in results:
            w.writerow({k: r.get(k) for k in cols})

    report = {
        "watchlist": watchlist,
        "total": len(syms),
        "ok": len(results),
        "failed": len(errs),
        "errors": errs[:50],
        "out_jsonl": out_jsonl,
        "out_csv": out_csv,
    }
    Path("data/reports/zacks_fetch_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nDone. OK={report['ok']} / {report['total']}  → {out_jsonl} ; {out_csv}")

# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="watchlists/mylist.txt", help="txt oder csv (Spalte 'symbol')")
    ap.add_argument("--out-jsonl", default="data/processed/zacks_earnings_summary.jsonl")
    ap.add_argument("--out-csv",   default="data/processed/zacks_earnings_summary.csv")
    ap.add_argument("--translate", action="store_true", help="MarianMT EN→DE aktivieren (transformers+PyTorch nötig)")
    args = ap.parse_args()
    asyncio.run(runner(args.watchlist, args.out_jsonl, args.out_csv, args.translate))
