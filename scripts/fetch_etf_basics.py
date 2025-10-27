import os
import csv
import requests
import sys
import time
from util import load_env
from cache import RateLimiter

API   = "https://finnhub.io/api/v1"
ENV   = load_env() or {}
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

# -------- Helpers --------

def read_list(path: str):
    """
    Liest eine Watchlist-Datei (TXT oder CSV) ein.
    - Leerzeilen/Kommentare (# ...) werden ignoriert.
    - Headerzeilen wie 'symbol,...' werden ignoriert.
    - Nimmt die erste Spalte als Symbol.
    """
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            # CSV? nimm erstes Feld
            sym = raw.split(",")[0].strip()
            if not sym or sym.lower().startswith("symbol"):
                continue
            out.append(sym.upper())
    return out

def get_json(url: str, params: dict, retries: int = 3, sleep_sec: float = 2.0):
    for attempt in range(retries):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429 and attempt + 1 < retries:
            time.sleep(sleep_sec)
            continue
        r.raise_for_status()
        return r.json() or {}
    return {}

def val(d, *keys):
    """Sicherer Zugriff mit mehreren Kandidat-Schlüsseln; gibt None wenn alle fehlen."""
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, "", []):
            return d[k]
    return None

# -------- Main logic --------

def fetch_one_etf(symbol: str, token: str, rl: RateLimiter) -> dict:
    """Versucht mehrere Endpunkte, um möglichst viele Felder zu füllen."""
    base = {
        "symbol": symbol,
        "name": None,
        "category": None,
        "asset_class": None,
        "expense_ratio": None,
        "aum": None,
        "nav": None,
        "beta": None,
        "currency": None,
    }

    # 1) Primär: etf/profile
    rl.wait()
    try:
        j = get_json(f"{API}/etf/profile", params={"symbol": symbol, "token": token})
    except Exception as e:
        print("etf/profile fail", symbol, e)
        j = {}
    base["name"]         = val(j, "name", "etfName")
    base["category"]     = val(j, "category")
    base["asset_class"]  = val(j, "assetClass", "asset_class")
    base["expense_ratio"]= val(j, "expenseRatio", "expense_ratio")
    base["aum"]          = val(j, "totalAssets", "aum")
    base["nav"]          = val(j, "nav")
    base["currency"]     = val(j, "currency")

    # 2) Fallback: stock/profile2 (liefert u.a. name, currency, exchange)
    if not base["name"] or not base["currency"]:
        rl.wait()
        try:
            p2 = get_json(f"{API}/stock/profile2", params={"symbol": symbol, "token": token})
            base["name"]     = base["name"] or val(p2, "name")
            base["currency"] = base["currency"] or val(p2, "currency")
        except Exception as e:
            print("stock/profile2 fail", symbol, e)

    # 3) Optional: Beta über stock/metric (kann bei ETFs leer sein, klappt aber oft)
    if base["beta"] is None:
        rl.wait()
        try:
            m = get_json(f"{API}/stock/metric", params={"symbol": symbol, "metric": "all", "token": token})
            metrics = m.get("metric") or {}
            base["beta"] = val(metrics, "beta", "Beta")
        except Exception as e:
            print("stock/metric fail", symbol, e)

    return base

def main(wl: str, outcsv: str) -> int:
    if not TOKEN:
        print("No FINNHUB token")
        return 0

    syms = read_list(wl)
    if not syms:
        print(f"Watchlist '{wl}' ist leer oder nicht gefunden.")
        return 0

    os.makedirs(os.path.dirname(outcsv), exist_ok=True)

    fieldnames = ["symbol", "name", "category", "asset_class", "expense_ratio", "aum", "nav", "beta", "currency"]
    rl = RateLimiter(50, 1300)  # per_minute, sleep_ms

    rows = []
    for s in syms:
        try:
            row = fetch_one_etf(s, TOKEN, rl)
            rows.append(row)
        except Exception as e:
            print("etf fail", s, e)

    # rausfiltern, wenn wirklich gar nichts kam (optional)
    cleaned = [r for r in rows if any(r.get(k) not in (None, "", []) for k in fieldnames if k != "symbol")]

    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(cleaned)

    print("wrote", outcsv, len(cleaned), "rows (from", len(rows), "symbols)")
    return 0

if __name__ == "__main__":
    wl = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/etf_sample.txt"
    outcsv = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/etf_basics.csv"
    sys.exit(main(wl, outcsv))
