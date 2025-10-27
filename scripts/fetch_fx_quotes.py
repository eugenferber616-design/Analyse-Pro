import os, csv, requests, sys, time, re
from datetime import datetime, timezone
from util import load_env
from cache import RateLimiter

API   = "https://finnhub.io/api/v1"
ENV   = load_env() or {}
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

# ---------- Helpers ----------

def read_list(path: str):
    """
    TXT/CSV-Watchlist: ignoriert leere Zeilen, Kommentare, Header.
    Nimmt die erste Spalte als Paar.
    """
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            sym = raw.split(",")[0].strip()
            if not sym or sym.lower().startswith("symbol"):
                continue
            out.append(sym)
    return out

def normalize_pair(p: str, default_exchange: str = "OANDA") -> str:
    """
    Akzeptiert: 'EURUSD', 'EUR/USD', 'FX:EURUSD', 'OANDA:EUR_USD', ...
    Gibt i. d. R. 'OANDA:EUR_USD' zurück.
    """
    p = p.strip().upper()

    # Schon vollständig (EXCH:AAA_BBB)?
    if re.match(r"^[A-Z0-9]+:[A-Z]{3}_[A-Z]{3}$", p):
        return p

    # EXCH:EURUSD -> EXCH:EUR_USD
    m = re.match(r"^([A-Z0-9]+):([A-Z]{3})[/_]?([A-Z]{3})$", p)
    if m:
        exch, a, b = m.groups()
        return f"{exch}:{a}_{b}"

    # Nur Paar ohne Exchange (EURUSD / EUR/USD)
    m = re.match(r"^([A-Z]{3})[/_]?([A-Z]{3})$", p)
    if m:
        a, b = m.groups()
        return f"{default_exchange}:{a}_{b}"

    # FX:EURUSD -> OANDA:EUR_USD
    m = re.match(r"^FX:([A-Z]{3})([A-Z]{3})$", p)
    if m:
        a, b = m.groups()
        return f"{default_exchange}:{a}_{b}"

    # Fallback: alles Nicht-Buchstaben entfernen und 6er-Paar versuchen
    letters = re.sub(r"[^A-Z]", "", p)
    if len(letters) == 6:
        return f"{default_exchange}:{letters[:3]}_{letters[3:]}"

    # Wenn nix passt, zurückgeben wie ist (damit man es im Log sieht)
    return p

def get_json(url: str, params: dict, retries: int = 3, sleep_sec: float = 2.0):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429 and i + 1 < retries:
            time.sleep(sleep_sec)
            continue
        r.raise_for_status()
        return r.json() or {}
    return {}

def ts_to_iso(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return None

# ---------- Main ----------

def main(wl, outcsv):
    if not TOKEN:
        print("No FINNHUB token"); return 0

    raw_syms = read_list(wl)
    if not raw_syms:
        print(f"Watchlist '{wl}' ist leer / fehlt."); return 0

    syms = [normalize_pair(s) for s in raw_syms]
    os.makedirs(os.path.dirname(outcsv), exist_ok=True)

    fieldnames = ["pair","bid","ask","last","timestamp"]
    rl = RateLimiter(50, 1300)

    rows = []
    for s in syms:
        rl.wait()
        try:
            j = get_json(f"{API}/forex/quote", params={"symbol": s, "token": TOKEN})
            bid = j.get("bid"); ask = j.get("ask"); last = j.get("price"); t = j.get("timestamp")
            # Falls komplett leer (kann bei exotischen Paaren vorkommen) -> überspringen
            if bid is None and ask is None and last is None:
                print("fx empty", s)
                continue
            rows.append({
                "pair": s,
                "bid": bid,
                "ask": ask,
                "last": last,
                "timestamp": ts_to_iso(t) if t else None
            })
        except Exception as e:
            print("fx fail", s, e)

    # Nur nicht-leere Zeilen schreiben
    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print("wrote", outcsv, len(rows), "rows from", len(syms), "pairs")
    return 0

if __name__ == "__main__":
    wl = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/fx_sample.txt"
    outcsv = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/fx_quotes.csv"
    sys.exit(main(wl, outcsv))
