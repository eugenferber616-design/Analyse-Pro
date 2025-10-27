# scripts/fetch_earnings.py
import os, sys, time, csv, requests
from datetime import datetime, timedelta
from util import read_yaml, write_json, load_env
from cache import RateLimiter, get_json, set_json

CONF = read_yaml('config/config.yaml') or {}
ENV  = load_env() or {}
FINNHUB = 'https://finnhub.io/api/v1'

earn_cfg = CONF.get('earnings', {}) or {}
LOOKAHEAD_DAYS = int(earn_cfg.get('lookahead_days', 365))
WINDOW_DAYS    = int(earn_cfg.get('window_days', 14))
CACHE_TTL_DAYS = int(earn_cfg.get('cache_ttl_days', 7))

rl_cfg   = CONF.get('rate_limits', {}) or {}
PER_MIN  = int(rl_cfg.get('finnhub_per_minute', 50))
SLEEP_MS = int(rl_cfg.get('finnhub_sleep_ms', max(1200, 60000 // max(1, PER_MIN))))

def load_watchlist(path):
    if not path or not os.path.exists(path):
        return None
    syms = set()
    with open(path, 'r', newline='', encoding='utf-8') as f:
        # CSV mit Header "symbol" ODER einfache Ein-Zeilen-Liste
        sample = f.read(2048)
        f.seek(0)
        if ',' in sample or 'symbol' in sample.lower():
            rdr = csv.DictReader(f)
            for row in rdr:
                s = (row.get('symbol') or row.get('ticker') or '').strip().upper()
                if s: syms.add(s)
        else:
            for line in f:
                s = line.strip().upper()
                if s and not s.startswith('#'):
                    syms.add(s)
    return syms if syms else None

def daterange(a, b, step):
    cur = a
    while cur <= b:
        nxt = min(b, cur + timedelta(days=step - 1))
        yield cur, nxt
        cur = nxt + timedelta(days=1)

def fetch(a, b, lim, token, retries=3):
    params = {'from': a.strftime('%Y-%m-%d'), 'to': b.strftime('%Y-%m-%d'), 'token': token}
    for attempt in range(retries):
        lim.wait()
        r = requests.get(f"{FINNHUB}/calendar/earnings", params=params, timeout=30)
        if r.status_code == 429 and attempt + 1 < retries:
            time.sleep(2.5); continue
        r.raise_for_status()
        j = r.json() or {}
        return j.get('earningsCalendar', []) or j.get('earnings', []) or []
    return []

def main():
    token = (ENV.get('FINNHUB_TOKEN') or ENV.get('FINNHUB_API_KEY') or
             os.getenv('FINNHUB_TOKEN') or os.getenv('FINNHUB_API_KEY'))
    if not token:
        print('No FINNHUB_TOKEN (or FINNHUB_API_KEY) provided'); return 0

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--watchlist', default='', help='watchlists/mylist.csv (optional)')
    ap.add_argument('--window-days', type=int, default=WINDOW_DAYS)
    ap.add_argument('--lookahead-days', type=int, default=LOOKAHEAD_DAYS)
    args = ap.parse_args()

    wl = load_watchlist(args.watchlist) if args.watchlist else None
    if wl: print(f"[watchlist] {len(wl)} Symbole geladen")

    start = datetime.utcnow()
    end   = start + timedelta(days=args.lookahead_days)
    step  = max(1, int(args.window_days))
    lim   = RateLimiter(PER_MIN, SLEEP_MS)

    all_rows = []
    total_windows = 0
    for a, b in daterange(start, end, step):
        total_windows += 1
        key = f"earn:{a:%Y-%m-%d}:{b:%Y-%m-%d}"
        cached = get_json(key)
        if cached:
            print(f"[cache] {key} -> {len(cached)}")
            all_rows += cached; continue
        try:
            rows = fetch(a, b, lim, token)
            print(f"[fetch] {a:%Y-%m-%d}..{b:%Y-%m-%d} -> {len(rows)}")
            if rows:
                set_json(key, rows, ttl_days=CACHE_TTL_DAYS)
                all_rows += rows
        except Exception as e:
            print('earn window fail', a, b, e)

    # konsolidieren: frühestes kommendes Datum pro Symbol
    by = {}
    for r in all_rows:
        sym = (r.get('symbol') or r.get('ticker') or '').upper()
        d   = r.get('date') or r.get('time') or r.get('epsReportDate')
        if not sym or not d:
            continue
        if wl and sym not in wl:
            continue
        day = d[:10]
        if sym not in by or day < by[sym]['next_date']:
            by[sym] = {'symbol': sym, 'next_date': day}

    out = list(by.values())
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('docs', exist_ok=True)
    write_json('data/processed/earnings_next.json', out)
    write_json('docs/earnings_next.json', out)

    print(f"[summary] windows={total_windows} symbols={len(out)} out=data/processed/earnings_next.json")
    return 0

if __name__ == '__main__':
    sys.exit(main())
