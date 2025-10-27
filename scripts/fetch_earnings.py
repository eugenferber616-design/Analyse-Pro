import os, sys, time, requests
from datetime import datetime, timedelta
from util import read_yaml, write_json, load_env
from cache import RateLimiter, get_json, set_json

CONF = read_yaml('config/config.yaml') or {}
ENV  = load_env() or {}
FINNHUB = 'https://finnhub.io/api/v1'

# ---- robuste Defaults aus config.yaml ziehen ----
earn_cfg = CONF.get('earnings', {}) or {}
LOOKAHEAD_DAYS = int(earn_cfg.get('lookahead_days', 365))
WINDOW_DAYS    = int(earn_cfg.get('window_days', 14))
CACHE_TTL_DAYS = int(earn_cfg.get('cache_ttl_days', 7))

rl_cfg     = CONF.get('rate_limits', {}) or {}
PER_MIN    = int(rl_cfg.get('finnhub_per_minute', 50))
# Falls kein Wert gesetzt wurde: ~60_000 / PER_MIN, mind. 1.2s
SLEEP_MS   = int(rl_cfg.get('finnhub_sleep_ms', max(1200, 60000 // max(1, PER_MIN))))

def daterange(a, b, step=14):
    cur = a
    while cur <= b:
        nxt = min(b, cur + timedelta(days=step - 1))
        yield cur, nxt
        cur = nxt + timedelta(days=1)

def fetch(a, b, lim, token, retries=3):
    """Finnhub Earnings-Kalender für [a,b] abholen (mit RateLimit & 429-Retry)."""
    params = {
        'from': a.strftime('%Y-%m-%d'),
        'to':   b.strftime('%Y-%m-%d'),
        'token': token
    }
    for attempt in range(retries):
        lim.wait()
        r = requests.get(f"{FINNHUB}/calendar/earnings", params=params, timeout=30)
        if r.status_code == 429 and attempt + 1 < retries:
            # kurz warten und erneut versuchen
            time.sleep(2.5)
            continue
        r.raise_for_status()
        j = r.json() or {}
        return j.get('earningsCalendar', []) or j.get('earnings', []) or []
    return []

def main():
    # Token-Fallback: FINNHUB_TOKEN oder FINNHUB_API_KEY
    token = ENV.get('FINNHUB_TOKEN') or ENV.get('FINNHUB_API_KEY') or os.getenv('FINNHUB_TOKEN') or os.getenv('FINNHUB_API_KEY')
    if not token:
        print('No FINNHUB_TOKEN (or FINNHUB_API_KEY) provided')
        return 0

    start = datetime.utcnow()
    end   = start + timedelta(days=LOOKAHEAD_DAYS)
    step  = max(1, int(WINDOW_DAYS))

    lim = RateLimiter(PER_MIN, SLEEP_MS)


    all_rows = []
    for a, b in daterange(start, end, step):
        key = f"earn:{a:%Y-%m-%d}:{b:%Y-%m-%d}"
        cached = get_json(key)
        if cached:
            all_rows += cached
            continue
        try:
            rows = fetch(a, b, lim, token)
            if rows:
                set_json(key, rows, ttl_days=CACHE_TTL_DAYS)
                all_rows += rows
        except Exception as e:
            print('earn window fail', a, b, e)

    # jeweils frühestes kommendes Datum pro Symbol
    by = {}
    for r in all_rows:
        sym = r.get('symbol') or r.get('ticker')
        d   = r.get('date') or r.get('time') or r.get('epsReportDate')
        if not sym or not d:
            continue
        day = d[:10]
        if sym not in by or day < by[sym]['next_date']:
            by[sym] = {'symbol': sym, 'next_date': day}

    out = list(by.values())
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('docs', exist_ok=True)
    write_json('data/processed/earnings_next.json', out)
    write_json('docs/earnings_next.json', out)
    print('earnings', len(out))
    return 0

if __name__ == '__main__':
    sys.exit(main())
