import os, sys, requests
from util import read_yaml, write_json, load_env
from cache import RateLimiter
CONF=read_yaml('config/config.yaml'); ENV=load_env(); FINNHUB='https://finnhub.io/api/v1'

def fetch_exchange(code,lim):
  lim.wait(); r=requests.get(f"{FINNHUB}/stock/symbol",params={'exchange':code,'token':ENV['FINNHUB_TOKEN']},timeout=30); r.raise_for_status(); return r.json()

def main():
  if not ENV['FINNHUB_TOKEN']:
    print('No FINNHUB_TOKEN'); return 0
  lim=RateLimiter(CONF['rate_limits']['finnhub_per_minute'], CONF['rate_limits']['finnhub_sleep_ms'])
  rows=[]
  if 'US' in CONF['universe']['regions']:
    rows+=fetch_exchange('US',lim)
  if 'EU' in CONF['universe']['regions']:
    for ex in CONF['universe']['exchanges_eu']:
      try: rows+=fetch_exchange(ex,lim)
      except Exception as e: print('EU ex err',ex,e)
  seen=set(); out=[]
  for r in rows:
    sym=r.get('symbol') or r.get('displaySymbol');
    if sym and sym not in seen:
      seen.add(sym); out.append({'symbol':sym,'description':r.get('description',''),'currency':r.get('currency',''),'type':r.get('type','')})
  write_json('data/processed/symbols_universe.json', out)
  write_json('docs/symbols_universe.json', out)
  print('symbols',len(out))
if __name__=='__main__':
  os.makedirs('data/processed',exist_ok=True); os.makedirs('docs',exist_ok=True); sys.exit(main())
