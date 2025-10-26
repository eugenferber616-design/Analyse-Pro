import os, sys, requests
from datetime import datetime, timedelta
from util import read_yaml, write_json, load_env
from cache import RateLimiter, get_json, set_json
CONF=read_yaml('config/config.yaml'); ENV=load_env(); FINNHUB='https://finnhub.io/api/v1'

def daterange(a,b,step=14):
  cur=a
  while cur<=b:
    nxt=min(b,cur+timedelta(days=step-1)); yield cur,nxt; cur=nxt+timedelta(days=1)

def fetch(a,b,lim):
  lim.wait(); r=requests.get(f"{FINNHUB}/calendar/earnings",params={'from':a.strftime('%Y-%m-%d'),'to':b.strftime('%Y-%m-%d'),'token':ENV['FINNHUB_TOKEN']},timeout=30); r.raise_for_status(); j=r.json(); return j.get('earningsCalendar',[]) or j.get('earnings',[]) or []

def main():
  if not ENV['FINNHUB_TOKEN']:
    print('No FINNHUB_TOKEN'); return 0
  start=datetime.utcnow(); end=start+timedelta(days=CONF['earnings']['lookahead_days']); step=CONF['earnings']['window_days']
  lim=RateLimiter(CONF['rate_limits']['finnhub_per_minute'], CONF['rate_limits']['finnhub_sleep_ms'])
  all=[]
  for a,b in daterange(start,end,step):
    key=f"earn:{a:%Y-%m-%d}:{b:%Y-%m-%d}"; c=get_json(key)
    if c: all+=c; continue
    try:
      rows=fetch(a,b,lim); set_json(key,rows,ttl_days=CONF['earnings']['cache_ttl_days']); all+=rows
    except Exception as e:
      print('earn window fail',a,b,e)
  by={}
  for r in all:
    sym=r.get('symbol') or r.get('ticker'); d=r.get('date') or r.get('time') or r.get('epsReportDate')
    if not sym or not d: continue
    day=d[:10]
    if sym not in by or day<by[sym]['next_date']: by[sym]={'symbol':sym,'next_date':day}
  out=list(by.values())
  write_json('data/processed/earnings_next.json', out)
  write_json('docs/earnings_next.json', out)
  print('earnings',len(out))
if __name__=='__main__': os.makedirs('data/processed',exist_ok=True); os.makedirs('docs',exist_ok=True); sys.exit(main())
