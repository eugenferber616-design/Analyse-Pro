import os, sys, requests
from util import read_yaml, write_json, load_env
CONF=read_yaml('config/config.yaml'); ENV=load_env(); FRED='https://api.stlouisfed.org/fred/series/observations'

def fred_series(s):
  r=requests.get(FRED, params={'series_id':s,'api_key':ENV['FRED_API_KEY'],'file_type':'json','observation_start':'1990-01-01'}, timeout=30); r.raise_for_status(); return r.json().get('observations',[])

def main():
  if not ENV['FRED_API_KEY']:
    print('No FRED_API_KEY'); return 0
  out={}
  for s in CONF['fred']['series']:
    sid=s['id']; obs=fred_series(sid); out[sid]={'label':s.get('label',sid),'observations':obs[-120:]}
  write_json(CONF['fred']['out_heatmap'], {'series':out})
  print('fred ok')
if __name__=='__main__': os.makedirs('docs',exist_ok=True); sys.exit(main())
