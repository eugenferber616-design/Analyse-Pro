import os, sys, pandas as pd
from datetime import datetime
from util import read_json, write_json, read_yaml
CONF=read_yaml('config/config.yaml')
uni=read_json('data/processed/symbols_universe.json',[])
earn={x['symbol']:x for x in read_json('data/processed/earnings_next.json',[])}
rows=[]; today=datetime.utcnow().date()
for r in uni[:CONF['universe']['max_symbols']]:
  sym=r['symbol']; e=earn.get(sym)
  if e:
    nd=datetime.fromisoformat(e['next_date']).date(); d=(nd-today).days
    rows.append({'symbol':sym,'has_earnings_date':True,'next_earnings_date':nd.isoformat(),'days_to_earnings':d})
  else:
    rows.append({'symbol':sym,'has_earnings_date':False,'next_earnings_date':'','days_to_earnings':''})
import pandas as pd
os.makedirs('data/processed',exist_ok=True)
pd.DataFrame(rows).to_csv('data/processed/scanner_basics.csv', index=False)
write_json('docs/scanner_basics.json', rows)
print('scanner rows',len(rows))
