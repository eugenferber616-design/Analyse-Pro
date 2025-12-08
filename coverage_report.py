import sys
from util import read_json, write_json, now_utc_iso
uni=read_json('data/processed/symbols_universe.json',[])
earn=read_json('data/processed/earnings_next.json',[])
us=set([x['symbol'] for x in uni]); es=set([x['symbol'] for x in earn])
rep={'timestamp':now_utc_iso(),'universe_total':len(us),'earnings_available':len(us & es),'coverage_pct':(100.0*len(us & es)/len(us) if us else 0.0),'sample_missing':sorted(list(us-es))[:2000]}
write_json('data/processed/coverage_report.json', rep)
write_json('docs/coverage_report.json', rep)
print('ok')
