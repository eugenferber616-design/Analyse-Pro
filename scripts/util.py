from datetime import datetime, timezone
import os, json, yaml

def now_utc_iso(): return datetime.now(timezone.utc).isoformat()

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def read_yaml(p):
  with open(p,'r',encoding='utf-8') as f: return yaml.safe_load(f)

def load_env():
  import os
  return {k:os.getenv(k,'') for k in ['FINNHUB_TOKEN','FRED_API_KEY','OPENAI_API_KEY']}

def write_json(p,obj):
  ensure_dir(os.path.dirname(p))
  with open(p,'w',encoding='utf-8') as f: json.dump(obj,f,ensure_ascii=False,indent=2)

def read_json(p, default=None):
  try:
    with open(p,'r',encoding='utf-8') as f: return json.load(f)
  except FileNotFoundError:
    return default
