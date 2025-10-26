import os, sqlite3, json, time, threading
DB_PATH='data/cache/cache.db'
os.makedirs('data/cache', exist_ok=True)
lock=threading.Lock()

def set_json(k,v,ttl_days=7):
  with lock:
    con=sqlite3.connect(DB_PATH)
    con.execute('CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT, ts REAL)')
    con.execute('INSERT OR REPLACE INTO kv VALUES(?,?,?)',(k,json.dumps({'value':v,'ttl_days':ttl_days,'saved_at':time.time()}),time.time()))
    con.commit(); con.close()

def get_json(k):
  with lock:
    con=sqlite3.connect(DB_PATH)
    cur=con.execute('SELECT v FROM kv WHERE k=?',(k,))
    row=cur.fetchone(); con.close()
  if not row: return None
  d=json.loads(row[0]); ttl=d.get('ttl_days',7); saved=d.get('saved_at',time.time())
  if time.time()-saved>ttl*86400: return None
  return d.get('value')

class RateLimiter:
  def __init__(self, per_minute=30, sleep_ms=2500):
    self.per_minute=per_minute; self.sleep_ms=sleep_ms; self.calls=[]
  def wait(self):
    import time
    now=time.time(); self.calls=[t for t in self.calls if now-t<60]
    if len(self.calls)>=self.per_minute: time.sleep(self.sleep_ms/1000.0)
    self.calls.append(time.time())
