# scripts/cache.py
import os
import json
import time
import sqlite3
import threading

# --- SQLite Key/Value Cache -----------------------------------------------

DB_PATH = os.path.join("data", "cache", "cache.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
_con = sqlite3.connect(DB_PATH, check_same_thread=False)
_cur = _con.cursor()
_cur.execute("""
CREATE TABLE IF NOT EXISTS kv (
  k  TEXT PRIMARY KEY,
  v  TEXT NOT NULL,
  ts INTEGER DEFAULT (strftime('%s','now'))
)""")
_cur.execute("CREATE INDEX IF NOT EXISTS ix_kv_ts ON kv(ts)")
_con.commit()

def get_json(key: str):
    """Liest JSON aus dem Cache, gibt Python-Objekt oder None zurück."""
    row = _cur.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None

def set_json(key: str, value):
    """Schreibt Python-Objekt als JSON in den Cache (upsert)."""
    payload = json.dumps(value, separators=(",", ":"))
    _cur.execute(
        "INSERT OR REPLACE INTO kv(k,v,ts) VALUES (?, ?, strftime('%s','now'))",
        (key, payload),
    )
    _con.commit()

# --- Einfache Rate-Limiter (z.B. für Finnhub) ------------------------------

class RateLimiter:
    """
    Token-Bucket über Sekunden- und Minutenfenster.
    Standardwerte sind konservativ, passe sie ggf. an deinen Plan an.
    """
    def __init__(self, per_second: int = 4, per_minute: int = 50):
        self.per_second = per_second
        self.per_minute = per_minute
        self._sec_tokens = per_second
        self._min_tokens = per_minute
        self._last_sec = time.time()
        self._last_min = self._last_sec
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.time()

            # Sekundentopf regenerieren
            if now - self._last_sec >= 1:
                self._sec_tokens = self.per_second
                self._last_sec = now

            # Minutentopf regenerieren
            if now - self._last_min >= 60:
                self._min_tokens = self.per_minute
                self._last_min = now

            # Falls keine Tokens da sind -> schlafen bis mindestens ein Topf wieder gefüllt ist
            while self._sec_tokens <= 0 or self._min_tokens <= 0:
                now = time.time()
                sleep_sec  = max(0.0, 1.0  - (now - self._last_sec))
                sleep_min  = max(0.0, 60.0 - (now - self._last_min))
                to_sleep = max(0.05, min(sleep_sec if self._sec_tokens <= 0 else 0.0,
                                         sleep_min if self._min_tokens <= 0 else 0.0) or 0.05)
                time.sleep(to_sleep)

                # nach dem Schlafen nochmal regenerieren
                now = time.time()
                if now - self._last_sec >= 1:
                    self._sec_tokens = self.per_second
                    self._last_sec = now
                if now - self._last_min >= 60:
                    self._min_tokens = self.per_minute
                    self._last_min = now

            # Token verbrauchen
            self._sec_tokens -= 1
            self._min_tokens -= 1
