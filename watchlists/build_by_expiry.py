import csv, os, datetime as dt

OUT = "data/processed/options_oi_by_expiry.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

# TODO: Hier später echte Daten einfügen (dein Pipeline-Fetch).
rows = [
    {"symbol":"AAPL","expiry":"2025-12-19","total_call_oi":"120000","total_put_oi":"90000"},
    {"symbol":"ES1!","expiry":"2025-11-21","total_call_oi":"350000","total_put_oi":"380000"},
]
with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["symbol","expiry","total_call_oi","total_put_oi"])
    w.writeheader(); w.writerows(rows)
print(f"Wrote {OUT} ({len(rows)} rows)")
