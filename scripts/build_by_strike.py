import csv, os

OUT = "data/processed/options_oi_by_strike.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

# TODO: Hier später echte Daten einfügen.
rows = [
    {"symbol":"AAPL","expiry":"2025-12-19","strike":"240","call_oi":"50000","put_oi":"30000"},
    {"symbol":"AAPL","expiry":"2025-12-19","strike":"230","call_oi":"42000","put_oi":"34000"},
    {"symbol":"ES1!","expiry":"2025-11-21","strike":"5000","call_oi":"180000","put_oi":"220000"},
]
with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["symbol","expiry","strike","call_oi","put_oi"])
    w.writeheader(); w.writerows(rows)
print(f"Wrote {OUT} ({len(rows)} rows)")
