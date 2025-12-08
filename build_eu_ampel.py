# scripts/build_eu_ampel.py
import os, sys, pandas as pd, json
p = "data/processed/fred_oas.csv"
if not os.path.exists(p):
    print("EU-Ampel: kein fred_oas.csv – skip")
    sys.exit(0)

df = pd.read_csv(p)
if df.empty or "date" not in df or "value" not in df:
    print("EU-Ampel: fred_oas.csv leer/ohne Spalten – skip")
    sys.exit(0)

# Beispiel: nur letzte verfügbare EU_IG Zahl als „Ampel“
df["date"] = pd.to_datetime(df["date"])
eu_ig = df[(df["bucket"]=="EU_IG")].sort_values("date").tail(1)
value = None if eu_ig.empty else float(eu_ig["value"].iloc[0])

os.makedirs("data/reports/eu_checks", exist_ok=True)
out = {"last_eu_ig_oas": value}
open("data/reports/eu_checks/ampel_preview.json","w").write(json.dumps(out, indent=2))
print("EU-Ampel OK:", out)
