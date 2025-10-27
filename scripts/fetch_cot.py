#!/usr/bin/env python3
import os, json
import pandas as pd
from datetime import datetime

def main():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)
    rows = 0; errors = []

    try:
        from cot_reports import COT
        # Beispiel: Disaggregated, Futures+Options (breiteste Abdeckung)
        cot = COT(report_type="disaggregated", combine=True)
        df = cot.to_dataframe()  # vereinheitlichte Spalten inkl. market_and_exchange_names, report_date
        # Kleine, repo-freundliche Summary: Netto Non-Commercial + Open Interest
        keep = ["market_and_exchange_names","report_date","open_interest_all",
                "prod_merc_long_all","prod_merc_short_all",
                "swap_long_all","swap_short_all",
                "m_money_long_all","m_money_short_all"]
        df = df[keep].copy()
        df.rename(columns={"market_and_exchange_names":"market"}, inplace=True)
        df["report_date"] = pd.to_datetime(df["report_date"])
        df.sort_values(["market","report_date"], inplace=True)
        df.to_csv("data/processed/cot_summary.csv", index=False)
        rows = len(df)
    except Exception as e:
        errors.append({"stage":"cot_reports", "msg": str(e)})

    with open("data/reports/cot_errors.json","w") as f:
        json.dump({"ts": datetime.utcnow().isoformat()+"Z", "rows": rows, "errors": errors}, f, indent=2)
    print(f"wrote data/processed/cot_summary.csv rows={rows}")

if __name__=="__main__":
    main()
