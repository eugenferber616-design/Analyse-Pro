
import pandas as pd

DISAGG_FILE = "c:/Users/eugen/Documents/AgenaTrader_QuantCache/cot_20y_disagg_merged.csv"

df = pd.read_csv(DISAGG_FILE, usecols=['market_and_exchange_names', 'report_date_as_yyyy_mm_dd'])
df['report_date_as_yyyy_mm_dd'] = pd.to_datetime(df['report_date_as_yyyy_mm_dd'], errors='coerce')

# Search for Treasury-related markets
tn = df[df['market_and_exchange_names'].str.contains('10-YEAR|TREASURY|BOND|NOTE', case=False, na=False, regex=True)]

print('Treasury-related markets in Disaggregated dataset:')
for m in tn['market_and_exchange_names'].unique():
    max_date = tn[tn['market_and_exchange_names']==m]['report_date_as_yyyy_mm_dd'].max()
    print(f'  {max_date.strftime("%Y-%m-%d") if pd.notna(max_date) else "N/A"} | {m}')
