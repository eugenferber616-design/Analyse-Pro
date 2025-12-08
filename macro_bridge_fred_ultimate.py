import yfinance as yf
from fredapi import Fred
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# --- KONFIGURATION ---
# Hol dir einen Key: https://fred.stlouisfed.org/docs/api/api_key.html
# Wenn leer, versucht es Fallback (kann aber limitiert sein)
FRED_API_KEY = "a62b1a06c6cdc4bb8c32d733a492326f" 

BASE_DIR = os.path.join(os.path.expanduser("~"), "Documents", "AgenaTrader_QuantCache")
SAVE_PATH_CSV = os.path.join(BASE_DIR, "macro_status.csv")
SAVE_PATH_AI  = os.path.join(BASE_DIR, "ai_context.txt") # Neue Datei für die KI

class MacroBridgeDetailed:
    def __init__(self):
        self.tickers_yahoo = {
            "YEN": "JPY=X", "BTC": "BTC-USD", "TLT": "TLT",
            "KRE": "KRE",   "SPY": "SPY",     "HYG": "HYG", "LQD": "LQD"
        }
        self.tickers_fred = {
            "SOFR": "SOFR", "IORB": "IORB", "WALCL": "WALCL",
            "WDTGAL": "WDTGAL", "RRP": "RRPONTSYD", "T10Y2Y": "T10Y2Y",
            "HY_SPREAD": "BAA10Y", "EMERGENCY": "TOTBORR"
        }
        self.df_yahoo = pd.DataFrame()
        self.df_fred = pd.DataFrame()
        self.stats = {} # Hier speichern wir die exakten Werte

    def fetch_data(self):
        print("--- LADE DATEN FÜR DETAIL-ANALYSE ---")
        end = datetime.now()
        start = end - timedelta(days=400)
        
        # Yahoo
        try:
            self.df_yahoo = yf.download(
                list(self.tickers_yahoo.values()),
                start=start,
                progress=False
            )['Close']
            inv_y = {v: k for k, v in self.tickers_yahoo.items()}
            self.df_yahoo.rename(columns=inv_y, inplace=True)
            self.df_yahoo.ffill(inplace=True)
        except:
            pass

        # FRED
        try:
            fred = Fred(api_key=FRED_API_KEY)
            fred_data = {}
            for name, ticker in self.tickers_fred.items():
                try:
                    s = fred.get_series(ticker, observation_start=start)
                    fred_data[name] = s
                except:
                    pass
            if fred_data:
                self.df_fred = pd.DataFrame(fred_data)
                self.df_fred.ffill(inplace=True)
        except:
            pass

    def calculate_stats(self):
        # Wir berechnen die Werte, egal ob Alarm oder nicht
        s = {}
        
        # 1. LIVE DATEN
        try:
            s['Yen_5d'] = (self.df_yahoo['YEN'].iloc[-1] / self.df_yahoo['YEN'].iloc[-6] - 1)
            s['BTC_3d'] = (self.df_yahoo['BTC'].iloc[-1] / self.df_yahoo['BTC'].iloc[-4] - 1)
            s['Bond_3d'] = (self.df_yahoo['TLT'].iloc[-1] / self.df_yahoo['TLT'].iloc[-4] - 1)
            s['Bank_vs_Spy'] = (
                self.df_yahoo['KRE'].pct_change(10).iloc[-1]
                - self.df_yahoo['SPY'].pct_change(10).iloc[-1]
            )
            
            # Credit Ratio (Yahoo Fallback)
            ratio = self.df_yahoo['HYG'] / self.df_yahoo['LQD']
            s['Credit_Ratio_Change'] = (
                ratio.iloc[-1] / ratio.rolling(20).mean().iloc[-1] - 1
            )
        except:
            pass

        # 2. FRED DATEN
        if not self.df_fred.empty:
            try:
                s['SOFR'] = self.df_fred['SOFR'].iloc[-1]
                s['IORB'] = self.df_fred['IORB'].iloc[-1]
                s['Funding_Spread'] = s['SOFR'] - s['IORB']
                
                s['NetLiq_Change'] = (
                    (self.df_fred['WALCL'].iloc[-1]
                     - self.df_fred['WDTGAL'].iloc[-1]
                     - self.df_fred['RRP'].iloc[-1] * 1000) / 1000
                    -
                    (self.df_fred['WALCL'].iloc[-20]
                     - self.df_fred['WDTGAL'].iloc[-20]
                     - self.df_fred['RRP'].iloc[-20] * 1000) / 1000
                )
                
                s['Emergency_Change'] = (
                    self.df_fred['EMERGENCY'].iloc[-1]
                    - self.df_fred['EMERGENCY'].iloc[-2]
                )
                s['Credit_Spread'] = self.df_fred['HY_SPREAD'].iloc[-1]
                s['Yield_Curve'] = self.df_fred['T10Y2Y'].iloc[-1]

                # NEU: Yield Curve vor ~1 Monat (ca. 20 Beobachtungen zurück)
                yc_series = self.df_fred['T10Y2Y'].dropna()
                if len(yc_series) > 21:
                    s['Yield_Curve_prev'] = yc_series.iloc[-21]
                else:
                    s['Yield_Curve_prev'] = yc_series.iloc[0]
            except:
                pass
            
        self.stats = s
        return s

    def evaluate_risk(self):
        s = self.stats
        msgs = []
        score = 0
        
        # Logik mit den berechneten Werten
        if s.get('Yen_5d', 0) < -0.025: 
            score += 3; msgs.append("BOJ: Yen Anstieg")
        if s.get('BTC_3d', 0) < -0.04 and s.get('Bond_3d', 0) < -0.01:
            score += 5; msgs.append("TRAP: BTC & Bonds crash")
        if s.get('Funding_Spread', 0) > 0.05:
            score += 3; msgs.append("FUNDING: Stress")
        if s.get('Emergency_Change', 0) > 5:
            score += 4; msgs.append("BANKEN: Notkredite")

        # FIX: echtes Re-Steepening nach Inversion (statt doppelt Yield_Curve)
        yc_now  = s.get('Yield_Curve', 0)
        yc_prev = s.get('Yield_Curve_prev', 0)
        if yc_now > 0 and yc_prev < -0.1:
            score += 4; msgs.append("RECESSION: Steepening")

        total = min(10, score)
        if total == 0 and not msgs:
            msgs.append("Stabil")
        
        return total, msgs

    def run(self):
        self.fetch_data()
        self.calculate_stats()
        score, msgs = self.evaluate_risk()
        s = self.stats

        # 1. CSV für AgenaTrader (Kompakt + Detailblock)
        # Format: Score | Msg | ... || Key:Val | Key:Val ...
        
        # Formatierung der Details für Agena-Anzeige
        details = [
            f"Funding Spr: {s.get('Funding_Spread',0):.3f}%",
            f"NetLiq Chg: {s.get('NetLiq_Change',0):+.0f} Mrd",
            f"Credit Spr: {s.get('Credit_Spread',0):.2f}%",
            f"Yen 5d: {s.get('Yen_5d',0):.1%}",
            f"BTC 3d: {s.get('BTC_3d',0):.1%}",
            f"Bank Rel: {s.get('Bank_vs_Spy',0):.1%}"
        ]
        
        timestamp = datetime.now().strftime('%H:%M')
        # Wir nutzen ein spezielles Trennzeichen "||" für den Detail-Block
        csv_content = (
            f"{score}|"
            + "|".join(msgs)
            + "||"
            + "|".join(details)
            + f"|Update: {timestamp}"
        )
        
        os.makedirs(BASE_DIR, exist_ok=True)
        with open(SAVE_PATH_CSV, "w") as f:
            f.write(csv_content)

        # 2. Text-Datei für KI-Prompt (Ausführlich)
        ai_prompt = f"""
ANALYSE ZEITPUNKT: {datetime.now()}
SYSTEMIC HAZARD REPORT (RAW DATA):

1. INTERBANKEN & FED:
- Funding Spread (SOFR-IORB): {s.get('Funding_Spread', 0):.4f}% (Positiv = Stress)
- Bank Notkredite (Discount Window) Änderung: {s.get('Emergency_Change', 'N/A')} Mrd USD
- Net Liquidity 1M Change: {s.get('NetLiq_Change', 'N/A')} Mrd USD

2. KREDIT & KONJUNKTUR:
- Credit Spreads (BAA10Y): {s.get('Credit_Spread', 'N/A')}%
- Yield Curve (10Y-2Y): {s.get('Yield_Curve', 'N/A')}%
- Yield Curve vor 1M (T10Y2Y_prev): {s.get('Yield_Curve_prev','N/A')}%

3. MARKTSENTIMENT & TRAP:
- USD/JPY 5-Tage: {s.get('Yen_5d', 0):.2%} (BOJ Risiko)
- Bitcoin 3-Tage: {s.get('BTC_3d', 0):.2%}
- US Bonds (TLT) 3-Tage: {s.get('Bond_3d', 0):.2%}
- Regionalbanken vs SPY: {s.get('Bank_vs_Spy', 0):.2%}

AUFGABE FÜR KI: Deuten diese Daten auf eine 'Convexity Trap', einen 'Carry Trade Unwind' oder eine Bankenkrise hin?
"""
        with open(SAVE_PATH_AI, "w") as f:
            f.write(ai_prompt)

        print(f"\nSTATUS: {score}/10")
        print(f"Details gespeichert für Agena.")
        print(f"KI-Prompt erstellt: {SAVE_PATH_AI}")

if __name__ == "__main__":
    MacroBridgeDetailed().run()
