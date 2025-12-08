import yfinance as yf
from fredapi import Fred
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from colorama import Fore, Style, init

# --- KONFIGURATION ---
# API Key für FRED (Federal Reserve Economic Data)
# Kostenlos hier: https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = "a62b1a06c6cdc4bb8c32d733a492326f" 

# Pfad zum AgenaTrader Cache (Automatisch für Windows Nutzer)
BASE_DIR = os.path.join(os.path.expanduser("~"), "Documents", "AgenaTrader_QuantCache")
SAVE_PATH_CSV = os.path.join(BASE_DIR, "macro_status.csv")
SAVE_PATH_AI  = os.path.join(BASE_DIR, "ai_context.txt") 

# Farben für Konsole initialisieren
init(autoreset=True)

class MacroBridgeFinal:
    def __init__(self):
        # 1. YAHOO TICKERS (Live Markt-Daten)
        self.tickers_yahoo = {
            "YEN": "JPY=X",     # BOJ / Carry Trade
            "BTC": "BTC-USD",   # Crypto Liquidity / Trap
            "TLT": "TLT",       # Bond Prices (Trap)
            "KRE": "KRE",       # Regionalbanken (Solvenz)
            "SPY": "SPY",       # Aktienmarkt
            "HYG": "HYG",       # High Yield (Junk)
            "LQD": "LQD",       # Investment Grade
            "COPPER": "HG=F",   # Konjunktur (Dr. Copper)
            "GOLD": "GC=F"      # Angst / Safe Haven
        }
        
        # 2. FRED TICKERS (Offizielle Fed Daten)
        self.tickers_fred = {
            "SOFR": "SOFR",           # Cost of Cash (Repo Markt)
            "IORB": "IORB",           # Fed Zins auf Reserven
            "WALCL": "WALCL",         # Fed Bilanzsumme
            "WDTGAL": "WDTGAL",       # Treasury General Account
            "RRP": "RRPONTSYD",       # Reverse Repo Facility
            "T10Y2Y": "T10Y2Y",       # Yield Curve
            "HY_SPREAD": "BAMLH0A0HYM2", # Echter Credit Spread (Master II)
            "EMERGENCY": "TOTBORR",   # Discount Window / Bank Term Funding
            "INFLATION_EXP": "T5YIE"  # 5-Year Breakeven Inflation Rate
        }
        
        self.df_yahoo = pd.DataFrame()
        self.df_fred = pd.DataFrame()
        self.stats = {}

    def fetch_data(self):
        print(f"{Fore.CYAN}--- LADE DATEN (HEDGE FUND EDITION) ---{Style.RESET_ALL}")
        end = datetime.now()
        start = end - timedelta(days=400)
        
        # A) Yahoo Finance
        try:
            print("- Lade Live-Märkte (Yahoo)...")
            self.df_yahoo = yf.download(list(self.tickers_yahoo.values()), start=start, progress=False)['Close']
            inv_y = {v: k for k, v in self.tickers_yahoo.items()}
            self.df_yahoo.rename(columns=inv_y, inplace=True)
            self.df_yahoo.ffill(inplace=True)
        except Exception as e: 
            print(f"{Fore.RED}Fehler Yahoo: {e}")

        # B) FRED API
        try:
            print("- Lade Makro-Daten (FRED)...")
            fred = Fred(api_key=FRED_API_KEY)
            fred_data = {}
            for name, ticker in self.tickers_fred.items():
                try:
                    s = fred.get_series(ticker, observation_start=start)
                    fred_data[name] = s
                except: pass
            if fred_data:
                self.df_fred = pd.DataFrame(fred_data)
                self.df_fred.ffill(inplace=True)
        except Exception as e:
            print(f"{Fore.RED}Fehler FRED: {e}")

    def calculate_stats(self):
        s = {}
        
        # 1. BERECHNUNGEN LIVE DATEN
        try:
            if not self.df_yahoo.empty and len(self.df_yahoo) > 20:
                # BOJ Risiko
                s['Yen_5d'] = (self.df_yahoo['YEN'].iloc[-1] / self.df_yahoo['YEN'].iloc[-6] - 1)
                
                # Convexity Trap (Korrelation)
                s['BTC_3d'] = (self.df_yahoo['BTC'].iloc[-1] / self.df_yahoo['BTC'].iloc[-4] - 1)
                s['Bond_3d'] = (self.df_yahoo['TLT'].iloc[-1] / self.df_yahoo['TLT'].iloc[-4] - 1)
                
                # Bank Stress (Relative Stärke)
                kre_perf = self.df_yahoo['KRE'].pct_change(10).iloc[-1]
                spy_perf = self.df_yahoo['SPY'].pct_change(10).iloc[-1]
                s['Bank_vs_Spy'] = kre_perf - spy_perf

                # Konjunktur: Copper/Gold Ratio (Trend über 20 Tage)
                cog = self.df_yahoo['COPPER'] / self.df_yahoo['GOLD']
                s['Copper_Gold_Trend'] = (cog.iloc[-1] / cog.iloc[-20]) - 1

                # Bond Volatilität (Proxy für MOVE Index)
                # Annualisierte Vola der letzten 20 Tage von TLT
                tlt_ret = self.df_yahoo['TLT'].pct_change()
                s['Bond_Vol'] = tlt_ret.rolling(20).std().iloc[-1] * (252 ** 0.5) * 100
        except: pass

        # 2. BERECHNUNGEN MAKRO DATEN
        if not self.df_fred.empty:
            try:
                # Funding Stress
                s['SOFR'] = self.df_fred['SOFR'].iloc[-1]
                s['IORB'] = self.df_fred['IORB'].iloc[-1]
                s['Funding_Spread'] = s['SOFR'] - s['IORB']
                
                # Net Liquidity (in Milliarden USD)
                # WALCL/WDTGAL sind in Mio -> durch 1000. RRP ist in Mrd.
                liq_now = (self.df_fred['WALCL'].iloc[-1]/1000) - (self.df_fred['WDTGAL'].iloc[-1]/1000) - self.df_fred['RRP'].iloc[-1]
                liq_prev = (self.df_fred['WALCL'].iloc[-20]/1000) - (self.df_fred['WDTGAL'].iloc[-20]/1000) - self.df_fred['RRP'].iloc[-20]
                s['NetLiq_Change'] = liq_now - liq_prev
                
                # Bank Panic (Discount Window)
                s['Emergency_Change'] = self.df_fred['EMERGENCY'].iloc[-1] - self.df_fred['EMERGENCY'].iloc[-2]
                
                # Credit & Yields & Inflation
                s['Credit_Spread'] = self.df_fred['HY_SPREAD'].iloc[-1]
                s['Yield_Curve'] = self.df_fred['T10Y2Y'].iloc[-1]
                s['Yield_Curve_Prev'] = self.df_fred['T10Y2Y'].iloc[-20]
                s['Inflation_Exp'] = self.df_fred['INFLATION_EXP'].iloc[-1]
            except: pass
            
        self.stats = s
        return s

    def evaluate_risk(self):
        s = self.stats
        msgs = []
        score = 0
        
        # --- 1. BOJ (Carry Trade) ---
        if s.get('Yen_5d', 0) < -0.025: 
            score += 3; msgs.append("BOJ: Yen steigt (Carry Risk)")

        # --- 2. CONVEXITY TRAP ---
        if s.get('BTC_3d', 0) < -0.04 and s.get('Bond_3d', 0) < -0.01:
            score += 5; msgs.append("TRAP: BTC & Bonds crashen")

        # --- 3. FUNDING (Interbanken) ---
        if s.get('Funding_Spread', 0) > 0.05:
            score += 3; msgs.append("FUNDING: Stress (SOFR > IORB)")

        # --- 4. BANKEN STRESS ---
        if s.get('Emergency_Change', 0) > 5:
            score += 4; msgs.append("BANKEN: Notkredite!")
        if s.get('Bank_vs_Spy', 0) < -0.05:
            score += 2; msgs.append("BANKEN: Aktien schwach")

        # --- 5. CREDIT MARKETS ---
        if s.get('Credit_Spread', 0) > 3.0:
            score += 3; msgs.append(f"CREDIT: Spreads hoch ({s.get('Credit_Spread'):.2f}%)")
        
        # --- 6. LIQUIDITÄT & FED ---
        if s.get('NetLiq_Change', 0) < -50:
            score += 2; msgs.append(f"LIQUIDITÄT: Abfluss ({s.get('NetLiq_Change'):.0f} Mrd)")
        
        # Fed-Falle: Banken brauchen Geld, aber Inflation steigt
        if s.get('Emergency_Change', 0) > 0 and s.get('Inflation_Exp', 0) > 2.5:
            score += 2; msgs.append("FED FALLE: Krise + Inflation")

        # --- 7. MAKRO & KONJUNKTUR ---
        # Yield Curve Steepening
        if s.get('Yield_Curve', 0) > 0 and s.get('Yield_Curve_Prev', 0) < -0.1:
             score += 4; msgs.append("REZESSION: Steepening")
        
        # Bond Panik (MOVE Proxy)
        if s.get('Bond_Vol', 0) > 20:
            score += 2; msgs.append(f"BOND PANIK: Vola hoch ({s.get('Bond_Vol'):.1f})")

        # Copper/Gold Warnung
        if s.get('Copper_Gold_Trend', 0) < -0.05:
            score += 1; msgs.append("KONJUNKTUR: Copper/Gold fällt")

        total = min(10, score)
        if total == 0 and not msgs: msgs.append("System Stabil")
        
        return total, msgs

    def run(self):
        self.fetch_data()
        self.calculate_stats()
        score, msgs = self.evaluate_risk()
        s = self.stats

        # CSV ERSTELLEN (Für AgenaTrader Indikator)
        details = [
            f"Funding Spr: {s.get('Funding_Spread',0):.3f}%",
            f"NetLiq Chg: {s.get('NetLiq_Change',0):+.0f} Mrd",
            f"Credit Spr: {s.get('Credit_Spread',0):.2f}%",
            f"Bond Vola: {s.get('Bond_Vol',0):.1f}",
            f"Infl. Exp: {s.get('Inflation_Exp',0):.2f}%",
            f"Yen 5d: {s.get('Yen_5d',0):.1%}",
            f"BTC 3d: {s.get('BTC_3d',0):.1%}",
            f"Bank Rel: {s.get('Bank_vs_Spy',0):.1%}",
            f"Notkredite: {s.get('Emergency_Change',0):+.1f} Mrd"
        ]
        
        timestamp = datetime.now().strftime('%H:%M')
        csv_content = f"{score}|" + "|".join(msgs) + "||" + "|".join(details) + f"|Update: {timestamp}"
        
        os.makedirs(BASE_DIR, exist_ok=True)
        with open(SAVE_PATH_CSV, "w") as f: f.write(csv_content)

        # KI PROMPT ERSTELLEN (Für Analyse)
        ai_prompt = f"""
ANALYSE ZEITPUNKT: {datetime.now()}
SYSTEMIC HAZARD REPORT (HEDGE FUND VIEW):

1. LIQUIDITÄT & FUNDING:
- Funding Spread (SOFR-IORB): {s.get('Funding_Spread', 0):.4f}% (Positiv = Stress)
- Net Liquidity 1M Change: {s.get('NetLiq_Change', 'N/A')} Mrd USD
- Bond Volatility (TLT Vola): {s.get('Bond_Vol', 0):.2f} (Normal ~10-15, Panik >20)

2. SOLVENZ & KREDIT:
- Bank Notkredite (Discount Window): {s.get('Emergency_Change', 'N/A')} Mrd USD
- Credit Spreads (High Yield): {s.get('Credit_Spread', 'N/A')}%
- Regionalbanken vs. SPY: {s.get('Bank_vs_Spy', 0):.2%}

3. MAKRO & KONJUNKTUR:
- Yield Curve (10Y-2Y): {s.get('Yield_Curve', 'N/A')}%
- Inflation Expectations (5Y Break): {s.get('Inflation_Exp', 'N/A')}%
- Copper/Gold Ratio Trend: {s.get('Copper_Gold_Trend', 0):.2%}

4. CRASH SENSOREN:
- USD/JPY 5-Tage: {s.get('Yen_5d', 0):.2%}
- Bitcoin 3-Tage: {s.get('BTC_3d', 0):.2%}
- US Bonds (TLT) 3-Tage: {s.get('Bond_3d', 0):.2%}

FRAGE: Analysiere die Gefahr einer 'Convexity Trap' oder 'Stagflation' basierend auf diesen Daten.
"""
        with open(SAVE_PATH_AI, "w") as f: f.write(ai_prompt)

        print(f"\nSTATUS: {score}/10")
        print("Alle Daten aktualisiert (CSV & KI-Prompt).")

if __name__ == "__main__":
    MacroBridgeFinal().run()
