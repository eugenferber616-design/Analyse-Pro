import yfinance as yf
import pandas as pd
import os
import sys

# Konfiguration
OUT_DIR = "data/processed"
OUT_FILE = os.path.join(OUT_DIR, "market_core.csv.gz")

# Tickers Mapping (Yahoo Symbol -> Spaltenname in CSV)
TICKERS = {
    "SPY": "SPY",         # S&P 500
    "^VIX": "VIX",        # Volatility
    "HYG": "HYG",         # High Yield
    "LQD": "LQD",         # Corp Bonds
    "XLF": "XLF",         # Financials
    "JPY=X": "USDJPY",    # Währung
    "BTC-USD": "BTC",     # Bitcoin
    "TLT": "TLT",         # Treasury Bond ETF
    "^TNX": "DGS10",      # 10 Year Yield (als Proxy)
    "DX-Y.NYB": "DXY"     # Dollar Index
}

def fetch_market_data():
    print("--- Lade Market Core Daten (via yfinance) ---")
    
    try:
        # Download (letzte 20 Jahre)
        symbols = list(TICKERS.keys())
        # auto_adjust=False behält den echten Close, progress=False macht es leise
        df = yf.download(symbols, period="20y", progress=False, auto_adjust=False)['Close']
        
        if df.empty:
            print("FEHLER: Keine Daten von Yahoo erhalten.")
            return

        # Spalten umbenennen
        df.rename(columns=TICKERS, inplace=True)
        
        # Datum formatieren
        df.index.name = "date"
        df.sort_index(inplace=True)
        
        # Letzte Zeile auffüllen (Forward Fill für Feiertage)
        df.ffill(inplace=True)

        # Ordner erstellen
        os.makedirs(OUT_DIR, exist_ok=True)
        
        # Speichern
        df.to_csv(OUT_FILE, compression="gzip")
        
        print(f"[OK] Gespeichert: {OUT_FILE}")
        print(f"     Zeilen: {len(df)}")
        print(f"     Spalten: {list(df.columns)}")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_market_data()