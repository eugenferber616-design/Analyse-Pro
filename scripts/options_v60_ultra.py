# ------------------------------------------------------------------------
      # NEU: V60 ULTRA SKRIPT ERSTELLEN (Damit es gefunden wird)
      # ------------------------------------------------------------------------
      - name: Create scripts/options_v60_ultra.py
        run: |
          cat > scripts/options_v60_ultra.py <<'PY'
          #!/usr/bin/env python3
          # -*- coding: utf-8 -*-
          """
          Options Data V60 ULTRA - Gamma Exposure (GEX) & Max Pain
          """
          import os
          import sys
          import math
          from datetime import datetime
          
          import numpy as np
          import pandas as pd
          import yfinance as yf
          from scipy.stats import norm

          # Settings
          RISK_FREE_RATE = 0.045
          DAYS_TACTICAL_MAX = 14
          DAYS_MEDIUM_MAX = 120
          MONEYNESS_BAND_PCT = 0.30

          # Mathe-Kern (Black-Scholes Gamma)
          def bs_gamma(S, K, T, r, sigma):
              if T <= 0 or sigma <= 0: return 0.0
              d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
              gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
              return gamma

          def compute_gex(row, spot):
              K = row["strike"]
              T = row["dte"] / 365.0
              sigma = row.get("impliedVolatility", 0)
              oi = row["openInterest"]
              if T <= 0.001: T = 0.001
              if sigma <= 0.001: sigma = 0.3
              
              gamma_val = bs_gamma(spot, K, T, RISK_FREE_RATE, sigma)
              gex = gamma_val * (spot**2) * 0.01 * oi * 100
              return gex

          # Helpers
          def load_symbols():
              wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
              symbols = []
              if os.path.exists(wl_path):
                  with open(wl_path, "r") as f:
                      for line in f:
                          s = line.split("#")[0].strip()
                          if s: symbols.append(s)
              if not symbols:
                  symbols = ["SPY", "QQQ", "NVDA", "TSLA", "MSFT", "AAPL", "AMD", "META", "AMZN", "GOOGL"]
              return symbols

          def calculate_max_pain(df, strikes):
              if df.empty: return None
              pain_map = {}
              calls = df[df["kind"] == "call"]
              puts = df[df["kind"] == "put"]
              
              for center_strike in strikes:
                  call_loss = 0
                  if not calls.empty:
                      itm_calls = calls[calls["strike"] < center_strike]
                      if not itm_calls.empty:
                          call_loss = ((center_strike - itm_calls["strike"]) * itm_calls["openInterest"]).sum()
                  
                  put_loss = 0
                  if not puts.empty:
                      itm_puts = puts[puts["strike"] > center_strike]
                      if not itm_puts.empty:
                          put_loss = ((itm_puts["strike"] - center_strike) * itm_puts["openInterest"]).sum()
                  
                  pain_map[center_strike] = call_loss + put_loss
                  
              if not pain_map: return None
              return min(pain_map, key=pain_map.get)

          def get_smart_wall(df, spot, kind="call", max_pct=0.30):
              if df.empty: return None, 0, 0
              low = spot * (1.0 - max_pct)
              high = spot * (1.0 + max_pct)
              sub = df[(df["strike"] >= low) & (df["strike"] <= high)].copy()
              
              if kind == "call": sub = sub[sub["strike"] >= spot]
              else: sub = sub[sub["strike"] <= spot]
                  
              if sub.empty: return None, 0, 0
              
              sub["gex"] = sub["gex"].fillna(0)
              sub["notional"] = sub["openInterest"] * sub["strike"]
              
              top = sub.sort_values("gex", ascending=False).iloc[0]
              if top["gex"] == 0:
                  top = sub.sort_values("notional", ascending=False).iloc[0]
                  
              return top["strike"], top["openInterest"], top["gex"]

          def main():
              os.makedirs("data/processed", exist_ok=True)
              symbols = load_symbols()
              print(f"🚀 V60 ULTRA: Processing {len(symbols)} symbols with GEX & MaxPain...")
              
              now = datetime.utcnow()
              results = []
              
              for sym in symbols:
                  try:
                      tk = yf.Ticker(sym)
                      hist = tk.history(period="5d")
                      if hist.empty: continue
                      spot = float(hist["Close"].iloc[-1])
                      
                      exps = tk.options
                      if not exps: continue
                      
                      all_opts = []
                      for e_str in exps:
                          try:
                              dt = datetime.strptime(e_str, "%Y-%m-%d")
                              dte = (dt - now).days
                              if dte < 0: continue
                          except: continue
                          
                          try:
                              chain = tk.option_chain(e_str)
                              calls = chain.calls
                              puts = chain.puts
                          except: continue
                          
                          if not calls.empty:
                              calls = calls.assign(kind="call", expiry=dt, dte=dte)
                              all_opts.append(calls)
                          if not puts.empty:
                              puts = puts.assign(kind="put", expiry=dt, dte=dte)
                              all_opts.append(puts)
                              
                      if not all_opts: continue
                      
                      df = pd.concat(all_opts, ignore_index=True)
                      cols = ["contractSymbol", "strike", "openInterest", "volume", "impliedVolatility", "kind", "expiry", "dte"]
                      if "impliedVolatility" not in df.columns: df["impliedVolatility"] = 0.0
                      
                      df = df[[c for c in cols if c in df.columns]].copy()
                      df["openInterest"] = df["openInterest"].fillna(0).astype(float)
                      df["volume"] = df["volume"].fillna(0).astype(float)
                      df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
                      
                      df["gex"] = df.apply(lambda row: compute_gex(row, spot), axis=1)
                      
                      relevant_strikes = df[(df["strike"] > spot*0.7) & (df["strike"] < spot*1.3)]["strike"].unique()
                      max_pain = calculate_max_pain(df, relevant_strikes)
                      
                      df_tac = df[df["dte"] <= DAYS_TACTICAL_MAX]
                      tac_call_strike, _, tac_call_gex = get_smart_wall(df_tac, spot, "call")
                      tac_put_strike, _, _ = get_smart_wall(df_tac, spot, "put")
                      
                      gl_call_strike, _, gl_call_gex = get_smart_wall(df, spot, "call")
                      gl_put_strike, _, _ = get_smart_wall(df, spot, "put")
                      
                      df_strat = df[df["dte"] > DAYS_MEDIUM_MAX]
                      strat_call_strike, _, _ = get_smart_wall(df_strat, spot, "call", max_pct=0.5)
                      strat_put_strike, _, _ = get_smart_wall(df_strat, spot, "put", max_pct=0.5)

                      df["vol_oi_ratio"] = df["volume"] / (df["openInterest"] + 1)
                      hot_strikes = df[df["vol_oi_ratio"] > 1.5].sort_values("volume", ascending=False).head(1)
                      fresh_money_strike = hot_strikes["strike"].iloc[0] if not hot_strikes.empty else 0
                      fresh_money_type = hot_strikes["kind"].iloc[0] if not hot_strikes.empty else ""

                      res = {
                          "Symbol": sym,
                          "Spot": round(spot, 2),
                          "Max_Pain": max_pain,
                          "Tac_Call_Wall": tac_call_strike,
                          "Tac_Call_GEX": int(tac_call_gex) if not pd.isna(tac_call_gex) else 0,
                          "Tac_Put_Wall": tac_put_strike,
                          "Global_Call_Wall": gl_call_strike,
                          "Global_Put_Wall": gl_put_strike,
                          "Global_Call_GEX": int(gl_call_gex) if not pd.isna(gl_call_gex) else 0,
                          "Strat_Call_Target": strat_call_strike,
                          "Strat_Put_Target": strat_put_strike,
                          "Fresh_Money_Strike": fresh_money_strike,
                          "Fresh_Money_Type": fresh_money_type
                      }
                      results.append(res)
                      sys.stdout.write(".")
                      sys.stdout.flush()
                      
                  except Exception as e:
                      continue

              print("\nSaving V60 Ultra Data...")
              if results:
                  pd.DataFrame(results).to_csv("data/processed/options_v60_ultra.csv", index=False)
                  print("✔ Done.")

          if __name__ == "__main__":
              main()
          PY

      - name: Build options_v60_ultra (GEX, Max Pain, Whales)
        shell: bash
        run: |
          set -euo pipefail
          python scripts/options_v60_ultra.py

          if [ -s data/processed/options_v60_ultra.csv ]; then
            echo "✔ options_v60_ultra.csv erstellt"
            head -n 20 data/processed/options_v60_ultra.csv || true
          else
            echo "❌ options_v60_ultra.csv fehlt oder ist leer!" >&2
            exit 1
          fi
