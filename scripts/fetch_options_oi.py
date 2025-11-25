#!/usr/bin/env python3

# -*- coding: utf-8 -*-

"""


Options Data V41 - The "Three-Stage-Rocket".


Options Data V50 Pro - Smart Walls "like the big boys"




1. TACTICAL (0-14 Tage): Gamma, Max Pain -> Timing.


2. MEDIUM   (15-120 Tage): Swing Magneten, Quartals-Levels -> Trend.


3. STRATEGIC (>120 Tage): LEAPS, Stock Replacement -> Big Picture.


3-Stufen-Rakete mit Profi-Heuristiken:





1. TACTICAL (0-14 Tage):


   - Nächster Verfall


   - Call-/Put-Walls nur im sinnvollen Moneyness-Band


   - Deckel = Calls über Spot, Unterstützung = Puts unter Spot





2. MEDIUM (15-120 Tage):


   - Aggregierte "Swing-Magneten" (Call/Put) mit Notional- & DTE-Gewichtung


   - Medium-Put/Call-Ratio & Bias





3. STRATEGIC (>120 Tage):


   - LEAPS-Ziele (Call/Put) mit Notional- & DTE-Gewichtung


   - Strategic-Bias





4. GLOBAL:


   - Globale Call-/Put-Walls über alle Verfallstage mit DTE-Gewichtung

"""



import os

import sys


import math


from datetime import datetime, timedelta


from datetime import datetime




import numpy as np

import pandas as pd

import yfinance as yf


from scipy.stats import norm



# ──────────────────────────────────────────────────────────────

# Settings

# ──────────────────────────────────────────────────────────────


RISK_FREE_RATE = 0.045


RISK_FREE_RATE = 0.045             # Reserviert, falls du später echte Gamma-Berechnungen einbaust

DAYS_TACTICAL_MAX = 14


DAYS_MEDIUM_MAX = 120  # Alles darüber ist Strategic


DAYS_MEDIUM_MAX = 120              # Alles darüber ist Strategic


MONEYNESS_BAND_PCT = 0.35          # +/- 35% um Spot für sinnvolle Walls






# ──────────────────────────────────────────────────────────────

# Helpers

# ──────────────────────────────────────────────────────────────


def calculate_max_pain(calls, puts):


    try:


        strikes = sorted(list(set(calls["strike"].tolist() + puts["strike"].tolist())))


        if not strikes: return 0.0


        loss = []


        for s in strikes:


            c_l = calls.apply(lambda r: max(0, s - r["strike"]) * r["openInterest"], axis=1).sum()


            p_l = puts.apply(lambda r: max(0, r["strike"] - s) * r["openInterest"], axis=1).sum()


            loss.append(c_l + p_l)


        return float(strikes[np.argmin(loss)])


    except: return 0.0





def get_top_oi_strikes(df, n=1):


    if df.empty: return 0


    # Gruppieren nach Strike (falls mehrere Expiries im Bucket sind)


    grp = df.groupby("strike")["openInterest"].sum().sort_values(ascending=False)


    if grp.empty: return 0


    return grp.index[0]


def load_symbols():


    """Liest die Watchlist; Fallback auf ein paar bekannte Namen."""


    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")





    symbols = []


    if os.path.exists(wl_path):


        with open(wl_path, "r") as f:


            for line in f:


                line = line.strip()


                if not line:


                    continue


                # Kommentare mit # entfernen


                sym = line.split("#")[0].strip()


                if sym:


                    symbols.append(sym)





    if not symbols:


        symbols = ["SPY", "QQQ", "NVDA", "TSLA", "MSFT", "AAPL", "AMD"]





    return symbols








def clean_chain(df):


    """Säubert eine Optionskette: strike/openInterest numerisch & ohne NaNs."""


    if df is None or df.empty:


        return pd.DataFrame(columns=["strike", "openInterest"])





    out = df.copy()





    if "strike" not in out.columns:


        out["strike"] = 0.0


    if "openInterest" not in out.columns:


        out["openInterest"] = 0.0





    out["strike"] = pd.to_numeric(out["strike"], errors="coerce")


    out["openInterest"] = pd.to_numeric(out["openInterest"], errors="coerce").fillna(0)





    out = out.dropna(subset=["strike"])


    return out[["strike", "openInterest"]]








def compute_wall(df, spot, kind="call", max_pct=0.35, use_dte_weight=True):


    """


    Berechnet eine "Wall" (Deckel/Unterstützung) mit Profi-Heuristik:





    - Nur Strikes im Band [spot*(1-max_pct), spot*(1+max_pct)]


    - Calls: nur Strikes >= Spot (Deckel)


      Puts:  nur Strikes <= Spot (Unterstützung)


    - Score = notional_OI * DTE-Gewichtung


      notional_OI = openInterest * strike


      DTE-Gewichtung = exp(-dte/60) (kurzfristiges OI wiegt stärker)


    """


    if df is None or df.empty:


        return None





    df2 = df.copy()


    df2 = df2[np.isfinite(df2["strike"])]





    if df2.empty:


        return None





    # Moneyness-Fenster um den Spot


    low = spot * (1.0 - max_pct)


    high = spot * (1.0 + max_pct)


    df2 = df2[(df2["strike"] >= low) & (df2["strike"] <= high)]





    # Directional: Deckel über Spot, Unterstützung unter Spot


    if kind == "call":


        df2 = df2[df2["strike"] >= spot]


    else:


        df2 = df2[df2["strike"] <= spot]





    if df2.empty:


        return None





    # Nur sinnvolle OI


    df2["openInterest"] = df2["openInterest"].clip(lower=0)





    # Notional OI


    df2["notional_oi"] = df2["openInterest"] * df2["strike"].abs()





    # DTE-Gewichtung (optional)


    if use_dte_weight and "dte" in df2.columns:


        df2["w"] = np.exp(-df2["dte"] / 60.0)


    else:


        df2["w"] = 1.0





    df2["score"] = df2["notional_oi"] * df2["w"]





    grp = (


        df2.groupby("strike", as_index=False)


        .agg(score=("score", "sum"), total_oi=("openInterest", "sum"))


        .sort_values("score", ascending=False)


    )





    if grp.empty:


        return None





    strike = float(grp.iloc[0]["strike"])


    score = float(grp.iloc[0]["score"])


    total_oi = float(grp.iloc[0]["total_oi"])





    # Welche Expiry trägt am meisten zu diesem Strike bei?


    sub = df2[df2["strike"] == strike].sort_values("score", ascending=False).iloc[0]


    expiry = sub.get("expiry", None)


    dte = int(sub["dte"]) if "dte" in sub else None





    expiry_str = ""


    if isinstance(expiry, datetime):


        expiry_str = expiry.strftime("%Y-%m-%d")


    elif isinstance(expiry, str):


        expiry_str = expiry





    return {


        "strike": strike,


        "score": score,


        "total_oi": total_oi,


        "expiry": expiry_str,


        "dte": dte,


    }






# ──────────────────────────────────────────────────────────────

# Main

# ──────────────────────────────────────────────────────────────

def main():

    os.makedirs("data/processed", exist_ok=True)


    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")


    


    symbols = []


    if os.path.exists(wl_path):


        with open(wl_path, "r") as f:


            symbols = [line.strip().split("#")[0].strip() for line in f if line.strip()]


    if not symbols: symbols = ["SPY", "QQQ", "NVDA", "TSLA", "MSFT", "AAPL", "AMD"]


    symbols = load_symbols()




    print(f"Processing V41 (3-Stages) for {len(symbols)} symbols...")





    res_tactical = []


    res_medium = []


    res_strategic = []


    print(f"Processing V50 Pro (Smart Walls) for {len(symbols)} symbols...")



    now = datetime.utcnow()


    rows_out = []



    for sym in symbols:

        try:

            tk = yf.Ticker(sym)





            # Spot bestimmen

            try:

                hist = tk.history(period="5d", interval="1d")


                if hist.empty:


                    print(f"\n{sym}: keine History, skip.")


                    continue

                spot = float(hist["Close"].iloc[-1])


            except: continue


            


            exps = tk.options


            if not exps: continue


            except Exception as e:


                print(f"\n{sym}: History-Fehler ({e}), skip.")


                continue





            try:


                exps = tk.options


            except Exception as e:


                print(f"\n{sym}: Options-Fehler ({e}), skip.")


                continue




            # Bucket Container


            bucket_tactical = [] # (calls, puts, expiry_date)


            bucket_medium = []


            bucket_strategic = []


            if not exps:


                print(f"\n{sym}: keine Optionen, skip.")


                continue




            # 1. Daten laden und sortieren


            # Alle Options-Reihen (Calls & Puts) mit Expiry & DTE sammeln


            rows = []

            for e_str in exps:

                try:

                    dt = datetime.strptime(e_str, "%Y-%m-%d")


                    days = (dt - now).days


                    


                    if days < 0: continue # Vergangen


                except Exception:


                    continue





                dte = (dt - now).days


                if dte < 0:


                    # Vergangene Verfälle ignorieren


                    continue




                    # Fetch Chain


                try:

                    chain = tk.option_chain(e_str)


                    c = chain.calls.fillna(0)


                    p = chain.puts.fillna(0)


                    for df in [c, p]:


                        if "openInterest" not in df.columns: df["openInterest"] = 0


                        if "strike" not in df.columns: df["strike"] = 0


                    


                    data_tuple = (c, p, dt)





                    if days <= DAYS_TACTICAL_MAX:


                        bucket_tactical.append(data_tuple)


                    elif days <= DAYS_MEDIUM_MAX:


                        bucket_medium.append(data_tuple)


                    else:


                        bucket_strategic.append(data_tuple)


                except: continue





            # 2. ANALYSE: TACTICAL (Fokus auf Next Expiry Gamma & Pain)


            # Wir nehmen nur den allerersten (nächsten) Verfall für "Tactical Precision"


            if bucket_tactical:


                c, p, dt = bucket_tactical[0] 


                mp = calculate_max_pain(c, p)


                


                # Simple Net GEX Approximation (Call OI - Put OI anstatt komplexes Gamma, 


                # da Gamma bei yfinance oft fehlt. Für Richtung reicht OI oft als Proxy kurzfristig)


                # Besser: Wenn Strike nahe Spot -> Gamma hoch.


                # Hier nehmen wir Walls.


                c_wall = get_top_oi_strikes(c)


                p_wall = get_top_oi_strikes(p)





                res_tactical.append({


                    "Symbol": sym,


                    "Expiry": dt.strftime("%Y-%m-%d"),


                    "Spot": spot,


                    "Max_Pain": mp,


                    "Call_Wall_Tac": c_wall,


                    "Put_Wall_Tac": p_wall,


                    "Days": (dt - now).days


                })





            # 3. ANALYSE: MEDIUM (Der Swing-Trend)


            if bucket_medium:


                # Wir aggregieren ALLE Expiries im Medium Bucket (15-120 Tage)


                all_c = pd.concat([x[0] for x in bucket_medium])


                all_p = pd.concat([x[1] for x in bucket_medium])


                


                # Der "Quarterly Magnet" (Strike mit absolut meistem OI in diesem Zeitraum)


                swing_target_c = get_top_oi_strikes(all_c)


                swing_target_p = get_top_oi_strikes(all_p)


                


                total_c_oi = all_c["openInterest"].sum()


                total_p_oi = all_p["openInterest"].sum()


                


                res_medium.append({


                    "Symbol": sym,


                    "Spot": spot,


                    "Swing_Magnet_Call": swing_target_c, # Das Ziel der Bullen


                    "Swing_Magnet_Put": swing_target_p,  # Das Ziel der Bären


                    "Medium_PCR": round(total_p_oi / max(1, total_c_oi), 2),


                    "Bias_Medium": "Bullish" if total_c_oi > total_p_oi else "Bearish"


                })





            # 4. ANALYSE: STRATEGIC (Big Money / LEAPS)


            if bucket_strategic:


                all_c = pd.concat([x[0] for x in bucket_strategic])


                all_p = pd.concat([x[1] for x in bucket_strategic])


                


                leaps_c = get_top_oi_strikes(all_c)


                leaps_p = get_top_oi_strikes(all_p)


                


                res_strategic.append({


                    "Symbol": sym,


                    "Leaps_Target_Call": leaps_c,


                    "Leaps_Target_Put": leaps_p,


                    "Strategic_Bias": "Bullish" if all_c["openInterest"].sum() > all_p["openInterest"].sum() else "Bearish"


                })


                except Exception:


                    continue





                c_raw = getattr(chain, "calls", None)


                p_raw = getattr(chain, "puts", None)





                c = clean_chain(c_raw)


                p = clean_chain(p_raw)





                if c.empty and p.empty:


                    continue





                if not c.empty:


                    tmp_c = c[["strike", "openInterest"]].copy()


                    tmp_c["expiry"] = dt


                    tmp_c["dte"] = dte


                    tmp_c["kind"] = "call"


                    rows.append(tmp_c)





                if not p.empty:


                    tmp_p = p[["strike", "openInterest"]].copy()


                    tmp_p["expiry"] = dt


                    tmp_p["dte"] = dte


                    tmp_p["kind"] = "put"


                    rows.append(tmp_p)





            if not rows:


                print(f"\n{sym}: keine verwertbaren Optionsdaten.")


                continue





            df_all = pd.concat(rows, ignore_index=True)





            # Grund-Separation Calls / Puts


            df_calls = df_all[df_all["kind"] == "call"].copy()


            df_puts = df_all[df_all["kind"] == "put"].copy()





            if df_calls.empty and df_puts.empty:


                print(f"\n{sym}: weder Calls noch Puts nach Filter.")


                continue





            # ──────────────────────────────────────────────


            # GLOBAL WALLS (über alle DTE, mit DTE-Gewichtung)


            # ──────────────────────────────────────────────


            global_call_wall = compute_wall(df_calls, spot, "call",


                                            max_pct=MONEYNESS_BAND_PCT,


                                            use_dte_weight=True)


            global_put_wall = compute_wall(df_puts, spot, "put",


                                           max_pct=MONEYNESS_BAND_PCT,


                                           use_dte_weight=True)





            # ──────────────────────────────────────────────


            # TACTICAL (0–14 Tage) – nächster Verfall


            # ──────────────────────────────────────────────


            df_tac = df_all[(df_all["dte"] >= 0) & (df_all["dte"] <= DAYS_TACTICAL_MAX)].copy()


            tactical_call_wall = None


            tactical_put_wall = None


            tac_expiry_str = ""


            tac_dte = None





            if not df_tac.empty:


                # Nächsten Verfall finden


                min_dte = df_tac["dte"].min()


                earliest_expiries = df_tac[df_tac["dte"] == min_dte]["expiry"].drop_duplicates()


                tac_expiry = earliest_expiries.iloc[0]


                tac_dte = int(min_dte)





                tac_expiry_str = tac_expiry.strftime("%Y-%m-%d") if isinstance(tac_expiry, datetime) else str(tac_expiry)





                df_tac_calls = df_tac[(df_tac["kind"] == "call") & (df_tac["expiry"] == tac_expiry)].copy()


                df_tac_puts = df_tac[(df_tac["kind"] == "put") & (df_tac["expiry"] == tac_expiry)].copy()





                tactical_call_wall = compute_wall(


                    df_tac_calls, spot, "call",


                    max_pct=MONEYNESS_BAND_PCT,


                    use_dte_weight=False  # innerhalb eines Verfalls


                )


                tactical_put_wall = compute_wall(


                    df_tac_puts, spot, "put",


                    max_pct=MONEYNESS_BAND_PCT,


                    use_dte_weight=False


                )




            # ──────────────────────────────────────────────


            # MEDIUM (15–120 Tage) – Swing-Magneten


            # ──────────────────────────────────────────────


            df_med = df_all[(df_all["dte"] >= DAYS_TACTICAL_MAX + 1) &


                            (df_all["dte"] <= DAYS_MEDIUM_MAX)].copy()


            medium_call_wall = None


            medium_put_wall = None


            medium_pcr = None


            medium_bias = ""





            if not df_med.empty:


                df_med_calls = df_med[df_med["kind"] == "call"].copy()


                df_med_puts = df_med[df_med["kind"] == "put"].copy()





                medium_call_wall = compute_wall(


                    df_med_calls, spot, "call",


                    max_pct=MONEYNESS_BAND_PCT,


                    use_dte_weight=True


                )


                medium_put_wall = compute_wall(


                    df_med_puts, spot, "put",


                    max_pct=MONEYNESS_BAND_PCT,


                    use_dte_weight=True


                )





                total_c_oi_med = float(df_med_calls["openInterest"].sum()) if not df_med_calls.empty else 0.0


                total_p_oi_med = float(df_med_puts["openInterest"].sum()) if not df_med_puts.empty else 0.0





                if total_c_oi_med > 0:


                    medium_pcr = total_p_oi_med / total_c_oi_med


                else:


                    medium_pcr = None





                if total_c_oi_med > total_p_oi_med:


                    medium_bias = "Bullish"


                elif total_c_oi_med < total_p_oi_med:


                    medium_bias = "Bearish"


                else:


                    medium_bias = "Neutral"





            # ──────────────────────────────────────────────


            # STRATEGIC (>120 Tage) – LEAPS


            # ──────────────────────────────────────────────


            df_strat = df_all[df_all["dte"] > DAYS_MEDIUM_MAX].copy()


            strat_call_wall = None


            strat_put_wall = None


            strat_bias = ""





            if not df_strat.empty:


                df_strat_calls = df_strat[df_strat["kind"] == "call"].copy()


                df_strat_puts = df_strat[df_strat["kind"] == "put"].copy()





                strat_call_wall = compute_wall(


                    df_strat_calls, spot, "call",


                    max_pct=MONEYNESS_BAND_PCT,


                    use_dte_weight=True


                )


                strat_put_wall = compute_wall(


                    df_strat_puts, spot, "put",


                    max_pct=MONEYNESS_BAND_PCT,


                    use_dte_weight=True


                )





                total_c_oi_strat = float(df_strat_calls["openInterest"].sum()) if not df_strat_calls.empty else 0.0


                total_p_oi_strat = float(df_strat_puts["openInterest"].sum()) if not df_strat_puts.empty else 0.0





                if total_c_oi_strat > total_p_oi_strat:


                    strat_bias = "Bullish"


                elif total_c_oi_strat < total_p_oi_strat:


                    strat_bias = "Bearish"


                else:


                    strat_bias = "Neutral"





            # ──────────────────────────────────────────────


            # Output-Zeile für dieses Symbol bauen


            # ──────────────────────────────────────────────


            row = {


                "Symbol": sym,


                "Spot": round(spot, 4),





                # Tactical


                "Tac_Expiry": tac_expiry_str,


                "Tac_DTE": tac_dte,


                "Tac_Call_Wall": tactical_call_wall["strike"] if tactical_call_wall else 0.0,


                "Tac_Call_Wall_OI": tactical_call_wall["total_oi"] if tactical_call_wall else 0.0,


                "Tac_Put_Wall": tactical_put_wall["strike"] if tactical_put_wall else 0.0,


                "Tac_Put_Wall_OI": tactical_put_wall["total_oi"] if tactical_put_wall else 0.0,





                # Global Walls über alle DTE


                "Global_Call_Wall": global_call_wall["strike"] if global_call_wall else 0.0,


                "Global_Call_Wall_Expiry": global_call_wall["expiry"] if global_call_wall else "",


                "Global_Call_Wall_DTE": global_call_wall["dte"] if global_call_wall else None,


                "Global_Call_Wall_OI": global_call_wall["total_oi"] if global_call_wall else 0.0,





                "Global_Put_Wall": global_put_wall["strike"] if global_put_wall else 0.0,


                "Global_Put_Wall_Expiry": global_put_wall["expiry"] if global_put_wall else "",


                "Global_Put_Wall_DTE": global_put_wall["dte"] if global_put_wall else None,


                "Global_Put_Wall_OI": global_put_wall["total_oi"] if global_put_wall else 0.0,





                # Medium (Swing)


                "Medium_Call_Magnet": medium_call_wall["strike"] if medium_call_wall else 0.0,


                "Medium_Call_Magnet_Expiry": medium_call_wall["expiry"] if medium_call_wall else "",


                "Medium_Put_Magnet": medium_put_wall["strike"] if medium_put_wall else 0.0,


                "Medium_Put_Magnet_Expiry": medium_put_wall["expiry"] if medium_put_wall else "",


                "Medium_PCR": round(medium_pcr, 3) if medium_pcr is not None else None,


                "Medium_Bias": medium_bias,





                # Strategic (LEAPS)


                "Strategic_Call_Target": strat_call_wall["strike"] if strat_call_wall else 0.0,


                "Strategic_Call_Target_Expiry": strat_call_wall["expiry"] if strat_call_wall else "",


                "Strategic_Put_Target": strat_put_wall["strike"] if strat_put_wall else 0.0,


                "Strategic_Put_Target_Expiry": strat_put_wall["expiry"] if strat_put_wall else "",


                "Strategic_Bias": strat_bias,


            }





            rows_out.append(row)

            sys.stdout.write(".")

            sys.stdout.flush()



        except Exception as e:


            # print(e)


            print(f"\n{sym}: Fehler im Hauptloop: {e}")

            continue




    print("\nSaving 3-Stage Reports...")


    


    if res_tactical: pd.DataFrame(res_tactical).to_csv("data/processed/tactical.csv", index=False)


    if res_medium: pd.DataFrame(res_medium).to_csv("data/processed/medium.csv", index=False)


    if res_strategic: pd.DataFrame(res_strategic).to_csv("data/processed/strategic.csv", index=False)


    


    print("\nSaving Pro 3-Stage Report...")





    if rows_out:


        df_out = pd.DataFrame(rows_out)


        df_out.to_csv("data/processed/options_3stage_pro.csv", index=False)


        print("✔ Saved data/processed/options_3stage_pro.csv")


    else:


        print("Keine Daten zum Speichern.")




    print("Done.")






if __name__ == "__main__":

    main()
