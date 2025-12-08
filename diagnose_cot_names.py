import pandas as pd
import os
import difflib
from datetime import datetime

# PFADE
RAW_FILE_DISAGG = os.path.join("data", "processed", "cot_20y_disagg.csv.gz")
WATCHLIST_FILE = os.path.join("watchlists", "cot_markets.txt")

def diagnose():
    print("--- COT DIAGNOSE MODUS (ROBUST) ---")
    
    if not os.path.exists(RAW_FILE_DISAGG):
        print(f"FEHLER: Datei {RAW_FILE_DISAGG} fehlt!")
        return

    print("Lade CFTC Rohdaten...")
    try:
        df = pd.read_csv(RAW_FILE_DISAGG, compression='gzip', low_memory=False)
    except Exception as e:
        print(f"Fehler beim Lesen: {e}")
        return

    # Alles in Kleinbuchstaben umwandeln für den Vergleich
    df.columns = [c.strip().lower().replace(' ','_') for c in df.columns]
    
    print(f"Spalten in der Datei: {list(df.columns)}") # DEBUG AUSGABE

    # 1. DATUM FINDEN
    # Sucht nach 'date' oder 'yyyy'
    date_col = next((c for c in df.columns if 'date' in c and 'report' in c), None)
    if not date_col:
        date_col = next((c for c in df.columns if 'date' in c), None)

    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        latest_date = df[date_col].max()
        print(f"\n>>> NEUESTES DATUM: {latest_date.date()} <<<")
    else:
        print("\nKRITISCH: Keine Datumsspalte gefunden!")
        return

    # 2. MARKTNAMEN FINDEN
    # Sucht nach 'market' und 'name'
    name_col = next((c for c in df.columns if 'market' in c and 'name' in c), None)
    
    if not name_col:
        print("KRITISCH: Konnte Spalte für Marktnamen nicht finden!")
        return

    print(f"Nutze Namens-Spalte: '{name_col}'")

    # Watchlist Abgleich
    available_markets = df[name_col].dropna().unique().tolist()
    # Bereinigen (Upper Case für Vergleich)
    avail_upper = {x.strip().upper(): x for x in available_markets}
    
    if not os.path.exists(WATCHLIST_FILE):
        print(f"Keine Watchlist gefunden: {WATCHLIST_FILE}")
        return

    with open(WATCHLIST_FILE, 'r') as f:
        wanted_markets = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    print(f"\n{'-'*80}")
    print(f"{'DEIN NAME (Watchlist)':<35} | {'STATUS':<10} | {'INFO / VORSCHLAG'}")
    print(f"{'-'*80}")

    for wanted in wanted_markets:
        w_up = wanted.upper()
        
        if w_up in avail_upper:
            # Exakter Treffer
            real_name = avail_upper[w_up]
            # Datum checken
            last_date = df[df[name_col] == real_name][date_col].max().date()
            status = "OK" if last_date == latest_date.date() else "ALT"
            print(f"{wanted:<35} | {status:<10} | Datum: {last_date}")
        else:
            # Fuzzy Suche
            matches = difflib.get_close_matches(w_up, avail_upper.keys(), n=1, cutoff=0.5)
            if matches:
                better_name = avail_upper[matches[0]] # Originalen Namen holen
                print(f"{wanted:<35} | FEHLT      | Meintest du: '{better_name}'?")
            else:
                print(f"{wanted:<35} | FEHLT      | Keine Ähnlichkeit gefunden.")

if __name__ == "__main__":
    diagnose()
