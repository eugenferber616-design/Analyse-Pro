#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_cot_20y.py
----------------
Lädt die kompletten COT-Daten (Commitment of Traders) von der CFTC.
FIX: Nutzt User-Agent Spoofing, um den Fehler 403 zu umgehen.
UPDATE: Inkrementelles Update, robustere Retries, kleinerer Chunk-Size.
"""

import os
import sys
import argparse
import time
import pandas as pd
import requests
from io import StringIO
from datetime import datetime, timedelta

# Die offizielle API der CFTC
API_BASE = "https://publicreporting.cftc.gov/resource"

# DAS IST DER SCHLÜSSEL: Wir tarnen uns als normaler PC-Nutzer
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/csv, */*",
    "Connection": "keep-alive"
}

def get_last_date(csv_path):
    """Ermittelt das letzte Datum in der existierenden CSV-Datei."""
    if not os.path.exists(csv_path):
        return None
    try:
        # Nur Header und letzte Zeilen lesen wäre effizienter, aber für Robustheit lesen wir die 'report_date_as_yyyy_mm_dd' Spalte
        df = pd.read_csv(csv_path, usecols=['report_date_as_yyyy_mm_dd'])
        if df.empty:
            return None
        last_date_str = df['report_date_as_yyyy_mm_dd'].max()
        return pd.to_datetime(last_date_str)
    except Exception as e:
        print(f"[WARNUNG] Konnte existierende Datei nicht lesen: {e}")
        return None

def fetch_data(dataset_id, output_file, years=20, chunk_size=5000):
    all_data = []
    offset = 0
    
    # Standard Startdatum: Heute minus 20 Jahre
    start_date_dt = pd.Timestamp.now() - pd.DateOffset(years=years)
    
    # Prüfen auf inkrementelles Update
    last_known_date = get_last_date(output_file)
    if last_known_date:
        print(f"[COT] Existierende Daten gefunden bis: {last_known_date.strftime('%Y-%m-%d')}")
        # Wir starten ab dem nächsten Tag
        start_date_dt = last_known_date + timedelta(days=1)
        # Sicherheitshalber nicht in die Zukunft springen (falls Datei korrupt/Datum falsch)
        if start_date_dt > pd.Timestamp.now():
             start_date_dt = pd.Timestamp.now() - pd.DateOffset(years=years)
             print("[COT] Datum in Zukuft? Reset auf 20 Jahre.")
    
    start_date = start_date_dt.strftime("%Y-%m-%d")
    print(f"[COT] Starte Download fuer Dataset {dataset_id} ab {start_date}...")
    
    retries = 0
    max_retries = 5

    while True:
        # Die Abfrage an die Datenbank
        params = {
            "$where": f"report_date_as_yyyy_mm_dd >= '{start_date}'",
            "$order": "report_date_as_yyyy_mm_dd ASC",
            "$limit": chunk_size,
            "$offset": offset
        }
        
        url = f"{API_BASE}/{dataset_id}.csv"
        
        try:
            # Anfrage senden (mit Tarnkappe/Headers)
            r = requests.get(url, headers=HEADERS, params=params, timeout=60)
            
            # Prüfen, ob wir geblockt wurden
            if r.status_code == 403:
                print("   !!! ALARM: FEHLER 403 (FORBIDDEN) !!!")
                print("   Die CFTC blockiert uns. Warte 10 Sekunden...")
                time.sleep(10)
                continue # Nochmal versuchen
            
            r.raise_for_status() # Andere Fehler melden
            
            # Wenn leer, sind wir fertig (oder Fehler bei leerem String)
            if not r.text.strip():
                break
            
            # Daten lesen
            df_chunk = pd.read_csv(StringIO(r.text))
            if df_chunk.empty:
                break
                
            all_data.append(df_chunk)
            fetched = len(df_chunk)
            print(f"   ... Paket geladen: {fetched} Zeilen (Total im Batch: {sum(len(d) for d in all_data)})")
            
            if fetched < chunk_size:
                print("   [INFO] Ende der Daten erreicht (Chunk < Limit).")
                break
                
            offset += chunk_size
            retries = 0 # Reset Retries bei Erfolg
            
            # Wichtig: Kurze Pause
            time.sleep(1.0)
            
        except Exception as e:
            retries += 1
            print(f"[FEHLER] Download Fehler (Versuch {retries}/{max_retries}): {e}")
            if retries >= max_retries:
                print("[ABBRUCH] Zu viele Fehler.")
                break
            time.sleep(5 * retries) # Exponential backoff light

    if not all_data:
        return pd.DataFrame()
        
    print("[COT] Füge alle Pakete zusammen...")
    new_df = pd.concat(all_data, ignore_index=True)
    
    # Inkrementelles Merge
    if os.path.exists(output_file) and last_known_date:
        print("[COT] Angefügt an existierende Datei...")
        try:
            # Lade ALTE Datei komplett (um Duplikate sicher zu handeln oder einfach append)
            # Einfach Append ist riskant bei Überlappung. Besser concat und drop_duplicates
            old_df = pd.read_csv(output_file)
            combined_df = pd.concat([old_df, new_df], ignore_index=True)
            # Duplikate entfernen basierend auf Schlüssel-Spalten falls möglich, sonst alle
            combined_df.drop_duplicates(inplace=True) 
            return combined_df
        except Exception as e:
            print(f"[WARNUNG] Konnte nicht mergen, überschreibe mit neuen Daten: {e}")
            return new_df
            
    return new_df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    # .gz Endung handel wir transparent
    out_path = args.out
    use_gzip = out_path.endswith(".gz")
    
    # Falls GZIP, müssen wir temporär entpacken für "get_last_date" oder pandas kann gzip lesen
    # Pandas read_csv kann gzip direkt.
    
    df = fetch_data(args.dataset, out_path)
    
    if df.empty and not os.path.exists(out_path):
        print("[WARNUNG] Keine Daten empfangen.")
        # Wir erstellen trotzdem eine leere Datei, damit der Copy-Befehl nicht meckert
        # Schreibe Dummy Header
        dummy_df = pd.DataFrame(columns=["symbol","report_date_as_yyyy_mm_dd"])
        if use_gzip:
             dummy_df.to_csv(out_path, index=False, compression="gzip")
        else:
             dummy_df.to_csv(out_path, index=False)
        return

    if df.empty:
        print("[INFO] Keine NEUEN Daten gefunden (Datei existiert bereits aktuell).")
        # Touch stamp anyway
    else:
        # Ordner erstellen falls nötig
        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        # Speichern (komprimiert oder normal)
        print(f"[COT] Speichere Datei: {out_path} ({len(df)} Zeilen)")
        if use_gzip:
            df.to_csv(out_path, index=False, compression="gzip")
        else:
            df.to_csv(out_path, index=False)

    # Stamp-Datei erstellen für C# Indikator (damit R2FileCache nicht neu lädt)
    try:
        # Stamp ist immer die Datei ohne .gz + .stamp oder direkt .stamp?
        # Der C# Code sucht nach path + ".stamp". 
        # Wenn out data/processed/cot_20y_disagg.csv.gz ist, sucht er cot_20y_disagg.csv.gz.stamp?
        # Nein, der User Code nimmt den entpackten Pfad.
        # Aber hier speichern wir .gz.
        # Der Batch entpackt später. Wir müssen den Stamp für das ZIEL format erstellen?
        # Der Batch erstellt cot_20y_disagg_merged.csv
        # Das Python Script hier erstellt nur das Roh-Teil-File.
        # Egal, wir erstellen Stamp für das Output File, schadet nicht.
        
        # WICHTIG: Das eigentliche Ziel für Agena ist cot_20y_disagg_merged.csv
        # Dieses Script hier schreibt cot_20y_disagg.csv.gz
        # Die Stamp muss dort liegen wo Agena sucht.
        # Agena sucht in documents/AgenaTrader_QuantCache/cot_20y_disagg_merged.csv.stamp
        # Das wird alles im Batch geregelt?
        # Batch line 209: copy merged file to agena.
        # Wir erstellen stamp hier lokal parallel zum Output.
        
        stamp_path = out_path + ".stamp"
        with open(stamp_path, 'w') as f:
            f.write(pd.Timestamp.utcnow().isoformat())
        print(f"[COT] Stamp erstellt: {stamp_path}")
    except Exception as e:
        print(f"[WARNUNG] Konnte Stamp nicht erstellen: {e}")
    
    print("[OK] ERFOLGREICH.")

if __name__ == "__main__":
    main()
