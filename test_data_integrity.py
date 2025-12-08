import os
import pandas as pd
from datetime import datetime

# PFAD zur AgenaTrader Datei
csv_path = os.path.join(os.path.expanduser("~"), "Documents", "AgenaTrader_QuantCache", "macro_status.csv")

def check_macro_status():
    print(f"Prüfe Datei: {csv_path}")
    
    # 1. Existiert die Datei?
    if not os.path.exists(csv_path):
        print("FEHLER: Datei existiert nicht!")
        return False

    # 2. Ist Inhalt drin?
    try:
        with open(csv_path, "r") as f:
            content = f.read()
        
        if not content:
            print("FEHLER: Datei ist leer!")
            return False
            
        parts = content.split("|")
        
        # 3. Struktur-Check (Score | Nachrichten | Update)
        if len(parts) < 3:
            print(f"FEHLER: Falsches Format! Erwartet Trennzeichen '|'. Gefunden: {content}")
            return False
            
        # 4. Score Check
        score = int(parts[0])
        if score < 0 or score > 10:
            print(f"FEHLER: Score {score} ist unlogisch (Muss 0-10 sein).")
            return False

        # 5. Aktualität Check (Optional, warnt nur)
        print(f"Inhalt OK. Score: {score}")
        print(f"Nachricht: {parts[1]}")
        return True

    except Exception as e:
        print(f"FEHLER beim Lesen: {e}")
        return False

if __name__ == "__main__":
    if check_macro_status():
        print("[OK] TEST BESTANDEN: Das System laeuft korrekt.")
        exit(0) # Erfolgscode für den Agenten
    else:
        print("[FEHLER] TEST FEHLGESCHLAGEN: Bitte Skript reparieren.")
        exit(1) # Fehlercode für den Agenten
