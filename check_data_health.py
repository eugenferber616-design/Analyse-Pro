import pandas as pd
import os
import glob
from datetime import datetime, timedelta
from colorama import Fore, Style, init

init(autoreset=True)

# PFAD ZUM DATEN-CACHE (Passe User an wenn nötig, oder nutze relativen Pfad via os)
CACHE_DIR = os.path.join(os.path.expanduser("~"), "Documents", "AgenaTrader_QuantCache")

# KONFIGURATION: Welche Dateien prüfen wir und wie heißt die Datumsspalte?
# (Dateiname : Datums-Spalte)
FILES_TO_CHECK = {
    "cot_20y_disagg_merged.csv": "report_date_as_yyyy_mm_dd",  # COT Daten (Weekly)
    "cot_20y_tff.csv": "report_date_as_yyyy_mm_dd",            # TFF Daten (Weekly)
    "options_v60_ultra.csv": "Time",           # Optionsdaten (Daily Snapshot) - Achtung: Check Timestamp
    "earnings_results.csv": "date",            # Earnings
    "riskindex_timeseries.csv": "date",        # Dein Risk Index
    "macro_status.csv": "SPECIAL"              # Sonderfall (Textdatei)
}

def get_file_date(filepath):
    """Liest das Änderungsdatum der Datei"""
    return datetime.fromtimestamp(os.path.getmtime(filepath))

def check_csv_content(filename, date_col):
    path = os.path.join(CACHE_DIR, filename)
    
    if not os.path.exists(path):
        print(f"{Fore.RED}[FEHLT] {filename} wurde nicht gefunden!{Style.RESET_ALL}")
        return False

    try:
        # Sonderfall Macro Status (Pipe getrennt)
        if date_col == "SPECIAL":
            with open(path, 'r') as f:
                content = f.read()
                # Suche nach Update Zeitstempel im Text (vereinfacht: File Modify Date nehmen)
                mod_date = get_file_date(path)
                is_fresh = (datetime.now() - mod_date).total_seconds() < 3600*12 # 12 Std
                
                col = Fore.GREEN if is_fresh else Fore.YELLOW
                status = "AKTUELL (Heute)" if is_fresh else f"ALT ({mod_date.strftime('%Y-%m-%d')})"
                print(f"{col}[OK] {filename:<30} | Stand: {status}{Style.RESET_ALL}")
                return True

        # Normale CSVs
        df = pd.read_csv(path)
        if df.empty:
            print(f"{Fore.RED}[LEER] {filename} ist leer!{Style.RESET_ALL}")
            return False
            
        # Versuche Datumsspalte zu finden (Case insensitive)
        actual_col = None
        for c in df.columns:
            if c.lower() == date_col.lower() or date_col in c:
                actual_col = c
                break
        
        if not actual_col:
            # Fallback für Options (oft kein Datum in Spalte, nur File-Date)
            mod_date = get_file_date(path)
            print(f"{Fore.YELLOW}[WARN] {filename:<30} | Keine Datumsspalte '{date_col}' gef. | File-Datum: {mod_date.strftime('%Y-%m-%d')}{Style.RESET_ALL}")
            return True

        # Letztes Datum in der Datei finden
        df[actual_col] = pd.to_datetime(df[actual_col], errors='coerce')
        last_date = df[actual_col].max()
        
        if pd.isna(last_date):
            print(f"{Fore.RED}[ERR] {filename:<30} | Datum nicht lesbar!{Style.RESET_ALL}")
            return False

        # Bewertung: Wie alt?
        days_old = (datetime.now() - last_date).days
        
        # Logik für Warnungen
        if days_old > 10: # Älter als 10 Tage (kritisch für COT/Options)
            print(f"{Fore.RED}[VERALTET] {filename:<26} | Letzter Datenpunkt: {last_date.date()} ({days_old} Tage alt!){Style.RESET_ALL}")
        elif days_old > 4: # Älter als 4 Tage (ok für Weekly COT, schlecht für Daily)
            print(f"{Fore.YELLOW}[ALT] {filename:<31} | Letzter Datenpunkt: {last_date.date()} ({days_old} Tage alt){Style.RESET_ALL}")
        else:
            print(f"{Fore.GREEN}[OK] {filename:<32} | Letzter Datenpunkt: {last_date.date()}{Style.RESET_ALL}")

    except Exception as e:
        print(f"{Fore.RED}[ERROR] {filename}: {e}{Style.RESET_ALL}")

def run_check():
    print("="*60)
    print(f"SYSTEM DATA HEALTH CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Prüfe Ordner: {CACHE_DIR}")
    print("="*60)
    
    for fname, dcol in FILES_TO_CHECK.items():
        check_csv_content(fname, dcol)
    
    print("="*60)

if __name__ == "__main__":
    run_check()
