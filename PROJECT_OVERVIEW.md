# Projekt-Dokumentation: Analyse-Pro & AgenaTrader Integration

Dieses Dokument beschreibt die Architektur, den Zweck und die Komponenten des Trading-Analyse-Systems.

## 1. System-Überblick

Das Projekt ist ein **hybrides Analyse-System**, das komplexe Marktberechnungen (Optionen, COT, Makro) in **Python** durchführt und die Ergebnisse in **AgenaTrader (C#)** visualisiert.

**Der Datenfluss:**
1.  **Python Backend (`/scripts`):** Lädt Rohdaten (Yahoo, FRED, CFTC), berechnet Metriken (GEX, Max Pain, COT Index) und erstellt CSV-Dateien.
2.  **Cloud Storage (R2):** Die Ergebnisse werden (teilweise über GitHub Actions "Nightly") in einen Cloud-Speicher (Cloudflare R2) hochgeladen.
3.  **Lokal-Cache (`AgenaTrader_QuantCache`):** Ein lokaler Ordner auf deinem PC, der als Zwischenspeicher dient.
4.  **AgenaTrader Frontend (`/UserCode/Indicators`):** C#-Indikatoren laden die CSVs aus dem Cache und zeichnen Linien/Signale in den Chart.

---

## 2. Haupt-Komponenten

### A. Options-Analyse (Quant Pro)
*   **Ziel:** Analyse der Marktpositionierung durch Optionsdaten (Gamma Exposure, Max Pain).
*   **Wichtige Skripte:**
    *   `scripts/options_v60_ultra.py`: Das Herzstück. Berechnet GEX, Max Pain, Gamma-Flip Levels und erstellt `options_v60_ultra.csv` sowie Profile.
    *   `scripts/fetch_options_oi.py`: Leichtere Version für reines Open Interest.
*   **Indikatoren (C#):**
    *   `Options_Magnet_Scanner.cs`: Zeigt Max Pain, Call Walls und Put Walls im Chart.
    *   `Gamma_Profile.cs`: Zeigt ein Histogramm der Gamma-Verteilung am rechten Chartrand.
    *   `R2FileCache_Utility.cs`: Lädt die Daten automatisch im Hintergrund von R2 herunter.

### B. COT (Commitment of Traders)
*   **Ziel:** Verfolgung der Positionierung der "Big Boys" (Commercials vs. Managed Money).
*   **Wichtige Skripte:**
    *   `scripts/fetch_cot_20y.py`: Lädt 20 Jahre COT-Historie von der CFTC.
    *   `scripts/diagnose_cot_names.py`: Hilft beim Mapping von Futures-Namen.
*   **Indikatoren (C#):**
    *   `COT_ManagedMoney.cs` & sonstige: Visualisieren die Netto-Positionierung unter dem Chart.

### C. Makro & Market Core
*   **Ziel:** Überwachung des "Big Picture" (Zinsen, Inflation, Volatilität).
*   **Wichtige Skripte:**
    *   `scripts/macro_bridge_universal.py`: Lädt Zinsen (US10Y), Inflation etc.
    *   `scripts/fetch_market_core.py`: Lädt Basisdaten wie SPY, VIX, BTC für Korrelationen.
*   **Indikatoren (C#):**
    *   `Macro_Hazard_Display.cs`: Ein Dashboard im Chart für Makro-Risiken.

---

## 3. Ordner-Struktur

*   **`/scripts`**: Alle Python-Logik.
*   **`../AgenaTrader_QuantCache`**: Dein lokaler Daten-Hub. Hierhin müssen alle CSVs, damit AgenaTrader sie sieht.
*   **`../AgenaTrader/UserCode/Indicators`**: Der C#-Code, der in AgenaTrader läuft.
*   **`.github/workflows`**: Automatisierung. Hier ist definiert, dass jeden Nacht (`nightly.yml`) die Daten auf dem Server aktualisiert werden.

## 4. Wie man es benutzt (Workflow)

**Automatisch (Standard):**
1.  GitHub Actions läuft nachts und aktualisiert die Daten auf R2.
2. Der `OptionsData_Loader_Full` (Teil von `R2FileCache`) prüft alle 5 Minuten im Hintergrund auf neue Daten und lädt sie automatisch herunter (auch wenn AgenaTrader über Nacht läuft).

**Manuell (bei Problemen):**
*   Du kannst Skripte wie `force_download_r2.py` nutzen, um den Download zu erzwingen, falls der Automatismus hängt.
*   Du kannst `fetch_...` Skripte lokal ausführen, um Daten ohne Cloud-Umweg zu erzeugen.

---
*Erstellt am 13.12.2025*
