@echo off
setlocal ENABLEDELAYEDEXPANSION
title Analyse-Pro: FORCE RUN (EXACT MATCH)
color 0B

:: ==============================================================================
:: 1. PFAD-FINDUNG
:: ==============================================================================
set "WORK_DIR=%~dp0"
if "%WORK_DIR:~-1%"=="\" set "WORK_DIR=%WORK_DIR:~0,-1%"
cd /d "%WORK_DIR%"

:: Log Reset
set "ERROR_LOG=%WORK_DIR%\run_errors.log"
if exist "%ERROR_LOG%" del "%ERROR_LOG%" >nul 2>&1

echo.
echo ========================================================
echo   ALGO SYSTEM TEST (FORCE RUN)
echo   Status: Alles wird ausgefuehrt (inkl. COT & Options)
echo   Skript-Abgleich: Vollstaendige Pipeline
echo ========================================================
echo.

:: ==============================================================================
:: 2. API KEYS (Hier deine echten Keys eintragen!)
:: ==============================================================================
set "AGENA_DIR=C:\Users\eugen\Documents\AgenaTrader_QuantCache"
set "WATCHLIST_STOCKS=watchlists\mylist.txt"
set "WATCHLIST_ETF=watchlists\etf_sample.txt"

set "FINNHUB_API_KEY=d4j6aepr01queualmnkgd4j6aepr01queualmnl0"
set "FINNHUB_TOKEN=%FINNHUB_API_KEY%"
set "FRED_API_KEY=a62b1a06c6cdc4bb8c32d733a492326f"
set "ALPHAVANTAGE_API_KEY=G1MN4RGBHV2IZW98"
set "CFTC_APP_TOKEN=4bpishqo2ruvbx4xufghn43i2"
set "SEC_USER_AGENT=eugenferber616@gmail.com"
set "SIMFIN_API_KEY="

:: API-Drosselung
set "FINNHUB_SLEEP_MS=1300"

:: IDs für COT
set "COT_DISAGG_DATASET_ID=kh3c-gbw2"
set "COT_TFF_DATASET_ID=yw9f-hn96"

:: Requirements (nur sicherheitshalber)
pip install pandas_datareader yfinance scipy pyyaml >nul 2>&1

:: ==============================================================================
:: 3. HEAVY LIFT (COT, Fundamentals, Earnings, ETF-Basics)
:: ==============================================================================
color 0D
echo.
echo [1/3] HEAVY LIFT (dauert laenger!)...

:: --- COT DATEN ---
echo   - COT Reports...
python scripts\fetch_cot_20y.py --dataset "%COT_DISAGG_DATASET_ID%" --out data/processed/cot_20y_disagg.csv.gz || echo [%time%] ERR: COT Disagg >> "%ERROR_LOG%"
python scripts\fetch_cot_20y.py --dataset "%COT_TFF_DATASET_ID%"    --out data/processed/cot_20y_tff.csv.gz    || echo [%time%] ERR: COT TFF    >> "%ERROR_LOG%"

python scripts\build_cot_coverage.py
python scripts\fetch_cftc_energy_disagg.py
python scripts\merge_cot_energy_into_20y.py



:: --- FUNDAMENTALS / EARNINGS / ETF BASICS ---
echo   - Fundamentals & Earnings...
python scripts\fetch_earnings_calendar.py
python scripts\fetch_fundamentals_pro.py
python scripts\fetch_etf_basics.py --watchlist "%WATCHLIST_ETF%" --out data/processed/etf_basics.csv

:: OPTIONAL: Financials (PE, Margen etc.)
echo   - Financials Timeseries...
python scripts\fetch_financials.py || echo [%time%] ERR: Financials >> "%ERROR_LOG%"

:: ==============================================================================
:: 4. DAILY ROUTINE (Preise, HV, CDS, Risk, Options)
:: ==============================================================================
color 0A
echo.
echo [2/3] DAILY ROUTINE (Market, Risk, Options)...

:: A) Preis-Loader (für HV & Sonstiges)
echo   - Lade Preis-Historie...
(
echo import os
echo import yfinance as yf
echo import pandas as pd
echo from datetime import datetime, timedelta
echo.
echo os.makedirs('data/prices', exist_ok=True^)
echo wl = []
echo with open(r'%WATCHLIST_STOCKS%', 'r'^) as f:
echo     for line in f:
echo         if ',' in line:
echo             wl.append(line.split(','^)[0].strip(^))
echo.
echo start = (datetime.now(^) - timedelta(days=400^)).strftime('%%Y-%%m-%%d')
echo for sym in wl:
echo     try:
echo         df = yf.download(sym, start=start, progress=False, threads=False^)
echo         if not df.empty:
echo             if isinstance(df.columns, pd.MultiIndex^):
echo                 df.columns = df.columns.get_level_values(0^)
echo             df.to_csv(f"data/prices/{sym}.csv"^)
echo     except:
echo         pass
) > scripts\temp_fetch_prices.py

python scripts\temp_fetch_prices.py
del scripts\temp_fetch_prices.py 2>nul

:: B) RRG & Saisonalität (falls vorhanden)
echo   - RRG & Seasonality...
if exist scripts\build_rrg.py (
    python scripts\build_rrg.py
) else (
    echo [WARN] build_rrg.py fehlt im scripts-Ordner!
)

if exist scripts\build_seasonality.py (
    python scripts\build_seasonality.py
) else (
    echo [WARN] build_seasonality.py fehlt im scripts-Ordner!
)

:: C) Risk Engine (HV, CDS, RiskIndex)
echo   - Risk Engine (HV, CDS, Core)...
python scripts\fetch_market_core.py
python scripts\fetch_fred_core.py

python scripts\build_hv_summary.py --watchlist "%WATCHLIST_STOCKS%" --days 252 --out data/processed/hv_summary.csv
python scripts\build_cds_proxy_v2.py --watchlist "%WATCHLIST_STOCKS%" --fred-oas data/processed/fred_oas.csv --fundamentals data/processed/fundamentals_core.csv --hv data/processed/hv_summary.csv

python scripts\build_riskindex.py
python scripts\build_regime_state.py

:: D) Equity Master & Factor Scores
echo   - Equity Master & Factor Scores...
python scripts\build_equity_master.py --out data/processed/equity_master.csv
python scripts\build_factor_scores.py

:: E) OPTIONS STACK (IV, Expiry, Summary, Signals, v60)
echo   - Options Stack (IV + Expiry + Summary)...
if exist "data\processed\options_oi_totals.csv" del "data\processed\options_oi_totals.csv" >nul 2>&1

:: 1) Rohdaten ziehen
python scripts\fetch_options_oi.py

:: 2) Standardisierung nach Expiry & Strike
python scripts\build_by_expiry.py
python scripts\build_by_strike.py

:: 3) Summary anreichern (IV-Proxy, hv20-Alias, upper/lower_bound, expiry mit max OI, expected_move, Magnet)
python scripts\build_options_oi_summary.py

:: 4) Focus-Strike Datei für Scanner/Overlays
python scripts\build_options_by_strike.py

:: 5) Optionaler Fallback für Max-Strikes (falls Script vorhanden)
if exist scripts\post_build_strike_max.py (
    python scripts\post_build_strike_max.py
)

:: 6) Kompakte Signaltabelle + v60-ULTRA-File
python scripts\build_options_signals.py
python scripts\options_v60_ultra.py

:: 7) Sentiment (Whales + Borrow + OI)
python scripts\build_sentiment_scores.py

:: F) Short Interest LIGHT (nur Borrow)
echo   - Short Interest (Borrow Only)...
python scripts\fetch_short_interest.py

:: ==============================================================================
:: 5. KOPIEREN NACH AGENATRADER
:: ==============================================================================
echo.
echo [3/3] COPY nach AgenaTrader...

if not exist "%AGENA_DIR%" mkdir "%AGENA_DIR%" >nul 2>&1

:: Bloomberg Pro / RRG / Seasonality
if exist "data\processed\rrg_sectors.csv"          copy /Y "data\processed\rrg_sectors.csv"          "%AGENA_DIR%" >nul
if exist "data\processed\seasonality.csv"          copy /Y "data\processed\seasonality.csv"          "%AGENA_DIR%" >nul

:: Risk & Market
if exist "data\processed\hv_summary.csv"           copy /Y "data\processed\hv_summary.csv"           "%AGENA_DIR%" >nul
if exist "data\processed\cds_proxy.csv"            copy /Y "data\processed\cds_proxy.csv"            "%AGENA_DIR%" >nul
if exist "data\processed\riskindex_timeseries.csv" copy /Y "data\processed\riskindex_timeseries.csv" "%AGENA_DIR%" >nul
if exist "data\processed\market_core.csv"          copy /Y "data\processed\market_core.csv"          "%AGENA_DIR%" >nul
if exist "data\processed\equity_master.csv"        copy /Y "data\processed\equity_master.csv"        "%AGENA_DIR%" >nul
if exist "data\processed\factor_scores.csv"        copy /Y "data\processed\factor_scores.csv"        "%AGENA_DIR%" >nul

:: Options Core
if exist "data\processed\options_v60_ultra.csv"    copy /Y "data\processed\options_v60_ultra.csv"    "%AGENA_DIR%" >nul
if not exist "%AGENA_DIR%\profiles" mkdir "%AGENA_DIR%\profiles"
if exist "data\processed\profiles\*.csv" xcopy /Y "data\processed\profiles\*.csv" "%AGENA_DIR%\profiles" >nul
if exist "data\processed\options_oi_summary.csv"   copy /Y "data\processed\options_oi_summary.csv"   "%AGENA_DIR%" >nul
if exist "data\processed\options_signals.csv"      copy /Y "data\processed\options_signals.csv"      "%AGENA_DIR%" >nul
if exist "data\processed\short_interest.csv"       copy /Y "data\processed\short_interest.csv"       "%AGENA_DIR%" >nul
if exist "data\processed\sentiment_scores.csv"     copy /Y "data\processed\sentiment_scores.csv"     "%AGENA_DIR%" >nul
if exist "data\processed\whale_alerts.csv"         copy /Y "data\processed\whale_alerts.csv"         "%AGENA_DIR%" >nul

:: Heavy Data / COT / Fundamentals / Earnings
python scripts\deploy_to_agena.py
if exist "data\processed\earnings_results.csv"      copy /Y "data\processed\earnings_results.csv"      "%AGENA_DIR%" >nul
if exist "data\processed\fundamentals_core.csv"     copy /Y "data\processed\fundamentals_core.csv"     "%AGENA_DIR%" >nul
if exist "data\processed\financials_timeseries.csv" copy /Y "data\processed\financials_timeseries.csv" "%AGENA_DIR%" >nul
if exist "data\processed\etf_basics.csv"            copy /Y "data\processed\etf_basics.csv"            "%AGENA_DIR%" >nul

echo.
echo Starte Macro Monitor (Hintergrund)...
start "Macro Monitor" "Start_Agena_Macro.bat"
echo.
if exist "%ERROR_LOG%" (
    color 4F
    echo ======================================================
    echo   WARNUNG: FEHLER GEFUNDEN!
    echo   Details siehe: %ERROR_LOG%
    echo ======================================================
) else (
    echo ======================================================
    echo   ALLES GRUEN. SYSTEM LAEUFT PERFEKT.
    echo ======================================================
)
echo.
pause

