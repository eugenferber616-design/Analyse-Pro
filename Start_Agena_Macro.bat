@echo off
TITLE AgenaTrader Macro Monitor Controller  (ATRT_AGENA_MACRO)
COLOR 0B

:: ========================================================
:: KONFIGURATION – bitte Pfade prüfen/anpassen
:: ========================================================

:: 1. Pfad zur python.exe
set "PYTHON_EXE=C:\Users\eugen\AppData\Local\Programs\Python\Python313\python.exe"

:: 2. Pfad zu deinem Macro-Script
::    (falls du macro_bridge_detailed.py oder einen anderen Namen nutzt, hier anpassen)
set "SCRIPT_FILE=C:\Users\eugen\Desktop\ALGO\Analyse-Pro-main\Analyse-Pro-main\scripts\macro_bridge_universal.py"

:: 3. Pfad zu AgenaTrader
set "AGENA_EXE=C:\Program Files\AgenaTrader\AgenaTrader.exe"

:: 4. Update-Intervall in Sekunden (z.B. 3600 = 60 Minuten)
set "INTERVAL=3600"

echo.
echo [INIT] ATRT_AGENA_MACRO wird gestartet...
echo   Python : %PYTHON_EXE%
echo   Script : %SCRIPT_FILE%
echo   Agena  : %AGENA_EXE%
echo   Intervall: %INTERVAL% Sekunden
echo.

:: ========================================================
:: SCHRITT 1: AgenaTrader starten (falls noch nicht läuft)
:: ========================================================
tasklist /FI "IMAGENAME eq AgenaTrader.exe" 2>NUL | find /I /N "AgenaTrader.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo [INFO] AgenaTrader laeuft bereits. Klinke mich ein...
) else (
    echo [START] Starte AgenaTrader...
    if exist "%AGENA_EXE%" (
        start "" "%AGENA_EXE%"
        echo [WAIT] Warte 20 Sekunden, bis AgenaTrader bereit ist...
        timeout /t 20 >nul
    ) else (
        color 4F
        echo [ERROR] AgenaTrader.exe nicht gefunden!
        echo Bitte Pfad in atrt_agena_macro.bat pruefen:
        echo   %AGENA_EXE%
        pause
        exit /b 1
    )
)

:: ========================================================
:: SCHRITT 2: Wachhund-Loop
:: ========================================================
:LOOP
cls
echo ========================================================
echo   MACRO MONITOR ACTIVE   (ATRT_AGENA_MACRO)
echo   Gekoppelt an: AgenaTrader.exe
echo   Letztes Update: %TIME%
echo ========================================================
echo.

:: A) Prüfen: ist Agena noch an?
tasklist /FI "IMAGENAME eq AgenaTrader.exe" 2>NUL | find /I /N "AgenaTrader.exe">NUL
if "%ERRORLEVEL%"=="1" (
    echo [SHUTDOWN] AgenaTrader wurde beendet.
    echo Beende Macro Monitor...
    timeout /t 3 >nul
    exit /b 0
)

:: B) Python Script ausführen (Macro-Bridge)
if not exist "%PYTHON_EXE%" (
    color 4F
    echo [ERROR] python.exe nicht gefunden: %PYTHON_EXE%
    echo Bitte Pfad in atrt_agena_macro.bat anpassen.
    pause
    exit /b 1
)

if not exist "%SCRIPT_FILE%" (
    color 4F
    echo [ERROR] Macro-Script nicht gefunden:
    echo   %SCRIPT_FILE%
    echo Bitte Pfad in atrt_agena_macro.bat anpassen.
    pause
    exit /b 1
)

echo [%TIME%] Starte Macro-Analyse (FRED/Yahoo)...
"%PYTHON_EXE%" "%SCRIPT_FILE%"

echo.
echo [SLEEP] Warte %INTERVAL% Sekunden bis zum naechsten Update...
echo (Dieses Fenster bitte im Hintergrund offen lassen.)
echo.
timeout /t %INTERVAL% >nul

goto LOOP
