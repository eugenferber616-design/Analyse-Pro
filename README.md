Analyse-Pro – Quant Pro Starter (GitHub Actions ready)

Automatisierter Datapipeline-Starter für Fundamentaldaten, Earnings-Resultate und Makrozeitreihen.
Die Pipeline läuft per GitHub Actions (nächtlich oder manuell) und schreibt die Ergebnisse in dieses Repository, sodass AgenaTrader/Scanner später ohne API-Calls auf vorbereitete Dateien zugreifen kann.

Was macht die Pipeline?

Läuft als Nightly-Job (und per Run workflow manuell).

Holt Daten API-schonend (Rate-Limiter + lokaler Cache in data/cache/cache.db).

Schreibt normalisierte CSV/JSON in data/processed/ und docs/.

Ziel: stabile, kostenlose/low-cost Basis, die später von Indikatoren/Scannern in AgenaTrader genutzt wird.


Aktuell abgedeckte Datenquellen & Outputs
1) Fundamentals (Finnhub)

Quelle: Finnhub (/stock/profile2, /stock/metric – Core/TTM/Margins)

Output: data/processed/fundamentals_core.csv

Beispielspalten: symbol, market_cap, beta, shares_out, pe_ttm, ps_ttm, pb_ttm, roe_ttm, gross_margin, oper_margin, net_margin, debt_to_equity

2) Earnings-Resultate (Finnhub)

Quelle: Finnhub (/stock/earnings) – IST/Schätzungen/Surprise

Output: data/processed/earnings_results.csv

Beispielspalten: symbol, period, eps_actual, eps_estimate, surprise, surprise_pct, revenue, revenue_estimate

Hinweis: Der Earnings-Kalender (reines Datum) ist optional und derzeit deaktiviert, da der Fokus zuerst auf Resultaten & Fundamentals liegt. Der Kalender kann jederzeit wieder zugeschaltet werden.

3) Makro (FRED)

Quelle: FRED (CPI, Arbeitslosenquote, 10Y/2Y, 10Y–3M, Fed Funds …)

Outputs:

Einzel-JSONs: data/macro/fred/*.json (z. B. CPIAUCSL.json, UNRATE.json, DGS10.json, …)

Kombi-JSON: data/macro/fred_all.json



Ablauf (Workflows)

Workflow: .github/workflows/nightly.yml

Trigger: täglich 00:30 UTC + manueller Start

Schritte (vereinfacht):

Python + Abhängigkeiten

Cache-DB initialisieren (data/cache/cache.db mit Tabelle kv)

Watchlist laden (oder Fallback erzeugen)

Fundamentals für die Watchlist ziehen → fundamentals_core.csv

Earnings-Resultate ziehen → earnings_results.csv

FRED Zeitreihen ziehen → JSONs

Dateien auflisten, committen, als Artifact bereitstellen



Verzeichnisstruktur
data/
 ├─ cache/
 │   └─ cache.db                 # SQLite KV-Cache (API-Responses / Fenster)
 ├─ earnings/
 │   └─ results/                 # pro-Symbol JSON (optional, je nach Script)
 ├─ fundamentals/                # Rohdaten/Cache je Symbol (optional)
 ├─ macro/
 │   └─ fred/
 │       ├─ CPIAUCSL.json
 │       ├─ UNRATE.json
 │       ├─ DGS10.json
 │       ├─ DGS2.json
 │       ├─ T10Y3M.json
 │       ├─ FEDFUNDS.json
 │       └─ fred_all.json
 ├─ processed/
 │   ├─ fundamentals_core.csv    # Scanner-/Indicator-Input
 │   ├─ earnings_results.csv     # Scanner-/Indicator-Input
 │   └─ earnings_next.json       # (optional, wenn Kalender aktiv)
 └─ reports/
     ├─ last_run.json            # Laufprotokoll/Stats
     └─ fred_errors.json         # FRED-Fehler (falls vorhanden)

docs/
 └─ earnings_next.json           # (optional publizierbarer Kalender-Export)

config/
 └─ config.yaml                  # Fenster/Ratelimits/Serien

scripts/
 ├─ fetch_fundamentals.py
 ├─ fetch_earnings_results.py
 ├─ fetch_fred.py
 ├─ cache.py                     # KV-Cache + RateLimiter
 └─ util.py                      # Helpers (YAML/JSON/Env)



 Konfiguration & Secrets
config/config.yaml (Auszug)

Rate Limits: finnhub_per_minute, finnhub_sleep_ms (mind. ~1300 ms empfohlen)

FRED-Serien: Liste der IDs (CPIAUCSL, UNRATE, DGS10, …)

(Optional) Earnings-Kalender-Fenster: finnhub.window_days, calendar_lookahead_days

GitHub Secrets (Repository → Settings → Secrets and variables → Actions)

FINNHUB_TOKEN (oder FINNHUB_API_KEY)

FRED_API_KEY

Budget-Tipp: Wir vermeiden LLM-Calls in der Nachtpipeline; OpenAI ist aktuell nicht aktiv. Später für Zusammenfassungen/Kommentare möglich, mit Monatslimit (z. B. 30 €).



Watchlists

Ordner: watchlists/

Akzeptiert:

mylist.csv mit Kopfzeile symbol

oder mylist.txt (eine Zeile = ein Symbol)

Fehlt die Datei, erzeugt der Workflow einen Fallback (AAPL, MSFT, NVDA, SPY).



Felder/Schema der wichtigsten Dateien
data/processed/fundamentals_core.csv
Spalte	Bedeutung (typisch)
symbol	Ticker
market_cap	Marktkapitalisierung
beta	Beta
shares_out	Aktienanzahl
pe_ttm, ps_ttm	KGV/PS TTM
pb_ttm	KBV TTM
roe_ttm	Return on Equity TTM
gross_margin	Bruttomarge (%)
oper_margin	Operative Marge (%)
net_margin	Nettomarge (%)
debt_to_equity	Verschuldungsgrad
data/processed/earnings_results.csv
Spalte	Bedeutung
symbol	Ticker
period	Berichtsperiode (YYYY-MM-DD)
eps_actual	EPS (tatsächlich)
eps_estimate	EPS (Schätzung)
surprise	Differenz (actual-estimate)
surprise_pct	Überraschung in %
revenue	Umsatz (tatsächlich)
revenue_estimate	Umsatz (Schätzung)



Nutzungsideen (AgenaTrader/Scanner)

Fundamental-Score / Filter: aus fundamentals_core.csv (z. B. niedrige pb_ttm, hohe roe_ttm, positive Margen).

Earnings-Surprise-Signal: positive surprise_pct + Umsatz-Beat (revenue > revenue_estimate).

Makro-Heatmap: aus data/macro/fred_all.json (Inflation, Zinsen, Spreads, Arbeitsmarkt).



Troubleshooting

Leere CSVs (nur Header):

Watchlist leer?

Finnhub-Endpunkte begrenzt (Plan)? → größere Symbolliste testen.

Rate-Limit zu streng? → finnhub_sleep_ms erhöhen (≥ 1300 ms).

Nichts committet:

Workflow schreibt nur bei nicht-leeren Dateien; Logs unter „List written files (sizes)“ prüfen.

Earnings-Kalender fehlt:

Kalender-Job ist absichtlich optional. Wenn gewünscht, Kalender-Schritt im Workflow aktivieren.
