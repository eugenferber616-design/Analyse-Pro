name: Nightly Data Pull & Cache (No LLM)

on:
  schedule:
    - cron: "30 0 * * *"
  workflow_dispatch:

permissions:
  contents: write

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Pull earnings (Finnhub)
        env:
          # beide Bezeichner setzen – Skripte lesen FINNHUB_TOKEN
          FINNHUB_TOKEN:   ${{ secrets.FINNHUB_TOKEN || secrets.FINNHUB_API_KEY }}
          FINNHUB_API_KEY: ${{ secrets.FINNHUB_API_KEY }}
          LLM_DISABLED: "1"
        run: |
          mkdir -p data/earnings
          python scripts/fetch_earnings.py \
            --watchlist watchlists/mylist.csv \
            --window-days 365 \
            --out data/earnings

      - name: Pull macro (FRED)
        env:
          FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
          LLM_DISABLED: "1"
        run: |
          mkdir -p data/macro/fred
          python scripts/fetch_fred.py \
            --out data/macro/fred

      - name: List written files (sizes)
        run: |
          echo "== data tree =="; find data -type f -printf "%p\t%k KB\n" | sort || true
          echo "== reports tree =="; find data/reports -type f -printf "%p\t%k KB\n" | sort || true
          test -f data/reports/last_run.json && echo "== last_run.json ==" && cat data/reports/last_run.json || true

      - name: Fail if outputs empty
        run: |
          cnt=$(find data -type f -size +0c | wc -l || true)
          if [ "$cnt" -eq 0 ]; then echo "No non-empty files produced."; exit 1; fi

      - name: Commit updated cache & reports
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add data/ || true
          git commit -m "Nightly cache update (no LLM)" || echo "Nothing to commit"
          git push

      - name: Upload data artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: data-bundle
          path: |
            data/**
