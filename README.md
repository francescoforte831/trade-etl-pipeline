# trade-etl-pipeline

Pipeline designed to extract, transform, and load simulated trade data with data quality issues.

## How to run
1. Place the three input CSV files (defined in `src/config.yaml`) in the `data/` directory.
2. Create and activate a virtual environment:
```
   python3 -m venv venv
   source venv/bin/activate
```
3. Install dependencies:
```
   pip install -r requirements.txt
```
4. Run the pipeline:
```
   python src/pipeline.py
```
5. Output files will appear in the output/ directory.

## Assumptions
- Only active symbols from `symbols_reference.csv` are accepted in `cleaned_trades.json`; invalid symbols go to exceptions.
- Cancelled trades are completely filtered out and do not appear in cleaned_trades.json.
- Prices are rounded to exactly two decimal places.
- Duplicate trade_ids are deduplicated, keeping only the first occurrence.
- Discrepancies are flagged exactly as specified: price difference > $0.01 OR quantity mismatch (when a counterparty record exists).
- Symbol mismatch between trades and counterparty is also flagged as a discrepancy.
- Trades with discrepancies are kept in `cleaned_trades.json` with `discrepancy_flag: true` and also logged in `exceptions_report.json`.
- Trades without any counterparty record stay in `cleaned_trades.json` with `counterparty_confirmed: false` and `discrepancy_flag: false`.
- Records with missing quantity, missing price, or invalid symbols are sent only to `exceptions_report.json`.
- Timestamps from all formats (ISO, US date, Unix) are normalized to UTC ISO 8601 with Z suffix.

## Design decisions
- Used pandas for efficient handling of large CSV files
- Validation rules are loaded from `config.yaml` so they can be configured as needed
- Added logging and basic metrics for visibility into processing