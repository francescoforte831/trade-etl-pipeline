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
- Only active symbols from the symbols reference are accepted
- Cancelled trades are removed from the cleaned output
- Prices are rounded to two decimal places
- Duplicate trade_ids keep only the first record
- Discrepancies are flagged when price differs by more than 0.01 or quantity does not match
- Missing values in required fields cause the record to go to the exceptions report
- Timestamps are converted to UTC ISO 8601 format

## Design decisions
- Used pandas for efficient handling of large CSV files
- Validation rules are loaded from `config.yaml` so they can be configured as needed
- Added logging and basic metrics for visibility into processing