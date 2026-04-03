import pandas as pd
import yaml
import logging
from datetime import datetime, timezone
from dateutil import parser
import os
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    config_path = 'src/config.yaml'
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def normalize_timestamp(ts):
    """Convert various timestamp formats to UTC ISO 8601 string."""
    if pd.isna(ts) or ts == '' or ts is None:
        return None
    try:
        if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.strip().isdigit()):
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            return dt.isoformat().replace('+00:00', 'Z')
        dt = parser.parse(str(ts))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            dt = dt.replace(tzinfo=None)
        return dt.isoformat() + 'Z'
    except Exception as e:
        logger.warning(f"Could not parse timestamp: {ts} - Error: {e}")
        return None

def load_symbols(config):
    symbols_path = config['paths']['symbols_reference']
    if not os.path.exists(symbols_path):
        raise FileNotFoundError(f"Symbols file not found: {symbols_path}")
    df = pd.read_csv(symbols_path)
    active_symbols = df[df['is_active'] == True]['symbol'].str.strip().str.upper().tolist()
    valid_symbols = set(active_symbols)
    logger.info(f"Loaded {len(valid_symbols)} active symbols from reference data")
    return valid_symbols

def load_and_clean_trades(config):
    trades_path = config['paths']['trades']
    if not os.path.exists(trades_path):
        raise FileNotFoundError(f"Trades file not found: {trades_path}")
    
    df = pd.read_csv(trades_path)
    logger.info(f"Loaded {len(df)} raw trade records")
    
    # Required columns check
    required = config['validation']['required_fields_trades']
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in trades.csv: {missing}")
    
    # Deduplicate
    df = df.drop_duplicates(subset=['trade_id'], keep='first')
    logger.info(f"After deduplication: {len(df)} records")
    
    # Filter cancelled
    filter_status = config['validation']['filter_status']
    df = df[~df['trade_status'].isin(filter_status)]
    logger.info(f"After filtering cancelled trades: {len(df)} records")
    
    # Clean
    df['timestamp_utc'] = df['timestamp'].apply(normalize_timestamp)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce').round(config['validation']['round_price_to'])
    df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
    
    logger.info(f"Trades cleaning complete. Current shape: {df.shape}")
    return df

def join_counterparty_and_flag_discrepancies(trades_df, config):
    counterparty_path = config['paths']['counterparty_fills']
    if not os.path.exists(counterparty_path):
        raise FileNotFoundError(f"Counterparty file not found: {counterparty_path}")
    
    cp_df = pd.read_csv(counterparty_path)
    logger.info(f"Loaded {len(cp_df)} counterparty records")
    
    # Required columns check
    required_cp = config['validation']['required_fields_counterparty']
    missing = [col for col in required_cp if col not in cp_df.columns]
    if missing:
        raise ValueError(f"Missing required columns in counterparty_fills.csv: {missing}")
    
    cp_df = cp_df.rename(columns={'our_trade_id': 'trade_id'})
    merged = trades_df.merge(cp_df, on='trade_id', how='left', suffixes=('', '_cp'))
    
    price_tolerance = config['validation']['price_tolerance']
    merged['counterparty_confirmed'] = merged['external_ref_id'].notna()
    
    # Exact spec logic + symbol mismatch (as requested)
    price_diff = abs(merged['price'] - merged['price_cp'])
    quantity_match = (merged['quantity'] == merged['quantity_cp']) | merged['quantity_cp'].isna()
    symbol_match = (merged['symbol'] == merged['symbol_cp']) | merged['symbol_cp'].isna()
    
    merged['discrepancy_flag'] = merged['counterparty_confirmed'] & (
        (price_diff > price_tolerance) | ~quantity_match | ~symbol_match
    )
    
    final_columns = ['trade_id', 'timestamp_utc', 'symbol', 'quantity', 'price', 
                     'buyer_id', 'seller_id', 'counterparty_confirmed', 'discrepancy_flag']
    cleaned = merged[final_columns].copy()
    cleaned['quantity'] = cleaned['quantity'].astype('Int64')
    
    logger.info(f"After counterparty join: {len(cleaned)} records")
    logger.info(f"Discrepancies flagged: {cleaned['discrepancy_flag'].sum()}")
    return cleaned, merged  # return merged for accurate exception details

def generate_outputs(cleaned_trades, full_merged, config, valid_symbols):
    os.makedirs('output', exist_ok=True)
    
    valid_trades = cleaned_trades[
        (cleaned_trades['symbol'].isin(valid_symbols)) &
        cleaned_trades['price'].notna() &
        cleaned_trades['quantity'].notna()
    ].copy()
    
    cleaned_list = valid_trades.to_dict(orient='records')
    with open(config['paths']['output_cleaned'], 'w') as f:
        json.dump(cleaned_list, f, indent=2)
    
    logger.info(f"Generated cleaned_trades.json with {len(cleaned_list)} records")
    
    exceptions = []
    
    # Invalid symbols
    invalid = cleaned_trades[~cleaned_trades['symbol'].isin(valid_symbols)]
    for _, row in invalid.iterrows():
        exceptions.append({
            "record_id": row['trade_id'],
            "source_file": "trades.csv",
            "exception_type": "Invalid Symbol",
            "details": f"Symbol '{row['symbol']}' is not active or not found in symbols_reference.csv",
            "raw_data": row.to_dict()
        })
    
    # Missing numeric data
    missing = cleaned_trades[cleaned_trades['price'].isna() | cleaned_trades['quantity'].isna()]
    for _, row in missing.iterrows():
        fields = []
        if pd.isna(row['price']): fields.append("price")
        if pd.isna(row['quantity']): fields.append("quantity")
        exceptions.append({
            "record_id": row['trade_id'],
            "source_file": "trades.csv",
            "exception_type": "Missing Data",
            "details": f"Missing field(s): {', '.join(fields)}",
            "raw_data": row.to_dict()
        })
    
    # Data discrepancies (exact spec + symbol mismatch)
    discrep = valid_trades[valid_trades['discrepancy_flag']]
    for _, row in discrep.iterrows():
        cp_row = full_merged[full_merged['trade_id'] == row['trade_id']].iloc[0]
        reasons = []
        if abs(row['price'] - cp_row['price_cp']) > config['validation']['price_tolerance']:
            reasons.append("price difference > $0.01")
        if pd.notna(cp_row['quantity_cp']) and row['quantity'] != cp_row['quantity_cp']:
            reasons.append("quantity mismatch")
        if pd.notna(cp_row['symbol_cp']) and row['symbol'] != cp_row['symbol_cp']:
            reasons.append("symbol mismatch")
        details = "Discrepancy: " + ", ".join(reasons) if reasons else "Data discrepancy with counterparty"
        
        exceptions.append({
            "record_id": row['trade_id'],
            "source_file": "counterparty_fills.csv",
            "exception_type": "Data Discrepancy",
            "details": details,
            "raw_data": row.to_dict()
        })
    
    with open(config['paths']['output_exceptions'], 'w') as f:
        json.dump(exceptions, f, indent=2)
    
    logger.info(f"Generated exceptions_report.json with {len(exceptions)} exceptions")

def main():
    try:
        config = load_config()
        logger.info("Starting ETL pipeline")
        
        valid_symbols = load_symbols(config)
        trades_df = load_and_clean_trades(config)
        cleaned_trades, full_merged = join_counterparty_and_flag_discrepancies(trades_df, config)
        generate_outputs(cleaned_trades, full_merged, config, valid_symbols)
        
        logger.info("ETL pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()