import pandas as pd
import yaml
import logging
from datetime import datetime, timezone
from dateutil import parser
import os

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
        # Handle Unix timestamp (seconds since epoch)
        if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.strip().isdigit()):
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            return dt.isoformat().replace('+00:00', 'Z')
        
        # Parse other formats (ISO, 1/15/2024, mixed formats)
        dt = parser.parse(str(ts))
        
        # Convert to UTC and make naive for consistent Z format
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            # Assume input without tz is already UTC
            dt = dt.replace(tzinfo=None)
            
        return dt.isoformat() + 'Z'
    
    except Exception as e:
        logger.warning(f"Could not parse timestamp: {ts} - Error: {e}")
        return None

def load_symbols(config):
    """Load symbols reference and return set of valid active symbols."""
    symbols_path = config['paths']['symbols_reference']
    if not os.path.exists(symbols_path):
        raise FileNotFoundError(f"Symbols file not found: {symbols_path}")
    
    df = pd.read_csv(symbols_path)
    # Filter only active symbols
    active_symbols = df[df['is_active'] == True]['symbol'].str.strip().str.upper().tolist()
    valid_symbols = set(active_symbols)
    
    logger.info(f"Loaded {len(valid_symbols)} active symbols from reference data")
    return valid_symbols

def load_and_clean_trades(config):
    """Load trades.csv, apply basic cleaning and validation."""
    trades_path = config['paths']['trades']
    if not os.path.exists(trades_path):
        raise FileNotFoundError(f"Trades file not found: {trades_path}")
    
    df = pd.read_csv(trades_path)
    logger.info(f"Loaded {len(df)} raw trade records")
    
    # Deduplicate based on trade_id (keep first occurrence)
    df = df.drop_duplicates(subset=['trade_id'], keep='first')
    logger.info(f"After deduplication: {len(df)} records")
    
    # Filter cancelled trades using config
    filter_status = config['validation']['filter_status']
    df = df[~df['trade_status'].isin(filter_status)]
    logger.info(f"After filtering cancelled trades: {len(df)} records")
    
    # Basic cleaning
    df['timestamp_utc'] = df['timestamp'].apply(normalize_timestamp)
    
    # Convert quantity and price
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Round price
    round_to = config['validation']['round_price_to']
    df['price'] = df['price'].round(round_to)
    
    # Symbol validation - uppercase for matching
    df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
    
    logger.info(f"Trades cleaning complete. Current shape: {df.shape}")
    return df

def main():
    config = load_config()
    logger.info("Starting ETL pipeline")
    logger.info("Config loaded successfully")
    
    # Load valid symbols
    valid_symbols = load_symbols(config)
    
    # Load and clean trades
    trades_df = load_and_clean_trades(config)
    
    logger.info(f"Price rounding set to {config['validation']['round_price_to']} decimals")

if __name__ == "__main__":
    main()