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

def main():
    config = load_config()
    logger.info("Starting ETL pipeline")
    logger.info("Config loaded successfully")
    
    # Load valid symbols
    valid_symbols = load_symbols(config)
    
    logger.info(f"Price rounding set to {config['validation']['round_price_to']} decimals")
    
    # Test timestamp normalization
    test_times = [
        "2024-01-15T15:59:39.000Z",
        "1/15/2024 12:17:17",
        "1705351086",
        "2024-01-15T14:42:31.000Z",
        "1/15/2024 9:55:29"
    ]
    for t in test_times:
        normalized = normalize_timestamp(t)
        logger.info(f"Original: {t} -> Normalized: {normalized}")

if __name__ == "__main__":
    main()