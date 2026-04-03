import sys
import os
import pytest
import pandas as pd
from io import StringIO

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pipeline import load_and_clean_trades

# Mock config for testing
test_config = {
    'paths': {
        'trades': 'data/trades.csv'  # not actually used in unit test
    },
    'validation': {
        'round_price_to': 2,
        'filter_status': ["CANCELLED"]
    }
}

def test_load_and_clean_trades_filters_cancelled():
    # Create small test dataframe
    test_data = """trade_id,timestamp,symbol,quantity,price,buyer_id,seller_id,trade_status
TRD001,2024-01-15T10:00:00, AAPL,100,150.5,BUY1,SEL1,EXECUTED
TRD002,2024-01-15T11:00:00,MSFT,200,250.0,BUY2,SEL2,CANCELLED
TRD003,2024-01-15T12:00:00,GOOGL,300,350.75,BUY3,SEL3,EXECUTED
"""
    df = pd.read_csv(StringIO(test_data))
    
    # Mock the read_csv to return our test data
    original_read_csv = pd.read_csv
    pd.read_csv = lambda x: df
    
    try:
        result = load_and_clean_trades(test_config)
        assert len(result) == 2, f"Expected 2 records after filtering, got {len(result)}"
        assert "CANCELLED" not in result['trade_status'].values, "Cancelled trades were not filtered"
    finally:
        pd.read_csv = original_read_csv

if __name__ == "__main__":
    pytest.main([__file__])