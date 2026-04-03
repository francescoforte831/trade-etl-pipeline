import sys
import os
import pytest
import pandas as pd
from io import StringIO

# Add project root to path so it can find src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pipeline import join_counterparty_and_flag_discrepancies

# Mock config for testing (now matches current pipeline)
test_config = {
    'paths': {
        'counterparty_fills': 'data/counterparty_fills.csv'
    },
    'validation': {
        'price_tolerance': 0.01,
        'required_fields_counterparty': ["our_trade_id", "symbol", "quantity", "price"]
    }
}

def test_join_counterparty_and_flag_discrepancies():
    # Create small test trades dataframe
    trades_data = """trade_id,timestamp_utc,symbol,quantity,price,buyer_id,seller_id
TRD001,2024-01-15T10:00:00Z,AAPL,100,150.50,BUY1,SEL1
TRD002,2024-01-15T11:00:00Z,MSFT,200,250.00,BUY2,SEL2
TRD003,2024-01-15T12:00:00Z,GOOGL,300,350.75,BUY3,SEL3
TRD004,2024-01-15T13:00:00Z,TSLA,400,180.25,BUY4,SEL4
"""
    trades_df = pd.read_csv(StringIO(trades_data))

    # Create counterparty data with some matches and some discrepancies
    cp_data = """external_ref_id,our_trade_id,timestamp,symbol,quantity,price,counterparty_id
EXT001,TRD001,2024-01-15T10:00:00,AAPL,100,150.50,CP1
EXT002,TRD002,2024-01-15T11:00:00,MSFT,200,250.10,CP2   # price diff > 0.01
EXT003,TRD003,2024-01-15T12:00:00,GOOGL,350,350.75,CP3   # quantity mismatch
EXT005,TRD005,2024-01-15T14:00:00,NVDA,500,300.00,CP5    # no match in trades
"""
    cp_df = pd.read_csv(StringIO(cp_data))

    # Mock pd.read_csv to return our test data
    original_read_csv = pd.read_csv
    pd.read_csv = lambda x: cp_df if 'counterparty' in str(x).lower() else trades_df

    try:
        # Now returns (cleaned, full_merged)
        cleaned, _ = join_counterparty_and_flag_discrepancies(trades_df, test_config)

        assert len(cleaned) == 4, f"Expected 4 records, got {len(cleaned)}"

        # Check specific cases
        trd001 = cleaned[cleaned['trade_id'] == 'TRD001'].iloc[0]
        assert trd001['counterparty_confirmed'] == True
        assert trd001['discrepancy_flag'] == False   # exact match

        trd002 = cleaned[cleaned['trade_id'] == 'TRD002'].iloc[0]
        assert trd002['counterparty_confirmed'] == True
        assert trd002['discrepancy_flag'] == True    # price diff > 0.01

        trd003 = cleaned[cleaned['trade_id'] == 'TRD003'].iloc[0]
        assert trd003['counterparty_confirmed'] == True
        assert trd003['discrepancy_flag'] == True    # quantity mismatch

        trd004 = cleaned[cleaned['trade_id'] == 'TRD004'].iloc[0]
        assert trd004['counterparty_confirmed'] == False
        assert trd004['discrepancy_flag'] == False   # no counterparty = no discrepancy flag

    finally:
        # Restore original function
        pd.read_csv = original_read_csv

if __name__ == "__main__":
    pytest.main([__file__])