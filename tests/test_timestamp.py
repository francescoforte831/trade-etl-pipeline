import sys
import os
import pytest

# Add the project root to Python path so it can find src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pipeline import normalize_timestamp

def test_normalize_timestamp():
    test_cases = [
        # ISO with Z
        ("2024-01-15T15:59:39.000Z", "2024-01-15T15:59:39Z"),
        # US date format
        ("1/15/2024 12:17:17", "2024-01-15T12:17:17Z"),
        # Unix timestamp
        (1705351086, "2024-01-15T20:38:06Z"),
        # Another ISO
        ("2024-01-15T14:42:31.000Z", "2024-01-15T14:42:31Z"),
        # Another US format
        ("1/15/2024 9:55:29", "2024-01-15T09:55:29Z"),
        # Empty / missing values
        ("", None),
        (None, None),
        ("   ", None),
    ]
    
    for input_val, expected in test_cases:
        result = normalize_timestamp(input_val)
        assert result == expected, f"Failed for input {input_val}. Got {result}, expected {expected}"

if __name__ == "__main__":
    pytest.main([__file__])