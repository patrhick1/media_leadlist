# scripts/test_ms_conversion.py
import logging
import sys
import os
import json # _serialize_value uses json

# Add src directory to path to allow relative imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the function we want to test
try:
    from src.utils.csv_utils import _serialize_value
    print("Successfully imported _serialize_value")
except ImportError as e:
    print(f"Failed to import _serialize_value: {e}")
    print("Ensure you run this script from the project root directory, e.g., python scripts/test_ms_conversion.py")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.DEBUG) # Use DEBUG to see function logs
logger = logging.getLogger(__name__)

print("\n--- Testing Millisecond Timestamp Conversion ---")

test_cases = {
    "latest_pub_date_ms": 1746171600000,      # Example valid ms timestamp
    "earliest_pub_date_ms": 1458071685499,    # Another valid ms timestamp
    "listen_score": 85,                     # A non-timestamp field
    "update_frequency_hours": None,         # A None value field
    "another_field_ms": None,               # A field ending in _ms but with None value
    "bad_value_ms": "not a number",         # Invalid value for a timestamp field
    "string_field": "hello",                # A regular string
    "list_field": ["a", "b"],               # A list
}

for key, value in test_cases.items():
    print(f"\nInput Key  : {key}")
    print(f"Input Value: {value} (Type: {type(value).__name__})")
    try:
        converted_value = _serialize_value(key, value)
        print(f"Output Value: {converted_value} (Type: {type(converted_value).__name__})")
    except Exception as e:
        print(f"ERROR during conversion: {e}")

print("\n--- Test Complete ---") 