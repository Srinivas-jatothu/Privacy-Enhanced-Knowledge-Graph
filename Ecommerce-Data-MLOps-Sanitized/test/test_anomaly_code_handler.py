"""
A module for testing anomaly_code_handler module.
"""

import os
import pickle
from module_l2_007 import data_cleaning_func_001

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_transaction_status.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_anomaly_code.pkl')

def data_cleaning_func_009():
    """
    Test that data_cleaning_func_001 correctly removes rows with stock codes 
    that have 0 or 1 numeric characters.
    """
    result = data_cleaning_func_001(param_001=INPUT_PICKLE_PATH,
                                    param_002=OUTPUT_PICKLE_PATH)
    assert result == OUTPUT_PICKLE_PATH,\
        f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."
    # Load the output pickle file to check the 'StockCode' column
    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)
    # Check for stock codes with 0 or 1 numeric characters
    unique_stock_codes = df['StockCode'].unique()
    anomalous_stock_codes = [code for code in unique_stock_codes if
                             sum(c.isdigit() for c in str(code)) in (0, 1)]
    # Assert that no such anomalous stock codes exist
    assert len(anomalous_stock_codes) == 0, "Anomalous stock codes found in the dataframe."
