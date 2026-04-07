"""
A module for testing transaction_status_handler module.
"""

import os
import pickle
from module_l2_006 import data_cleaning_func_006

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_duplicates.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_transaction_status.pkl')

def analysis_func_032():
    """
    Test that data_cleaning_func_006 correctly adds the 'transaction_status' column 
    based on the 'InvoiceNo' and ensures statuses are 'Cancelled' or 'Completed'.
    """
    result = data_cleaning_func_006(param_001=INPUT_PICKLE_PATH,
                                       param_002=OUTPUT_PICKLE_PATH)
    assert result == OUTPUT_PICKLE_PATH, f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."
    # Load the output pickle file and check the 'transaction_status' column
    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)
    # Assert that 'transaction_status' column exists
    assert 'transaction_status' in df.columns,\
        "'transaction_status' column not found in the dataframe."
    # Check if all values in 'transaction_status' are either 'Cancelled' or 'Completed'
    unique_statuses = df['transaction_status'].unique()
    assert set(unique_statuses) == {'Cancelled', 'Completed'},\
        "Unexpected values found in 'transaction_status' column."
