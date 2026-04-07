"""
A test module for testing missing_values_handler module.
"""

import os
import pytest
from module_l2_004 import data_cleaning_func_003

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed', 'raw_data.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed','after_missing_values.pkl')

def data_cleaning_func_010():
    """
    Test successful removal of rows with missing values and saving of the dataframe.
    """
    result = data_cleaning_func_003(param_001=INPUT_PICKLE_PATH,
                            param_002=OUTPUT_PICKLE_PATH)
    assert result == OUTPUT_PICKLE_PATH, f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."
def data_cleaning_func_011():
    """
    Test that data_cleaning_func_003 raises an error when the input pickle doesn't exist.
    """
    # Rename the input pickle temporarily to simulate its absence
    if os.path.exists(INPUT_PICKLE_PATH):
        os.rename(INPUT_PICKLE_PATH, INPUT_PICKLE_PATH + ".bak")
    with pytest.raises(FileNotFoundError):
        data_cleaning_func_003(param_001=INPUT_PICKLE_PATH,
                       param_002=OUTPUT_PICKLE_PATH)
    # Rename the input pickle back to its original name
    if os.path.exists(INPUT_PICKLE_PATH + ".bak"):
        os.rename(INPUT_PICKLE_PATH + ".bak", INPUT_PICKLE_PATH)
