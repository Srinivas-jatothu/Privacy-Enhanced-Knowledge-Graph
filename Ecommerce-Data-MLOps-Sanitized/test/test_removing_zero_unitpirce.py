"""
A test for check the removal of zero unit prices
"""

import os
import pickle
from module_l2_009 import data_cleaning_func_005


# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_cleaning_description.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_removing_zero_unitprice.pkl')


def data_cleaning_func_012():
    """
    Test to check if zero unit prices have been removed
    """
    result = data_cleaning_func_005(param_001=INPUT_PICKLE_PATH,
                                    param_002=OUTPUT_PICKLE_PATH)

    assert result == OUTPUT_PICKLE_PATH, \
        f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."

    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)

    #checking if zero unit prices have been removed
    assert len(df[df['UnitPrice'] == 0]) == 0
