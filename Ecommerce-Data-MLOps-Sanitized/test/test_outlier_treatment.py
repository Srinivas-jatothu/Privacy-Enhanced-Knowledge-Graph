"""
A test for checking outlier treatment
"""

import os
import pickle
from module_l2_016 import data_cleaning_func_004


# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','seasonality.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_outlier_treatment.pkl')


def analysis_func_020():
    """
    Test to check if outliers are removed
    """
    result = data_cleaning_func_004(param_001=INPUT_PICKLE_PATH,
                                    param_002=OUTPUT_PICKLE_PATH)

    assert result == OUTPUT_PICKLE_PATH, \
        f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."

    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)

    with open(INPUT_PICKLE_PATH, "rb") as file:
        df_input= pickle.load(file)

    #checking if outliers have been removed

    assert len(df_input) > len(df)
