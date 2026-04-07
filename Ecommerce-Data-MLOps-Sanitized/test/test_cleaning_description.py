"""
A test for checking cleaning description
"""

import os
import pickle
from module_l2_008 import utility_func_001


# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_anomaly_code.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_cleaning_description.pkl')


def analysis_func_006():
    """
    Test to check if correct descriptions have been removed
    """
    result = utility_func_001(param_001=INPUT_PICKLE_PATH,
                                    param_002=OUTPUT_PICKLE_PATH)

    assert result == OUTPUT_PICKLE_PATH, \
        f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."

    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)

    #checking if correct descriptions have been removed
    service_related_descriptions = ["Next Day Carriage", "High Resolution Image"]
    assert len(df[df['Description'].isin(service_related_descriptions)]) == 0
