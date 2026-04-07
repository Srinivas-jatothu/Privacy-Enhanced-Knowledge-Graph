"""
This module tests if feature_engineering_func_003 script is working.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'feature_engineering_func_003.pkl')

if os.path.exists(param_001):
    with open(param_001, "rb") as file:
        result= pickle.load(file)

def analysis_func_019():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'Unique_Products_Purchased' in result.columns
    assert 'Is_UK' in result.columns
