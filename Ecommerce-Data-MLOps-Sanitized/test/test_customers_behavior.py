"""
This module checks if the feature_engineering_func_002 function works.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'feature_engineering_func_002.pkl')

if os.path.exists(param_001):
    with open(param_001, "rb") as file:
        result= pickle.load(file)

def analysis_func_013():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'CustomerID' in result.columns
    assert 'Hour' in result.columns
