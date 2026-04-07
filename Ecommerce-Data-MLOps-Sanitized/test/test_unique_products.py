"""
This module tests if the feature_engineering_func_006 script works.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'feature_engineering_func_006.pkl')

if os.path.exists(param_001):
    with open(param_001, "rb") as file:
        result= pickle.load(file)

def feature_engineering_func_014():
    """
     This function raises an Assertion error if the columns are not present in the result.
    """
    assert 'Unique_Products_Purchased' in result.columns
    assert 'Average_Transaction_Value' in result.columns
