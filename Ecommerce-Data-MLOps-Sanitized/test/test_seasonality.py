"""
This module checks if the seasonality scripts works.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'seasonality.pkl')

if os.path.exists(param_001):
    with open(param_001, "rb") as file:
        result= pickle.load(file)

def analysis_func_031():
    """
    This function raises an AssertionError if the specified 
    columns are not present in the result DataFrame.
    """
    assert 'Spending_Trend' in result.columns
    assert 'Monthly_Spending_Std' in result.columns
