"""
This module tests if the RFM function is working.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_RFM.pkl')

if os.path.exists(param_001):
    with open(param_001, "rb") as file:
        result= pickle.load(file)

def analysis_func_026():
    """
      This function raises an AssertionError if the specified columns are not 
      present in the result DataFrame.
    """
    assert 'Days_Since_Last_Purchase' in result.columns
    assert 'Total_Transactions' in result.columns
