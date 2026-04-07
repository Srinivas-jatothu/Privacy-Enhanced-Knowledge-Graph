"""
A test module for testing PCA module.
"""
import os
import pytest
from module_l2_018 import ml_model_func_001

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH = os.path.join(PROJECT_DIR, "data", "processed","seasonality.pkl")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "data", "test_data","test_pca_output.parquet")
drop = []
remain_4=['Days_Since_Last_Purchase', 'Total_Transactions','Total_Products_Purchased',\
    'Total_Spend', 'Average_Transaction_Value','Unique_Products_Purchased',\
    'Average_Days_Between_Purchases','Day_Of_Week','Hour', 'Is_UK', 'Cancellation_Frequency',\
    'Cancellation_Rate', 'Monthly_Spending_Mean', 'Monthly_Spending_Std','Spending_Trend']

def analysis_func_021():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # non-existent input file path
        ml_model_func_001(param_006="nonexistent_file.csv")

def analysis_func_022():
    """
    Test that raises an error when the columns don't exist in df.
    """
    with pytest.raises(KeyError):
        #Call the function with a non-existent file path
        ml_model_func_001(param_007=OUTPUT_PATH,param_015=['Days_Since_Purchase'])

def analysis_func_023():
    """
    Test that raises an error if output file does not exist.
    """
    try:
        os.remove(OUTPUT_PATH)
    except FileNotFoundError:
        # Call the function with the sample data
        ml_model_func_001(param_007=OUTPUT_PATH,param_015=drop)
        assert os.path.exists(OUTPUT_PATH)

def analysis_func_024():
    """
    Test that raises an error when threshold >1.
    """
    with pytest.raises(ValueError):
        #Call the function with a non-existent file path
        ml_model_func_001(param_007=OUTPUT_PATH,param_015=drop, param_016=80)

def analysis_func_025():
    """
    Test that raises an error if columns already <4.
    """
    with pytest.raises(ValueError):
        # Call the function with the sample data
        ml_model_func_001(param_007=OUTPUT_PATH, param_015=remain_4)
