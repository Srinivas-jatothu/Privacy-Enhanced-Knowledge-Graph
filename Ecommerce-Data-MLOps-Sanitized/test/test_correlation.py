"""
A test module for testing correlation module.
"""
import os
import pytest
from module_l2_019 import analysis_func_001

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE_PATH = os.path.join(PROJECT_DIR, "data", "processed","scaler_output.parquet")
test_img_path =  os.path.join(PROJECT_DIR, "data", "test_data","corr.png")
test_corr_path = os.path.join(PROJECT_DIR, "data", "test_data","corr.parquet")
OUTPUT_PATH=(test_img_path,test_corr_path)

def analysis_func_007():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # Call the function with a non-existent file path
        analysis_func_001(param_006="nonexistent_file.csv")

def analysis_func_008():
    """
    Test that raises an error path is not a string.
    """
    # Add assertions based on expected outcomes
    with pytest.raises(TypeError):
        analysis_func_001(param_007=(1233,OUTPUT_PATH[1]))

def analysis_func_009():
    """
    Test that raises an error path is not a string.
    """
    # Add assertions based on expected outcomes
    with pytest.raises(TypeError):
        analysis_func_001(param_007=(OUTPUT_PATH[0],1233))

def analysis_func_010():
    """
    Test that raises an error when image not saved correctly.
    """
    # Call the function with the sample data
    try:
        os.remove(OUTPUT_PATH[0])
    except FileNotFoundError:
        # Call the function with the sample data
        analysis_func_001(param_007=OUTPUT_PATH)
        assert os.path.exists(OUTPUT_PATH[0])

def analysis_func_011():
    """
    Test that raises an error when parquet not saved correctly.
    """
    # Call the function with the sample data
    try:
        os.remove(OUTPUT_PATH[1])
    except FileNotFoundError:
        # Call the function with the sample data
        analysis_func_001(param_007=OUTPUT_PATH)
        assert os.path.exists(OUTPUT_PATH[1])

def analysis_func_012():
    """
    Test that raises an error when not 0< param_017 <1.
    """
    with pytest.raises(ValueError):
        #Call the function with a non-existent file path
        analysis_func_001(param_007=OUTPUT_PATH,param_008=80)
