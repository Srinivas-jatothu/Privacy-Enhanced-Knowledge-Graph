"""
Tests for data_loader module.
"""

import os
import pytest
from module_l2_003 import data_loader_func_001

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Use the param_035 directory to construct paths to other directories
PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed', 'raw_data.pkl')
EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'Online Retail.xlsx')

@pytest.mark.skip(reason="Skipping test_data_loader for now")
def analysis_func_014():
    """
    Test that data_loader_func_001 correctly loads data from Excel and saves as pickle
    when pickle doesn't exist.
    """
    # Temporarily rename the pickle to simulate its absence
    if os.path.exists(PICKLE_PATH):
        os.rename(PICKLE_PATH, PICKLE_PATH + ".bak")
    result = data_loader_func_001(param_011=PICKLE_PATH, param_012=EXCEL_PATH)
    assert result == PICKLE_PATH, f"Expected {PICKLE_PATH}, but got {result}."
    # Rename pickle back to its original name
    if os.path.exists(PICKLE_PATH + ".bak"):
        os.rename(PICKLE_PATH + ".bak", PICKLE_PATH)
@pytest.mark.skip(reason="Skipping test_data_loader for now")
def analysis_func_015():
    """
    Test that data_loader_func_001 raises an error when neither pickle nor Excel exists.
    """
    # Temporarily rename both files
    if os.path.exists(PICKLE_PATH):
        os.rename(PICKLE_PATH, PICKLE_PATH + ".bak")
    if os.path.exists(EXCEL_PATH):
        os.rename(EXCEL_PATH, EXCEL_PATH + ".bak")
    with pytest.raises(FileNotFoundError):
        data_loader_func_001(param_011=PICKLE_PATH, param_012=EXCEL_PATH)
    # Rename files back to their original names
    if os.path.exists(PICKLE_PATH + ".bak"):
        os.rename(PICKLE_PATH + ".bak", PICKLE_PATH)
    if os.path.exists(EXCEL_PATH + ".bak"):
        os.rename(EXCEL_PATH + ".bak", EXCEL_PATH)
