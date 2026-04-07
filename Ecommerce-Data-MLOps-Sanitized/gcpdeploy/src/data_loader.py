"""
Module to handle the loading of e-commerce dataset from either pickle or Excel file format.
"""

import pickle
import os
import pandas as pd

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Use the param_035 directory to construct paths to other directories
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                   'processed', 'raw_data.pkl')
DEFAULT_EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'Online Retail.xlsx')

def data_loader_func_001(param_011=DEFAULT_PICKLE_PATH, param_012=DEFAULT_EXCEL_PATH):
    """
    Load the e-commerce dataset.
    First, try to load from the pickle file. If it doesn't exist, load from the excel file.
    Regardless of the source, save the loaded data as a pickle for future use and
    return the path to that pickle.
    
    :param param_011: Path to the pickle file.
    :param param_012: Path to the Excel file.
    :return: Path to the saved pickle file.
    """
    # Placeholder for the DataFrame
    df = None
    # Check if pickle file exists
    if os.path.exists(param_011):
        with open(param_011, "rb") as file:
            df = pickle.load(file)
        print(f"Data loaded successfully from {param_011}.")
    # If pickle doesn't exist, load from Excel
    elif os.path.exists(param_012):
        df = pd.read_excel(param_012)
        print(f"Data loaded from {param_012}.")
    else:
        error_message = f"No data found in the specified paths: {param_011} or {param_012}"
        print(error_message)
        raise FileNotFoundError(error_message)
    # Save the data to pickle for future use (or re-save it if loaded from existing pickle)
    os.makedirs(os.path.dirname(param_011), exist_ok=True)
    with open(param_011, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {param_011} for future use.")
    return param_011
