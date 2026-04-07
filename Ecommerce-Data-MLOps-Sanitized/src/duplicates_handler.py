"""
A module for removing duplicates in dataset based on subset of 
following columns:
- InvoiceNo
- StockCode
- Description
- CustomerID
- Quantity
"""

import pickle
import os

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_missing_values.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_duplicates.pkl')

def data_cleaning_func_002(param_001=INPUT_PICKLE_PATH, param_002=OUTPUT_PICKLE_PATH):
    """
    Load the DataFrame from the input pickle, drop duplicates based on certain columns.
    Save the DataFrame back to a pickle and return its path.
    
    :param param_001: Path to the input pickle file.
    :param param_002: Path to the output pickle file.
    :return: Path to the saved pickle file.
    """
    # Load DataFrame from input pickle
    if os.path.exists(param_001):
        with open(param_001, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {param_001}")
    # Columns to check for duplicates
    columns_to_check = ['InvoiceNo', 'StockCode', 'Description', 'CustomerID', 'Quantity']
    # Drop duplicates
    df = df.drop_duplicates(subset=columns_to_check)
    # Save the data to output pickle
    with open(param_002, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {param_002}.")
    return param_002
