"""
A module for cleaning the description column
"""

import os
import pickle

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','after_anomaly_code.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','after_cleaning_description.pkl')

def utility_func_001(param_001=INPUT_PICKLE_PATH,
                            param_002=OUTPUT_PICKLE_PATH):
    """
    Load the DataFrame from the input pickle, 
    remove rows where the description cointains service related information
    and Standardize the text to uppercase
    save the DataFrame back to a pickle and return its path.
    
    """
    # Load DataFrame from input pickle
    if os.path.exists(param_001):
        with open(param_001, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {param_001}")

    service_related_descriptions = ["Next Day Carriage", "High Resolution Image"]

    df = df[~df['Description'].isin(service_related_descriptions)]

    df['Description'] = df['Description'].str.upper()

    with open(param_002, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {param_002}.")
    return param_002
