"""
A module to detect and remove outliers
"""


import os
import pickle
from sklearn.ensemble import IsolationForest

# Determine the absolute path of the param_035 directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','seasonality.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','after_outlier_treatment.pkl')

def data_cleaning_func_004(param_001=INPUT_PICKLE_PATH, param_002=OUTPUT_PICKLE_PATH):
    """
     Load the DataFrame from the input pickle, 
    detect and remove outliers
    save the DataFrame back to a pickle and return its path
    
    """
    # Load DataFrame from input pickle
    if os.path.exists(param_001):
        with open(param_001, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {param_001}")

    param_034 = IsolationForest(contamination=0.05, random_state=0)

    df['Outlier_Scores'] = param_034.fit_predict(df.iloc[:, 1:].to_numpy())

    df['Is_Outlier'] = [1 if x == -1 else 0 for x in df['Outlier_Scores']]

    param_060 = df[df['Is_Outlier'] == 0]

    #dropping the columns
    param_060 = param_060.drop(columns=['Outlier_Scores', 'Is_Outlier'])

    #reseting the index
    param_060.reset_index(drop=True, inplace=True)


    with open(param_002, "wb") as file:
        pickle.dump(param_060, file)
    print(f"Data saved to {param_002}.")
    return param_002
