"""
This module defines the distribution of the customers' data with respect to regions.
"""
import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
behavorial_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','feature_engineering_func_002.pkl')
param_002 = os.path.join(PROJECT_DIR, 'data','processed', 'feature_engineering_func_003.pkl')

def feature_engineering_func_003(param_003=param_001,
param_014=behavorial_pickle_path, param_005=param_002):
    """
    Process geographic features and merge with behavioral data.

    :param param_003: Input pickle file path containing
                              transaction data with geographic information.
    :param behavioral_pickle_file: Input pickle file path after customer_behavior.
    :param param_005: Output pickle file path for storing processed
                               customer data with geographic features.
    :return: Processed customer data with added
             geographic features(whether the datapoint is from UK or not).
    """
    if os.path.exists(param_003):
        with open(param_003, "rb") as file:
            df = pickle.load(file)

    if os.path.exists(param_014):
        with open(param_014, "rb") as file:
            param_057 = pickle.load(file)

    df['Country'].value_counts(normalize=True).head()
    customer_country =(
        df.groupby(['CustomerID', 'Country']).size().reset_index(name='Number_of_Transactions')
    )
    customer_main_country =(
        customer_country.sort_values('Number_of_Transactions',
        ascending=False).drop_duplicates('CustomerID')
    )
    customer_main_country['Is_UK'] =(
        customer_main_country['Country'].apply(lambda x: 1 if x == 'United Kingdom' else 0)
    )
    param_057 =(
        pd.merge(param_057, customer_main_country[['CustomerID', 'Is_UK']],
    on='CustomerID', how='left')
    )
    with open(param_005, "wb") as file:
        pickle.dump(param_057, file)
    print(f"Data saved to {param_005}.")
    return param_005
