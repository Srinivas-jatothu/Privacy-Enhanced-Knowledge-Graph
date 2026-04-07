"""
This module shows how cancelling of orders affect the business and the data.
It also shows the frequency of cancellation and cancellation rate.
"""
import  pickle
import os
import pandas as pd
import numpy as np

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
geographic_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','feature_engineering_func_003.pkl')
param_002 = os.path.join(PROJECT_DIR, 'data','processed', 'feature_engineering_func_001.pkl')

def feature_engineering_func_001(param_003=param_001,
param_004=geographic_pickle_path, param_005= param_002):
    """
    Process cancellation frequency and calculate cancellation rates for customers.

    :param param_003: Input pickle file path containing transaction data.
    :param param_004: Input pickle file path after feature_engineering_func_003.
    :param param_005: Output pickle file path for storing processed customer data.
    :return: Processed customer data with cancellation details and rates.
    """
    if os.path.exists(param_003):
        with open(param_003, "rb") as file:
            df = pickle.load(file)

    if os.path.exists(param_004):
        with open(param_004, "rb") as file:
            param_057 = pickle.load(file)

    total_transactions = df.groupby('CustomerID')['InvoiceNo'].nunique().reset_index()
    df['Transaction_Status'] = np.where(df['InvoiceNo'].astype(str).str.startswith('C'),
    'Cancelled', 'Completed')
    cancelled_transactions = df[df['Transaction_Status'] == 'Cancelled']
    cancellation_frequency = (
        cancelled_transactions.groupby('CustomerID')['InvoiceNo'].nunique().reset_index()
    )
    cancellation_frequency.rename(columns={'InvoiceNo': 'Cancellation_Frequency'}, inplace=True)
    param_057 = pd.merge(param_057, cancellation_frequency, on='CustomerID', how='left')
    param_057['Cancellation_Frequency'].fillna(0, inplace=True)
    param_057['Cancellation_Rate'] =(
        param_057['Cancellation_Frequency'] / total_transactions['InvoiceNo']
    )
    with open(param_005, "wb") as file:
        pickle.dump(param_057, file)
    print(f"Data saved to {param_005}.")
    return param_005
