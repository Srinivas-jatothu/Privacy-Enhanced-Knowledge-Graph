"""
This module groups the values based on unique values of CustomerID and orders.
"""
import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
rfm_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_RFM.pkl')
param_002 = os.path.join(PROJECT_DIR, 'data','processed', 'feature_engineering_func_006.pkl')

def feature_engineering_func_006(param_003=param_001, param_022=rfm_pickle_path ,
param_005=param_002):
    """
    Calculate the number of unique products purchased by each customer.

    :param param_003: Input pickle file path containing transaction data.
    :param param_022: Input pickle file path containing RFM data.
    :param param_005: Output pickle file path for storing processed customer data
                               with unique product information.
    :return: Processed customer data with added information about unique products purchased.
    """
    param_057 = pd.DataFrame()

    if os.path.exists(param_003):
        with open(param_003, "rb") as file:
            df = pickle.load(file)

    if os.path.exists(param_022):
        with open(param_022, "rb") as file:
            param_057 = pickle.load(file)

    if 'CustomerID' in df.columns and not df.empty:
        unique_products_purchased = (
            df.groupby('CustomerID')['StockCode']
            .nunique()
            .reset_index()
            .rename(columns={'StockCode': 'Unique_Products_Purchased'})
        )

        param_057 = pd.merge(
            param_057,
            unique_products_purchased[unique_products_purchased['CustomerID']
            .isin(param_057['CustomerID'])],
            on='CustomerID',
        )
    with open(param_005, "wb") as file:
        pickle.dump(param_057, file)
    print(f"Data saved to {param_005}.")
    return param_005
