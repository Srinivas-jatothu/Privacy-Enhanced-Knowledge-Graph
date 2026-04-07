"""
The module shows how the behaviorial patterns of customers affect the business
based on weekly frequency.
"""

import pickle
import os
import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001 = os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
unique_pickle_path = os.path.join(PROJECT_DIR, 'data', 'processed', 'feature_engineering_func_006.pkl')
param_002 = os.path.join(PROJECT_DIR, 'data', 'processed', 'feature_engineering_func_002.pkl')

def feature_engineering_func_002(param_003=param_001, param_010=unique_pickle_path,
param_005=param_002):
    """
    Process customer behavior and generate relevant insights.

    :param param_003: Input pickle file path containing customer transaction data.
    :param param_010: Input pickle file path after feature_engineering_func_006.
    :param param_005: Output pickle file path for storing processed
                               customer behavior data.
    :return: Processed customer behavior data with insights.
    """
    if os.path.exists(param_003):
        with open(param_003, "rb") as file:
            df = pickle.load(file)

    if os.path.exists(param_010):
        with open(param_010, "rb") as file:
            param_057 = pickle.load(file)

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['InvoiceDay'] = df['InvoiceDate'].dt.date
    df['Day_Of_Week'] = df['InvoiceDate'].dt.dayofweek
    df['Hour'] = df['InvoiceDate'].dt.hour

    days_between_purchases = (
        df.groupby('CustomerID')['InvoiceDay']
        .apply(lambda x: (x.diff().dropna()).apply(lambda y: y.days))
    )
    average_days_between_purchases = (
        days_between_purchases.groupby('CustomerID').mean().reset_index()
    )
    average_days_between_purchases.rename(
        columns={'InvoiceDay': 'Average_Days_Between_Purchases'}, inplace=True)

    favorite_shopping_day = (
        df.groupby(['CustomerID', 'Day_Of_Week']).size().reset_index(name='Count')
    )
    favorite_shopping_day = (
        favorite_shopping_day.loc[favorite_shopping_day.groupby('CustomerID')['Count'].idxmax()]
        [['CustomerID', 'Day_Of_Week']]
    )
    favorite_shopping_hour = df.groupby(['CustomerID', 'Hour']).size().reset_index(name='Count')
    favorite_shopping_hour = (
        favorite_shopping_hour.loc[favorite_shopping_hour.groupby('CustomerID')
        ['Count'].idxmax()][['CustomerID', 'Hour']]
    )
    param_057 = pd.merge(param_057, average_days_between_purchases, on='CustomerID')
    param_057 = pd.merge(param_057, favorite_shopping_day, on='CustomerID')
    param_057 = pd.merge(param_057, favorite_shopping_hour, on='CustomerID')

    with open(param_005, "wb") as file:
        pickle.dump(param_057, file)
    print(f"Data saved to {param_005}.")
    return param_005
