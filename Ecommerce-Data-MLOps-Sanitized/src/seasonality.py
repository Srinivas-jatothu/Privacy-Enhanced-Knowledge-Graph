"""
This module analyzes the seasonal trends and how it affects customers and business
"""
import  pickle
import os
import pandas as pd
import numpy as np
import scipy.param_042 as sc

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
cancellation_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','feature_engineering_func_001.pkl')
param_002 = os.path.join(PROJECT_DIR, 'data','processed', 'seasonality.pkl')

def feature_engineering_func_005(param_003=param_001,
param_019=cancellation_pickle_path, param_005=param_002):
    """
    Calculate seasonality impacts and trends for customer spending.

    :param param_003: Input pickle file path containing customer spending data.
    :param param_019: Input pickle file path containing data from
    feature_engineering_func_001.
    :param param_005: Output pickle file path for storing processed customer data.
    :return: Processed customer data with seasonality impacts and trends.
    """

    if os.path.exists(param_003):
        with open(param_003, "rb") as file:
            df = pickle.load(file)

    if os.path.exists(param_019):
        with open(param_019, "rb") as file:
            param_057 = pickle.load(file)

    df['Total_Spend'] = df['UnitPrice'] * df['Quantity']
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month

    monthly_spending = df.groupby(['CustomerID', 'Year',
    'Month'])['Total_Spend'].sum().reset_index()
    seasonal_buying_patterns =(
        monthly_spending.groupby('CustomerID')['Total_Spend'].agg(['mean', 'std']).reset_index()
    )
    seasonal_buying_patterns.rename(columns={'mean': 'Monthly_Spending_Mean',
    'std': 'Monthly_Spending_Std'}, inplace=True)
    seasonal_buying_patterns['Monthly_Spending_Std'].fillna(0, inplace=True)

    def analysis_func_002(param_021):

        if len(param_021) > 1:
            x = np.arange(len(param_021))
            slope, _, _, _, _ = sc.linregress(x, param_021)
            result= slope

            return result

        return 0

    spending_trends =(
        monthly_spending.groupby('CustomerID')['Total_Spend'].apply(analysis_func_002).reset_index()
    )
    spending_trends.rename(columns={'Total_Spend': 'Spending_Trend'}, inplace=True)
    param_057 = pd.merge(param_057, seasonal_buying_patterns, on='CustomerID')
    param_057 = pd.merge(param_057, spending_trends, on='CustomerID')
    param_057['CustomerID'] = param_057['CustomerID'].astype(str)
    param_057 = param_057.convert_dtypes()

    with open(param_005, "wb") as file:
        pickle.dump(param_057, file)
    print(f"Data saved to {param_005}.")
    return param_005
