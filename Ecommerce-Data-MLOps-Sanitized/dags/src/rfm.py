"""
This module analyzes Recency, Frequency and Monetary methods to know about the value
of customers and dividing the base.
"""
import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
param_001=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
param_002 = os.path.join(PROJECT_DIR, 'data','processed', 'after_RFM.pkl')

def feature_engineering_func_004(param_003=param_001, param_005=param_002):
    """
    Process customer RFM data based on input pickle file.

    :param param_003: Input pickle file path containing customer data.
    :param param_005: Output pickle file path for storing processed RFM data.
    :return: Processed RFM data.
    """
    if os.path.exists(param_003):
        with open(param_003, "rb") as file:
            df = pickle.load(file)

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['InvoiceDay'] = df['InvoiceDate'].dt.date
    param_057 = df.groupby('CustomerID')['InvoiceDay'].max().reset_index()
    most_recent_date = df['InvoiceDay'].max()
    param_057['InvoiceDay'] = pd.to_datetime(param_057['InvoiceDay'])
    most_recent_date = pd.to_datetime(most_recent_date)
    param_057['Days_Since_Last_Purchase'] =(
        (most_recent_date - param_057['InvoiceDay']).dt.days
    )
    param_057.drop(columns=['InvoiceDay'], inplace=True)

    total_transactions = df.groupby('CustomerID')['InvoiceNo'].nunique().reset_index()
    total_transactions.rename(columns={'InvoiceNo': 'Total_Transactions'}, inplace=True)
    total_products_purchased = df.groupby('CustomerID')['Quantity'].sum().reset_index()
    total_products_purchased.rename(columns={'Quantity': 'Total_Products_Purchased'}, inplace=True)
    param_057 = pd.merge(param_057, total_transactions, on='CustomerID')
    param_057 = pd.merge(param_057, total_products_purchased, on='CustomerID')

    df['Total_Spend'] = df['UnitPrice'] * df['Quantity']
    total_spend = df.groupby('CustomerID')['Total_Spend'].sum().reset_index()
    average_transaction_value = total_spend.merge(total_transactions, on='CustomerID')
    average_transaction_value['Average_Transaction_Value'] =(
        average_transaction_value['Total_Spend'] / average_transaction_value['Total_Transactions']
    )
    param_057 = pd.merge(param_057, total_spend, on='CustomerID')
    param_057 =(
        pd.merge(param_057, average_transaction_value[
            ['CustomerID', 'Average_Transaction_Value']], on='CustomerID')
    )
    with open(param_005, "wb") as file:
        pickle.dump(param_057, file)
    print(f"Data saved to {param_005}.")
    return param_005
