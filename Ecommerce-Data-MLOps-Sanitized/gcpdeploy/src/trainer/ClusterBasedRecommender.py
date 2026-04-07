"""
This module returns a dataframe with top three products which individual customers has not purchased yet based on the top purchased products from their corresponding cluster.
"""

import pandas as pd

# Removing outliers from transactions dataframe
def data_cleaning_func_008(df, param_054):
  """
  Removes transactions related to outlier customers from the pipeline_func_002 dataframe.

  Parameters:
  df (DataFrame): The pipeline_func_002 transaction dataframe.
  param_054 (DataFrame): Dataframe containing outlier customer IDs.

  Returns:
  DataFrame: A filtered dataframe excluding outlier customer transactions.
  """
  # Extract the CustomerIDs of the outliers and convert them to float for consistency
  outlier_customer_ids = param_054['CustomerID'].astype('float').unique()

  # Filter the pipeline_func_002 dataframe to exclude transactions from outlier customers
  df_filtered = df[~df['CustomerID'].isin(outlier_customer_ids)]

  return df_filtered

# Merging clusters to the transactions dataframe
def feature_engineering_func_009(df, param_055):
  """
  Merges the transaction data with customer data to include cluster information
  for each transaction.

  Parameters:
  df (DataFrame): The transaction dataframe, filtered to exclude outliers.
  param_055 (DataFrame): Customer data with clustering information.

  Returns:
  DataFrame: A merged dataframe including both transaction and cluster data.
  """
  # Ensure consistent data type for CustomerID across both dataframes before merging
  df = df.copy()
  df['CustomerID'] = df['CustomerID'].astype('float')

  param_055['CustomerID'] = param_055['CustomerID'].astype('float')

  # Merge the transaction data with the customer data
  param_056 = df.merge(param_055[['CustomerID', 'cluster']], on='CustomerID', how='inner')

  return param_056

# Identify top 10 products for each cluster
def feature_engineering_func_010(param_056):
  """
  Identifies the top 10 best-selling products in each customer cluster.

  Parameters:
  param_056 (DataFrame): The dataframe from merging transaction data with 
                            customer cluster information.

  Returns:
  DataFrame: A dataframe of the top 10 products for each cluster.
  """
  # Group by cluster, StockCode, and Description and sum the quantities
  grouped_data = param_056.groupby(['cluster', 'StockCode', 'Description'])['Quantity'].sum().reset_index()

  # Sort the products in each cluster by the total quantity sold in descending order
  sorted_grouped_data = grouped_data.sort_values(by=['cluster', 'Quantity'], ascending=[True, False])

  # Select the top 10 products for each cluster
  param_058 = sorted_grouped_data.groupby('cluster').head(10)

  return param_058

# create a record of products purchased by each customer in each cluster
def utility_func_006(param_056):
  """
  Creates a record of the quantities of each product purchased by each customer 
  in each cluster.

  Parameters:
  param_056 (DataFrame): The dataframe from merging transaction data with 
                            customer cluster information.

  Returns:
  DataFrame: A dataframe detailing customer purchases in each cluster.
  """
  # Group by CustomerID, cluster, and StockCode and sum the quantities
  param_059 = param_056.groupby(['CustomerID', 'cluster', 'StockCode'])['Quantity'].sum().reset_index()

  return param_059

# generate product recommendations for each customer
def feature_engineering_func_011(param_057, param_058, param_059):
  """
  Generates product recommendations for each customer based on the top products 
  in their cluster and their purchase history.

  Parameters:
  param_057 (DataFrame): Cleaned customer data with cluster information.
  param_058 (DataFrame): Dataframe of top 10 products for each cluster.
  param_059 (DataFrame): Dataframe detailing products purchased by each customer.

  Returns:
  list: A list of recommendations for each customer.
  """
  recommendations = []

  for cluster in param_058['cluster'].unique():
      # Retrieve top products for the current cluster
      top_products = param_058[param_058['cluster'] == cluster]
      customers_in_cluster = param_057[param_057['cluster'] == cluster]['CustomerID']
      
      for customer in customers_in_cluster:
          # Identify products already purchased by the customer
          customer_purchased_products = param_059[
              (param_059['CustomerID'] == customer) & 
              (param_059['cluster'] == cluster)
          ]['StockCode'].tolist()
          
          # Find top products in the best-selling list that the customer hasn't purchased yet
          top_products_not_purchased = top_products[~top_products['StockCode'].isin(customer_purchased_products)]
          top_3_products_not_purchased = top_products_not_purchased.head(3)
          
          # Append the recommendations to the list
          recommended_items = top_3_products_not_purchased[['StockCode', 'Description']].values.flatten().tolist()
          recommendations.append([customer, cluster] + recommended_items)

  return recommendations

# orchestrate the recommendation generation process by utilizing the previously defined functions
def feature_engineering_func_012(df, param_054, param_055):
  """
  Generates product recommendations for each customer based on clustering.

  Parameters:
  df (DataFrame): Transaction data.
  param_054 (DataFrame): Data of outlier customers.
  param_055 (DataFrame): Cleaned customer data with clustering info.

  Returns:
  DataFrame: Recommendations for each customer.
  """
  # Step 1: Remove outliers from the transaction data
  df_filtered = data_cleaning_func_008(df, param_054)

  # Step 2: Merge the transaction data with customer data to get cluster information
  param_056 = feature_engineering_func_009(df_filtered, param_055)

  # Step 3: Identify top-selling products in each cluster
  param_058 = feature_engineering_func_010(param_056)

  # Step 4: Record the products purchased by each customer
  param_059 = utility_func_006(param_056)

  # Step 5: Generate personalized product recommendations
  recommendations_list = feature_engineering_func_011(param_055, param_058, param_059)

  # Step 6: Convert the recommendations list to a DataFrame
  recommendations_columns = ['CustomerID', 'cluster', 'Rec1_StockCode', 'Rec1_Description',
                              'Rec2_StockCode', 'Rec2_Description', 'Rec3_StockCode', 'Rec3_Description']
  recommendations_df = pd.DataFrame(recommendations_list, columns=recommendations_columns)

  return recommendations_df

