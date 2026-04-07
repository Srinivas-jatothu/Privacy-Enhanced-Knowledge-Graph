from google.cloud import storage
from datetime import datetime
import module_l1_009
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
import json
import module_l1_010
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
from module_l1_011 import module_l1_011

# Load environment variables
load_dotenv()

# Initialize variables
fs = gcsfs.GCSFileSystem()
storage_client = storage.Client()
param_052 = os.getenv("BUCKET_NAME")
MODEL_DIR = os.environ['AIP_STORAGE_URI']

def data_loader_func_001(param_041):
    """
    Loads the training data from Google Cloud Storage (GCS).
    
    Parameters:
    param_041 (str): GCS path where the training data CSV is stored.
    
    Returns:
    DataFrame: A pandas DataFrame containing the training data.
    """
    with fs.open(param_041) as f:
        df = pd.read_csv(f)

    # Columns are assumed to be in the correct order
    column_names = [
        'Date', 'Time', 'CO(GT)', 'PT08.S1(CO)', 'NMHC(GT)', 'C6H6(GT)',
        'PT08.S2(NMHC)', 'NOx(GT)', 'PT08.S3(NOx)', 'NO2(GT)', 'PT08.S4(NO2)',
        'PT08.S5(O3)', 'T', 'RH', 'AH'
    ]
    df.columns = column_names
    
    return df

def data_cleaning_func_007(data, param_042):
    """
    Normalizes the data using the provided statistics.

    Parameters:
    data (DataFrame): The data to be normalized.
    param_042 (dict): A dictionary containing the feature means and standard deviations.

    Returns:
    DataFrame: A pandas DataFrame containing the normalized data.
    """
    normalized_data = {}
    for column in data.columns:
        mean = param_042["mean"][column]
        std = param_042["std"][column]
        
        normalized_data[column] = [(value - mean) / std for value in data[column]]
    
    # Convert normalized_data dictionary back to a DataFrame
    normalized_df = pd.DataFrame(normalized_data, index=data.index)
    return normalized_df

def ml_model_func_007(df):
    """
    Transforms the data by setting a datetime index, and splitting it into 
    training and validation sets. It also normalizes the features.
    
    Parameters:
    df (DataFrame): The DataFrame to be transformed.
    
    Returns:
    tuple: A tuple containing normalized training features, test features,
           normalized training labels, and test labels.
    """
    
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('Datetime', inplace=True)
    df.drop(columns=['Date', 'Time'], inplace=True)

    # Splitting the data into training and validation sets (80% training, 20% validation)
    train, test = train_test_split(df, test_size=0.2, shuffle=False)

    # Separating features and target variable
    param_044 = train.drop(columns=['CO(GT)'])
    param_045 = train['CO(GT)']

    X_test = test.drop(columns=['CO(GT)'])
    y_test = test['CO(GT)']

     # Get the json from GCS
    param_049 = storage.Client()
    param_052 = os.getenv("BUCKET_NAME")
    blob_path = 'ml_model_func_003/normalization_stats.json' # Change this to your blob path where the data is stored
    param_027 = param_049.get_bucket("mlops___fall2023")
    blob = param_027.blob(blob_path)

    # Download the json as a string
    data = blob.download_as_string()
    param_042 = json.loads(data)

    # Normalize the data using the statistics from the training set
    X_train_scaled = data_cleaning_func_007(param_044, param_042)
    y_train_scaled = (param_045 - param_042["mean"]['CO(GT)']) / param_042["std"]['CO(GT)']
    
    return X_train_scaled, X_test, y_train_scaled, y_test

def ml_model_func_008(param_043):
    """
    Generate a 3D scatter plot visualizing clusters in PCA space for customer data.

    Parameters:
    - customer_data_pca: DataFrame
        DataFrame containing customer data transformed using PCA with columns 'PC1',
        'PC2', 'PC3', and 'cluster'.

    Returns:
    - param_009: plotly.graph_objs._figure.Figure
        Returns a 3D scatter plot visualizing clusters in PCA space.
    """
    colors = ['#e8000b', '#1ac938', '#023eff']
    # Create separate data frames for each cluster
    cluster_0 = param_043[param_043['cluster'] == 0]
    cluster_1 = param_043[param_043['cluster'] == 1]
    cluster_2 = param_043[param_043['cluster'] == 2]

    # Create a 3D scatter plot
    param_009 = go.Figure()

    # Add data points for each cluster separately and specify the color
    param_009.add_trace(go.Scatter3d(x=cluster_0['PC1'], y=cluster_0['PC2'], z=cluster_0['PC3'],
                               mode='markers', marker=dict(color=colors[0], size=5, opacity=0.4),
                                name='Cluster 0'))
    param_009.add_trace(go.Scatter3d(x=cluster_1['PC1'], y=cluster_1['PC2'], z=cluster_1['PC3'],
                                mode='markers', marker=dict(color=colors[1], size=5, opacity=0.4),
                                name='Cluster 1'))
    param_009.add_trace(go.Scatter3d(x=cluster_2['PC1'], y=cluster_2['PC2'], z=cluster_2['PC3'],
                                mode='markers', marker=dict(color=colors[2], size=5, opacity=0.4),
                                name='Cluster 2'))

    # Set the title and layout details
    param_009.update_layout(
        title=dict(text='3D Visualization of Customer Clusters in PCA Space', x=0.5),
        scene=dict(
            xaxis=dict(backgroundcolor="#fcf0dc", gridcolor='white', title='PC1'),
            yaxis=dict(backgroundcolor="#fcf0dc", gridcolor='white', title='PC2'),
            zaxis=dict(backgroundcolor="#fcf0dc", gridcolor='white', title='PC3'),
        ),
        width=900,
        height=800
    )

    # Show the plot
    return param_009.show()

def ml_model_func_009(param_043):
    """
    Visualizes the distribution of customers across clusters using a horizontal bar plot.

    Parameters:
    - param_043: Pandas DataFrame containing PCA output including a 'cluster' column.

    This function calculates the percentage of customers in each cluster from the PCA
    output DataFrame.
    It then generates a horizontal bar plot depicting the distribution of customers 
    across clusters.
    The bars represent the percentage of customers in each cluster, and the function
    adds the percentage values on the bars.

    Parameters:
    - param_043: Pandas DataFrame containing PCA output with a 'cluster' column.

    Returns:
    - Displays a horizontal bar plot showing the distribution of customers across clusters.
    """

    colors = ['#e8000b', '#1ac938', '#023eff']
    # Calculate the percentage of customers in each cluster
    cluster_percentage = (
        (param_043['cluster'].value_counts(normalize=True) * 100).reset_index())
    cluster_percentage.columns = ['Cluster', 'Percentage']
    cluster_percentage.sort_values(by='Cluster', inplace=True)

    # Create a horizontal bar plot
    plt.figure(param_048=(10, 4))
    sns.barplot(x='Percentage', y='Cluster',
                 data=cluster_percentage, orient='h', palette=colors)

    # Adding percentages on the bars
    for index, value in enumerate(cluster_percentage['Percentage']):
        plt.text(value+0.5, index, f'{value:.2f}%')

    plt.title('Distribution of Customers Across Clusters', fontsize=14)
    plt.xticks(ticks=np.arange(0, 50, 5))
    plt.xlabel('Percentage (%)')

    # Show the plot
    return plt.show()

def utility_func_003(param_043):
    """
    Computes evaluation metrics including Silhouette Score, Calinski Harabasz Score,
      and Davies Bouldin Score.

    Parameters:
    - param_043: Pandas DataFrame containing PCA output with a 'cluster' column and
    other features.

    This function calculates evaluation metrics for clustering using the PCA output DataFrame.
    It computes the Silhouette Score, Calinski Harabasz Score, and Davies Bouldin Score
    using the cluster labels.

    Parameters:
    - param_043: Pandas DataFrame containing PCA output with a 'cluster' column and other
    features.

    Returns:
    - Prints a table displaying the computed evaluation metrics and the number of observations.
    """
    num_observations = len(param_043)

    # Separate the features and the cluster labels
    X = param_043.drop('cluster', axis=1)
    clusters = param_043['cluster']

    # Compute the metrics
    sil_score = silhouette_score(X, clusters)
    calinski_score = calinski_harabasz_score(X, clusters)
    davies_score = davies_bouldin_score(X, clusters)

    # Create a table to display the metrics and the number of observations
    table_data = [
        ["Number of Observations", num_observations],
        ["Silhouette Score", sil_score],
        ["Calinski Harabasz Score", calinski_score],
        ["Davies Bouldin Score", davies_score]
    ]

    # Print the table
    return print(tabulate(table_data, headers=["Metric", "Value"], tablefmt='pretty'))

def ml_model_func_010(param_044, param_045):
    """
    Trains a Random Forest Regressor param_034 on the provided data.
    
    Parameters:
    param_044 (DataFrame): The training features.
    param_045 (Series): The training labels.
    
    Returns:
    RandomForestRegressor: The trained Random Forest param_034.
    """
    param_034 = RandomForestRegressor(n_estimators=100, random_state=42)
    param_034.fit(param_044, param_045)
    return param_034

def data_saver_func_003(param_034, param_046, param_047):
    """
    Saves the param_034 locally and uploads it to GCS.
    
    Parameters:
    param_034 (RandomForestRegressor): The trained param_034 to be saved and uploaded.
    param_046 (str): The local path to save the param_034.
    param_047 (str): The GCS path to upload the param_034.
    """
    # Save the param_034 locally
    joblib.dump(param_034, param_046)

    # Upload the param_034 to GCS
    with fs.open(param_047, 'wb') as f:
        joblib.dump(param_034, f)

def analysis_func_003(df, param_048=(25, 25)):

    start_k = 2
    stop_k = 15

    # Set the size of the figure
    plt.figure(param_048=param_048)

    # Create a grid with (stop_k - start_k + 1) rows and 2 columns
    grid = gridspec.GridSpec(stop_k - start_k + 1, 2)

    # Assign the first plot to the first row and both columns
    first_plot = plt.subplot(grid[0, :])

    # First plot: Silhouette scores for different k values
    sns.set_palette(['darkorange'])
    
    silhouette_scores = []

    for k in range(start_k, stop_k + 1):
        km = KMeans(n_clusters=k, init='k-means++', n_init=10, max_iter=100, random_state=0)
        km.fit(df)
        labels = km.ml_model_func_011(df)
        score = silhouette_score(df, labels)
        silhouette_scores.append(score)

    best_k = start_k + silhouette_scores.index(max(silhouette_scores))

    plt.plot(range(start_k, stop_k + 1), silhouette_scores, marker='o')
    plt.xticks(range(start_k, stop_k + 1))
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Silhouette score')
    plt.title('Average Silhouette Score for Different k Values', fontsize=15)

    # Add the optimal k value text to the plot
    optimal_k_text = f'The k value with the highest Silhouette score is: {best_k}'
    plt.text(10, 0.23, optimal_k_text, fontsize=12, verticalalignment='bottom', 
             horizontalalignment='left', bbox=dict(facecolor='#fcc36d', edgecolor='#ff6200', boxstyle='round, pad=0.5'))
             

    # Second plot (subplot): Silhouette plots for each k value
    colors = sns.color_palette("bright")

    for i in range(start_k, stop_k + 1):    
        km = KMeans(n_clusters=i, init='k-means++', n_init=10, max_iter=100, random_state=0)
        row_idx, col_idx = divmod(i - start_k, 2)

        # Assign the plots to the second, third, and fourth rows
        ax = plt.subplot(grid[row_idx + 1, col_idx])

        visualizer = SilhouetteVisualizer(km, colors=colors, ax=ax)
        visualizer.fit(df)

        # Add the Silhouette score text to the plot
        score = silhouette_score(df, km.labels_)
        ax.text(0.97, 0.02, f'Silhouette Score: {score:.2f}', fontsize=12, \
                ha='right', transform=ax.transAxes, color='red')

        ax.set_title(f'Silhouette Plot for {i} Clusters', fontsize=15)

    plt.tight_layout()
    plt.show()

    return best_k