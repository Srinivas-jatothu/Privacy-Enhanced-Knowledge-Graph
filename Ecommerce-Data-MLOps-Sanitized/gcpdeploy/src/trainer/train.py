from google.cloud import storage
from datetime import datetime
import module_l1_009
import pandas as pd
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor
from sklearn.cluster import KMeans
import joblib
import json
import module_l1_010
import os
# import pickle
from dotenv import load_dotenv
# import plotly.graph_objects as go
# import seaborn as sns
# import matplotlib.pyplot as plt
import numpy as np
# from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
# from tabulate import tabulate
from collections import Counter

from module_l1_012 import feature_engineering_func_012

# Load environment variables
load_dotenv()

# Initialize variables
fs = gcsfs.GCSFileSystem()
storage_client = storage.Client()
param_052 = os.getenv("BUCKET_NAME")
MODEL_DIR = os.environ['AIP_STORAGE_URI']

def utility_func_007(param_060, param_061, param_062=3):
    """
    Clustering our data with kmeans
    """

    kmeans = KMeans(n_clusters=param_062, init='k-means++', n_init=10, max_iter=100, random_state=0)
    kmeans.fit(param_061)

    cluster_frequencies = Counter(kmeans.labels_)


    label_mapping = {label: new_label for new_label, (label, _) in
                 enumerate(cluster_frequencies.most_common())}

    label_mapping = {v: k for k, v in {2: 1, 1: 0, 0: 2}.items()}


    new_labels = np.array([label_mapping[label] for label in kmeans.labels_])


    param_060['cluster'] = new_labels
    param_061['cluster'] = new_labels              
    
    return param_060, kmeans, param_061

def data_saver_func_003(param_034, param_046, param_047):
    """
    Saves the param_034 locally and uploads it to GCS.
    
    Parameters:
    param_034 (kmeans): The trained param_034 to be saved and uploaded.
    param_046 (str): The local path to save the param_034.
    param_047 (str): The GCS path to upload the param_034.
    """
    # Save the param_034 locally
    joblib.dump(param_034, param_046)

    # Upload the param_034 to GCS
    with fs.open(param_047, 'wb') as f:
        joblib.dump(param_034, f)


def pipeline_func_002():
    """
    Main function to orchestrate the loading of data, training of the param_034,
    and uploading the param_034 to Google Cloud Storage.
    """
    # Load and transform data
    # param_041 = "gs://mlops___fall2023/data/train/train_data.csv"
    # df = data_loader_func_001(param_041)
    # param_044, X_test, param_045, y_test = ml_model_func_007(df)

    # data_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = "gs://ecommerce_retail_online_mlops/data"

    param_043 = pd.read_parquet(data_dir_path + "/param_043.parquet")
    after_outlier = pd.read_pickle(data_dir_path + "/after_outlier_treatment.pkl")
    param_054 = pd.read_parquet(data_dir_path + "/df_outlier.parquet")
    df_transactions = pd.read_parquet(data_dir_path + "/transaction_dataframe.parquet")
    
    # Training the param_034
    param_055, param_034, customer_data_pca = utility_func_007(after_outlier, param_043)
    print("clustering ran successfully!")
    recommendations_df = feature_engineering_func_012(df_transactions, param_054, param_055)
    print("recommendations_df generated successfully!")
    print(recommendations_df.shape)

    # Save the param_034 locally and upload to GCS
    edt = pytz.timezone('US/Eastern')
    current_time_edt = datetime.now(edt)
    version = current_time_edt.strftime('%Y%m%d_%H%M%S')
    param_046 = "param_034.pkl"
    param_047 = f"{MODEL_DIR}/model_{version}.pkl"
    print(param_047)
    data_saver_func_003(param_034, param_046, param_047)

if __name__ == "__main__":
    pipeline_func_002()



