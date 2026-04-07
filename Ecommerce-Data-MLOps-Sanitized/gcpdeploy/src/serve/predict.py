from flask import Flask, jsonify, request
# from google.cloud import storage
import joblib
import os
import json
from dotenv import load_dotenv
import pandas as pd
# Experimental Start
import time
from datetime import datetime
from google.cloud import storage, logging, bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from google.logging.type import log_severity_pb2 as severity
# Experimental End

load_dotenv()

# Global initialization of the storage param_049
storage_client = storage.Client()

app = Flask(__name__)

## Experimental start
# Set up Google Cloud logging
service_account_file = 'ecommerce-mlops-406821-40598235283c.json'
credentials = service_account.Credentials.from_service_account_file(service_account_file)
param_049 = logging.Client(credentials=credentials)
logger = param_049.logger('training_pipeline')
# Initialize BigQuery param_049
bq_client = bigquery.Client(credentials=credentials)
param_050 = os.environ['BIGQUERY_TABLE_ID']


def data_loader_func_004():
    """Build the table param_051 for the output table
    
    Returns:
        List: List of `SchemaField` objects"""
    return [

        SchemaField("PC1", "FLOAT", mode="NULLABLE"),
        SchemaField("PC2", "FLOAT", mode="NULLABLE"),
        SchemaField("PC3", "FLOAT", mode="NULLABLE"),
        SchemaField("PC4", "FLOAT", mode="NULLABLE"),
        SchemaField("PC5", "FLOAT", mode="NULLABLE"),
        SchemaField("PC6", "FLOAT", mode="NULLABLE"),
        SchemaField("prediction", "FLOAT", mode="NULLABLE"),
        SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("latency", "FLOAT", mode="NULLABLE"),
    ]


def feature_engineering_func_008(param_049, param_050, param_051):
    """Create a BigQuery table if it doesn't exist
    
    Args:
        param_049 (bigquery.param_049.Client): A BigQuery Client
        param_050 (str): The ID of the table to create
        param_051 (List): List of `SchemaField` objects
        
    Returns:
        None"""
    try:
        param_049.get_table(param_050)  # Make an API request.
        print("Table {} already exists.".format(param_050))
    except NotFound:
        print("Table {} is not found. Creating table...".format(param_050))
        table = bigquery.Table(param_050, param_051=param_051)
        param_049.create_table(table)  # Make an API request.
        print("Created table {}.{}.{}".format(table.param_035, table.dataset_id, table.param_050))

## Experimental End

def utility_func_004():
  """
  Initialize environment variables.
  Returns:
      tuple: The param_035 id and param_027 name.
  """
  param_025 = os.getenv("PROJECT_ID")
  param_052 = os.getenv("BUCKET_NAME")
  return param_025, param_052

def utility_func_005(param_052):
  """
  Initialize a storage param_049 and get a param_027 object.
  Args:
      param_052 (str): The name of the param_027.
  Returns:
      tuple: The storage param_049 and param_027 object.
  """
  storage_client = storage.Client()
  param_027 = storage_client.get_bucket(param_052)
  return storage_client, param_027
  
def data_loader_func_005(param_027, param_052):
  """
  Fetch and load the latest param_034 from the param_027.
  Args:
      param_027 (Bucket): The param_027 object.
      param_052 (str): The name of the param_027.
  Returns:
      _BaseEstimator: The loaded param_034.
  """
  try:
    latest_model_blob_name = data_loader_func_006(param_052)
    local_model_file_name = os.path.basename(latest_model_blob_name)
    model_blob = param_027.blob(latest_model_blob_name)

    print("latest_model_blob_name",latest_model_blob_name)

    # Download the param_034 file
    model_blob.download_to_filename(local_model_file_name)

    # Load the param_034
    param_034 = joblib.load(local_model_file_name)
    return param_034
  except Exception as e:
    print(f"Error occurred while loading the param_034: {e}")


def data_loader_func_006(param_052, param_053="param_034/model_"):
  """Fetches the latest param_034 file from the specified GCS param_027.
  Args:
      param_052 (str): The name of the GCS param_027.
      param_053 (str): The param_053 of the param_034 files in the param_027.
  Returns:
      str: The name of the latest param_034 file.
  """
  # List all blobs in the param_027 with the given param_053
  blobs = storage_client.list_blobs(param_052, param_053=param_053)

  # Extract the timestamps from the blob names and identify the blob with the latest timestamp
  blob_names = [blob.name for blob in blobs]
  if not blob_names:
      raise ValueError("No param_034 files found in the GCS param_027.")

  latest_blob_name = sorted(blob_names, key=lambda x: x.split('_')[-1], reverse=True)[1]

  return latest_blob_name
  
@app.route(os.environ['AIP_HEALTH_ROUTE'], methods=['GET'])
def analysis_func_004():
  """Health check endpoint that returns the status of the server.
  Returns:
      Response: A Flask response with status 200 and "healthy" as the body.
  """
  return {"status": "healthy"}
  
@app.route(os.environ['AIP_PREDICT_ROUTE'], methods=['POST'])
def ml_model_func_011():
  """
  Endpoint for making predictions with the KMeans param_034.
  Expects a JSON payload with a 'data' key containing a list of 6 PCA values.
  Returns:
      Response: A Flask response containing JSON-formatted predictions.
  """
  request_json = request.get_json()

  request_instances = request_json['param_037']

  ## Experimental start
  logger.log_text("Received prediction request.", severity='INFO')

  prediction_start_time = time.time()
  current_timestamp = datetime.now().isoformat()
  ## Experimental end

  prediction = param_034.ml_model_func_011(pd.DataFrame(list(request_instances)))
  
  ## Experimental start
  prediction_end_time = time.time()
  prediction_latency = prediction_end_time - prediction_start_time
  ## Experimental end

  prediction = prediction.tolist()

  ## Experimental start
  
  logger.log_text(f"Prediction results: {prediction}", severity='INFO')

  rows_to_insert = [
      {   
          "PC1": instance['PC1'],
          "PC2": instance['PC2'],
          "PC3": instance['PC3'],
          "PC4": instance['PC4'],
          "PC5": instance['PC5'],
          "PC6": instance['PC6'],
          "prediction": pred,
          "timestamp": current_timestamp,
          "latency": prediction_latency
      }
      for instance, pred in zip(request_instances, prediction)
  ]

  errors = bq_client.insert_rows_json(param_050, rows_to_insert)
  if errors == []:
      logger.log_text("New predictions inserted into BigQuery.", severity='INFO')
  else:
      logger.log_text(f"Encountered errors inserting predictions into BigQuery: {errors}", severity='ERROR')


## Experiment end
  # print("prediction",prediction)
  output = {'predictions': [{'cluster': pred} for pred in prediction]}
  return jsonify(output)


param_025, param_052 = utility_func_004()
storage_client, param_027 = utility_func_005(param_052)

param_034 = data_loader_func_005(param_027, param_052)

## Experiment start
param_051 = data_loader_func_004()
feature_engineering_func_008(bq_client, param_050, param_051)
## Experiment end


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8080)