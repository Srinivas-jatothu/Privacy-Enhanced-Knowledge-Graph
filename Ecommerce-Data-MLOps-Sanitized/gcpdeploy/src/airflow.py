"""
The Airflow Dag for the preprocessing datapipeline
"""

# Import necessary libraries and modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import configuration as conf
from module_l2_001 import data_loader_func_002
from module_l2_002 import data_loader_func_003
from module_l2_003 import data_loader_func_001
from module_l2_004 import data_cleaning_func_003
from module_l2_005 import data_cleaning_func_002
from module_l2_006 import data_cleaning_func_006
from module_l2_007 import data_cleaning_func_001
from module_l2_008 import utility_func_001
from module_l2_009 import data_cleaning_func_005
from module_l2_010 import feature_engineering_func_004
from module_l2_011 import feature_engineering_func_006
from module_l2_012 import feature_engineering_func_002
from module_l2_013 import feature_engineering_func_003
from module_l2_014 import feature_engineering_func_001
from module_l2_015 import feature_engineering_func_005
from module_l2_016 import data_cleaning_func_004
from module_l2_017 import ml_model_func_003
from module_l2_018 import ml_model_func_001
from module_l2_019 import analysis_func_001

# Enable pickle support for XCom, allowing data to be passed between tasks
conf.set('core', 'enable_xcom_pickling', 'True')
conf.set('core', 'enable_parquet_xcom', 'True')

# Define default arguments for your DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 11, 9),
    'retries': 0, # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5), # Delay before retries
}

# Create a DAG instance named 'datapipeline' with the defined default arguments
dag = DAG(
    'datapipeline',
    default_args=default_args,
    description='Airflow DAG for the datapipeline',
    schedule_interval=None,  # Set the schedule interval or use None for manual triggering
    catchup=False,
)

# Define PythonOperators for each function

# Task to download data from source, calls the 'data_loader_func_002' Python function
ingest_data_task = PythonOperator(
    task_id='ingest_data_task',
    python_callable=data_loader_func_002,
    op_args=["[URL]"],
    dag=dag,
)

# Task to unzip the downloaded data, depends on 'data_loader_func_002'
unzip_file_task = PythonOperator(
    task_id='unzip_file_task',
    python_callable=data_loader_func_003,
    op_args=[ingest_data_task.output],
    dag=dag,
)

# Task to load data, depends on unzip_file_task
load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=data_loader_func_001,
    op_kwargs={
        'param_012': '{{ ti.xcom_pull(task_ids="unzip_file_task") }}',
    },
    dag=dag,
)

# Task to handle missing values, depends on load_data_task
handle_missing_task = PythonOperator(
    task_id='missing_values_task',
    python_callable=data_cleaning_func_003,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="load_data_task") }}',
    },
    dag=dag,
)

# Task to handle duplicates, depends on missing_values_task
remove_duplicates_task = PythonOperator(
    task_id='remove_duplicates_task',
    python_callable=data_cleaning_func_002,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="handle_missing_task") }}',
    },
    dag=dag,
)

# Task to handle transaction status, depends on remove_duplicates_task
transaction_status_task = PythonOperator(
    task_id='transaction_status_task',
    python_callable=data_cleaning_func_006,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="remove_duplicates_task") }}',
    },
    dag=dag,
)

# Task to handle anomaly codes, depends on transaction_status_task
anomaly_codes_task = PythonOperator(
    task_id='anomaly_codes_task',
    python_callable=data_cleaning_func_001,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="transaction_status_task") }}',
    },
    dag=dag,
)

# Task to handle cleaning description, depends on anomaly codes
cleaning_description_task = PythonOperator(
    task_id='cleaning_description_task',
    python_callable=utility_func_001,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="anomaly_codes_task") }}',
    },
    dag=dag,
)

# Task to handle removing zero unitprices, depends on cleaning description
removing_zero_unitprice_task = PythonOperator(
    task_id='removing_zero_unitprice_task',
    python_callable=data_cleaning_func_005,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="cleaning_description_task") }}',
    },
    dag=dag,
)

# Task to handle RFM analysis, depends on removing zero unitprices
rfm_task = PythonOperator(
    task_id='rfm_task',
    python_callable=feature_engineering_func_004,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="removing_zero_unitprice_task") }}',
    },
    dag=dag,
)

# Task to handle grouping based on unique products, depends on RFM analysis
unique_products_task = PythonOperator(
    task_id='unique_products_task',
    python_callable=feature_engineering_func_006,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="rfm_task") }}',
    },
    dag=dag,
)

# Task to handle behavorial patterns, depends on grouping based on unique products
customers_behavior_task = PythonOperator(
    task_id='customers_behavior_task',
    python_callable=feature_engineering_func_002,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="unique_products_task") }}',
    },
    dag=dag,
)

# Task to handle geographic features, depends on behavorial patterns
geographic_features_task = PythonOperator(
    task_id='geographic_features_task',
    python_callable=feature_engineering_func_003,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="customers_behavior_task") }}',
    },
    dag=dag,
)

# Task to handle cancellation frequency and rate, depends on geographic features
cancellation_details_task = PythonOperator(
    task_id='cancellation_details_task',
    python_callable=feature_engineering_func_001,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="geographic_features_task") }}',
    },
    dag=dag,
)

# Task to handle seasonality trends, depends on cancellation frequency and rate
seasonality_task = PythonOperator(
    task_id='seasonality_task',
    python_callable=feature_engineering_func_005,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="cancellation_details_task") }}',
    },
    dag=dag,
)

# Task to handle outlier treatment, depends on seasonality trends
outlier_treatment_task = PythonOperator(
    task_id='outlier_treatment_task',
    python_callable=data_cleaning_func_004,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="seasonality_task") }}',
    },
    dag=dag,
)

# Task to standardize the columns
column_values_scaler_task = PythonOperator(
    task_id='column_values_scaler_task',
    python_callable=ml_model_func_003,
    op_kwargs={
        'param_006': '{{ ti.xcom_pull(task_ids="outlier_treatment_task") }}',
    },
    dag=dag,
)

# Task for dimensionality reduction
pca_task = PythonOperator(
    task_id='pca_task',
    python_callable=ml_model_func_001,
    op_kwargs={
        'param_006': '{{ ti.xcom_pull(task_ids="column_values_scaler_task") }}',
    },
    dag=dag,
)

# Task to check correlation amongst columns at this stage
correlation_check_task = PythonOperator(
    task_id='correlation_check_task',
    python_callable=analysis_func_001,
    op_kwargs={
        'param_006': '{{ ti.xcom_pull(task_ids="column_values_scaler_task") }}',
    },
    dag=dag,
)

# Set task dependencies
ingest_data_task >> unzip_file_task >> load_data_task >> handle_missing_task \
>> remove_duplicates_task >> transaction_status_task >> anomaly_codes_task >> cleaning_description_task \
>> removing_zero_unitprice_task >> rfm_task >> unique_products_task >> customers_behavior_task \
>> geographic_features_task >> cancellation_details_task >> seasonality_task >> outlier_treatment_task \
>> column_values_scaler_task >> pca_task >> correlation_check_task

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.cli()
