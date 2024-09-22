import os
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from google.cloud import storage

# Env variables
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'nyc_taxi_dataset')
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
INPUT_FILETYPE = "parquet"
DATASET = "tripdata"

# Define the schema
green_columns_definition = {
    'VendorID': 'INTEGER',
    'lpep_pickup_datetime': 'TIMESTAMP',
    'lpep_dropoff_datetime': 'TIMESTAMP',
    'store_and_fwd_flag': 'STRING',  
    'RatecodeID': 'INTEGER',
    'PULocationID': 'INTEGER',
    'DOLocationID': 'INTEGER',
    'passenger_count': 'INTEGER',
    'trip_distance': 'FLOAT64',
    'fare_amount': 'FLOAT64',
    'extra': 'FLOAT64',
    'mta_tax': 'FLOAT64',
    'tip_amount': 'FLOAT64',
    'tolls_amount': 'FLOAT64',
    'ehail_fee': 'FLOAT64',
    'improvement_surcharge': 'FLOAT64',
    'total_amount': 'FLOAT64',
    'payment_type': 'INTEGER',
    'trip_type': 'INTEGER',
    'congestion_surcharge': 'FLOAT64'
}

yellow_columns_definition = {
    'VendorID': 'INTEGER',
    'tpep_pickup_datetime': 'TIMESTAMP',
    'tpep_dropoff_datetime': 'TIMESTAMP',
    'passenger_count': 'INTEGER',
    'trip_distance': 'FLOAT64',
    'RatecodeID': 'INTEGER',
    'store_and_fwd_flag': 'STRING',
    'PULocationID': 'INTEGER',
    'DOLocationID': 'INTEGER',
    'payment_type': 'INTEGER',
    'fare_amount': 'FLOAT64',
    'extra': 'FLOAT64',
    'mta_tax': 'FLOAT64',
    'tip_amount': 'FLOAT64',
    'tolls_amount': 'FLOAT64',
    'improvement_surcharge': 'FLOAT64',
    'total_amount': 'FLOAT64',
    'congestion_surcharge': 'FLOAT64'
}

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Upload data to GCS
def upload_data_to_gcs(bucket_name, source_folder, colour):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for root, dirs, files in os.walk(os.path.join(source_folder, colour)):
        for file in files:
            file_path = os.path.join(root, file)
            destination_blob_name = os.path.join(os.path.relpath(file_path, source_folder))
            blob = bucket.blob(destination_blob_name)
            try:
                blob.upload_from_filename(file_path)
                logger.info(f'File {file_path} uploaded to {destination_blob_name}.')
            except Exception as e:
                logger.error(f'Failed to upload {file_path} to {destination_blob_name}: {e}')

with DAG(
    dag_id='data_ingestion',
    schedule_interval=None,  # Set to None to avoid automatic execution
    start_date=datetime(2024, 8, 13, tzinfo=timezone.utc),  # Use a fixed start date
    catchup=False,  # Don't catch up if the DAG is paused and resumed
    default_args=default_args,
    tags=['nyc-taxi-analysis-project']
) as dag:

    download_taxi_data_task = BashOperator(
        task_id='download_taxi_data',
        bash_command=f"{path_to_local_home}/scripts/download_data.sh "
    )
    
    download_zone_data_task = BashOperator(
        task_id='download_zone_data',
        bash_command=f'curl -L -o {path_to_local_home}/data/taxi_zone_lookup.csv https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    )
      
    format_to_parquet_task = BashOperator(
        task_id='taxi_data_to_parquet',
        bash_command=f'python {path_to_local_home}/scripts/csv_to_parquet_duckdb.py'
    )
    
    for colour, ds_col in COLOUR_RANGE.items():
        
        local_to_gcs_task = PythonOperator(
            task_id=f'local_{colour}_data_to_gcs_task',
            python_callable=upload_data_to_gcs,
            op_kwargs={
                "bucket_name": BUCKET,
                "source_folder": f'{path_to_local_home}/data/parquet',
                "colour": colour
            }
        )
            
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{colour}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{colour}_{DATASET}_external_table",
                },
                "externalDataConfiguration": {
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f'gs://{BUCKET}/{colour}/2019/*.parquet', f'gs://{BUCKET}/{colour}/2020/*.parquet'],
                    'schema': {
                        'fields': [{'name': name, 'type': dtype} for name, dtype in (green_columns_definition if colour == 'green' else yellow_columns_definition).items()]
                    }
                }
            }
        )
        
        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
        )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )
        [download_taxi_data_task, download_zone_data_task] >> format_to_parquet_task >> local_to_gcs_task
        local_to_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job