import os
import duckdb
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

green_columns_definition = {
    'VendorID': 'INTEGER',
    'lpep_pickup_datetime': 'TIMESTAMP',
    'lpep_dropoff_datetime': 'TIMESTAMP',
    'store_and_fwd_flag': 'VARCHAR',
    'RatecodeID': 'INTEGER',
    'PULocationID': 'INTEGER',
    'DOLocationID': 'INTEGER',
    'passenger_count': 'INTEGER',
    'trip_distance': 'DOUBLE',
    'fare_amount': 'DOUBLE',
    'extra': 'DOUBLE',
    'mta_tax': 'DOUBLE',
    'tip_amount': 'DOUBLE',
    'tolls_amount': 'DOUBLE',
    'ehail_fee': 'DOUBLE',
    'improvement_surcharge': 'DOUBLE',
    'total_amount': 'DOUBLE',
    'payment_type': 'INTEGER',
    'trip_type': 'INTEGER',
    'congestion_surcharge': 'DOUBLE'
}

yellow_columns_definition = {
    'VendorID': 'INTEGER',
    'tpep_pickup_datetime': 'TIMESTAMP',
    'tpep_dropoff_datetime': 'TIMESTAMP',
    'passenger_count': 'INTEGER',
    'trip_distance': 'DOUBLE',
    'RatecodeID': 'INTEGER',
    'store_and_fwd_flag': 'VARCHAR',
    'PULocationID': 'INTEGER',
    'DOLocationID': 'INTEGER',
    'payment_type': 'INTEGER',
    'fare_amount': 'DOUBLE',
    'extra': 'DOUBLE',
    'mta_tax': 'DOUBLE',
    'tip_amount': 'DOUBLE',
    'tolls_amount': 'DOUBLE',
    'improvement_surcharge': 'DOUBLE',
    'total_amount': 'DOUBLE',
    'congestion_surcharge': 'DOUBLE'
}

def convert_csvs_to_parquet_duckdb(color, input_path_prefix, output_path_prefix, schema):
    for year in ['2019', '2020']:
        input_file_path = os.path.join(input_path_prefix, color, year)
        output_file_path = os.path.join(output_path_prefix, color, year)
        
        if not os.path.exists(output_file_path):
            os.makedirs(output_file_path)
            logger.info(f"Created directory: {output_file_path}")
        
        csv_files = [file for file in os.listdir(input_file_path) if file.endswith('.csv.gz')]
        
        for csv_file in csv_files:
            input_csv_path = os.path.join(input_file_path, csv_file)
            output_parquet_file = os.path.join(output_file_path, csv_file.replace('.csv.gz', '.parquet'))
            
            try:
                # Using DuckDB to read the CSV and write to Parquet
                duckdb.query(f"""
                    COPY (SELECT * FROM read_csv('{input_csv_path}', delim=',', header=True, columns = {schema})) 
                    TO '{output_parquet_file}' (FORMAT 'PARQUET', CODEC 'ZSTD');
                """)
                logger.info(f"Successfully converted {csv_file} to {output_parquet_file}")
            except Exception as e:
                logger.error(f"Failed to convert {csv_file} to Parquet: {e}")

input_path_prefix = f'{path_to_local_home}/data/raw'
output_path_prefix = f'{path_to_local_home}/data/parquet'

convert_csvs_to_parquet_duckdb('yellow', input_path_prefix, output_path_prefix, yellow_columns_definition)
convert_csvs_to_parquet_duckdb('green', input_path_prefix, output_path_prefix, green_columns_definition)
