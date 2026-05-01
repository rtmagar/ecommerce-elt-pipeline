import os
import json
import jsonschema
from jsonschema import validate
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import boto3
import io

# Configuration
TABLES = ['customers', 'orders', 'order_items', 'products', 'sellers', 'geolocation', 'order_payments', 'order_reviews', 'product_category_name_translation']
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'staging-lake'

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def validate_data_contract(df, table_name):
    """Loads the JSON schema contract and validates the extracted dataframe."""
    # Resolve the absolute path to the contracts directory
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    contract_path = os.path.join(dag_dir, 'contracts', f'{table_name}.json')
    
    try:
        with open(contract_path, 'r') as file:
            contract_schema = json.load(file)
            
        # Convert DataFrame to a list of dictionaries for JSON validation
        # Handling dates/timestamps by converting them to strings for JSON compliance
        df_json_ready = df.astype(str)
        records = df_json_ready.to_dict(orient='records')
        
        # Enforce the contract
        validate(instance=records, schema=contract_schema)
        print(f"✅ Data Contract validation passed for {table_name}.")
        
    except FileNotFoundError:
        print(f"⚠️ No contract found for {table_name} at {contract_path}. Bypassing validation.")
    except jsonschema.exceptions.ValidationError as e:
        raise ValueError(f"❌ DATA CONTRACT VIOLATION in {table_name}: {e.message}")

def extract_to_minio(table_name, execution_date):
    """Extracts data from OLTP, validates contract, and uploads to MinIO partitioned by date."""
    # 1. Extract from Postgres OLTP
    src_hook = PostgresHook(postgres_conn_id='OLTP_DB')
    df = src_hook.get_pandas_df(f"SELECT * FROM {table_name};")
    
    # ---------------------------------------------------------
    # NEW: The Data Contract Interceptor
    # ---------------------------------------------------------
    validate_data_contract(df, table_name)
    
    # 2. Convert to Parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    
    # 3. Upload to MinIO using Hive-style partitioning (dt=YYYY-MM-DD)
    s3_client = get_s3_client()
    
    # Ensure bucket exists
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        
    # Dynamic S3 Key using the execution_date
    partitioned_key = f"{table_name}/dt={execution_date}/{table_name}.parquet"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=partitioned_key,
        Body=parquet_buffer.getvalue()
    )
    print(f"Uploaded {table_name} to MinIO bucket {BUCKET_NAME} at {partitioned_key}")

def load_from_minio_to_dw(table_name, execution_date):
    """Downloads partitioned Parquet from MinIO and loads into DW."""
    s3_client = get_s3_client()
    
    # Read from the exact partition for this DAG run
    partitioned_key = f"{table_name}/dt={execution_date}/{table_name}.parquet"
    
    # 1. Download from MinIO
    response = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=partitioned_key
    )
    parquet_data = response['Body'].read()
    
    # 2. Read into Pandas
    df = pd.read_parquet(io.BytesIO(parquet_data))
    
    # 3. Load into Data Warehouse (raw schema)
    dest_hook = PostgresHook(postgres_conn_id='DW_DB')
    engine = dest_hook.get_sqlalchemy_engine()
    
    df.to_sql(
        name=f"raw_{table_name}", 
        con=engine, 
        schema='public', 
        if_exists='replace', 
        index=False
    )
    print(f"Loaded {table_name} from MinIO partition {partitioned_key} to Data Warehouse")

# Define the DAG
with DAG(
    dag_id='oltp_s3_olap_pipeline',
    description='Extract from OLTP, load to S3 partitioned, and load to Redshift-simulated DW',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    for table in TABLES:
        extract_task = PythonOperator(
            task_id=f'extract_{table}_to_minio',
            python_callable=extract_to_minio,
            op_kwargs={'table_name': table, 'execution_date': '{{ ds }}'}
        )
        
        load_task = PythonOperator(
            task_id=f'load_{table}_to_dw',
            python_callable=load_from_minio_to_dw,
            op_kwargs={'table_name': table, 'execution_date': '{{ ds }}'}
        )
        
        # Set task dependencies: Extract must finish before Load
        extract_task >> load_task