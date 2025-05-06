from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# GCS and BigQuery Constants
GCP_PROJECT = "sahayaproject"
GCS_BUCKET = "us-central1-nhtsa-composer--9adc8323-bucket"

# File Paths
GCS_JSON_FILE = "data/parsed_nhtsa_data.json"
GCS_CSV_FILE = "data/nhtsa_lookup_file.csv"
GCS_GZ_FILE = "data/nhtsa_file.jsonl.gz"

LOCAL_DIR = "/home/airflow/gcs/data/" 
LOCAL_JSON_FILE = f"{LOCAL_DIR}parsed_nhtsa_data.json"
LOCAL_CSV_FILE = f"{LOCAL_DIR}nhtsa_lookup_file.csv"
LOCAL_GZ_FILE = f"{LOCAL_DIR}nhtsa_file.jsonl.gz"

# BigQuery Tables
BQ_DATASET = "nhtsa_dataset"
BQ_PROCESSED_TABLE = "processed_nhtsa_data"
BQ_LOOKUP_TABLE = "nhtsa_lookup_table"

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "nhtsa_data_pipeline",
    default_args=default_args,
    description="ETL pipeline for NHTSA data using BigQuery",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Download `.gz` file from GCS before parsing
download_gz_from_gcs = GCSToLocalFilesystemOperator(
    task_id="download_gz_from_gcs",
    bucket=GCS_BUCKET,
    object_name=GCS_GZ_FILE,
    filename=LOCAL_GZ_FILE,  
    dag=dag,
)

# Download Lookup CSV from GCS before processing
download_csv_from_gcs = GCSToLocalFilesystemOperator(
    task_id="download_csv_from_gcs",
    bucket=GCS_BUCKET,
    object_name=GCS_CSV_FILE,
    filename=LOCAL_CSV_FILE,  
    dag=dag,
)

# Run Python script to parse `.gz` and generate `parsed_nhtsa_data.json`
parse_task = BashOperator(
    task_id="run_nhtsa_parser",
    bash_command=f"python /home/airflow/gcs/dags/nhtsa_file_parser.py {LOCAL_GZ_FILE} {LOCAL_JSON_FILE}",  # ✅ Pass correct paths
    dag=dag,
)

# Upload `parsed_nhtsa_data.json` to GCS
upload_json_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_json_to_gcs",
    src=LOCAL_JSON_FILE, 
    dst=GCS_JSON_FILE,
    bucket=GCS_BUCKET,
    mime_type="application/json",
    dag=dag,
)

# Upload Lookup CSV file to GCS
upload_csv_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs",
    src=LOCAL_CSV_FILE,  
    dst=GCS_CSV_FILE,
    bucket=GCS_BUCKET,
    mime_type="text/csv",
    dag=dag,
)

# Load JSON data into BigQuery
load_json_to_bq = BigQueryInsertJobOperator(
    task_id="load_json_to_bq",
    configuration={
        "load": {
            "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_JSON_FILE}"],
            "destinationTable": {
                "projectId": GCP_PROJECT,
                "datasetId": BQ_DATASET,
                "tableId": BQ_PROCESSED_TABLE,
            },
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "autodetect": True,
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
    dag=dag,
)

# Load Lookup CSV data into BigQuery
load_csv_to_bq = BigQueryInsertJobOperator(
    task_id="load_csv_to_bq",
    configuration={
        "load": {
            "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_CSV_FILE}"],
            "destinationTable": {
                "projectId": GCP_PROJECT,
                "datasetId": BQ_DATASET,
                "tableId": BQ_LOOKUP_TABLE,
            },
            "sourceFormat": "CSV",
            "skipLeadingRows": 1,
            "autodetect": True,
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
    dag=dag,
)

# Define Task Dependencies
download_gz_from_gcs >> parse_task
download_csv_from_gcs >> parse_task
parse_task >> upload_json_to_gcs >> load_json_to_bq
parse_task >> upload_csv_to_gcs >> load_csv_to_bq
