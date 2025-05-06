from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta

# GCP and resource configuration
PROJECT_ID = "sahayaproject"
REGION = "us-central1"
CLUSTER_NAME = "your-dataproc-cluster"  # Replace with actual cluster name
BUCKET_NAME = "us-central1-nhtsa-composer--9adc8323-bucket"
BQ_DATASET = "your_dataset"  # Replace with actual BigQuery dataset name
BQ_TABLE = "processed_nhtsa_data"

# PySpark script location and output path
PYSPARK_URI = f"gs://{BUCKET_NAME}/scripts/nhtsa_dataproc_parser.py"
OUTPUT_JSON_PATH = f"gs://{BUCKET_NAME}/output/parsed_nhtsa_data/*.json"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="nhtsa_dataproc_pipeline",
    default_args=default_args,
    description="Dataproc PySpark job to parse NHTSA data and load into BigQuery",
    schedule_interval=None,  # Run manually or trigger via API
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc", "bigquery", "nhtsa"],
) as dag:

    # Task 1: Run Dataproc PySpark Job
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job_to_dataproc",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI
            }
        }
    )

    # Task 2: Load JSON result into BigQuery
    load_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_output_to_bigquery",
        location=REGION,
        configuration={
            "load": {
                "sourceUris": [OUTPUT_JSON_PATH],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        }
    )

    submit_pyspark_job >> load_to_bigquery
