from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import timedelta

# === GCP Configuration ===
PROJECT_ID = "sahayaproject"
REGION = "us-central1"
CLUSTER_NAME = "sahaya-dataproc-cluster"  # cluster name
BUCKET_NAME = "us-central1-nhtsa-composer--9adc8323-bucket"
BQ_DATASET = "processed_nhtsa_data"  # BigQuery dataset

# === File Paths ===
PYSPARK_URI = f"gs://{BUCKET_NAME}/scripts/nhtsa_dataproc_parser.py"
PARSED_OUTPUT_JSON = f"gs://{BUCKET_NAME}/output/parsed_nhtsa_data/*.json"
LOOKUP_CSV_PATH = "data/nhtsa_lookup_file.csv"

# === BigQuery Tables ===
PARSED_TABLE = "processed_nhtsa_data"
LOOKUP_TABLE = "nhtsa_lookup_table"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nhtsa_dataproc_pipeline",
    description="Parse NHTSA JSON, load to BigQuery, load lookup table",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc", "bigquery", "nhtsa"],
) as dag:

    # Task 1: Submit Dataproc PySpark Job
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

    # Task 2: Load Parsed Output into BigQuery
    load_output_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_output_to_bigquery",
        location=REGION,
        configuration={
            "load": {
                "sourceUris": [PARSED_OUTPUT_JSON],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": PARSED_TABLE,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    # Task 3: Load Lookup CSV to BigQuery
    load_lookup_to_bigquery = GCSToBigQueryOperator(
        task_id="load_lookup_csv_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[LOOKUP_CSV_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{LOOKUP_TABLE}",
        skip_leading_rows=1,
        source_format="CSV",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    # DAG Flow
    submit_pyspark_job >> load_output_to_bigquery >> load_lookup_to_bigquery
