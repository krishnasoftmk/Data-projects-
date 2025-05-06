import gzip
import json
import os
from google.cloud import storage

# GCS Configuration
BUCKET_NAME = "us-central1-nhtsa-composer--9adc8323-bucket"
GCS_INPUT_FILE = "data/nhtsa_file.jsonl.gz"  # File location in GCS
LOCAL_INPUT_FILE = "/home/airflow/gcs/data/nhtsa_file.jsonl.gz"  # Path in Cloud Composer
LOCAL_OUTPUT_FILE = "/home/airflow/gcs/data/parsed_nhtsa_data.json"
GCS_OUTPUT_FILE = "data/parsed_nhtsa_data.json"

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from GCS to Cloud Composer local storage."""
    os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)  # Ensure directory exists

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    if not os.path.exists(destination_file_name):
        raise FileNotFoundError(f"Download failed! {destination_file_name} does not exist.")
    
    print(f"Downloaded {source_blob_name} from GCS to {destination_file_name}")

def upload_to_gcs(local_file, bucket_name, destination_blob_name):
    """Uploads a file from Cloud Composer to GCS."""
    if not os.path.exists(local_file):
        raise FileNotFoundError(f"Cannot upload: {local_file} does not exist!")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file)

    print(f"Uploaded {local_file} to gs://{bucket_name}/{destination_blob_name}")

def parse_nhtsa_data(input_file, output_file):
    """Parses the NHTSA JSONL.gz file and extracts required fields."""
    extracted_data = []
    seen_vins = set()

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Error: Input file {input_file} not found!")

    # Read and process the compressed JSONL file
    with gzip.open(input_file, 'rt', encoding='utf-8') as file:
        for line in file:
            json_data = json.loads(line.strip())

            if isinstance(json_data, list):  
                for record in json_data:
                    if not isinstance(record, dict):
                        continue  

                    sent_vin = record.get("SearchCriteria", "").replace("VIN:", "").strip()
                    if not sent_vin or sent_vin in seen_vins:
                        continue  # Skip duplicates or empty VINs

                    seen_vins.add(sent_vin)
                    results = record.get("Results", []) or []  # handle missing Results

                    # Extract required fields
                    result_data = {item["Variable"]: item.get("Value", "") for item in results if isinstance(item, dict)}

                    extracted_data.append({
                        "Sent_VIN": sent_vin,
                        "Manufacturer_Name": result_data.get("Manufacturer Name", ""),
                        "Make": result_data.get("Make", ""),
                        "Model": result_data.get("Model", ""),
                        "Model_Year": result_data.get("Model Year", ""),
                        "Trim": result_data.get("Trim", ""),
                        "Vehicle_Type_ID": result_data.get("Vehicle Type", ""),
                        "Body_Class_ID": result_data.get("Body Class", ""),
                        "Base_Price": result_data.get("Base Price ($)", ""),
                        "NCSA_Make": result_data.get("NCSA Make", ""),
                        "NCSA_Model": result_data.get("NCSA Model", "")
                    })

    os.makedirs(os.path.dirname(output_file), exist_ok=True)  # Make sure  output directory exists
    with open(output_file, "w", encoding="utf-8") as json_out:
        json.dump(extracted_data, json_out, indent=4)

    print(f"Data parsing completed. Output saved locally to {output_file}")

# Download `.gz` file from GCS to Cloud Composer
download_from_gcs(BUCKET_NAME, GCS_INPUT_FILE, LOCAL_INPUT_FILE)

# Process the file
parse_nhtsa_data(LOCAL_INPUT_FILE, LOCAL_OUTPUT_FILE)

# Upload parsed JSON back to GCS
upload_to_gcs(LOCAL_OUTPUT_FILE, BUCKET_NAME, GCS_OUTPUT_FILE)
