import os
import json
import requests
from datetime import datetime
from flask import Flask, request
from google.cloud import storage

app = Flask(__name__)

API_URL = "https://api.questrade.com/v1/accounts"
BUCKET_NAME = os.environ["BUCKET_NAME"]
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]  # Set this as a secret

@app.route("/", methods=["POST"])
def ingest():
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    response = requests.get(API_URL, headers=headers)

    if response.status_code != 200:
        return f"Error: {response.status_code}", 500

    data = response.json()
    filename = f"questrade_raw_{datetime.utcnow().isoformat()}.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"bronze/{filename}")
    blob.upload_from_string(json.dumps(data), content_type="application/json")

    return "Data successfully ingested to GCS", 200
