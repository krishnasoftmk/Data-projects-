from pyspark.sql import SparkSession, Row
import json

def parse_record(record_json):
    try:
        record = json.loads(record_json)
        search_criteria = record.get("SearchCriteria", "")
        sent_vin = search_criteria.split("VIN:")[1].strip() if "VIN:" in search_criteria else None
        if not sent_vin:
            return None

        def to_int(val):
            return int(val) if val and str(val).isdigit() else None

        def to_float(val):
            try:
                return float(val)
            except:
                return None

        fields = {
            "Sent_VIN": sent_vin,
            "Manufacturer_Name": None,
            "Make": None,
            "Model": None,
            "Model_Year": None,
            "Trim": None,
            "Vehicle_Type_ID": None,
            "Body_Class_ID": None,
            "Base_Price": None,
            "NCSA_Make": None,
            "NCSA_Model": None
        }

        for result in record.get("Results", []):
            variable = result.get("Variable")
            value = result.get("Value")
            if variable == "Manufacturer Name":
                fields["Manufacturer_Name"] = value
            elif variable == "Make":
                fields["Make"] = value
            elif variable == "Model":
                fields["Model"] = value
            elif variable == "Model Year":
                fields["Model_Year"] = to_int(value)
            elif variable == "Trim":
                fields["Trim"] = value
            elif variable == "Vehicle Type":
                fields["Vehicle_Type_ID"] = to_int(value)
            elif variable == "Body Class":
                fields["Body_Class_ID"] = to_int(value)
            elif variable == "Base Price ($)":
                fields["Base_Price"] = to_float(value)
            elif variable == "NCSA Make":
                fields["NCSA_Make"] = value
            elif variable == "NCSA Model":
                fields["NCSA_Model"] = value

        return Row(**fields)

    except Exception:
        return None

# Initialize Spark
spark = SparkSession.builder.appName("NHTSA Parser").getOrCreate()
sc = spark.sparkContext

# bucket path
input_path = "gs://us-central1-nhtsa-composer--9adc8323-bucket/data/nhtsa_file.jsonl.gz"
output_path = "gs://us-central1-nhtsa-composer--9adc8323-bucket/output/parsed_nhtsa_data"

# Read from GCS
lines = sc.textFile(input_path)

# Parse + deduplicate
parsed_rdd = lines.map(parse_record).filter(lambda x: x is not None)
unique_rdd = parsed_rdd.map(lambda x: (x["Sent_VIN"], x)).reduceByKey(lambda a, b: a).map(lambda x: x[1])

# Infer schema automatically
df = spark.createDataFrame(unique_rdd)

# Save as newline-delimited JSON for BigQuery
df.write.mode("overwrite").json(output_path)
