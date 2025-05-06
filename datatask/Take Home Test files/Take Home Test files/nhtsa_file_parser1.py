# # Import necessary python library
# import gzip
# import json

# # define Input file
# returned_nhtsa_file = "~/Download/nhtsa_file.jsonl.gz"

# # Output file name
# parsed_nhtsa_file = "~/Download/parsed_nhtsa_file.json"

# # Defining variables
# parsed_nhtsa_dict = {
#             "Sent_VIN": "",
#             "Manufacturer_Name": "",
#             "Make": "",
#             "Model": "",
#             "Model_Year": "",
#             "Vehicle_Type_ID": "",
#             "Body_Class_ID": "",
#             "NCSA_Make": "",
#             "NCSA_Model": "",
#         }


# # Parse function to extract necessary data from nhtsa file 1
# def parse_nhtsa_file():

#     # TODO Read the input gzip file
#     pass

#     # TODO Process each row of input file to extract necessary data
#     pass

#     # TODO Write the updated dict parsed_nhtsa_dict to output file
#     pass

import gzip
import json
import pandas as pd
import os

# Input and output file paths
input_file = r"C:\Users\User\Desktop\Projects\datatask\Take Home Test files\Take Home Test files\nhtsa_file.jsonl.gz"
output_file = "parsed_nhtsa_data.json"

def parse_nhtsa_data(input_file):
    extracted_data = []
    seen_vins = set()
    
    with gzip.open(input_file, 'rt', encoding='utf-8') as file:
        for line in file:
            json_data = json.loads(line.strip())  # Load and strip line breaks
            
            if isinstance(json_data, list):  # make sure it is a list
                for record in json_data:
                    if not isinstance(record, dict):
                        continue  # Skip invalid records

                   
                    model = record.get("SearchCriteria", "")
                    if model:
                        model = model.replace("model:", "").strip()
                    sent_vin = record.get("SearchCriteria", "")
                    if sent_vin:
                        sent_vin = sent_vin.replace("VIN:", "").strip()
                    
                    if not sent_vin or sent_vin in seen_vins:
                        continue  # Skip duplicates or invalid VINs
                    
                    seen_vins.add(sent_vin)

                    # Extract the required fields
                    results = record.get("Results", [])
                    result_data = {item["Variable"]: item["Value"] for item in results if isinstance(item, dict)}

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

    # Save extracted data as JSON
    with open(output_file, "w", encoding="utf-8") as json_out:
        json.dump(extracted_data, json_out, indent=4)

    print(f"Data parsing completed. Output saved to {output_file}")

# Run the function
parse_nhtsa_data(input_file)
