import json
import requests


def get_data_from_api(api_url):
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()


def extract_data(data, key):
    try:
        return data[key]
    except KeyError:
        return None


i = 1
while True:
    api_url = f"https://loadqa.ndapapi.com/v1/openapi?API_Key=gAAAAABnRGmPj4L4w7upz_8qcBb_8W1EwDs-_-fyTa0nMn7R_7NPnPiqupvg6qOIrD9Wa34X7ttPPLGYWURgNtrStyv3VDzOHxrY8NvuQ9nUQtzY1XRBpSv4eqNzi8oeAylsXFPM79-b2nADIRamt9xa5tVxewslYyaBuvkZlBSekDdC_yVo0IdJZT1ZvmzxBLvlvJamJAoH&ind=I6066_4,I6066_5,I6066_6,I6066_7,I6066_8,I6066_9,I6066_10,I6066_11,I6066_12&dim=Country,StateName,StateCode,Year,D6066_3&pageno={i}"
    extracted_data = get_data_from_api(api_url)
    if i==81:
        break
    # print(extracted_data["IsError"])
    print(i)
    headers = get_data_from_api(api_url)["Headers"]["Items"]
    id_to_displayname = {header["ID"]: header["DisplayName"] for header in headers}
    modified_data = []
    data = extracted_data["Data"]
    for record in data:
        modified_record = {
            id_to_displayname.get(key, key): value for key, value in record.items()
        }
        modified_data.append(modified_record)
    # print(extracted_data['Headers']['Items'][1])
    import pandas as pd

    # Function to flatten nested dictionary
    def process_data(data):
        records = []
        for record in data:
            flattened_record = {}
            for key, value in record.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        flattened_record[f"{key}_{sub_key}"] = sub_value
                else:
                    flattened_record[key] = value
            records.append(flattened_record)
        return records

    # Example data: List of similar dictionaries
    data_list = modified_data
    # Flatten the data
    flattened_data = process_data(data_list)

    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)

    # Display the DataFrame
    print(df.columns)

    # Optionally, save to a CSV file
    # df.to_csv("education.csv", index=False)
    # append the content to "education.csv"
    df.to_csv("health.csv", mode="a", header=True, index=False)
    i += 1
