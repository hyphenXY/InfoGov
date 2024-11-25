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
    api_url = f"https://loadqa.ndapapi.com/v1/openapi?API_Key=gAAAAABnQeD5kqVWF5whVY74jPhA_UT1a5scOTT5pta64yv2zI4COTeeNLcqFKxb1mMQDOXOYfyK1ouoz8vmmXzwCsVzlYRkc-XWwW-OOAe6Jwh1KmkSv0n_C-0ibRqN18G_mGmu8MdsA0mOoGu7l3PlGU5OU3AvEu0YlsVfKfy1gzalpBDYb4qlu02JLmUP2fsZk4AVsfHX&ind=I9259_5,I9259_6,I9259_7,I9259_8,I9259_9,I9259_10,I9259_11,I9259_12,I9259_13,I9259_14,I9259_15,I9259_16&dim=Country,StateName,StateCode,Year,GENDER,D9259_3&pageno={i}"
    extracted_data = get_data_from_api(api_url)
    if i==15:
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
    df.to_csv("education.csv", mode="a", header=True, index=False)
    i += 1
