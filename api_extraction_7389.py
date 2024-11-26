import json
import requests

api_url1=""
# open api.json
with open("api.json") as f:
    data = json.load(f)
    api_url1=data["7389"][2]
    



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
    api_url=f"{api_url1[:-1]}{i}"
    print(api_url)

    extracted_data = get_data_from_api(api_url)
    if i==2:
        break
    
    
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
    if i==1:
        df.to_csv("7389.csv",header=True, index=False)
    else:
        df.to_csv("7389.csv", mode="a", header=False, index=False)
    i+=1