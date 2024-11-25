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
    api_url = f"https://loadqa.ndapapi.com/v1/openapi?API_Key=gAAAAABnPhJYrHqArsfGt6NamKH7GnR1L7E3xqtGHaTY6Fta47_YzALx--4bOC6XJ4YYPv8ljxHv7MaNAVn2jMAtbXechIKqUbcjYN_Pwx_UVy0_nbSymnjVzFNmXdmMd0l1UMcYqHgkhDCJ8rUCRmfQ1Nj3ectH8Vx_h-pab3zsdCn5MAPxdZDn1iJr3N3VJJJc1wWNK66P&ind=I6000_10,I6000_11,I6000_12,I6000_13,I6000_14,I6000_15,I6000_16,I6000_17,I6000_18,I6000_19,I6000_20,I6000_21,I6000_22,I6000_23,I6000_24,I6000_25,I6000_26,I6000_27,I6000_28,I6000_29,I6000_30,I6000_31,I6000_32,I6000_33,I6000_34,I6000_35,I6000_36,I6000_37,I6000_38,I6000_39,I6000_40,I6000_41,I6000_42,I6000_43,I6000_44,I6000_45,I6000_46,I6000_47,I6000_48,I6000_49,I6000_50,I6000_51,I6000_52,I6000_53,I6000_54,I6000_55,I6000_56,I6000_57,I6000_58,I6000_59,I6000_60,I6000_61,I6000_62,I6000_63,I6000_64,I6000_65,I6000_66,I6000_67,I6000_68,I6000_69,I6000_70,I6000_71,I6000_72,I6000_73,I6000_74,I6000_75,I6000_76,I6000_77,I6000_78,I6000_79,I6000_80,I6000_81,I6000_82,I6000_83,I6000_84,I6000_85,I6000_86,I6000_87,I6000_88,I6000_89,I6000_90,I6000_91,I6000_92,I6000_93,I6000_94&dim=Country,StateName,StateCode,DistrictName,DistrictCode,SubDistrictName,SubDistrictCode,ULB_RLB_VillageName,ULB_RLB_VillageCode,Year,TRU&pageno={i}"
    extracted_data = get_data_from_api(api_url)
    if i==6068:
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
    df.to_csv("population.csv", mode="a", header=True, index=False)
    i += 1
