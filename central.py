import pandas as pd
from fuzzywuzzy import fuzz
import requests
import re
import jellyfish

def match_state_name(input_state, unique_states_list):
    # Initialize variables to store the best match and highest score
    best_match = None
    highest_score = 0

    # Iterate through the unique states list and calculate Jaro-Winkler scores
    for state in unique_states_list:
        similarity_score = jellyfish.jaro_winkler_similarity(input_state, state)
        if similarity_score > highest_score:
            highest_score = similarity_score
            best_match = state

    # Return the best matching state name
    return best_match

def map_columns_to_global_schema(df, schema_mapping):
    # Reverse the mapping dictionary to map local schema to global schema
    local_to_global_mapping = {local: global_ for local, global_ in schema_mapping.items()}
    
    # Rename columns in the DataFrame
    df = df.rename(columns=local_to_global_mapping)
    return df
def convert_query(global_query, schema_mapping):
    for global_col, local_col in schema_mapping.items():
        global_query = re.sub(rf"\b{global_col}\b", local_col, global_query)
    return global_query
def run_sql_query(sql_query,api_url):
    try:
        # Make the GET request
        response = requests.post(api_url,json={"query":sql_query})


        # Check if the request was successful
        if response.status_code == 200:
            # Parse JSON data from the response
            return response.json()

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
# func: sub_query1,sub_query2
'''
sub_query1 : select 
sub-query1->modify local source 1
run_sql_query
pandas dataframe
column name change global schema
merge query
output dataframe
'''
def column_fetch(api_url):
    try:
        # Make the GET request
        response = requests.get(api_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse JSON data from the response
            data = response.json()
            print("Data fetched successfully:")
            print(data)
            col_list=[d['Field'] for d in data]
            return col_list
        else:
            print(f"Error: {response.status_code} - {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def matcher(col1,col_list):
    m_score = -1
    m_col = None
    for i in col_list:
        sim = fuzz.token_set_ratio(col1, i)
        if sim>m_score:
            m_score = sim
            m_col = i
    if(m_score/100 < 0.3):
        return None, m_score
    return m_col, m_score

def schema_matching(remote_schema,source_schema):
    schema_match={}
    for col in remote_schema:
        column_matched,score=matcher(col,source_schema)
        schema_match[col]=column_matched
    return schema_match
GST_registeration_data = pd.DataFrame({
    'State': ['State1', 'State2'],
    'StartDate': ['2023-01-01', '2023-01-01'],
    'EndDate': ['2023-01-31', '2023-01-31'],
    'EligiblePayers': [5000, 8000],
    'RegisteredPayers': [4500, 7500]
})

GST_payers_data = pd.DataFrame({
    'State': ['State1', 'State2'],
    'StartDate': ['2023-01-01', '2023-01-01'],
    'EndDate': ['2023-01-31', '2023-01-31'],
    'NormalPayers': [2000, 4000],
    'CompositePayers': [1000, 2000],
    'CasualPayers': [500, 1500],
})

remote_schema_1=GST_registeration_data.columns.to_list()
remote_schema_2=GST_payers_data.columns.to_list()

source_schema_2= [
    "State",
    "Start_Date",
    "End_Date",
    "Normal_Tax_Payers",
    "Composition_Tax_Payers",
    "Input_Service_Distributor",
    "Casual_Tax_Payers"
]
source_schema_1= [
    "State",
    "Start_Date",
    "End_Date",
    "Payer_Registered_For_GST",
    "Payer_Eligible_For_GST"
]
source_schema_1=column_fetch(api_url="https://reptile-growing-vastly.ngrok-free.app/columns/GST_Registration")
source_schema_2=column_fetch(api_url="https://choice-growing-sculpin.ngrok-free.app/columns/GST_Data")
gs_to_ls_match_1=schema_matching(remote_schema_1,source_schema_1)
gs_to_ls_match_2=schema_matching(remote_schema_2,source_schema_2)

ls_to_gs_match_1= {value: key for key, value in gs_to_ls_match_1.items()}
ls_to_gs_match_2={value: key for key, value in gs_to_ls_match_2.items()}


sub_query_1=''''''
sub_query_2=''''''

sub_query_1=convert_query(sub_query_1,gs_to_ls_match_1)
sub_query_2=convert_query(sub_query_2,gs_to_ls_match_2)

result_1=run_sql_query(sub_query_1,"https://reptile-growing-vastly.ngrok-free.app/chat")
result_2=run_sql_query(sub_query_2,"https://choice-growing-sculpin.ngrok-free.app/chat")

df1=pd.DataFrame(result_1)
df2=pd.DataFrame(result_2)

df1=df1.rename(ls_to_gs_match_1)
df2=df2.rename(ls_to_gs_match_2)
unique_states_df1 = df1["State"].unique()
state_mapping = {
    state: match_state_name(state, unique_states_df1) for state in df2["State"]
}
df2["State"] = df2["State"].map(state_mapping).fillna(df2["State"])




# here comes the pandas query










