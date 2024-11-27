import pymysql
import json
import requests
import jellyfish
from datetime import datetime
import re
import pandas

df = pandas.read_csv("7107.csv")
timeout = 10
connection = pymysql.connect(
  charset="utf8mb4",
  connect_timeout=timeout,
  cursorclass=pymysql.cursors.DictCursor,
  db="iia_group2",
  host="world-athletics-world-athletics.h.aivencloud.com",
  password="AVNS_q4qfMZEYhnIhreAFwYi",
  read_timeout=timeout,
  port=24936,
  user="avnadmin",
  write_timeout=timeout,
)

states = []
cursor = connection.cursor()
cursor.execute("select state from States")
x = cursor.fetchall()
cursor.close()

unique_state_list = [i["state"] for i in x ]

def check_similarity(col_name_sql,col_name_api):
    pass

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
        # print(similarity_score, state)

    # Return the best matching state name
    if(highest_score < 0.7):
        return None
    return best_match

def get_start_date(string:str):
    try:
        temp = re.split(r"[(),-]",string)
        date_string = "1 " + temp[1] + " " + temp[-1] 
        date_object = datetime.strptime(date_string, "%d %b %Y").date()
        return date_object
    except:
        return None

def get_end_date(string:str):
    try:
        temp = re.split(r"[(),-]",string)
        date_string = "1 " + temp[2] + " " + str(int(temp[-1]) +1)
        date_object = datetime.strptime(date_string, "%d %b %Y").date()
    except:
        return None
    return date_object

def get_month(string: str):
    try:
        temp = re.split(r"[(),-]",string)
        
        date_string = "1 " + temp[-2] + " " + str(int(temp[-1]))
        # print(date_string)
        date_object = datetime.strptime(date_string, "%d %B %Y").date()
    except:
        return None
    return date_object

df_per_year = df.drop(["State LGD Code","Month","Country"],axis=1)
df_per_month = df.drop(["Year","State LGD Code","Country"],axis=1)

df_per_year = df_per_year.groupby(["State","Year"]).mean().reset_index()

for i in df_per_year.keys():
    x = i.split('_')
    if(len(x) == 2 and x[1] != "avg"):
        print(df_per_year[i])
        df_per_year = df_per_year.drop([i],axis=1)

for i in df_per_month.keys():
    x = i.split('_')
    if(len(x) == 2 and x[1] != "avg"):
        print(df_per_month[i])
        df_per_month = df_per_month.drop([i],axis=1)

df_per_year["state"] = df_per_year.apply(lambda x : match_state_name(x["State"],unique_state_list),axis=1)
df_per_year["start_date"] = df_per_year.apply(lambda x : get_start_date(x["Year"]),axis=1)
df_per_year["end_date"] = df_per_year.apply(lambda x : get_end_date(x["Year"]),axis=1)

df_per_month["state"] = df_per_month.apply(lambda x : match_state_name(x["State"],unique_state_list),axis=1)
df_per_month["date"] = df_per_month.apply(lambda x : get_month(x["Month"]),axis=1)

col_name1 = ["state","start_date","end_date"]
for i in df_per_year.keys():
    x = i.split('_')
    if(len(x) == 2 and x[1] == "avg"):
        print(df_per_year[i])
        df_per_year[x[0]] = df_per_year[i]
        col_name1.append(x[0])


col_name = ["state","date"]
for i in df_per_month.keys():
    x = i.split('_')
    if(len(x) == 2 and x[1] == "avg"):
        print(df_per_month[i])
        df_per_month[x[0]] = df_per_month[i]
        col_name.append(x[0])
df_per_month_transformed =  df_per_month[col_name]
df_per_month_transformed.dropna()

df_per_month_transformed.to_csv("etl/trans_register_payers_per_month_data_2.csv")
df_per_year[col_name1].dropna().to_csv("etl/trans_GST_info_per_year_data_2.csv")

