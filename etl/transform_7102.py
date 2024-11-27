import pymysql
import json
import requests
import jellyfish
from datetime import datetime
import re
import pandas


df = pandas.read_csv("7102.csv")
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

def match_state_name(input_state, unique_states_list):
    # Initialize variables to store the best match and highest score
    best_match = None
    highest_score = 0
    for state in unique_states_list:
        similarity_score = jellyfish.jaro_winkler_similarity(input_state, state)
        if similarity_score > highest_score:
            highest_score = similarity_score
            best_match = state
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
        date_object = datetime.strptime(date_string, "%d %B %Y").date()
    except:
        return None
    return date_object
df_per_year = df.drop(["Month","Gst ( goods and service tax ) return type","State lgd code","Country"],axis=1)
df_per_year = df_per_year.groupby(["State","Year"]).mean().reset_index()

df_per_month = df.drop(["Year","State lgd code","Country"],axis=1)
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

col_list_year = [
    "state",
    "Gst ( goods and service tax ) payers registered",
    "Payer eligible for gst ( goods and service tax ) registration",
    "start_date",
    "end_date",
]
df_per_year["state"] = df_per_year.apply(lambda x : match_state_name(x["State"],unique_state_list),axis=1)
df_per_year["start_date"] = df_per_year.apply(lambda x : get_start_date(x["Year"]),axis=1)
df_per_year["end_date"] = df_per_year.apply(lambda x : get_end_date(x["Year"]),axis=1)
df_per_year["Gst ( goods and service tax ) payers registered"] = df_per_year.apply(lambda x : x["Gst ( goods and service tax ) payers registered before due date_avg"] +x["Gst ( goods and service tax ) payers registered after due date_avg"] ,axis=1) 
df_per_year["Payer eligible for gst ( goods and service tax ) registration"] = df_per_year.apply(lambda x : x["Payer eligible for gst ( goods and service tax ) registration_avg"],axis=1)
df_per_month["state"] = df_per_month.apply(lambda x : match_state_name(x["State"],unique_state_list),axis=1)

col_list_month = [
    "state",
    "date",
    "Gst ( goods and service tax ) payers registered",
    "Payer eligible for gst ( goods and service tax ) registration",
    "Gst ( goods and service tax ) return type"
    ]
df_per_month["date"] = df_per_month.apply(lambda x : get_month(x["Month"]),axis=1)
df_per_month["Gst ( goods and service tax ) payers registered"] = df_per_month.apply(lambda x : x["Gst ( goods and service tax ) payers registered before due date_avg"] +x["Gst ( goods and service tax ) payers registered after due date_avg"] ,axis=1) 
df_per_month["Payer eligible for gst ( goods and service tax ) registration"] = df_per_month.apply(lambda x : x["Payer eligible for gst ( goods and service tax ) registration_avg"],axis=1)
df_per_month["Gst ( goods and service tax ) return type"] = df_per_month.apply(lambda x : x["Gst ( goods and service tax ) return type"],axis=1)

df_per_year_transformed = df_per_year[col_list_year]
df_per_month_transformed =  df_per_month[col_list_month]

df_per_month_transformed.dropna()
df_per_year_transformed.dropna()

df_per_year_transformed.to_csv("etl/trans_register_payers_per_year_data.csv")
df_per_month_transformed.to_csv("etl/trans_register_payers_per_month_data.csv")