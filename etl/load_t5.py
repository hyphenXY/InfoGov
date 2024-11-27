import pymysql
import json
import requests
import jellyfish
from datetime import datetime
import re
from difflib import SequenceMatcher as SM
import pandas as pd
from fuzzywuzzy import fuzz

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

cursor = connection.cursor()
cursor.execute("desc GST_PerMonth_Info_NS")
x = cursor.fetchall()
sql_fields = [i["Field"] for i in x]
cursor.close()

df1 = pd.read_csv("trans_register_payers_per_month_data_2.csv",index_col=0)
schema_incoming_1 = list(df1.keys())

schema_map_1 = {}

for i in schema_incoming_1:
        schema_map_1[i] = matcher(i,sql_fields)[0]
        # print(schema_map_1[i])
        if(schema_map_1[i] == None):
                del schema_map_1[i]

for i in df1.keys():
    try:
        df1[schema_map_1[i]] = df1[i]
    except KeyError:
        continue

df_1 = df1[list(schema_map_1.values())]
primary_keys = ["state","datee"]

insert = {}
update = {}
from tqdm import tqdm
for i in sql_fields[1:]:
    insert[i] = []
    update[i] = []
print(schema_map_1.values())
cursor = connection.cursor()
for  _, i in tqdm(df_1.iterrows()):
    cursor.execute(
        "select * from GST_PerMonth_Info_NS  where state = %s AND datee = %s ",
        (
            i["state"],
            i["datee"]
            )
    )
    x = cursor.fetchall()
    # print(x)
    if(len(x) == 0):
        for j in insert.keys():
            try:
                insert[j].append(i[j])
            except KeyError:
                insert[j].append(None)
    else:
        x = x[0]
        for j in insert.keys():
            try:
                if(x[j] == None and j not in primary_keys):
                    update[j].append(i[j])
                elif(x[j] != i[j] or j in primary_keys):
                    update[j].append(i[j])
                else:
                    update[j].append(None)
            except KeyError:
                update[j].append(None)
cursor.close()

for _, i in pd.DataFrame(insert).iterrows():
    try:
        cursor = connection.cursor()
        cursor.execute(
            "insert into GST_PerMonth_Info_NS  ("+', '.join(sql_fields[1:])+") values ("+ ', '.join(["%s" for i in sql_fields[1:]]) +")",
            tuple(i.to_list())
        )
        connection.commit()
        cursor.close()
    except:
        continue

for _, i in pd.DataFrame(update).iterrows():
    try:
        cursor = connection.cursor()
        final  = []
        final_tup = []
        for j in i.keys():
            if(i[j] != None):
                final.append(' '.join([str(j),"=","%s"]))
                final_tup.append(i[j])
        final = ', '.join(final)
        final_tup.extend([i["state"],i["datee"]])
        # print(final_tup)
        cursor.execute(
            f"update GST_PerMonth_Info_NS  set {final} where state = %s datee = %s",
            tuple(final_tup)
        )
        connection.commit()
        cursor.close()
    except:
        continue