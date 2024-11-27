import pymysql
import json
import requests
import jellyfish
from datetime import datetime
import re
from difflib import SequenceMatcher as SM
import pandas as pd

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
        sim=SM(isjunk=None, a=col1, b=i).ratio()
        if sim>m_score:
            m_score = sim
            m_col = i
    if(m_score < 0.4):
        return None, m_score
    return m_col, m_score

cursor = connection.cursor()
cursor.execute("desc GST_Payers_PerYear_Data_NS")
x = cursor.fetchall()
sql_fields = [i["Field"] for i in x]
cursor.close()

df1 = pd.read_csv("trans_register_payers_per_year_data.csv",index_col=0)
df2 = pd.read_csv("trans_GST_info_per_year_data.csv",index_col=0)

schema_incoming_1 = list(df1.keys())
schema_incoming_2 = list(df2.keys())

schema_map_1 = {}
schema_map_2 = {}

for i in schema_incoming_1:
        schema_map_1[i] = matcher(i,sql_fields)[0]
        if(schema_map_1[i] == None):
                del schema_map_1[i]
           
for i in schema_incoming_2:
        schema_map_2[i] = matcher(i,sql_fields)[0]
        if(schema_map_2[i] == None):
                del schema_map_2[i]

for i in df1.keys():
    try:
        df1[schema_map_1[i]] = df1[i]
    except KeyError:
        continue

for i in df2.keys():
    try:
        df2[schema_map_2[i]] = df2[i]
    except KeyError:
        continue

df_1 = df1[list(schema_map_1.values())]
df_2 = df2[list(schema_map_2.values())]
primary_keys = ["state","start_date","end_date"]

insert = {}
update = {}
for i in sql_fields[1:]:
    insert[i] = []
    update[i] = []
print(schema_map_1.values())
for  _, i in df_1.iterrows():
    cursor = connection.cursor()
    cursor.execute(
        "select * from GST_Payers_PerYear_Data_NS where state = %s AND start_date = %s AND end_date = %s",
        (
            i["state"],
            i["start_date"],
            i["end_date"]
            )
    )
    x = cursor.fetchall()
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
                elif(x[j] != i[j]  or j in primary_keys):
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
            "insert into GST_Payers_PerYear_Data_NS ("+', '.join(sql_fields[1:])+") values ("+ ', '.join(["%s" for i in sql_fields[1:]]) +")",
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
        final_tup.extend([i["state"],i["start_date"],i["end_date"]])
    
        cursor.execute(
            f"update GST_Payers_PerYear_Data_NS set {final} where state = %s AND start_date = %s AND end_date = %s",
            tuple(final_tup)
        )
        connection.commit()
        cursor.close()
    except:
        continue

insert = {}
update = {}
for i in sql_fields[1:]:
    insert[i] = []
    update[i] = []
print(schema_map_2.values())
for  _, i in df_2.iterrows():
    cursor = connection.cursor()
    cursor.execute(
        "select * from GST_Payers_PerYear_Data_NS where state = %s AND start_date = %s AND end_date = %s",
        (
            i["state"],
            i["start_date"],
            i["end_date"]
            )
    )
    x = cursor.fetchall()
    if(len(x) == 0):
        for j in insert.keys():
            try:
                insert[j].append(i[j])
            except KeyError:
                insert[j].append(None)
    else:
        x = x[0]
        for j in update.keys():
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
            "insert into GST_Payers_PerYear_Data_NS ("+', '.join(sql_fields[1:])+") values ("+ ', '.join(["%s" for i in sql_fields[1:]]) +")",
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
        final_tup.extend([i["state"],i["start_date"],i["end_date"]])
        cursor.execute(
            f"update GST_Payers_PerYear_Data_NS set {final} where state = %s AND start_date = %s AND end_date = %s",
            tuple(final_tup)
        )
        connection.commit()
        cursor.close()
    except:
        continue