import csv
import pymysql

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

try:
    cursor = connection.cursor()
    cursor.execute("use iia_group2")
    
    with open('trans_GST_info_per_year_data.csv',mode='r') as f:
        csv_reader=csv.reader(f)
        headers=next(csv_reader)
        tablename="GST_Data"
        
        # table already created, just insert data
        columns1 = "insert into GST_Data ("
        