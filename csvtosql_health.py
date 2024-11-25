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
  # cursor.execute("INSERT INTO mytest (id) VALUES (1), (2)")
  # cursor.execute("SELECT * FROM mytest")
  # open eductaion.csv file
  
  with open('health.csv',mode='r') as f:
    csv_reader=csv.reader(f)
    headers=next(csv_reader)
    # print(headers)
    tablename="population"
    columns1 = "create table health ( health_id int auto_increment,"
    for i in range(5):
      temp=list(headers[i])
      if len(temp)>1:
        temp=headers[i].replace("( UHS )","")
        temp=temp.replace(" ","_")
        temp=temp.replace("\t","_")
        temp=temp.replace("(","_")
        temp=temp.replace(")","_")
        temp=temp.replace("/","_")
        temp=temp.replace(":","_")
        temp=temp.replace("-","_")
        # print(temp)
        if len(temp)>64:
            temp=temp[len(temp)-64:]
      columns1+= f"{temp} varchar(255),"
      
    for i in range(5,27):
      temp=list(headers[i])
      if len(temp)>1:
        temp=headers[i].replace(" ","_")
        temp=temp.replace("\t","_")
        temp=temp.replace("(","_")
        temp=temp.replace(")","_")
        temp=temp.replace("/","_")
        temp=temp.replace("-","_")
        temp=temp.replace(",","_")
        # temp=temp.replace("_","")
        if len(temp)>64:
            temp=temp[len(temp)-64:]
      columns1+= f"{temp} int,"
      
    columns1+="primary key (health_id))"
    
    print(columns1)
    cursor.execute(f"{columns1}")
    
    # for row in csv_reader:
      
  
  print(cursor.fetchall())
finally:
  connection.commit()
  connection.close()