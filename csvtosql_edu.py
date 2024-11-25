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
  
  # with open('education.csv',mode='r') as f:
  #   csv_reader=csv.reader(f)
  #   headers=next(csv_reader)
  #   # print(headers)
  #   tablename="education"
  #   columns1 = "create table education ( edu_id int auto_increment,"
  #   for i in range(6):
  #     temp=list(headers[i])
  #     if len(temp)>1:
  #       temp=headers[i].replace(" ","_")
  #       temp=temp.replace("\t","_")
  #       temp=temp.replace("(","_")
  #       temp=temp.replace(")","_")
  #       temp=temp.replace("/","_")
  #       # temp=temp.replace("-","_")
  #       # print(temp)
  #     columns1+= f"{temp} varchar(255),"
      
  #   for i in range(6,15):
  #     temp=list(headers[i])
  #     if len(temp)>1:
  #       temp=headers[i].replace(" ","_")
  #       temp=temp.replace("\t","_")
  #       temp=temp.replace("(","_")
  #       temp=temp.replace(")","_")
  #       temp=temp.replace("/","_")
  #       temp=temp.replace("-","_")
  #       # temp=temp.replace("_","")
  #     columns1+= f"{temp} int,"
      
  #   columns1+="primary key (edu_id))"
    
  #   print(columns1)
  #   cursor.execute(f"{columns1}")
  
  # fetch all column identifier name
  cursor.execute("DESCRIBE education")  # or SHOW COLUMNS FROM education

    # Fetch and print the column names
  columns = cursor.fetchall()
  # for column in columns:
  #     print(column['Field'])
  col=[]
  for c in columns:
    col.append(c['Field'])
  print(col)
  k=0
  
  with open('education.csv',mode='r') as f:
    # dont use csv_reader
    next(f)
    csv_reader=f.readlines()
    
    # print(headers)
    
    for row in csv_reader:
      row=row.split(',')
      for i in range(len(row)):
        if row[i]=="":
          row[i]=0
      print(row)
    
      print(f"INSERT INTO education ({col[1]},{col[2]},{col[3]},{col[4]},{col[5]},{col[6]},{col[7]},{col[8]},{col[9]},{col[10]},{col[11]},{col[12]},{col[13]},{col[14]},{col[15]}) VALUES ('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}',{row[6]},{row[7]},{row[8]},{row[9]},{row[10]},{row[11]},{row[12]},{row[13]},{row[14]})")
      # k+=1
      cursor.execute(f"INSERT INTO education ({col[1]},{col[2]},{col[3]},{col[4]},{col[5]},{col[6]},{col[7]},{col[8]},{col[9]},{col[10]},{col[11]},{col[12]},{col[13]},{col[14]},{col[15]}) VALUES ('{row[0]}','{row[1]}','{row[2]}','{row[4]}','{row[5]}','{row[6]}',{row[7]},{row[8]},{row[9]},{row[10]},{row[11]},{row[12]},{row[13]},{row[14]},{row[15]})")
      
      
      
  
  print(cursor.fetchall())
finally:
  connection.commit()
  connection.close()