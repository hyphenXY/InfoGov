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
  
#   with open('population.csv',mode='r') as f:
#     csv_reader=csv.reader(f)
#     headers=next(csv_reader)
#     # print(headers)
#     tablename="population"
#     columns1 = "create table population ( pop_id int auto_increment,"
#     for i in range(7):
#       temp=list(headers[i])
#       if len(temp)>1:
#         temp=headers[i].replace(" ","_")
#         temp=temp.replace("\t","_")
#         temp=temp.replace("(","_")
#         temp=temp.replace(")","_")
#         temp=temp.replace("/","_")
#         temp=temp.replace(":","_")
#         temp=temp.replace("-","_")
#         # print(temp)
#       columns1+= f"{temp} varchar(255),"
      
#     for i in range(7,36):
#       temp=list(headers[i])
#       if len(temp)>1:
#         temp=headers[i].replace(" ","_")
#         temp=temp.replace("\t","_")
#         temp=temp.replace("(","_")
#         temp=temp.replace(")","_")
#         temp=temp.replace("/","_")
#         temp=temp.replace("-","_")
#         temp=temp.replace(":","_")
#         # temp=temp.replace("_","")
#       columns1+= f"{temp} int,"
      
#     columns1+="primary key (pop_id))"
    
#     print(columns1)
#     cursor.execute(f"{columns1}")
    
    cursor.execute("DESCRIBE population")  # or SHOW COLUMNS FROM education

    # Fetch and print the column names
    columns = cursor.fetchall()
    # for column in columns:
    #     print(column['Field'])
    col=[]
    for c in columns:
        col.append(c['Field'])
    print(col)
        # for row in csv_reader:
    
    with open('population.csv',mode='r') as f:
        # use csv reader
        next(f)
        csv_reader=f.readlines()
        for row in csv_reader:
            row=row.split(',')
            for i in range(len(row)):
                if row[i]=="":
                    row[i]=0
            print(row[7])
            
            
            try:
                cursor.execute(f" insert into population ({col[1]},{col[2]},{col[3]},{col[4]},{col[5]},{col[6]},{col[7]},{col[8]},{col[9]},{col[10]},{col[11]},{col[12]},{col[13]},{col[14]},{col[15]},{col[16]},{col[17]},{col[18]},{col[19]},{col[20]},{col[21]},{col[22]},{col[23]},{col[24]},{col[25]},{col[26]},{col[27]},{col[28]},{col[29]},{col[30]},{col[31]},{col[32]},{col[33]},{col[34]},{col[35]},{col[36]}) values ('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[6]}','{row[7]}',{row[8]},{row[9]},{row[10]},{row[11]},{row[12]},{row[13]},{row[14]},{row[15]},{row[16]},{row[17]},{row[18]},{row[19]},{row[20]},{row[21]},{row[22]},{row[23]},{row[24]},{row[25]},{row[26]},{row[27]},{row[28]},{row[29]},{row[30]},{row[31]},{row[32]},{row[33]},{row[34]},{row[35]},{row[36]})")
                
            except Exception as e:
                continue
        
    
    print(cursor.fetchall())
finally:
  connection.commit()
  connection.close()