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
  cursor.execute("CREATE TABLE shivam (id INTEGER PRIMARY KEY)")
  cursor.execute("INSERT INTO mytest (id) VALUES (1), (2)")
  cursor.execute("SELECT * FROM mytest")
  print(cursor.fetchall())
finally:
  connection.close()