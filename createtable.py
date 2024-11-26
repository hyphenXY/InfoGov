from logging import exception
import pymysql
import json

   

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
    cursor.execute("create table GST_Payers_PerYear_Data(id int auto_increment, state varchar(255), keyy varchar(255), start_date date, end_date date, value float, primary key (id))")
    cursor.execute("create table GST_Info_PerYear_Data (id int auto_increment, state varchar(255), keyy varchar(255), start_date date, end_date date, value float, primary key (id))")
    cursor.execute("create table GST_Info_PerMonth (id int auto_increment, state varchar(255), keyy varchar(255), datee date, value float, primary key (id))")
    cursor.execute("create table GST_ReturnType_Info_Value (id int auto_increment, state varchar(255), keyy varchar(255), datee date, value float, primary key (id))")
    cursor.execute("create table Banking_Statistics (id int auto_increment, state varchar(255), start_date date, end_date date, bank_type  varchar(255), keyy varchar(255), value float, primary key (id))")
    cursor.execute("create table Outstanding_Liabilities_Info (id int auto_increment, state varchar(255), start_date date, end_date date, keyy varchar(255), value float, primary key (id))")
    cursor.execute("create table Loans_Taken_by_States (id int auto_increment, state varchar(255), start_date date, end_date date, provider varchar(255), loan_amount float, primary key (id))")
    cursor.execute("create table Fiscal_Indicator_of_States (id int auto_increment, state varchar(255), start_date date, end_date date, keyy varchar(255), value float, primary key (id))")
    cursor.execute("create table States (id int auto_increment, state varchar(255), primary key (id))")
    cursor.execute("create table Providers (id int auto_increment, provider varchar(255), primary key (id))")
    cursor.execute("create table BankTypes (id int auto_increment, bank_type varchar(255), primary key (id))")
    cursor.execute("create table GSTPayers_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    cursor.execute("create table GSTInfo_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    cursor.execute("create table GSTInfo_PerMonth_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    cursor.execute("create table BankStats_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    cursor.execute("create table Liabiity_Keys (id int auto_increment, keyy varchar(255), primary key (id))")
    cursor.execute("create table Fiscal_Keys (id int auto_increment, keyy varchar(255), primary key (id))")
finally:
  connection.commit()
  connection.close()