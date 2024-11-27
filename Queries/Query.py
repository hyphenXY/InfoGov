from logging import exception
import pymysql
import json


def q1(state, year):
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
        
        with open("Queries/query1.sql", "r") as file:
            sql_script = file.read()  
        
        # matching 
        
          
        sql_script = sql_script.replace("@state", f"'{state}'").replace("@year", f"'{year}'")

        sql_statements = sql_script.split(";")
        for statement in sql_statements:
            if statement.strip():  # Skip empty statements
                cursor.execute(statement)
        # Fetch the final result from the query
        result = cursor.fetchall()
            

    finally:
        connection.commit()
        connection.close()
        
    return result

def q2(n):
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
        
        with open("Queries/query2.sql", "r") as file:
            sql_script = file.read()    
        sql_script = sql_script.replace("@top_n", f"{n}")

        sql_statements = sql_script.split(";")
        for statement in sql_statements:
            if statement.strip():  # Skip empty statements
                cursor.execute(statement)
        # Fetch the final result from the query
        result = cursor.fetchall()
            

    finally:
        connection.commit()
        connection.close()
        
    return result
