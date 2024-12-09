import pymysql
from flask import Flask, request, jsonify

import pymysql
import json
import requests
import jellyfish
from datetime import datetime
import re
import pandas

timeout = 10
connection = pymysql.connect(
  charset="utf8mb4",
  connect_timeout=timeout,
  cursorclass=pymysql.cursors.DictCursor,
  db="iia_group2",
  host="localhost",
  password="root",
  read_timeout=timeout,
#   port=24936,
  user="mint",
  write_timeout=timeout,
)


app = Flask(__name__)

@app.route("/", methods=["GET"])
def ds():
    return "<h1>Yolo</h1>"


@app.route("/chat", methods=["POST"])
def chat():
    data = request.json
    query = data["query"]
    response = ""

    try:
        cursor = connection.cursor()
        cursor.execute("use iia_group2")

        res = cursor.execute(query)
        if res > 0:
            response = cursor.fetchall()
        else:
            response = "No records found"

        cursor.close()
    except Exception as e:
        response = f"An error occurred: {e}"
        cursor.close()

    return jsonify(response)


@app.route('/columns/<string:table>', methods=['GET'])
def get_columns(table):
    print(table)
    try:
        if len(table) == 0:
            return jsonify({"error": "No data available"}), 404
        try:
            cursor = connection.cursor()
            cursor.execute("use iia_group2")

            res = cursor.execute("desc "+ table)
            if res > 0:
                response = cursor.fetchall()
            else:
                response = "No records found"
            cursor.close()
            return jsonify(response), 200
        except Exception as e:
            response = f"An error occurred: {e}"
            cursor.close()
            return jsonify({"error": response}), 404      
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# if name == "main":
app.run(debug=True)