import pandas as pd
import jellyfish
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
cursor = connection.cursor()
cursor.execute("use iia_group2")

def match_state_name(input_state, unique_states_list):
    # Initialize variables to store the best match and highest score
    best_match = None
    highest_score = 0

    # Iterate through the unique states list and calculate Jaro-Winkler scores
    for state in unique_states_list:
        similarity_score = jellyfish.jaro_winkler_similarity(input_state, state)
        if similarity_score > highest_score:
            highest_score = similarity_score
            best_match = state

    # Return the best matching state name
    return best_match
csv_list = ['7107.csv', '7103.csv', '7102.csv', '7049.csv', '7048.csv', '7047.csv']

unique_states = set()  # Use a set to ensure uniqueness


for csv_file in csv_list:
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        # Check if the 'State' column exists
        if 'State' in df.columns:
            # Add unique states to the set
            unique_states.update(df['State'].dropna().unique())
    except Exception as e:
        print(f"Error processing {csv_file}: {e}")

# Convert the set to a list
unique_states_list = list(unique_states)
unique_states_list=[u.lower() for u in unique_states_list]
for state in unique_states_list:
    try:
        cursor.execute("INSERT INTO States (state) VALUES (%s)", (state,))
        print(f"Inserted state: {state}")
    except Exception as e:
        print(f"Error inserting state {state}: {e}")

# Commit the changes to the database
connection.commit()

# Close the connection
cursor.close()
connection.close()

print("All states have been inserted into the States table.")








