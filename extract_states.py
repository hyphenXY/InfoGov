import pandas as pd
import jellyfish
from logging import exception
import pymysql
import json

def match_state_name(input_state):
    # Initialize variables to store the best match and highest score
    input_state=input_state.lower()
    best_match = None
    highest_score = 0
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

    # Iterate through the unique states list and calculate Jaro-Winkler scores
    unique_states_list = list(unique_states)
    unique_states_list=[u.lower() for u in unique_states_list]
    for state in unique_states_list:
        similarity_score = jellyfish.jaro_winkler_similarity(input_state, state)
        if similarity_score > highest_score:
            highest_score = similarity_score
            best_match = state

    # Return the best matching state name
    return best_match


print(match_state_name("UP") )


# Convert the set to a list

