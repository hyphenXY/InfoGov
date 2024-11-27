import streamlit as st
from streamlit_chat import message
import Queries.Query as Query  # Import your custom Query module
import extract_states as es

# Define predefined queries
queries = {
    "Tell me about the IGST settlement trends": {
        "description": "Provide State & Year",
        "response": ""
    },
    "Which states have the highest GST contributors across data sources?": {
        "description": "Provide N - Number of States",
        "response": ""
    }
}

# Streamlit app layout
st.title("InfoGov")
st.sidebar.header("Select a Query")

# Sidebar for predefined queries
selected_query = st.sidebar.selectbox("Choose a query to ask the chatbot:", list(queries.keys()))

# Display the selected query details
query_details = queries[selected_query]
st.subheader("Welcome to a Unified Platform for all your queries!")
st.write(f"*{query_details['description']}*")

# Hardcoded parameter handling for each query
if selected_query == "Tell me about the IGST settlement trends":
    # Hardcoded parameters for this query
    state = st.text_input("Enter State:", value="Delhi")  # Default: Delhi
    year = st.text_input("Enter Year:", value="2023")  # Default: 2023

    if st.button("Run Query"):
        st.session_state.chat_history.append({"text": selected_query, "user": True})

        try:
            # Compute the response using the hardcoded inputs
            state=es.match_state_name(state)
            query_details["response"]=Query.q1(state,year)
            response = str(query_details["response"] )
            st.session_state.chat_history.append({"text": response, "user": False})
        except Exception as e:
            st.error(f"An error occurred: {e}")

elif selected_query == "Which states have the highest GST contributors across data sources?":
    # Hardcoded parameter for this query
    n = st.number_input("Enter Number of States (N):", min_value=1, max_value=50, value=5)  # Default: 5

    if st.button("Run Query"):
        st.session_state.chat_history.append({"text": selected_query, "user": True})

        try:
            # Compute the response using the hardcoded input
            query_details["response"]=Query.q2(n)
            response = str(query_details["response"] )
            st.session_state.chat_history.append({"text": response, "user": False})
        except Exception as e:
            st.error(f"An error occurred: {e}")

# Display chat history
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

for chat in st.session_state.chat_history:
    if chat["user"]:
        message(chat["text"], is_user=True)
    else:
        message(chat["text"])