import streamlit as st
from streamlit_chat import message  # Ensure this library is installed: pip install streamlit-chat
import Queries.Query as Query

# Define predefined queries with hardcoded parameters
queries = {
    "Tell me about the IGST settlement trends": {
        "description": "Provide State & Year",
        "parameters": {"State": "Delhi", "Year": "2023"},  # Example hardcoded parameters
        "response": lambda params: Query.q1(params['State'], params['Year'])
    },
    "Which states have the highest GST contributors across data sources?": {
        "description": "Provide N - Number of States",
        "parameters": {"N": 5},  # Example hardcoded parameter
        "response": lambda params: Query.q2(params['N'])  # Replace with actual Query.q2 function
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

# Session state to track chat history
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Display chat history
for chat in st.session_state.chat_history:
    if chat["user"]:
        message(chat["text"], is_user=True)
    else:
        message(chat["text"])

# Display query description and hardcoded parameters
st.write(f"**{query_details['description']}**")
st.write("**Hardcoded Parameters:**")
for param, value in query_details["parameters"].items():
    st.write(f"{param}: {value}")

# Handle user query execution when the button is clicked
if st.button("Run Query"):
    # Add user query to chat history
    st.session_state.chat_history.append({"text": selected_query, "user": True})

    # Compute and display the response
    try:
        response = query_details["response"](query_details["parameters"])
        st.session_state.chat_history.append({"text": response, "user": False})
    except Exception as e:
        st.error(f"An error occurred: {e}")

# Display updated chat history
for chat in st.session_state.chat_history:
    if chat["user"]:
        message(chat["text"], is_user=True)
    else:
        message(chat["text"])
