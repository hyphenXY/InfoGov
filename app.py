import streamlit as st

# Set the app title
st.title("InfoGov")
# Add a welcome message
st.write("Welcome to a Unified Platform to all Queries regarding Government Data!")
# Create a text input
widgetuser_input = st.text_input("Enter Query:", "demo")
# Display the customized message
st.write("Answer: ", widgetuser_input)
