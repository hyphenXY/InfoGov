import streamlit as st
from openai import OpenAI
from Queries.Query import query1


client = OpenAI(
  base_url = "https://integrate.api.nvidia.com/v1",
  api_key = "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"
)

# Streamlit app setup
st.set_page_config(page_title="Chatbot App", page_icon="🤖", layout="wide")
st.title("🤖 Chatbot App")
st.markdown("### Ask me anything and I'll do my best to respond!")

# Styling with CSS for a polished look
def query_gen(user_input):
    completion = client.chat.completions.create(
        model="nvidia/llama-3.1-nemotron-70b-instruct",
        messages=[{"role":"user","content":f'''
                   you are a query analyzer that returns SQL query for a given natural language query. You are provided with schema for which you need to generate sql queries, be mindful the names are case sensitive. Only output the sql query and nothing else. Incase you do nor have sufficient info to produce SQL query just outpo

                    TABLE SCHEMA :
                    
                    GST_Payers_PerYear_Data_NS:

                    state*, start_date, end_date , total_eligible_payers ,total_registered_payers, normal_tax_payers, composite_tax_payer, casual_tax_payer, nri_tax_payers

                    GST_Info_PerYear_Data_NS:

                    state* ,start_date, end_date, input_service_distributors, UIN_holders, settlement_IGST

                    GST_PerMonth_Info_NS:

                    state*,datee,settlement_IGST

                    GST_Payers_Info_PerMonth_NS:

                    state*, datee, return_type*, eligible_payers, registered_payers

                    NATURAL LANGUAGE QUERY: {user_input}
                    Make sure to add ; at end of each sql statement.
                   
                   
                   
                   
                   
                   
                   '''}],
        temperature=0.5,
        top_p=1,
        max_tokens=1024,
        stream=True
    )
    output=''
    for chunk in completion:
        if chunk.choices[0].delta.content is not None:
            output+=chunk.choices[0].delta.content
    normalized_output = output.replace('“', '"').replace('”', '"').replace("‘", "'").replace("’", "'")
    cleaned_output = normalized_output.replace("```", "").strip()
    return cleaned_output
    
def querry_processing(user_input):
    query=query_gen(user_input)
    print(query)
    st.write(query)
    result=query1(query)
    return result
    
st.markdown("""
    <style>
        .chat-box {
            background-color: #f8f9fa;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);
            margin-bottom: 20px;
        }
        .chat-bot {
            font-weight: bold;
            color: #007bff;
        }
        .chat-user {
            font-weight: normal;
            color: #495057;
        }
        .input-box {
            display: flex;
            justify-content: space-between;
            margin-top: 20px;
        }
        .input-box input {
            width: 90%;
            border-radius: 5px;
            padding: 10px;
            border: 1px solid #ced4da;
            box-shadow: 0px 1px 3px rgba(0, 0, 0, 0.1);
        }
        .input-box button {
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 5px;
            padding: 10px 15px;
            cursor: pointer;
        }
        .input-box button:hover {
            background-color: #0056b3;
        }
    </style>
""", unsafe_allow_html=True)

# Chat history container
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Display chat history
for chat in st.session_state.chat_history:
    if chat["role"] == "user":
        st.markdown(f'<div class="chat-box"><p class="chat-user">You: {chat["message"]}</p></div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="chat-box"><p class="chat-bot">Bot: {chat["message"]}</p></div>', unsafe_allow_html=True)

# User input section
user_input = st.text_input("Your message:", "")

# Button to send the message
if st.button("Send") and user_input.strip() != "":
    # Save user input to chat history
    st.session_state.chat_history.append({"role": "user", "message": user_input})

    # Simulate a bot response (replace this with actual NLP model logic)
    bot_response = querry_processing(user_input)

    # Save bot response to chat history
    st.session_state.chat_history.append({"role": "bot", "message": bot_response})
    st.write(bot_response)

    # Refresh the page (optional)
# Be cautious with this call to avoid infinite loops
