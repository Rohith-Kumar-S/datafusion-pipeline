import streamlit as st
import pandas as pd
from io import StringIO
st.title("Data Fusion Pipeline")

options = ["Link", "Upload"]
selection = st.segmented_control(
    "Data Source", options, selection_mode="single"
)

# Initialize the session state key if it doesn't exist
if "user_input" not in st.session_state:
    st.session_state.user_input = ""
if "links" not in st.session_state:
    st.session_state.links = []
    
def clear_input():
    st.session_state.links.append(st.session_state.user_input)
    st.session_state.user_input = ""
    
if not selection or "Link" in selection:
    col1, col2 = st.columns([4, 1], vertical_alignment="bottom")
    with col1:
        st.text_input("Link to data source", placeholder="https://example.com/data.csv", key="user_input")
    with col2:
        st.button("Import", on_click=clear_input)
    for link in st.session_state.links:
        st.write(link)

else:
    uploaded_files = st.file_uploader(
        "Choose a CSV file", accept_multiple_files=True
    )
    for uploaded_file in uploaded_files:
        bytes_data = uploaded_file.read()
        # st.write(uploaded_file.name)
        df = pd.read_csv(StringIO(bytes_data.decode('utf-8')))
        

        
