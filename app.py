import streamlit as st
import pandas as pd
from io import StringIO
from kagglehub import kagglehub
st.title("Data Fusion Pipeline")

options = ["Link", "Upload"]
selection = st.segmented_control(
    "Data Source", options, selection_mode="single"
)

# Initialize the session state key if it doesn't exist
if "user_input" not in st.session_state:
    st.session_state.user_input = ""
if "datasets" not in st.session_state:
    st.session_state.datasets = {}
    
def import_kaggle_data(dataset):
    """Import a dataset from Kaggle and return the DataFrame."""
    print(f"Importing dataset: {dataset}")
    path = kagglehub.dataset_download(dataset)
    return path
    
def import_data(links):
    if not links == "": 
        res = links.split(r'https://')
        datasets = {}
        for i in res:
            if i == "":
                continue
            if 'kaggle' in i.strip() and 'datasets' in i.strip():
                dataset_name = i.strip().split('datasets/')[1]
                datasets[dataset_name] = None
                datasets[dataset_name] = import_kaggle_data(dataset_name)
            else:
                print('not a kaggle link:', i)
        st.session_state.datasets = datasets
        st.session_state.user_input = links
        print(st.session_state.user_input)
    
if not selection or "Link" in selection:
    links = st.text_area("Link to data source", st.session_state.user_input, placeholder="https://example.com/data.csv")
    st.button("Import", on_click=lambda: import_data(links))
    st.write("Valid datasets imported from links:")
    for dataset_name, dataset_path in st.session_state.datasets.items():
        if dataset_path:
            st.write(dataset_name)


else:
    uploaded_files = st.file_uploader(
        "Choose a CSV file", accept_multiple_files=True
    )
    for uploaded_file in uploaded_files:
        bytes_data = uploaded_file.read()
        # st.write(uploaded_file.name)
        df = pd.read_csv(StringIO(bytes_data.decode('utf-8')))
        

        
