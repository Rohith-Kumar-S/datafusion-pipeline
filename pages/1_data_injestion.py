from fileinput import filename
import streamlit as st
import pandas as pd
from io import StringIO
from kagglehub import kagglehub
import re
from urllib.parse import urlparse
import requests
import os
import random
import string

from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(st.session_state.spark, st.session_state.datasets, st.session_state.dataset_paths, st.session_state.views, st.session_state.temp_datasets, st.session_state.user_input)

options = ["Link", "Upload"]
selection = st.segmented_control(
    "Data Source", options, selection_mode="single"
)

    
def add_dataset(file_name, dataset_path):
    dataset_name = file_name.lower().replace(' ', '_')
    st.session_state.datasets[dataset_name] = None
    st.session_state.dataset_paths[dataset_name] = dataset_path


def find_files(directory, extensions=['.csv', '.json', '.xlsx']):
    """Find all files in the directory with the specified extensions."""
    files = []
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            if any(filename.endswith(ext) for ext in extensions):
                pipeline_utils.add_dataset(filename, os.path.join(root, filename))
                files.append(os.path.join(root, filename))
    return files

    
def import_kaggle_data(dataset):
    """Import a dataset from Kaggle and return the DataFrame."""
    print(f"Importing dataset: {dataset}")
    path = kagglehub.dataset_download(dataset)
    find_files(path)
    print(f"Dataset downloaded to: {path}")
    return path


def import_github_raw(url):
    """Import a raw file from GitHub and return the DataFrame."""
    file_name = url.split("/")[-1]
    req = requests.get(url)
    url_content = req.content
    os.makedirs('data', exist_ok=True)
    csv_file_name = os.path.join('data', file_name)
    with open(csv_file_name, 'wb') as csv_file:
        csv_file.write(url_content)
    add_dataset(file_name, csv_file_name)
    return csv_file_name


HOST_PATTERNS = {
    "kaggle": {'matcher':re.compile(r"kaggle\.com"), 'function': import_kaggle_data},
    "github_raw": {'matcher':re.compile(r"raw\.githubusercontent\.com"), 'function': import_github_raw}
}

EXTENSIONS = {
    ".csv": "direct_file",
    ".json": "direct_file",
    ".xlsx": "direct_file",
    ".nc": "direct_file",
    # add more as needed
}


def detect_source_type(url):
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if 'kaggle' in host.strip() and 'datasets' in path.strip():
        dataset_name = path.strip().split('datasets/')[1]
        import_kaggle_data(dataset_name)
    elif re.compile(r"raw\.githubusercontent\.com").search(host.strip()):
        import_github_raw(url)
    else:
        print('not a kaggle link:', url)

        
def random_table_name(prefix='table_', length=8):
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return prefix + suffix

    
def import_data(links):
    if not links == "": 
        urls = re.findall(r'https?://[^\s]+|http?://[^\s]+', links)
        for url in urls:
            if url == "":
                continue
            detect_source_type(url)
        for dataset_name, dataset_path in st.session_state.dataset_paths.items():
            if dataset_path is None:
                print(f"Dataset {dataset_name} could not be imported.")
            else:
                print(f"Importing dataset: {dataset_name} from {dataset_path}")
                view_name = "_".join(dataset_name.split('.')[0].lower().split(' ')).replace("-", "_").replace(".", "_")
                view_name = "view_" + view_name
                st.session_state.views[dataset_name] = {"view_name": view_name }
                st.session_state.datasets[dataset_name] = st.session_state.spark.read.csv(dataset_path, header=True, inferSchema=True, nullValue='NA')
                st.session_state.temp_datasets[dataset_name] = st.session_state.datasets[dataset_name]
                st.session_state.temp_datasets[dataset_name].createOrReplaceTempView(view_name)
        st.session_state.user_input = links
        # print(st.session_state.user_input)

    
if not selection or "Link" in selection:
    
    links = st.text_area("Link to data source", st.session_state.user_input, placeholder="https://example.com/data.csv")
    st.button("Import", on_click=lambda: import_data(links))
    if st.session_state.datasets.items():
        st.write("Valid datasets imported from links:")
        for dataset_name, dataset in st.session_state.datasets.items():
            if dataset:
                st.write(dataset_name)
                print(f"Dataset {dataset_name} imported successfully.")
                # if len(st.session_state.datasets) < 9:
                #     dataset.show(1)

else:
    uploaded_files = st.file_uploader(
        "Choose a CSV file", accept_multiple_files=True
    )
    for uploaded_file in uploaded_files:
        bytes_data = uploaded_file.read()
        # st.write(uploaded_file.name)
        df = pd.read_csv(StringIO(bytes_data.decode('utf-8')))
        
