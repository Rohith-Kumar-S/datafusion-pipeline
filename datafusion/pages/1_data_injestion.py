import streamlit as st
import pandas as pd
from io import StringIO

from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(st.session_state.spark, st.session_state.datasets, st.session_state.dataset_paths, st.session_state.views, st.session_state.temp_datasets)
def save_input(links, from_ui=True):
    """Save the input source for later use."""
    pipeline_utils.import_data(links, 'link', from_ui=from_ui)
    if  st.session_state.input_source_name!='':
        if st.session_state.input_source_name not in st.session_state.inputs:
            st.session_state.inputs[st.session_state.input_source_name] = {}
            st.session_state.inputs[st.session_state.input_source_name]['source_type'] = st.session_state.data_source_selection
            if st.session_state.data_source_selection == 'Link':
                st.session_state.inputs[st.session_state.input_source_name]['source_links'] = st.session_state.source_links
            print(st.session_state.inputs[st.session_state.input_source_name])
            st.toast(f"Input source '{st.session_state.input_source_name}' saved successfully!", icon="✅")
        else:
            st.toast(f"Input source '{st.session_state.input_source_name}' already exists.", icon="❌")
    else:
        st.toast(f"Input source can't be empty.", icon="❌")


sidebar, main = st.columns([2, 9], vertical_alignment="top")

with main:
    input_selection = st.segmented_control("**Input Source builder**", ['Create input source', 'Load input source'], selection_mode="single", default='Create input source', key="input_selection")
    input = f'input_{st.session_state.total_inputs+1}'
    col1, col2, col3 = st.columns([3, 2, 12], vertical_alignment="bottom")
    with col1:
        if input_selection == 'Create input source':
            input_name = st.text_input("Input Source Name", key="input_source_name", disabled=input_selection != 'Create input source', help="Enter a name for the input source.", label_visibility="collapsed", placeholder="Input source name")
        else:
            input_name = st.selectbox("Select Input Source", options=list(st.session_state.inputs.keys()), index=0, key="selected_input_source", disabled=input_selection == 'Create input source', label_visibility="collapsed", help="Select an input source to load.", placeholder="Select input source")
    # col2.button('Save Input', on_click=lambda: save_input(), help="Save the input source for later use.", key=f"save_input_{input}", disabled=input_selection != 'Create input source')
    options = ["Link", "Stream","Upload"]
    selection = st.segmented_control(
        "Data Source", options, key="data_source_selection", selection_mode="single", default='Link'
    )

    if not selection or "Link" in selection:
        
        if 'source_links' not in st.session_state:
            st.session_state.source_links = ''
            
        if input_selection != 'Create input source':
            links = st.text_area("Link to data source", st.session_state.inputs[input_name]['source_links'], key= 'source_links',placeholder="https://example.com/data.csv")
        else:
            links = st.text_area("Link to data source", st.session_state.source_links, key= 'source_links',placeholder="https://example.com/data.csv")
        col1, col2 = st.columns(2)

        st.button("Import & Save", on_click=lambda: save_input(links, from_ui=True), help="Import data from the provided links.", disabled=input_selection != 'Create input source')

        if st.session_state.datasets.items():
            st.write("Valid datasets imported from links:")
            for dataset_name, dataset in st.session_state.datasets.items():
                if dataset:
                    st.write(dataset_name)
                    print(f"Dataset {dataset_name} imported successfully.")
                    # if len(st.session_state.datasets) < 9:
                    #     dataset.show(1)

    elif "Stream" in selection:
        stream_ = st.text_input("Bootstrap server", key="bootstrap_server", value='rk-kafka:29093', placeholder="rk-kafka:29093")
        st.text_input("Subscribe topic", key="stream_topic", value='iot-device-data', placeholder="iot-device-data")
        st.text_input("Imported data name", key="stream_data_name", value='iot_device_data', placeholder="iot_device_data.csv")
        if st.button("Import & Save",disabled=input_selection != 'Create input source'):
            
            try:
                print('Importing stream data...', flush=True)
                value = {}
                value['stream_format'] = 'kafka'
                value['bootstrap_server'] = st.session_state.bootstrap_server
                value['topic'] = st.session_state.stream_topic
                value['dataset_name'] = st.session_state.stream_data_name
                if pipeline_utils.import_data(value, 'stream', from_ui=True):
                    st.session_state.stream_data = value
                    st.toast(f"Stream data '{st.session_state.stream_data_name}' imported successfully!", icon="✅")
                
            except Exception as e:
                print(e, flush=True)
                

    else:
        uploaded_files = st.file_uploader(
            "Choose a CSV file", accept_multiple_files=True
        )
        for uploaded_file in uploaded_files:
            bytes_data = uploaded_file.read()
            df = pd.read_csv(StringIO(bytes_data.decode('utf-8')))
            
with sidebar:
    st.write(st.session_state.temp_datasets)
    st.write(st.session_state.views)
