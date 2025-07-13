import streamlit as st
import pandas as pd
from io import StringIO

from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(
    st.session_state.spark,
    st.session_state.datasets,
    st.session_state.dataset_paths,
    st.session_state.views,
    st.session_state.temp_datasets,
)


def save_input():
    """Save the input source for later use."""
    if "temp_input" not in st.session_state.temp_inputs:
        st.session_state.temp_inputs["temp_input"] = {}
    # st.session_state.inputs[st.session_state.input_source_name]['source_type'] = st.session_state.data_source_selection
    if "Link" not in st.session_state.temp_inputs["temp_input"]:
        st.session_state.temp_inputs["temp_input"]["Link"] = {}

    if "source_links" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Link"][
            "source_links"
        ] = st.session_state.source_links

    if "Stream" not in st.session_state.temp_inputs["temp_input"]:
        st.session_state.temp_inputs["temp_input"]["Stream"] = {}
    if "stream_from" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "stream_from"
        ] = st.session_state.stream_from
    if "bootstrap_server" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "server_spec"
        ] = st.session_state.bootstrap_server
    if "stream_topic" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "topic_to_subscribe"
        ] = st.session_state.stream_topic
    if "stream_data_schema" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "schema_skeleton"
        ] = st.session_state.stream_data_schema
    if "stream_data_name" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "dataframe_name"
        ] = st.session_state.stream_data_name

    if "Upload" not in st.session_state.temp_inputs["temp_input"]:
        st.session_state.temp_inputs["temp_input"]["Upload"] = {}


# sidebar, main = st.columns([2, 9], vertical_alignment="top")

# with main:
imports_col, views_col = st.columns(2)
with imports_col:
    input_selection = st.segmented_control(
        "**Input Source builder**",
        ["Create input source", "Load input source"],
        selection_mode="single",
        default="Create input source",
        key="input_selection",
    )
    input = f"input_{st.session_state.total_inputs+1}"
    input_state = st.session_state.temp_inputs

    if input_selection == "Create input source":
        input_name = st.text_input(
            "Input Source Name",
            key="input_source_name",
            disabled=input_selection != "Create input source",
            help="Enter a name for the input source.",
            placeholder="Input source name",
        )
        if "temp_input" not in input_state:
            input_state["temp_input"] = {}
        if "Link" not in input_state["temp_input"]:
            input_state["temp_input"]["Link"] = {}
        if "source_links" not in input_state["temp_input"]["Link"]:
            input_state["temp_input"]["Link"]["source_links"] = ""
    else:
        input_name = st.selectbox(
            "Select Input Source",
            options=list(st.session_state.inputs.keys()),
            index=0,
            key="selected_input_source",
            disabled=input_selection == "Create input source",
            label_visibility="collapsed",
            help="Select an input source to load.",
            placeholder="Select input source",
        )
        input_state = st.session_state.inputs
    # col2.button('Save Input', on_click=lambda: save_input(), help="Save the input source for later use.", key=f"save_input_{input}", disabled=input_selection != 'Create input source')
    options = ["Link", "Stream", "Upload"]
    selection = st.segmented_control(
        "Data Source",
        options,
        key="data_source_selection",
        selection_mode="single",
        default="Link",
    )

    if not selection or "Link" in selection:
        if input_selection != "Create input source":
            links = st.text_area(
                "Link to data source",
                input_state[input_name]["Link"]["source_links"],
                key="source_links",
                placeholder="https://example.com/data.csv",
            )
        else:
            links = st.text_area(
                "Link to data source",
                input_state["temp_input"]["Link"]["source_links"],
                key="source_links",
                placeholder="https://example.com/data.csv",
            )
        col1, col2 = st.columns(2)

        if st.button(
            "Import",
            help="Import data from the provided links.",
            disabled=input_selection != "Create input source",
        ):
            is_imported, dataframe_names = pipeline_utils.import_data(
                links, "Link", from_ui=True
            )
            if is_imported:
                for dataframe_name in dataframe_names:
                    st.session_state.imported_data.append([dataframe_name, selection])
                save_input()

    elif "Stream" in selection:
        col1, col2, col3, col4 = st.columns(4, vertical_alignment="bottom")
        data_formats = ["JSON"]
        stream_from_options = ["kafka", "socket"]
        with col1:
            st.selectbox(
                "Stream from",
                options=stream_from_options,
                key="stream_from",
                index=0,
                disabled=True,
                label_visibility="collapsed",
                help="Select the stream source.",
            )
        with col2:
            stream_ = st.text_input(
                "Bootstrap server",
                key="bootstrap_server",
                value="rk-kafka:29093",
                placeholder="rk-kafka:29093",
            )
        with col3:
            st.text_input(
                "Subscribe topic",
                key="stream_topic",
                value="iot-device-data",
                placeholder="iot-device-data",
            )
        with col4:
            st.selectbox(
                "Stream format",
                options=data_formats,
                key="stream_format",
                index=0,
                disabled=True,
                label_visibility="collapsed",
                help="Select the stream format.",
            )
        if st.session_state.stream_format == "JSON":
            st.text_area(
                "Sample JSON to Construct Schema",
                key="stream_data_schema",
                value='{"device_id": "string", "temperature": "float", "humidity": "float"}',
                placeholder='{"device_id": "string", "temperature": "float", "humidity": "float"}',
            )
        st.text_input(
            "Imported data name",
            key="stream_data_name",
            value="iot_device_data",
            placeholder="iot_device_data.csv",
        )
        if st.button("Import", disabled=input_selection != "Create input source"):

            try:
                print("Importing stream data...", flush=True)
                value = {}
                value["stream_from"] = st.session_state.stream_from
                value["server_spec"] = st.session_state.bootstrap_server
                value["topic_to_subscribe"] = st.session_state.stream_topic
                value["dataframe_name"] = st.session_state.stream_data_name
                value["schema_skeleton"] = st.session_state.stream_data_schema
                if pipeline_utils.import_data(value, "Stream", from_ui=True):
                    st.session_state.stream_data = value
                    st.session_state.imported_data.append(
                        [st.session_state.stream_data_name, selection]
                    )
                    save_input()
                    # st.toast(f"Stream data '{st.session_state.stream_data_name}' imported successfully!", icon="✅")

            except Exception as e:
                print(e, flush=True)

    else:
        uploaded_files = st.file_uploader(
            "Choose a CSV file", accept_multiple_files=True
        )
        for uploaded_file in uploaded_files:
            bytes_data = uploaded_file.read()
            df = pd.read_csv(StringIO(bytes_data.decode("utf-8")))
with views_col:
    st.write("Imported Data:")
    st.dataframe(
        pd.DataFrame(
            st.session_state.imported_data,
            columns=["Dataset Name", "Source Type"],
        )
        .drop_duplicates()
        .reset_index(drop=True),
        use_container_width=True,
    )
    if st.button(
        "Save Input",
        disabled=input_selection != "Create input source"
        or input_name == ""
        or len(st.session_state.imported_data) == 0,
    ):
        if input_name != "":
            if input_name not in st.session_state.inputs:
                st.session_state.imported_data = []
                st.session_state.inputs[input_name] = input_state["temp_input"]
                if st.session_state.inputs[input_name]["Stream"]:
                    st.session_state.part_of_stream.append(
                        st.session_state.inputs[input_name]["Stream"]["dataframe_name"]
                    )
                del st.session_state.temp_inputs["temp_input"]
                st.toast(
                    f"Input source '{input_name}' saved successfully!",
                    icon="✅",
                )
            else:
                st.toast(
                    f"Input source '{input_name}' already exists.",
                    icon="❌",
                )
        else:
            st.toast("Input source name cannot be empty.", icon="❌")

# with sidebar:
#     st.write(st.session_state.inputs)
