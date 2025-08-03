import streamlit as st
import pandas as pd
from io import StringIO
from bson.objectid import ObjectId

from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(
    st.session_state.spark,
    st.session_state.datasets,
    st.session_state.dataset_paths,
    st.session_state.views,
    st.session_state.temp_datasets,
    pipeline_db=st.session_state.pipeline_db,
)

input_key, input_sources = pipeline_utils.load_input_sources()
if not st.session_state.inputs:
    st.session_state.inputs = pipeline_utils.get_inputs()

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
    if "server_spec" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "server_spec"
        ] = st.session_state.server_spec
    if "topic_to_subscribe" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "topic_to_subscribe"
        ] = st.session_state.topic_to_subscribe
    if "stream_format" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "stream_format"
        ] = st.session_state.stream_format
    if "schema_skeleton" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "schema_skeleton"
        ] = st.session_state.schema_skeleton
    if "dataframe_name" in st.session_state:
        st.session_state.temp_inputs["temp_input"]["Stream"][
            "dataframe_name"
        ] = st.session_state.dataframe_name


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
    input_name = "temp_input"
    if input_selection == "Create input source":
        st.text_input(
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

    options = ["Link", "Stream"]
    selection = st.segmented_control(
        "Data Source",
        options,
        key="data_source_selection",
        selection_mode="single",
        default="Link",
    )

    if not selection or "Link" in selection:
        if input_name not in input_state:
            st.warning("No input source found", icon="⚠️")
        else:
            links = st.text_area(
                "Link to data source",
                input_state.get(input_name, {})
                .get("Link", {})
                .get("source_links", ""),
                key=(
                    "source_links"
                    if input_selection == "Create input source"
                    else None
                ),
                placeholder="https://example.com/data.csv",
            )

        col1, col2 = st.columns(2)

        if input_selection == "Create input source" and st.button(
            "Import",
            help="Import data from the provided links.",
        ):
            is_imported, dataframe_names = pipeline_utils.import_data(
                links, "Link", from_ui=True
            )
            if is_imported:
                for dataframe_name in dataframe_names:
                    st.session_state.imported_data.append(
                        [dataframe_name, selection]
                    )
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
                index=stream_from_options.index(
                    input_state[input_name]
                    .get("Stream", {})
                    .get("stream_from", "")
                ),
                disabled=True,
                label_visibility="collapsed",
                help="Select the stream source.",
            )
        with col2:
            stream_ = st.text_input(
                "Server Specification",
                key="server_spec",
                value=input_state[input_name]
                .get("Stream", {})
                .get("server_spec", ""),
            )
        with col3:
            st.text_input(
                "Topic to subscribe",
                key="topic_to_subscribe",
                value=input_state[input_name]
                .get("Stream", {})
                .get("topic_to_subscribe", ""),
            )
        with col4:
            st.selectbox(
                "Stream format",
                options=data_formats,
                key="stream_format",
                index=data_formats.index(
                    input_state[input_name]
                    .get("Stream", {})
                    .get("stream_format", "JSON")
                ),
                disabled=True,
                label_visibility="collapsed",
                help="Select the stream format.",
            )
        if st.session_state.stream_format == "JSON":
            st.text_area(
                "Sample JSON to Construct Schema",
                key="schema_skeleton",
                value=input_state[input_name]
                .get("Stream", {})
                .get(
                    "schema_skeleton",
                    '{"device_id": "string", "temperature": "float", "humidity": "float"}',
                )
            )
        st.text_input(
            "Imported data name",
            key="dataframe_name",
            value=input_state[input_name]
            .get("Stream", {})
            .get("dataframe_name", "")
        )
        if input_selection == "Create input source" and st.button("Import"):

            try:
                value = {}
                if input_selection == "Create input source":
                    value["stream_from"] = st.session_state["stream_from"]
                    value["server_spec"] = st.session_state["server_spec"]
                    value["topic_to_subscribe"] = st.session_state[
                        "topic_to_subscribe"
                    ]
                    value["dataframe_name"] = st.session_state["dataframe_name"]
                    value["stream_format"] = st.session_state["stream_format"]
                    value["schema_skeleton"] = st.session_state["schema_skeleton"]
                    print(value, flush=True)
                else:
                    value = input_state[input_name]["Stream"]
                if pipeline_utils.import_data(value, "Stream", from_ui=True):
                    st.session_state.stream_data = value
                    st.session_state.imported_data.append(
                        [st.session_state.dataframe_name, selection]
                    )
                    save_input()
                    # st.toast(f"Stream data '{st.session_state.dataframe_name}' imported successfully!", icon="✅")

            except Exception as e:
                print(e, flush=True)

    if (
        input_selection != "Create input source"
        and st.session_state.get("inputs")
        and st.button("Import")
    ):
        print("Importing all data sources...", flush=True)
        st.session_state.temp_datasets = {}
        st.session_state.datasets = {}
        st.session_state.dataset_paths = {}
        st.session_state.views = {}
        st.session_state.imported_data = []
        st.session_state.temp_rules = {}
        st.session_state.temp_fusions = {}
        st.session_state.temp_targets = {}
        pipeline_utils = PipelineUtils(
            st.session_state.spark,
            st.session_state.datasets,
            st.session_state.dataset_paths,
            st.session_state.views,
            st.session_state.temp_datasets,
        )
        if (
            "Link" in input_state[input_name]
            and "source_links" in input_state[input_name]["Link"]
        ):
            is_imported, dataframe_names = pipeline_utils.import_data(
                (
                    st.session_state["source_links"]
                    if input_selection == "Create input source"
                    else input_state[input_name]["Link"]["source_links"]
                ),
                "Link",
                from_ui=True,
            )
            if is_imported:
                for dataframe_name in dataframe_names:
                    st.session_state.imported_data.append([dataframe_name, "Link"])
        print(
            "Link data imported successfully!", st.session_state.views, flush=True
        )
        value = input_state[input_name]["Stream"]
        try:
            if pipeline_utils.import_data(value, "Stream", from_ui=True):
                st.session_state.dataframe = value
                st.session_state.imported_data.append(
                    [st.session_state.dataframe_name, "Stream"]
                )
        except Exception as e:
            print(e, flush=True)
        save_input()
        if input_selection == "Load input source":
            st.toast(
                f"Input source '{input_name}' loaded successfully!",
                icon="✅",
            )
            st.session_state.input_loaded = input_name
        else:
            st.session_state.input_loaded = ""

    if not st.session_state.imported_data:
        st.warning("No data imported", icon="⚠️")
    else:
        st.write("Imported Sources✅: ")
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
            "Register Input",
            disabled=input_selection != "Create input source"
            or st.session_state.input_source_name == ""
            or len(st.session_state.imported_data) == 0,
        ):
            if st.session_state.input_source_name != "":
                if (
                    st.session_state.input_source_name
                    not in st.session_state.inputs
                ):
                    st.session_state.imported_data = []
                    st.session_state.inputs[st.session_state.input_source_name] = (
                        input_state["temp_input"]
                    )
                    if st.session_state.inputs[st.session_state.input_source_name][
                        "Stream"
                    ]:
                        st.session_state.part_of_stream.append(
                            st.session_state.inputs[
                                st.session_state.input_source_name
                            ]["Stream"]["dataframe_name"]
                        )
                    del st.session_state.temp_inputs["temp_input"]
                    input_sources.update_one(
                        {"_id": ObjectId(input_key)},
                        {"$set": {"value": st.session_state.inputs}},
                    )
                    st.session_state.inputs = {}
                    for input_source in input_sources.find():
                        print("Input Source:", input_source, flush=True)
                        st.session_state.input_key = input_source["_id"]
                        st.session_state.inputs = input_source["value"]
                    st.toast(
                        f"Input source '{st.session_state.input_source_name}' saved successfully!",
                        icon="✅",
                    )
                else:
                    st.toast(
                        f"Input source '{st.session_state.input_source_name}' already exists.",
                        icon="❌",
                    )
            else:
                st.toast("Input source name cannot be empty.", icon="❌")

with views_col:
    pass


# with sidebar:
#     st.write(st.session_state.datasets)
