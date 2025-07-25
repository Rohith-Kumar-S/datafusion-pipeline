import streamlit as st
import copy
from pipeline_engine import Pipeline
from pipeline_utils import PipelineUtils
import time
import threading

pipeline_utils = PipelineUtils(pipeline_db=st.session_state.pipeline_db)
pipeline_utils.load_pipelines(read_only=True)
if not st.session_state.pipelines:
    st.session_state.pipelines = pipeline_utils.get_pipelines()

active_streams = pipeline_utils.load_active_streams()
col1, col2 = st.columns([8, 2], vertical_alignment="bottom")

if "pipes" not in st.session_state:
    st.session_state.pipes = []

with col1:
    selected_pipeline = st.selectbox(
        "Select Pipeline",
        options=list(st.session_state.pipelines.keys()),
        index=0,
        key="selected_pipeline",
    )

with col2:
    my_bar = None
    st.button(
        "Execute Pipeline",
        disabled=selected_pipeline == "" or selected_pipeline == None,
        on_click=lambda: execute(selected_pipeline),
        help="Execute the selected pipeline.",
        key="execute_pipeline",
    )

def execute(selected_pipeline):
    st.session_state.pipes.append(selected_pipeline)
    
def execute_pipeline(selected_pipeline, j, progress_data):
    pipeline_data = copy.deepcopy(pipeline_utils.get_pipelines()[selected_pipeline])
    input_selected = pipeline_utils.get_pipelines()[selected_pipeline][0].get(
        "layer_selection", None
    )
    for i, layer in enumerate(pipeline_data):
        if layer["layer_type"] == "Source":
            pipeline_utils.load_input_sources(read_only=True)
            layer["layer_selection"] = pipeline_utils.get_inputs()[layer["layer_selection"]]
        elif layer["layer_type"] == "Processor":
            pipeline_utils.load_rules(input_selected, read_only=True)
            layer["layer_selection"] = pipeline_utils.get_rules()[layer["layer_selection"]]
        elif layer["layer_type"] == "Fusion":
            pipeline_utils.load_fusions(input_selected, read_only=True)
            layer["layer_selection"] = pipeline_utils.get_fusions()[layer["layer_selection"]]
        elif layer["layer_type"] == "Target":
            pipeline_utils.load_targets(input_selected, read_only=True)
            layer["layer_selection"] = pipeline_utils.get_targets()[layer["layer_selection"]]
    # engine = Pipeline(st.session_state.spark)
    print(pipeline_data, flush=True)
    progress_data[j] = 10
    # exported_paths = engine.execute(pipeline_data, i, progress_data)

    # with st.container():
    #     for path in exported_paths:
    #         st.download_button(
    #             label=f"Download {path}", data=open(path, "rb"), file_name=path
    #         )
    # my_bar.progress(100, text="Pipeline executed successfully.")
    # st.toast("Pipeline executed successfully!", icon="✅")
    # print("exported_paths: ", exported_paths)
    
def print_cube(n, i, progress_data):
    print(f"Operation {i+1} started", flush=True)
    for j in range(1, n + 1):
        time.sleep(0.08)
    progress_data[i] = 100
    print(f"Operation {i+1} completed", flush=True)

with st.container(height=400):
    print("Container called", flush=True)
    for i, pipe in enumerate(st.session_state.pipes):
        if pipe:
            if f"thread{i}" not in st.session_state:
                st.session_state[f"thread{i}"] = False
            if "progress_data" not in st.session_state:
                st.session_state.progress_data = {}
            # Safely read progress
            col1, col2 = st.columns([8,2], vertical_alignment="bottom")
            # print(st.session_state.pipelines[pipe], flush=True)
            with col1:
                my_bar = st.progress(st.session_state.progress_data.get(i, 0), text=f"Operation {i+1} in progress")
            with col2:
                if st.button(f"Get Status", key=f"get_status_{i}"):
                    pass

            if not st.session_state[f"thread{i}"]:
                t = threading.Thread(target=execute_pipeline, args=(pipe, i, st.session_state.progress_data), daemon=True)
                t.start()
                st.session_state.child_threads.append(t)
                st.session_state[f"thread{i}"] = True

            st.write("---")
    

