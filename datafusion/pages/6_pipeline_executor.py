import streamlit as st
import copy
from pipeline_engine import Pipeline
from pipeline_utils import PipelineUtils
import time
import threading
from bson.objectid import ObjectId

pipeline_utils = PipelineUtils(pipeline_db=st.session_state.pipeline_db)
pipeline_utils.load_pipelines(read_only=True)
if not st.session_state.pipelines:
    st.session_state.pipelines = pipeline_utils.get_pipelines()
if st.session_state.active_streams_key is None:
    st.session_state.active_streams_key, st.session_state.db_active_pipe_streams, st.session_state.active_streams = pipeline_utils.load_active_streams()

col1, col2 = st.columns([10, 1.6], vertical_alignment="bottom")

if "pipes" not in st.session_state:
    st.session_state.pipes = list(st.session_state.active_streams.keys())
    st.session_state.part_of_stream = {}

if "allowed_pipes" not in st.session_state:
    st.session_state.allowed_pipes = list(st.session_state.pipelines.keys())
    
def restart_pipeline(selected_pipeline, i):
    st.session_state.progress_data[i] = {"progress": 0, "status": f"Initializing {selected_pipeline} pipeline..."}
    st.session_state[f"thread{i}"] = False

with col1:
    selected_pipeline = st.selectbox(
        "Select Pipeline",
        options=st.session_state.allowed_pipes,
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
    """Execute the selected pipeline."""
    st.session_state.allowed_pipes.remove(selected_pipeline)
    st.session_state.pipes.append(selected_pipeline)
    
def generate_pipeline_data(selected_pipeline):
    """
    Generate the pipeline data for the selected pipeline.
    """
    pipeline_data = copy.deepcopy(pipeline_utils.get_pipelines()[selected_pipeline])
    input_selected = pipeline_utils.get_pipelines()[selected_pipeline][0].get(
        "layer_selection", None
    )
    is_part_of_stream = False
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
            print("targets:", pipeline_utils.get_targets(), flush=True)
            print("layer_selection:", layer["layer_selection"], flush=True)
            layer["layer_selection"] = pipeline_utils.get_targets()[layer["layer_selection"]]
            part_of_stream = layer["layer_selection"].get("part_of_stream", [])
            if part_of_stream:
                is_part_of_stream = True
   
    print(pipeline_data, is_part_of_stream, flush=True)
    return pipeline_data, is_part_of_stream

def execute_pipeline(selected_pipeline, pipeline_data, j, progress_data, exported_paths, query, spark):
    """
    Execute the selected pipeline.
    """
    engine = Pipeline(spark=spark)
    progress_data[j] = {"progress":10, "status": f"Pipeline {selected_pipeline} initiated!"}
    engine.execute(pipeline_data, j, progress_data)
    if engine.get_exported_paths():
        exported_paths[j] = engine.get_exported_paths()
    if engine.get_query():
        print("Starting query execution", flush=True)
        query[j] = engine.get_query().start()
        query[j].awaitTermination()
    progress_data[j] = {"progress":100, "status": f"Pipeline {selected_pipeline} executed successfully."}
    print("exported_paths: ", exported_paths, flush=True)
    
def custom_progress_bar(label):
    """
    Display a custom progress bar with a label.
    """
    st.write(f"**{label}**")
    st.markdown("""
    <style>
    .loader {
    width: 100%;
    height: 20px;
    background: linear-gradient(90deg, #09f, #0cf, #09f);
    background-size: 200% 100%;
    animation: loading 0.7s infinite linear;
    border-radius: 10px;
    margin-bottom: 10px;
    }
    @keyframes loading {
    0% { background-position: 200% 0; }
    100% { background-position: -200% 0; }
    }
    </style>

    <div class="loader"></div>
    """, unsafe_allow_html=True)
    
def standard_progress_bar(label,percent):
    """
    Display a standard progress bar with a label.
    """
    st.write(f"**{label}**")
    st.markdown(f"""
        <div style="width: 100%; background-color: #2b2b2b; border-radius: 10px; margin-bottom: 10px;">
            <div style="
                width: {percent}%;
                background: linear-gradient(90deg, #09f, #0cf);
                height: 20px;
                border-radius: 10px;
                transition: width 0.1s ease-in-out;
            "></div>
        </div>
        """, unsafe_allow_html=True)
    
def stop_stream(i, pipe):
    """
    Stop the stream for a specific pipeline.
    """
    if st.session_state.query.get(i, None) is not None:
        st.session_state.query[i].stop()
        st.session_state.query[i] = None
        st.session_state.progress_data[i] = {"progress": 100, "status": f"Stream {pipe} stopped"}
        st.session_state.part_of_stream[i] = False
        del st.session_state.active_streams[pipe]
        st.session_state.db_active_pipe_streams.update_one(
                    {"_id": ObjectId(st.session_state.active_streams_key)},
                    {"$set": {"value": st.session_state.active_streams}},
        )
        st.toast(f"Stream {pipe} stopped successfully!", icon="✅")

with st.container(height=415):
    for i, pipe in enumerate(st.session_state.pipes):
        if pipe:
            if i not in st.session_state.part_of_stream:
                st.session_state.part_of_stream[i] = False
            if f"thread{i}" not in st.session_state:
                st.session_state[f"thread{i}"] = False
            if "progress_data" not in st.session_state:
                st.session_state.progress_data = {}
            if not st.session_state[f"thread{i}"]:
                pipeline_data, is_part_of_stream = generate_pipeline_data(pipe)
                st.session_state.part_of_stream[i] = is_part_of_stream
                if is_part_of_stream:
                    st.session_state.active_streams[pipe] = True
                    st.session_state.db_active_pipe_streams.update_one(
                    {"_id": ObjectId(st.session_state.active_streams_key)},
                    {"$set": {"value": st.session_state.active_streams}},
                )
                
            # Safely read progress
            col1, col2 = st.columns([11, 1.7], vertical_alignment="bottom")
            with col2:
                if st.session_state.part_of_stream[i]:
                    st.button(f"Stop Stream", key=f"stop_stream_{i}", on_click=lambda i=i, pipe=pipe: stop_stream(i, pipe))
                elif st.session_state.progress_data.get(i, {}).get('progress', 0) < 100:
                    st.button(f"Get Status", key=f"get_status_{i}")
                else:
                    st.button(f"Restart Pipeline", key=f"restart_pipeline_{i}", on_click=lambda pipe=pipe, i=i: restart_pipeline(pipe, i))

            with col1:
                if st.session_state.part_of_stream[i]:
                    label = f"Executing {pipe} stream..."
                    custom_progress_bar(label)
                else:
                    percent = st.session_state.progress_data.get(i, {}).get('progress', 0)
                    label = st.session_state.progress_data.get(i, {}).get('status', "Starting Pipeline...")
                    standard_progress_bar(label, percent)
                    # my_bar = st.progress(st.session_state.progress_data.get(i, {}).get('progress', 0), text=st.session_state.progress_data.get(i, {}).get('status', "Starting Pipeline..."))
            if st.session_state.exported_paths:
                for path in st.session_state.exported_paths.get(i, []):
                    st.download_button(
                        label=f"📥 {path}", key=f"download_{i}_{path}", data=open(path, "rb"), file_name=path
                    )
            
            if not st.session_state[f"thread{i}"]:
                print(f"Starting thread for {pipe}", flush=True)
                t = threading.Thread(target=execute_pipeline, args=(pipe, pipeline_data, i, st.session_state.progress_data, st.session_state.exported_paths, st.session_state.query, st.session_state.spark), daemon=True)
                t.start()
                st.session_state.child_threads.append(t)
                st.session_state[f"thread{i}"] = True
            if st.session_state.progress_data.get(i, {}).get('progress', 0) == 100:
                st.toast(f"Pipeline {pipe} executed successfully!", icon="✅")

            st.write("---")
    

