import streamlit as st
import time
from bson.objectid import ObjectId
from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(pipeline_db=st.session_state.pipeline_db)
pipeline_utils.load_input_sources(read_only=True)
pipe_key, db_pipes = pipeline_utils.load_pipelines()
if not st.session_state.pipelines:
    st.session_state.pipelines = pipeline_utils.get_pipelines()


def add_layer(pipeline):
    st.session_state.temp_pipelines[pipeline].append(
        {
            "layer_type": "",
            "layer_selection": "",
        }
    )


def update_layer_type(i, pipeline, option):
    if option != "-- Select an option --":
        st.session_state.temp_pipelines[pipeline][i]["layer_type"] = option

def update_layer_selection(i, pipeline, layer_selection):
    if layer_selection != "-- Select an option --":
        st.session_state.temp_pipelines[pipeline][i][
            "layer_selection"
        ] = layer_selection


# s1, s2 = st.columns([1, 8])

# with s2:
pipelines = None
pipeline_selection = st.segmented_control(
    "**Pipeline Builder**",
    ["Create pipeline", "Load pipeline"],
    default="Create pipeline",
    key="pipeline_control",
)
pipeline = f"pipeline_{st.session_state.total_fusions+1}"

col1, col2, col5, col4, col3 = st.columns([4, 2, 4, 12, 2], vertical_alignment="bottom")
with col2:
    if st.button("Save pipeline", disabled=pipeline_selection == "Load pipeline"):
        if st.session_state[f"{pipeline}"] != "":
            temp = st.session_state.temp_pipelines[pipeline]
            new_pipeline_name = st.session_state[f"{pipeline}"]
            if new_pipeline_name not in st.session_state.pipelines:
                del st.session_state.temp_pipelines[pipeline]
                st.session_state.pipelines[new_pipeline_name] = temp
                db_pipes.update_one(
                    {"_id": ObjectId(pipe_key)},
                    {"$set": {"value": st.session_state.pipelines}},
                )
                st.session_state.pipelines = {}
                for pipe in db_pipes.find():
                    print("Pipeline:", pipe, flush=True)
                    st.session_state.db_key = pipe["_id"]
                    st.session_state.pipelines = pipe["value"]
                st.toast("pipeline created successfully!", icon="✅")
                time.sleep(0.2)
        else:
            st.toast("pipeline name can't be empty.", icon="❌")
            time.sleep(0.2)

with col1:
    if pipeline_selection != "Create pipeline":
        pipeline = st.selectbox(
            "Select pipeline",
            options=list(st.session_state.pipelines.keys()),
            index=0,
            key="selected_pipeline",
            disabled=pipeline_selection == "Create pipeline",
            label_visibility="collapsed",
            help="Select a pipeline to load.",
            placeholder="Select pipeline",
        )
        pipelines = st.session_state.pipelines
    else:
        if pipeline not in st.session_state.temp_pipelines:
            st.session_state.temp_pipelines[pipeline] = [
                {"layer_type": "Source", "layer_selection": ""}
            ]
        st.text_input(
            "pipeline Name",
            key=f"{pipeline}",
            disabled=pipeline_selection != "Create pipeline",
            help="Enter a name for the pipeline.",
            label_visibility="collapsed",
            placeholder="pipeline name",
        )
        pipelines = st.session_state.temp_pipelines

with col5:
    if st.button("Delete pipeline", disabled=pipeline_selection != "Load pipeline"):
        del st.session_state.pipelines[pipeline]
        st.toast("pipeline deleted successfully!", icon="✅")
        time.sleep(0.2)

with col3:
    if st.button("Add Layer", disabled=pipeline_selection == "Load pipeline"):
        add_layer(pipeline)
        st.toast("Layer added successfully!", icon="✅")
        time.sleep(0.3)

with st.container(height=462):
    selected_input = pipelines[pipeline][0].get("layer_selection", None)
    for i, process in enumerate(pipelines[pipeline] if pipeline in pipelines else []):
        col1, col2, col3 = st.columns([4, 2, 1], vertical_alignment="bottom")
        options = ["-- Select an option --"]
        if i > 0:
            options.extend(["Processor", "Fusion", "Target"])
        else:
            options.extend(["Source"])

        col1.selectbox(
            "Layer type",
            options=options,
            index=(
                options.index(pipelines[pipeline][i]["layer_type"])
                if pipelines[pipeline][i]["layer_type"] != ""
                else 0
            ),
            key=f"layer_type_{i}",
            on_change=lambda i=i: (
                update_layer_type(i, pipeline, st.session_state[f"layer_type_{i}"])
                if pipeline_selection != "Load pipeline"
                else None
            ),
        )

        options = ["-- Select an option --"]
        if st.session_state[f"layer_type_{i}"] == "Source":
            options.extend(list(st.session_state.inputs.keys()))
        elif st.session_state[f"layer_type_{i}"] == "Processor":
            print("Input selected:########## ", selected_input, flush=True)
            pipeline_utils.load_rules(input_source=selected_input, read_only=True)
            options.extend(list(st.session_state.rules.keys()))
        elif st.session_state[f"layer_type_{i}"] == "Fusion":
            pipeline_utils.load_fusions(input_source=selected_input, read_only=True)
            options.extend(list(st.session_state.fusions.keys()))
        elif st.session_state[f"layer_type_{i}"] == "Target":
            pipeline_utils.load_targets(input_source=selected_input, read_only=True)
            options.extend(list(st.session_state.targets.keys()))
        col2.selectbox(
            "Select layer",
            options=options,
            index=(
                options.index(pipelines[pipeline][i]["layer_selection"])
                if pipelines[pipeline][i]["layer_selection"] != ""
                else 0
            ),
            key=f"layer_selection_{i}",
            on_change=lambda i=i: (
                update_layer_selection(
                    i, pipeline, st.session_state[f"layer_selection_{i}"]
                )
                if pipeline_selection != "Load pipeline"
                else None
            ),
        )

        with col3:
            st.button(
                "🗑️",  # delete icon
                help="Delete this process",
                key=f"delete_process_{i}",
                use_container_width=True,
                disabled=pipeline_selection == "Load pipeline",
                on_click=lambda p=process: (
                    pipelines[pipeline].remove(p)
                    if i != 0
                    else st.toast("Cannot delete the source layer.", icon="❌")
                ),
            )

        st.write("---")


# with s1:
#     st.write(st.session_state.temp_pipelines)
