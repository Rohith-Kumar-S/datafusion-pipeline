from time import time
import streamlit as st
from pyspark.sql import types
from pyspark.sql.functions import to_date, col
from bson.objectid import ObjectId
import time

from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(temp_datasets_state=st.session_state.temp_datasets,
    pipeline_db=st.session_state.pipeline_db)


def add_process(fusion):
    st.session_state.temp_fusions[fusion].append(
        {"datasets_to_fuse": [], "fuse_by": ""}
    )

def update_datasets_to_fuse(i, datasets_to_fuse):
    st.session_state.temp_fusions[fusion][i]["datasets_to_fuse"] = datasets_to_fuse


def update_fuse_how(i, option):
    st.session_state.temp_fusions[fusion][i]["fuse_how"] = option


def get_new_dataset_name(datasets_to_fuse):
    return "_".join([dataset.split(".")[0] for dataset in datasets_to_fuse])

def update_fuse_by(i, option):
    if option != "--Select an option--":
        st.session_state.temp_fusions[fusion][i]["fuse_by"] = option


def update_fusion_name(i, name):
    if name != "":
        st.session_state.temp_fusions[fusion][i]["fused_dataset_name"] = name


# a, b = st.columns([2, 9], gap="small")

# with b:
if st.session_state.input_loaded == "":
    st.session_state.temp_datasets = {}
    st.session_state.datasets = {}
    st.session_state.views = {}
    st.session_state.imported_data = []
    st.warning("No data found to fuse", icon="⚠️")
else:
    fusions_key, db_fusions = pipeline_utils.load_fusions(st.session_state.input_loaded)
    fusions = None
    fusion_selection = st.segmented_control(
        "**Fusion builder**",
        ["Create fusion", "Load fusion"],
        selection_mode="single",
        default="Create fusion",
        key="fusion_selection",
    )
    fusion = f"fusion_{st.session_state.total_fusions+1}"

    col1, col2, col4, col3 = st.columns([4, 2, 12, 2], vertical_alignment="bottom")
    with col2:
        if st.button("Save fusion", disabled=fusion_selection == "Load fusion"):
            if st.session_state[f"{fusion}"] != "":
                temp = st.session_state.temp_fusions[fusion]
                new_fusion_name = st.session_state[f"{fusion}"]
                if new_fusion_name not in st.session_state.fusions:
                    del st.session_state.temp_fusions[fusion]
                    st.session_state.fusions[new_fusion_name] = temp
                    for fusion_key in st.session_state.fusions[new_fusion_name]:
                        st.session_state.fusion_dataset_names.append(
                            fusion_key["fused_dataset_name"]
                        )
                        part_of_stream = False
                        for dataset in fusion_key["datasets_to_fuse"]:
                            if dataset in st.session_state.part_of_stream:
                                part_of_stream = True
                        if part_of_stream:
                            st.session_state.part_of_stream.append(
                                fusion_key["fused_dataset_name"]
                            )

                    st.session_state.fusion_dataset_names = list(
                        set(st.session_state.fusion_dataset_names)
                    )
                    db_fusions.update_one(
                            {"_id": ObjectId(fusions_key)},
                            {
                                "$set": {
                                    "value": st.session_state.fusions,
                                    "input_source": st.session_state.input_loaded,
                                }
                            },
                        )
                    st.session_state.fusions = {}
                    for db_fusion in db_fusions.find({"input_source": st.session_state.input_loaded}):
                        print("Fusion Source:", db_fusion, flush=True)
                        st.session_state.fusions = db_fusion["value"]
                    st.toast("fusion created successfully!", icon="✅")
                    time.sleep(0.2)
                else:
                    st.toast(
                        f"fusion '{new_fusion_name}' already exists.", icon="❌"
                    )
                    time.sleep(0.2)
            else:
                st.toast("fusion name can't be empty.", icon="❌")
                time.sleep(0.2)

    with col1:
        if fusion_selection != "Create fusion":
            fusion = st.selectbox(
                "Select fusion",
                options=list(st.session_state.fusions.keys()),
                index=0,
                key="selected_fusion",
                disabled=fusion_selection == "Create fusion",
                label_visibility="collapsed",
                help="Select a fusion to load.",
            )
            fusions = st.session_state.fusions
        else:
            if fusion not in st.session_state.temp_fusions:
                st.session_state.temp_fusions[fusion] = []
            st.text_input(
                "fusion Name",
                key=f"{fusion}",
                disabled=fusion_selection != "Create fusion",
                help="Enter a name for the fusion.",
                label_visibility="collapsed",
                placeholder="fusion name",
            )
            fusions = st.session_state.temp_fusions

    with col3:
        if st.button("Add Process", disabled=fusion_selection == "Load fusion"):
            pass
            add_process(fusion)
            st.toast("Process added successfully!", icon="✅")
            time.sleep(0.3)

    with st.container(height=462):

        for i, process in enumerate(fusions[fusion]):
            col1, col2, col3 = st.columns([4, 3, 1], vertical_alignment="bottom")
            datasets_to_fuse = col1.multiselect(
                "Datasets to fuse",
                options=list(st.session_state.temp_datasets.keys()),
                default=fusions[fusion][i]["datasets_to_fuse"],
                key=f"datasets_to_fuse_{i}",
                help="Select datasets to fuse.",
                on_change=lambda: update_datasets_to_fuse(
                    i, st.session_state[f"datasets_to_fuse_{i}"]
                ),
                disabled=fusion_selection == "Load fusion",
            )

            if "fused_dataset_name" not in fusions[fusion][i]:
                fusions[fusion][i]["fused_dataset_name"] = ""

            with col2:
                st.text_input(
                    "Fused dataset name",
                    value=fusions[fusion][i]["fused_dataset_name"],
                    key=f"fused_dataset_name_{i}",
                    disabled=fusion_selection == "Load fusion",
                    on_change=lambda: update_fusion_name(
                        i, st.session_state[f"fused_dataset_name_{i}"]
                    ),
                    help="Enter a name for the fused dataset.",
                )
            with col3:
                st.button(
                    "🗑️",  # delete icon
                    help="Delete this process",
                    key=f"delete_process_{i}",
                    use_container_width=True,
                    disabled=fusion_selection == "Load fusion",
                    on_click=lambda p=process: fusions[fusion].remove(p),
                )
            col1, col2 = st.columns([2, 13])
            if len(datasets_to_fuse) > 1:
                options = ['--Select an option--']
                options.extend(pipeline_utils.get_fusable_columns(datasets_to_fuse))
                if fusion_selection != "Load fusion":
                    update_fuse_by(
                        i, options[0] if options else "No common columns"
                    )
                with col2:
                    st.selectbox(
                        "Fusable by",
                        options=options,
                        key=f"fusable_by_{i}",
                        index=options.index(fusions[fusion][i].get("fuse_by", "--Select an option--")),
                        help="Select columns to fuse.",
                        on_change=lambda: update_fuse_by(
                            i, st.session_state[f"fusable_by_{i}"]
                        ),
                        disabled=fusion_selection == "Load fusion",
                    )
                options = ["inner", "left", "right", "outer"]
                if "fuse_how" not in fusions[fusion][i]:
                    fusions[fusion][i]["fuse_how"] = options[0]
                with col1:
                    option = st.selectbox(
                        label="Fuse how",
                        options=options,
                        index=options.index(fusions[fusion][i]["fuse_how"]),
                        key=f"fuse_how_{i}",
                        on_change=lambda: update_fuse_how(
                            i, st.session_state[f"fuse_how_{i}"]
                        ),
                        disabled=fusion_selection == "Load fusion",
                        help="Select how to fuse the datasets.",
                    )
            st.write("---")

# with a:
#     st.write(st.session_state.temp_fusions)
