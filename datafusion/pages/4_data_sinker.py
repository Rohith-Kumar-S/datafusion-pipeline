import streamlit as st
from bson.objectid import ObjectId
from pipeline_utils import PipelineUtils


def add_to_conversion(target_name, dataset, format_key):
    """
    Add the dataset to the conversion list with the specified format.
    """
    if format_key == "--Select an option--":
        return

    if "conversions" not in st.session_state.temp_targets[target_name]:
        st.session_state.temp_targets[target_name]["conversions"] = {}
    st.session_state.temp_targets[target_name]["conversions"][dataset] = (
        st.session_state[format_key]
    )
    st.toast(
        f"Dataset {dataset} added for conversion to {st.session_state[format_key]} format.",
        icon="✅",
    )


def save_target():
    """Save the target configuration."""
    if st.session_state["target_name"] != "":
        temp = st.session_state.temp_targets[target_name]
        new_target_name = st.session_state["target_name"]
        target_type = st.session_state["target_type"]
        target_datasets = st.session_state["target_datasets"]
        part_of_stream = []
        for dataset in target_datasets:
            if dataset in st.session_state.part_of_stream:
                part_of_stream.append(dataset)
        st.session_state.targets[new_target_name] = temp
        if target_type == "jdbc":
            try:
                if (
                    "jdbc_url" in st.session_state
                    and st.session_state["jdbc_url"] != ""
                ):
                    st.session_state.targets[new_target_name]["jdbc_url"] = (
                        st.session_state["jdbc_url"]
                    )
                else:
                    raise Exception("JDBC URL cannot be empty.")

                if (
                    "jdbc_table_name" in st.session_state
                    and st.session_state["jdbc_table_name"] != ""
                ):
                    st.session_state.targets[new_target_name]["jdbc_table_name"] = (
                        st.session_state["jdbc_table_name"]
                    )
                else:
                    raise Exception("JDBC Table Name cannot be empty.")
                if (
                    "jdbc_username" in st.session_state
                    and st.session_state["jdbc_username"] != ""
                ):
                    st.session_state.targets[new_target_name]["jdbc_username"] = (
                        st.session_state["jdbc_username"]
                    )
                else:
                    raise Exception("JDBC Username cannot be empty.")

                if (
                    "jdbc_password" in st.session_state
                    and st.session_state["jdbc_password"] != ""
                ):
                    st.session_state.targets[new_target_name]["jdbc_password"] = (
                        st.session_state["jdbc_password"]
                    )
                else:
                    raise Exception("JDBC Password cannot be empty.")
            except Exception as e:
                st.toast(e, icon="❌")
                del st.session_state.targets[new_target_name]
                return
        del st.session_state.temp_targets[target_name]
        st.session_state.targets[new_target_name]["type"] = target_type

        st.session_state.targets[new_target_name]["datasets"] = target_datasets
        st.session_state.targets[new_target_name]["part_of_stream"] = part_of_stream
        db_targets.update_one(
            {"_id": ObjectId(targets_key)},
            {
                "$set": {
                    "value": st.session_state.targets,
                    "input_source": st.session_state.input_loaded,
            }
        },
        )
        st.session_state.fusions = {}
        for db_target in db_targets.find({"input_source": st.session_state.input_loaded}):
            print("Target Source:", db_target, flush=True)
            st.session_state.targets = db_target["value"]


        st.toast(f"Target {new_target_name} created successfully!", icon="✅")
    else:
        st.toast("Target name and path cannot be empty.", icon="❌")

if st.session_state.input_loaded == "":
    st.session_state.temp_datasets = {}
    st.session_state.datasets = {}
    st.session_state.views = {}
    st.session_state.imported_data = []
    st.warning("No data found to sink", icon="⚠️")
else:
    pipeline_utils = PipelineUtils( pipeline_db=st.session_state.pipeline_db)
    targets_key, db_targets = pipeline_utils.load_targets(st.session_state.input_loaded)
    if not st.session_state.targets:
        st.session_state.targets = pipeline_utils.get_targets()
    st.segmented_control(
        "**Target Builder**",
        ["Create target", "Load target"],
        default="Create target",
        key="target_control",
    )

    # s1, s2 = st.columns([2, 8], vertical_alignment="bottom")

    # with s2:
    if st.session_state.target_control == "Create target":
        target_name = f"target_{st.session_state.total_targets + 1}"
        if target_name not in st.session_state.temp_targets:
            st.session_state.temp_targets[target_name] = {}
        conversion_options = ["--Select an option--", "parquet", "csv"]
        col1, col2 = st.columns([5, 8])
        with col1:
            st.selectbox(
                "Target type",
                options=["download", "console", "jdbc"],
                key="target_type",
            )
        if st.session_state.target_type == "jdbc":
            col1, col2, col3 = st.columns([4, 4, 6])
            with col1:
                st.text_input(
                    "JDBC URL",
                    key="jdbc_url",
                    placeholder="e.g. jdbc:postgresql://localhost:5432/mydb",
                )
            with col2:
                st.text_input(
                    "Database Table Name",
                    key="jdbc_table_name",
                    placeholder="e.g. my_table",
                )
            col1, col2, col3 = st.columns([4, 4, 6])
            with col1:
                st.text_input(
                    "Username",
                    key="jdbc_username",
                    placeholder="e.g. my_username",
                )
            with col2:
                st.text_input(
                    "Password",
                    key="jdbc_password",
                    placeholder="e.g. my_password",
                    type="password",
                )
        col1, col2 = st.columns([5, 8])
        with col1:
            st.text_input("Target name", key="target_name")
        col1, col2, col3 = st.columns(3)
        with col1:
            print("Available datasets:", list(st.session_state.datasets.keys()), flush=True)
            print("f datasets:", st.session_state.fusion_dataset_names, flush=True)
            target_datasets = list(st.session_state.datasets.keys())
            target_datasets.extend(st.session_state.fusion_dataset_names)
            st.multiselect(
                "Datasets to be sent to the target",
                options=target_datasets,
                key="target_datasets",
            )
        if st.session_state.target_type == "download":
            with col2:
                if st.session_state.target_datasets:
                    target_dataset = st.selectbox(
                        "Convert dataset",
                        options=list(st.session_state.target_datasets),
                        key="convert_dataset",
                        help="Select a dataset to convert before saving.",
                    )
            with col3:
                if st.session_state.target_datasets:
                    st.selectbox(
                        "Select target format",
                        options=conversion_options,
                        key=f"{target_dataset}_format",
                        on_change=lambda: add_to_conversion(
                            target_name, target_dataset, f"{target_dataset}_format"
                        ),
                        help="Select the format to convert the dataset before saving.",
                    )

        if st.button("Save target"):
            save_target()

    else:
        st.selectbox(
            "Select target",
            options=list(st.session_state.targets.keys()),
            key="selected_target",
            help="Select a target to load.",
        )
        if st.session_state.selected_target:
            target = st.session_state.targets[st.session_state.selected_target]
            st.write(f"**Target Name:** {st.session_state.selected_target}")
            st.write(f"**Target Type:** {target['type']}")
            if "path" in target:
                st.write(f"**Target Path:** {target['path']}")
            if "datasets" in target:
                st.write(f"**Datasets:** {', '.join(target['datasets'])}")
            if "conversions" in target:
                st.write(f"**Conversions:** {', '.join(target['conversions'])}")
                st.write(
                    f'Converts to: {", ".join(list(target["conversions"].values()))}'
                )

# with s2:
#     st.write(st.session_state.targets)
