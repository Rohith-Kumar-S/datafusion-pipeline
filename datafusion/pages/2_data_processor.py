from time import time
import streamlit as st
from pyspark.sql import types
from pyspark.sql.functions import to_date, col, explode, expr
from bson.objectid import ObjectId
import time

from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(
    st.session_state.spark,
    st.session_state.datasets,
    st.session_state.dataset_paths,
    st.session_state.views,
    st.session_state.temp_datasets,
    pipeline_db=st.session_state.pipeline_db,
)


def reset_views(dataset_name):
    st.session_state.temp_datasets[dataset_name] = st.session_state.datasets[
        dataset_name
    ]
    st.session_state.temp_datasets[dataset_name].cache()
    view_name = st.session_state.views[dataset_name]["view_name"]
    st.session_state.cast = {}
    st.session_state.temp_datasets[dataset_name].createOrReplaceTempView(view_name)
    st.toast("Data reseted successfully!", icon="✅")
    time.sleep(0.3)


dtypes_map = {
    types.IntegerType(): "int",
    types.StringType(): "string",
    types.DoubleType(): "double",
    types.FloatType(): "float",
    types.BooleanType(): "bool",
    types.TimestampType(): "timestamp",
    types.DateType(): "date",
    types.LongType(): "long",
    types.ShortType(): "short",
    types.ByteType(): "byte",
}
reverse_dtypes_map = {v: k for k, v in dtypes_map.items()}


def update_delete_type(rule, selected_dataset, index, drop_by=None):
    if drop_by == "--Select a delete type--":
        return
    if (
        "drop_by"
        not in st.session_state.temp_rules[rule][selected_dataset][index]["data_map"]
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "drop_by"
        ] = drop_by
    elif (
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "drop_by"
        ]
        != drop_by
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "drop_by"
        ] = drop_by


def update_column_name(rule, selected_dataset, index, new_column_name=None):
    if (
        "new_column_name"
        not in st.session_state.temp_rules[rule][selected_dataset][index]["data_map"]
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "new_column_name"
        ] = new_column_name
    elif (
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "new_column_name"
        ]
        != new_column_name
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "new_column_name"
        ] = new_column_name


def update_data_source(rule, selected_dataset, index, data_reference=None, process=""):
    if (
        "data_reference"
        not in st.session_state.temp_rules[rule][selected_dataset][index]["data_map"]
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "data_reference"
        ] = data_reference
    elif (
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "data_reference"
        ]
        != data_reference
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "data_reference"
        ] = data_reference


def update_new_dataset_name(rule, selected_dataset, index, new_dataset_name=None):
    st.session_state.temp_rules[rule][selected_dataset][index].get(
        "data_map", {}
    ).update({"save_as": new_dataset_name})
    if (
        new_dataset_name not in st.session_state.fusion_dataset_names
        and new_dataset_name != ""
    ):
        st.session_state.fusion_dataset_names.append(new_dataset_name)
        if selected_dataset in st.session_state.part_of_stream:
            st.session_state.part_of_stream.append(new_dataset_name)


def update_column_to_cast(rule, selected_dataset, index, column_to_cast=None):
    if column_to_cast == "--Select a column--":
        return
    if (
        "column_to_cast"
        not in st.session_state.temp_rules[rule][selected_dataset][index]["data_map"]
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "column_to_cast"
        ] = column_to_cast
    elif (
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "column_to_cast"
        ]
        != column_to_cast
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "column_to_cast"
        ] = column_to_cast


def update_cast_type(rule, selected_dataset, index, cast_type=None, process=""):
    if cast_type == "--Select a cast type--":
        return
    if (
        "cast_type"
        not in st.session_state.temp_rules[rule][selected_dataset][index]["data_map"]
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "cast_type"
        ] = cast_type
    elif (
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "cast_type"
        ]
        != cast_type
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            "cast_type"
        ] = cast_type


# d, b = st.columns([2, 8], vertical_alignment="bottom")

# with b:
if st.session_state.input_loaded == "":
    st.warning("No data found to process", icon="⚠️")
    st.session_state.temp_datasets = {}
    st.session_state.datasets = {}
    st.session_state.views = {}
    st.session_state.imported_data = []
else:
    rules_key, db_rules = pipeline_utils.load_rules(st.session_state.input_loaded)
    if not st.session_state.rules:
        st.session_state.rules = pipeline_utils.get_rules()
    col1, col2 = st.columns(2)
    with col1:
        selected_dataset = st.selectbox(
            "Select Dataset",
            options=list(st.session_state.temp_datasets.keys()),
            key="selected_dataset",
            help="Select a dataset to process.",
        )
        options = ["View", "Describe", "Schema", "Transform"]
        selection = st.segmented_control(
            "Operations",
            options,
            selection_mode="single",
            default="View",
            key="selection",
        )

        def add_process(rule, selected_dataset):
            if selected_dataset not in st.session_state.temp_rules[rule]:
                st.session_state.temp_rules[rule][selected_dataset] = []
            st.session_state.temp_rules[rule][selected_dataset].append(
                {"process_query": "", "process_type": "SQL"}
            )

        match selection:
            case "View":
                st.session_state.views_query = f'SELECT * FROM {st.session_state.views[selected_dataset]["view_name"]}'
                query = st.text_input(
                    "Query",
                    value=st.session_state.views_query,
                    placeholder=st.session_state.views_query,
                )
                if query:
                    df = st.session_state.spark.sql(query)
                    rows = df.count()
                    if rows > 10:
                        try: 
                            st.dataframe(
                                df.limit(rows if rows < 60 else 60).toPandas(),
                                height=350,
                            )
                        except Exception as e:
                            st.warning("⚠️ Data cant be viewed")
                    else:
                        try:
                            st.dataframe(df.limit(rows).toPandas())
                        except Exception as e:
                            for col, dtype in dict(df.dtypes).items():
                                if dtype.startswith("struct") or dtype.startswith(
                                    "array"
                                ):
                                    df = df.withColumn(
                                        col, expr(f"cast({col} as string)")
                                    )
                            st.dataframe(df.limit(rows).toPandas())
                    st.session_state.views_query = query
                else:
                    st.dataframe(
                        st.session_state.temp_datasets[selected_dataset]
                        .limit(10)
                        .toPandas()
                    )

            case "Describe":
                st.write(
                    st.session_state.temp_datasets[selected_dataset]
                    .describe()
                    .toPandas()
                )
            case "Schema":
                st.write(f'**{st.session_state.views[selected_dataset]["view_name"]}**')
                data = []
                for field in st.session_state.temp_datasets[selected_dataset].schema:
                    data.append(
                        (field.name, dtypes_map[field.dataType], field.nullable)
                    )
                df = st.session_state.spark.createDataFrame(
                    data, ["field", "type", "nullable"]
                )
                if df.count() > 10:
                    st.dataframe(df.toPandas(), height=350)
                else:
                    st.dataframe(df.toPandas())

    with col2:

        rules = None
        view_name = st.session_state.views[st.session_state.selected_dataset][
            "view_name"
        ]
        selected_dataset = st.session_state.selected_dataset
        # col1, col2, col3, col4, col5, col6 = st.columns([3.9, 3, 2.1, 2.2, 2, 1.8], vertical_alignment="bottom")
        rule_selection = st.segmented_control(
            "**Rule builder**",
            ["Create rule", "Load rule"],
            selection_mode="single",
            default="Create rule",
            key="rule_selection",
        )
        rule = f"rule_{st.session_state.total_rules+1}"

        def update_cast_list(rule, selected_dataset, index, column_to_cast):
            st.session_state.cast[f"cast_type_{column_to_cast}_{index}"] = (
                st.session_state[f"cast_type_{column_to_cast}_{index}"]
            )
            if (
                column_to_cast
                not in st.session_state.temp_rules[rule][selected_dataset][index][
                    "data_map"
                ]
            ):
                st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
                    column_to_cast
                ] = ""
            st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
                column_to_cast
            ] = st.session_state.cast[f"cast_type_{column_to_cast}_{index}"]

        col1, col2, col3, col4, col5, col6 = st.columns(
            [5, 1.8, 1.7, 1.9, 2.2, 0.1], vertical_alignment="bottom"
        )
        with col2:
            if st.button("Save Rule", disabled=rule_selection == "Load rule"):
                if st.session_state[f"{rule}"] != "":
                    temp = st.session_state.temp_rules[rule]
                    new_rule_name = st.session_state[f"{rule}"]
                    if new_rule_name not in st.session_state.rules:
                        del st.session_state.temp_rules[rule]
                        st.session_state.rules[new_rule_name] = temp
                        db_rules.update_one(
                            {"_id": ObjectId(rules_key)},
                            {
                                "$set": {
                                    "value": st.session_state.rules,
                                    "input_source": st.session_state.input_loaded,
                                }
                            },
                        )
                        st.session_state.rules = {}
                        for db_rule in db_rules.find({"input_source": st.session_state.input_loaded}):
                            print("Rule Source:", db_rule, flush=True)
                            st.session_state.rules = db_rule["value"]
                    st.toast("Rule created successfully!", icon="✅")
                    time.sleep(0.2)
                else:
                    st.toast("Rule name can't be empty.", icon="❌")
                    time.sleep(0.2)

        with col1:
            if rule_selection != "Create rule":
                st.selectbox(
                    "Select Rule",
                    options=list(st.session_state.rules.keys()),
                    index=0,
                    key="selected_rule",
                    disabled=rule_selection == "Create rule",
                    label_visibility="collapsed",
                    help="Select a rule to load.",
                    on_change=lambda: (
                        st.session_state.temp_rules.update(
                            st.session_state.rules[st.session_state.selected_rule]
                        )
                        if st.session_state.selected_rule in st.session_state.rules
                        else None
                    ),
                )
                rule = st.session_state.selected_rule
                rules = st.session_state.rules
            else:
                if rule not in st.session_state.temp_rules:
                    st.session_state.temp_rules[rule] = {}
                st.text_input(
                    "Rule Name",
                    key=f"{rule}",
                    disabled=rule_selection != "Create rule",
                    help="Enter a name for the rule.",
                    label_visibility="collapsed",
                    placeholder="Rule name",
                )
                rules = st.session_state.temp_rules
        with col3:
            if st.button("Test Rule"):
                pipeline_utils.test_rule(rules[rule], from_ui=True)
                pass
        with col4:
            if st.button("Reset Data"):
                reset_views(selected_dataset)
        with col5:
            if st.button("Add Process", disabled=rule_selection == "Load rule"):
                add_process(rule, selected_dataset)
                st.toast("Process added successfully!", icon="✅")
                time.sleep(0.3)

        with st.container(height=462):
            dataset_columns = ["--Select a column--"]
            dataset_columns.extend(
                st.session_state.temp_datasets[
                    st.session_state.selected_dataset
                ].columns
            )
            for i, process in enumerate(
                rules[rule][selected_dataset]
                if rule in rules and selected_dataset in rules[rule]
                else []
            ):
                col1, col3 = st.columns([2, 1], vertical_alignment="bottom")
                options = [
                    "Cast",
                    "Filter",
                    "Delete",
                    "Explode",
                    "Flatten",
                    "Save",
                    "",
                ]
                operation_type = col1.selectbox(
                    "Operation",
                    options=options,
                    index=options.index(
                        rules[rule][selected_dataset][i].get("operation", "Cast")
                    ),
                    key=f"operation_type_{i}",
                    disabled=rule_selection == "Load rule",
                    help="Select an operation to perform on the dataset.",
                )

                with col3:
                    st.button(
                        "🗑️",  # delete icon
                        help="Delete this process",
                        key=f"delete_process_{i}",
                        use_container_width=True,
                        disabled=rule_selection == "Load rule",
                        on_click=lambda p=process: rules[rule][selected_dataset].remove(
                            p
                        ),
                    )
                if "data_map" not in rules[rule][selected_dataset][i]:
                    rules[rule][selected_dataset][i]["data_map"] = {}
                match operation_type:
                    case "Cast":
                        if "operation" not in rules[rule][selected_dataset][i]:
                            rules[rule][selected_dataset][i]["operation"] = "Cast"
                        col1, col2 = st.columns(2)
                        with col1:
                            column_to_cast = st.selectbox(
                                "Column",
                                options=dataset_columns,
                                key=f"column_to_cast_{i}",
                                index=(
                                    dataset_columns.index(
                                        rules[rule][selected_dataset][i]
                                        .get("data_map", {})
                                        .get("column_to_cast", "--Select a column--")
                                    )
                                ),
                                on_change=lambda i=i: (
                                    update_column_to_cast(
                                        rule,
                                        selected_dataset,
                                        i,
                                        st.session_state.get(f"column_to_cast_{i}", ""),
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to cast.",
                            )
                        with col2:
                            options = ["--Select a cast type--"] + list(
                                dtypes_map.values()
                            )
                            cast_type = st.selectbox(
                                "Type",
                                options=options,
                                index=(
                                    options.index(
                                        rules[rule][selected_dataset][i]
                                        .get("data_map", {})
                                        .get("cast_type", "--Select a cast type--")
                                    )
                                ),
                                key=f"cast_type_{i}",
                                help="Select a type to cast the column.",
                                on_change=lambda i=i: (
                                    update_cast_type(
                                        rule,
                                        selected_dataset,
                                        i,
                                        st.session_state.get(f"cast_type_{i}", ""),
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                            )
                    case "Filter":
                        st.session_state.cast = {}
                        rules[rule][selected_dataset][i]["operation"] = "Filter"
                        if "data_map" not in rules[rule][selected_dataset][i]:
                            rules[rule][selected_dataset][i]["data_map"] = {}
                    case "Delete":
                        st.session_state.cast = {}
                        rules[rule][selected_dataset][i]["operation"] = "Delete"
                        options = ["--Select a delete type--", "row", "column"]

                        col1, col2 = st.columns(2)
                        with col1:
                            delete_type = st.selectbox(
                                "Delete Type",
                                options=options,
                                index=options.index(
                                    rules[rule][selected_dataset][i]["data_map"].get(
                                        "drop_by", "--Select a delete type--"
                                    )
                                ),
                                key=f"delete_type_{i}",
                                on_change=lambda i=i: (
                                    update_delete_type(
                                        rule,
                                        selected_dataset,
                                        i,
                                        st.session_state.get(
                                            f"delete_type_{i}",
                                            "--Select a delete type--",
                                        ),
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select whether to delete a column or a row.",
                            )
                        with col2:
                            st.text_input(
                                "Data to delete",
                                key=f"delete_data_{i}",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("data_reference", ""),
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(
                                            f"delete_data_{i}", ""
                                        ),
                                        process=process,
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select data to delete.",
                            )
                    case "Explode":
                        st.session_state.cast = {}
                        rules[rule][selected_dataset][i]["operation"] = "Explode"
                        col1, col2 = st.columns(2)
                        with col1:
                            st.text_input(
                                "New Column Name",
                                key=f"explode_column_name_{i}",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("new_column_name", ""),
                                on_change=lambda i=i: (
                                    update_column_name(
                                        rule,
                                        selected_dataset,
                                        i,
                                        new_column_name=st.session_state[
                                            f"explode_column_name_{i}"
                                        ],
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Enter a name for the new column created by exploding the selected column.",
                            )
                        with col2:
                            st.text_input(
                                "Data to explode",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("data_reference", ""),
                                key=f"explode_column_{i}",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state[
                                            f"explode_column_{i}"
                                        ],
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to explode.",
                            )
                    case "Flatten":
                        st.session_state.cast = {}
                        rules[rule][selected_dataset][i]["operation"] = "Flatten"
                        col1, col2 = st.columns(2)
                        with col1:
                            st.text_input(
                                "New Column Name",
                                key=f"flatten_column_name_{i}",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("new_column_name", ""),
                                on_change=lambda i=i: (
                                    update_column_name(
                                        rule,
                                        selected_dataset,
                                        i,
                                        new_column_name=st.session_state[
                                            f"flatten_column_name_{i}"
                                        ],
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Enter a name for the new column created by flattening the selected column.",
                            )
                        with col2:
                            st.text_input(
                                "Data to flatten",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("data_reference", ""),
                                key=f"flatten_column_{i}",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(
                                            f"flatten_column_{i}", ""
                                        ),
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to flatten.",
                            )

                    case "Save":
                        st.session_state.cast = {}
                        rules[rule][selected_dataset][i]["operation"] = "Save"
                        st.text_input(
                            "New Dataframe Name",
                            key=f"save_file_name_{i}",
                            value=rules[rule][selected_dataset][i]
                            .get("data_map", {})
                            .get("save_as", ""),
                            on_change=lambda i=i: (
                                update_new_dataset_name(
                                    rule,
                                    selected_dataset,
                                    i,
                                    new_dataset_name=st.session_state[
                                        f"save_file_name_{i}"
                                    ],
                                )
                                if rule_selection != "Load rule"
                                else None
                            ),
                            help="Enter a name for the file to save the dataset.",
                        )
                st.write("---")


# with d:
#     st.write(st.session_state.temp_rules)
# st.write(st.session_state.fusion_dataset_names)
