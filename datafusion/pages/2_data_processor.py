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


def update_data_source(rule, selected_dataset, index,  data_key=None, data_reference=None):
    """Update the data source for a specific rule and dataset."""
    if data_reference is not None and data_reference.startswith("--Select a"):
        return
    
    if (
        data_key
        not in st.session_state.temp_rules[rule][selected_dataset][index]["data_map"]
    ):
        st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
            data_key
        ] = data_reference
    elif(
            st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
               data_key
            ]
            != data_reference
        ):
            st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
                data_key
            ] = data_reference
    
    if data_key == "save_as":
        st.session_state.fusion_dataset_names.append(
            data_reference
        )
    if rule not in st.session_state.new_columns:
        st.session_state.new_columns[rule] = {}
    if selected_dataset not in st.session_state.new_columns[rule]:
        st.session_state.new_columns[rule][selected_dataset] = []
    if data_key == "new_column_name":
        st.session_state.new_columns[rule][selected_dataset].append(data_reference)

def update_column_to_drop(rule, selected_dataset, index, data_reference=None):
    """Update the column to drop for a specific rule and dataset."""
    if "All" in data_reference and len(data_reference) != 1:
        data_reference.remove("All")
    st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
        "data_reference"
    ] = data_reference
    
def update_column_to_filter(rule, selected_dataset, index, data_reference=None):
    st.session_state.temp_rules[rule][selected_dataset][index]["data_map"][
        "columns_to_filter"
    ] = data_reference


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
        options = ["View", "Describe", "Schema"]
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
                c1, c2 = st.columns([9.8, 1.2], vertical_alignment="bottom")
                query = c1.text_input(
                    "Query",
                    value=st.session_state.views_query,
                    placeholder=st.session_state.views_query,
                )
                c2.button("Run")
                if query:
                    try:
                        df = st.session_state.spark.sql(query)
                        rows = df.count()
                        if rows > 10:
                            try: 
                                st.dataframe(
                                    df.limit(rows if rows < 60 else 60).toPandas(),
                                    height=350,
                                )
                            except Exception as e:
                                st.warning(f"⚠️ Dataa cant be viewed: {e}", icon="⚠️")
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
                    except Exception as e:
                        st.warning(f"⚠️ Data cant be viewed: {e}", icon="⚠️")
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
                        st.toast(
                            "Rule name already exists. Please choose a different name.",
                            icon="❌",
                        )
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
                st.session_state.datasets[
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
                    "Rename",
                    "Filter",
                    "Drop",
                    "Fill",
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
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(f"column_to_cast_{i}", ""),
                                        data_key="column_to_cast",
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
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(f"cast_type_{i}", ""),
                                        data_key="cast_type",
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                            )
                        if st.session_state.get(f"cast_type_{i}", "") == 'date':
                            col1, col2 = st.columns(2)
                            options = ["--Select a format--", "yyyy-MM-dd", "yyyy/MM/dd", "MM-dd-yyyy", "dd-MM-yyyy", "MM/dd/yyyy", "dd/MM/yyyy", "M/d/yyyy H:mm", "d/M/yyyy H:mm", "M/d/yyyy H:mm:ss", "d/M/yyyy H:mm:ss", "yyyy-MM-dd HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "MM/dd/yyyy'T'HH:mm:ss.SSSXXX", "dd/MM/yyyy'T'HH:mm:ss.SSSXXX", "yyyy-MM-dd'T'HH:mm:ss", "MM/dd/yyyy'T'HH:mm:ss", "dd/MM/yyyy'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS", "MM/dd/yyyy'T'HH:mm:ss.SSS", "dd/MM/yyyy'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS", "MM/dd/yyyy'T'HH:mm:ss.SSSSSS", "dd/MM/yyyy'T'HH:mm:ss.SSSSSS", "EEE MMM dd HH:mm:ss z yyyy"]
                            with col1:
                                st.selectbox("From Format",
                                    options=options,
                                    index=(
                                    options.index(
                                        rules[rule][selected_dataset][i]
                                        .get("data_map", {})
                                        .get("cast_from",  "--Select a format--")
                                    )
                                    ),
                                    key=f"from_format_{i}",
                                    help="Select the format of the date to cast.",
                                    on_change=lambda i=i: (
                                        update_data_source(
                                            rule,
                                            selected_dataset,
                                            i,
                                            data_reference=st.session_state.get(f"from_format_{i}", ""),
                                            data_key="cast_from"
                                        )
                                        if rule_selection != "Load rule"
                                        else None
                                    ),
                                )
                            with col2:
                                st.selectbox("To Format",
                                    options=options,
                                    index=(
                                    options.index(
                                        rules[rule][selected_dataset][i]
                                        .get("data_map", {})
                                        .get("cast_to",  "--Select a format--")
                                    )
                                    ),
                                    key=f"to_format_{i}",
                                    help="Select the format to cast the date to.",
                                    on_change=lambda i=i: (
                                        update_data_source(
                                            rule,
                                            selected_dataset,
                                            i,
                                            data_reference=st.session_state.get(f"to_format_{i}", ""),
                                            data_key="cast_to"
                                        )
                                        if rule_selection != "Load rule"
                                        else None
                                    ),
                                )
                    case "Rename":
                        rules[rule][selected_dataset][i]["operation"] = "Rename"
                        col1, col2 = st.columns(2)
                        with col1:
                            st.selectbox(
                                "Column to Rename",
                                options=dataset_columns,
                                key=f"rename_column_from_{i}",
                                index=(
                                    dataset_columns.index(
                                        rules[rule][selected_dataset][i]
                                        .get("data_map", {})
                                        .get("source_reference", "--Select a column--")
                                    )
                                ),
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(f"rename_column_from_{i}", ""),
                                        data_key="source_reference",
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to rename from.",
                            )
                        with col2:
                            st.text_input(
                                "Rename Column to",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("data_reference", ""),
                                key=f"rename_column_to_{i}",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state[
                                            f"rename_column_to_{i}"
                                        ],
                                        data_key="data_reference"
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to Rename.",
                            )
                    case "Filter":
                        st.session_state.cast = {}
                        rules[rule][selected_dataset][i]["operation"] = "Filter"
                        if "data_map" not in rules[rule][selected_dataset][i]:
                            rules[rule][selected_dataset][i]["data_map"] = {}
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            filter_column = st.selectbox(
                                "Column",
                                options=dataset_columns,
                                key=f"filter_column_{i}",
                                index=(
                                    dataset_columns.index(
                                        rules[rule][selected_dataset][i]
                                        .get("data_map", {})
                                        .get("source_reference", "--Select a column--")
                                    )
                                ),
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(f"filter_column_{i}", ""),
                                        data_key="source_reference",
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to filter.",
                            )
                        with col2:
                            options = [
                                    "--Select an operator--",
                                    "==",
                                    "!=",
                                    "<",
                                    "<=",
                                    ">",
                                    ">=",
                                    "contains",
                                    "startswith",
                                    "endswith",
                                ]
                            filter_operator = st.selectbox(
                                "Operator",
                                options=options,
                                index=(
                                    options.index(rules[rule][selected_dataset][i]
                                    .get("data_map", {})
                                    .get("operator", "--Select an operator--"))
                                ),
                                key=f"filter_operator_{i}",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(f"filter_operator_{i}", ""),
                                        data_key="operator"
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select an operator to filter the column.",
                            )
                        with col3:
                            filter_value = st.text_input(
                                "Value",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("data_reference", ""),
                                key=f"filter_value_{i}",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(f"filter_value_{i}", ""),
                                        data_key="data_reference"
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Enter a value to filter the column.",
                            )
                    case "Drop":
                        st.session_state.cast = {}
                        rules[rule][selected_dataset][i]["operation"] = "Drop"
                        options = ["--Select a drop type--", "column", "null values", "duplicates"]

                        col1, col2 = st.columns(2)
                        with col1:
                            drop_type = st.selectbox(
                                "Drop Type",
                                options=options,
                                index=options.index(
                                    rules[rule][selected_dataset][i]["data_map"].get(
                                        "drop_by", "--Select a drop type--"
                                    )
                                ),
                                key=f"delete_type_{i}",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(
                                            f"delete_type_{i}",
                                            "--Select a delete type--",
                                        ),
                                        data_key="drop_by"
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select whether to delete a column or a row.",
                            )
                        with col2:
                            options = ["All"]
                            options.extend(dataset_columns[1:])
                            options.extend(rules[rule][selected_dataset][i]["data_map"].get(
                                        "data_reference", []
                                    ))
                            options.extend(st.session_state.new_columns.get(rule, {}).get(selected_dataset, []))
                            options = list(set(options))
                            st.multiselect(
                                "Columns to drop",
                                options=options,
                                default=(
                                    rules[rule][selected_dataset][i]["data_map"].get(
                                        "data_reference", []
                                    )
                                ),
                                key=f"delete_data_{i}",
                                on_change=lambda i=i: (
                                    update_column_to_drop(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(
                                            f"delete_data_{i}", []
                                        )
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to drop.",
                            )
                    case "Fill":
                        rules[rule][selected_dataset][i]["operation"] = "Fill"
                        options = ["--Select a fill type--", "column", "null values"]

                        col1, col2 = st.columns(2, vertical_alignment="bottom")
                        with col1:
                            fill_type = st.selectbox(
                                "Fill Type",
                                options=options,
                                index=options.index(
                                    rules[rule][selected_dataset][i]["data_map"].get(
                                        "fill_by", "--Select a fill type--"
                                    )
                                ),
                                key=f"fill_type_{i}",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state.get(
                                            f"fill_type_{i}",
                                            "--Select a fill type--",
                                        ),
                                        data_key="fill_by"
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select fill type.",
                            )
                        with col2:
                            if rules[rule][selected_dataset][i]["data_map"].get(
                                        "fill_by", "--Select a fill type--"
                                    ) == 'column':
                                apply_filter = st.radio("Apply Condition", options=["Apply Condition"], 
                                    index=0 if rules[rule][selected_dataset][i]["data_map"].get(
                                            "apply_filter", None
                                        ) else None, key=f"apply_filter_{i}",
                                    help="Select whether to apply a filter to the column.",
                                    label_visibility="collapsed",
                                    on_change=lambda i=i: (
                                        update_data_source(
                                            rule,
                                            selected_dataset,
                                            i,
                                            data_reference=st.session_state.get(f"apply_filter_{i}", ""),
                                            data_key="apply_filter"
                                        )
                                        if rule_selection != "Load rule"
                                        else None
                                    ),
                                )
                            elif rules[rule][selected_dataset][i]["data_map"].get(
                                        "fill_by", "--Select a fill type--"
                                    ) == 'null values':
                                options = ["--Select a fill condition--","mean", "median","standard deviation", "custom"]
                                st.selectbox("Fill condition", options=options, index=options.index(rules[rule][selected_dataset][i]["data_map"].get(
                                            "fill_condition", "--Select a fill condition--"
                                        )), key=f"fill_condition_{i}", on_change=lambda i=i: (
                                            update_data_source(
                                                rule,
                                                selected_dataset,
                                                i,
                                                data_reference=st.session_state.get(f"fill_condition_{i}", ""),
                                                data_key="fill_condition"
                                            )
                                            if rule_selection != "Load rule"
                                            else None
                                        ))
                        if rules[rule][selected_dataset][i]["data_map"].get(
                                        "apply_filter", None
                                    ):
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                filter_column = st.selectbox(
                                    "Column",
                                    options=dataset_columns,
                                    key=f"fill_filter_column_{i}",
                                    index=(
                                        dataset_columns.index(
                                            rules[rule][selected_dataset][i]
                                            .get("data_map", {})
                                            .get("fill_filter_source_reference", "--Select a column--")
                                        )
                                    ),
                                    on_change=lambda i=i: (
                                        update_data_source(
                                            rule,
                                            selected_dataset,
                                            i,
                                            data_reference=st.session_state.get(f"fill_filter_column_{i}", ""),
                                            data_key="fill_filter_source_reference",
                                        )
                                        if rule_selection != "Load rule"
                                        else None
                                    ),
                                    help="Select a column to filter.",
                                )
                            with col2:
                                options = [
                                        "--Select an operator--",
                                        "==",
                                        "!=",
                                        "<",
                                        "<=",
                                        ">",
                                        ">=",
                                        "contains",
                                        "startswith",
                                        "endswith",
                                    ]
                                filter_operator = st.selectbox(
                                    "Operator",
                                    options=options,
                                    index=(
                                        options.index(rules[rule][selected_dataset][i]
                                        .get("data_map", {})
                                        .get("operator", "--Select an operator--"))
                                    ),
                                    key=f"fill_operator_{i}",
                                    on_change=lambda i=i: (
                                        update_data_source(
                                            rule,
                                            selected_dataset,
                                            i,
                                            data_reference=st.session_state.get(f"fill_operator_{i}", ""),
                                            data_key="operator"
                                        )
                                        if rule_selection != "Load rule"
                                        else None
                                    ),
                                    help="Select an operator to filter the column.",
                                )
                            with col3:
                                filter_value = st.text_input(
                                    "Value",
                                    value=rules[rule][selected_dataset][i]
                                    .get("data_map", {})
                                    .get("fill_filter_value", ""),
                                    key=f"fill_filter_value_{i}",
                                    on_change=lambda i=i: (
                                        update_data_source(
                                            rule,
                                            selected_dataset,
                                            i,
                                            data_reference=st.session_state.get(f"fill_filter_value_{i}", ""),
                                            data_key="fill_filter_data_reference"
                                        )
                                        if rule_selection != "Load rule"
                                        else None
                                    ),
                                    help="Enter a value to filter the column.",
                                )
                        col1, col2 = st.columns(2)
                        with col1:
                            st.text_input(
                                "Column Name",
                                key=f"fill_column_name_{i}",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("column_name", ""),
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state[
                                            f"fill_column_name_{i}"
                                        ],
                                        data_key="column_name",
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Enter a name for the new column created by exploding the selected column.",
                            )
                        with col2:
                            st.text_input(
                                "Value to fill",
                                value=rules[rule][selected_dataset][i]
                                .get("data_map", {})
                                .get("data_reference", ""),
                                key=f"fill_column_{i}",
                                disabled= rules[rule][selected_dataset][i]["data_map"].get(
                                        "fill_condition", "custom"
                                    ) != "custom",
                                on_change=lambda i=i: (
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state[
                                            f"fill_column_{i}"
                                        ],
                                        data_key="data_reference"
                                    )
                                    if rule_selection != "Load rule"
                                    else None
                                ),
                                help="Select a column to fill.",
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
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state[
                                            f"explode_column_name_{i}"
                                        ],
                                        data_key="new_column_name",
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
                                        data_key="data_reference"
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
                                    update_data_source(
                                        rule,
                                        selected_dataset,
                                        i,
                                        data_reference=st.session_state[
                                            f"flatten_column_name_{i}"
                                        ],
                                        data_key="new_column_name",
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
                                        data_key="data_reference",
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
                                update_data_source(
                                    rule,
                                    selected_dataset,
                                    i,
                                    data_reference=st.session_state[
                                        f"save_file_name_{i}"
                                    ],
                                    data_key="save_as"
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
