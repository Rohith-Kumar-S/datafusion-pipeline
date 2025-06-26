import streamlit as st
from pyspark.sql import types
import random

d, col1, col2 = st.columns(3)

dtypes_map = { types.IntegerType(): 'int', types.StringType(): 'string', types.DoubleType(): 'double', types.FloatType(): 'float', types.BooleanType(): 'bool', types.TimestampType(): 'timestamp', types.DateType(): 'date', types.LongType(): 'long', types.ShortType(): 'short', types.ByteType(): 'byte' }
reverse_dtypes_map = {v: k for k, v in dtypes_map.items()}

        
def test_rule(rule):
    for dataset_name, processes in st.session_state.rules[rule].items():
        for process in processes:
            if process['operation'] == 'Cast':
                column_map = process.get('column_map', {})
                for column, cast_type in column_map.items():
                    if cast_type:
                        try:
                            st.session_state.datasets[dataset_name] = st.session_state.datasets[dataset_name].withColumn(column, st.session_state.datasets[dataset_name][column].cast(reverse_dtypes_map[cast_type]))
                            view_name = st.session_state.views[dataset_name]["view_name"]
                            st.session_state.datasets[dataset_name].createOrReplaceTempView(view_name)
                        except Exception as e:
                            st.error(f"Error casting column {column} to {cast_type}: {e}")
    

with col1:
    selected_dataset = st.selectbox("Select Dataset",
        options=list(st.session_state.datasets.keys()),
        key="selected_dataset",
        help="Select a dataset to process."
    )
    if list(st.session_state.datasets):
        options = ["View", "Describe", "Schema", "Visualize"]
        selection = st.segmented_control(
            "Operations", options, selection_mode="single", default="View", key="selection"
        )

        def add_process(rule, selected_dataset):
            if selected_dataset not in st.session_state.rules[rule]:
                st.session_state.rules[rule][selected_dataset] = []
            # print('Adding process to state:', state in st.session_state.rules)
            st.session_state.rules[rule][selected_dataset].append({'process_query': '', 'process_type': 'SQL'})

        match selection:
            case "View":
                st.session_state.views_query = f'SELECT * FROM {st.session_state.views[selected_dataset]["view_name"]}'
                query = st.text_input("Query", value=st.session_state.views_query, placeholder=st.session_state.views_query)
                if query:
                    df = st.session_state.spark.sql(query)
                    rows = df.count()
                    if rows > 10:
                        st.dataframe(df.limit(rows if rows < 60 else 60).toPandas(), height=350)
                    else:
                        st.dataframe(df.limit(rows).toPandas())
                    st.session_state.views_query = query
                else:
                    st.dataframe(st.session_state.datasets[selected_dataset].limit(10).toPandas())
                
            case "Describe":
                st.write(st.session_state.datasets[selected_dataset].describe().toPandas())
            case "Schema":
                st.write(f'**{st.session_state.views[selected_dataset]["view_name"]}**')
                data = []
                for field in st.session_state.datasets[selected_dataset].schema:
                    data.append((field.name, dtypes_map[field.dataType], field.nullable))
                df = st.session_state.spark.createDataFrame(data, ["field", "type", "nullable"])
                if df.count() > 10:
                    st.dataframe(df.toPandas(), height=350)
                else:
                    st.dataframe(df.toPandas())
            
with col2:
    if list(st.session_state.datasets):
        view_name = st.session_state.views[st.session_state.selected_dataset]["view_name"]
        selected_dataset = st.session_state.selected_dataset
        col1, col2, col3, col4 = st.columns([4, 3, 3, 2], gap="small", vertical_alignment="bottom")
        rule_selection = col1.segmented_control("Rule builder", ['Create rule', 'Load rule'], selection_mode="single", default='Create rule', key="rule_selection")
        rule = f'rule_{st.session_state.total_rules+1}'
        if rule_selection == 'Create rule':
            if rule not in st.session_state.rules:
                st.session_state.rules[rule] = {}
                # st.session_state.total_rules += 1
        col2.selectbox("Select Rule", options=list(st.session_state.rules.keys()), key="selected_rule", disabled=rule_selection == 'Create rule', help="Select a rule to edit or view.")
        with col3:
            if st.button("Add Process", disabled=rule_selection == 'Load rule'):
                add_process(rule, selected_dataset)
        with col4:
                if st.button("Test Rule"):
                    test_rule(rule)
                    pass
        
        def update_cast_list(rule, selected_dataset, index, column_to_cast):
            st.session_state.cast[f'cast_type_{column_to_cast}_{index}'] = st.session_state[f'cast_type_{column_to_cast}_{index}']
            if column_to_cast not in st.session_state.rules[rule][selected_dataset][index]['column_map']:
                st.session_state.rules[rule][selected_dataset][index]['column_map'][column_to_cast] = ''
            st.session_state.rules[rule][selected_dataset][index]['column_map'][column_to_cast] = st.session_state.cast[f'cast_type_{column_to_cast}_{index}']

        with st.container(height=512):
            dataset_columns = st.session_state.datasets[st.session_state.selected_dataset].columns
            for i, process in enumerate(st.session_state.rules[rule][selected_dataset] if rule in st.session_state.rules and selected_dataset in st.session_state.rules[rule] else []):
                col1, col3 = st.columns([2, 1], vertical_alignment="bottom")
                operation_type = col1.selectbox("Operation", options=['Cast', 'Filter', ''], index=0, key=f"operation_type_{i}")

                with col3:
                    st.button(
                        "🗑️",  # delete icon
                        help="Delete this process",
                        key=f'delete_process_{i}',
                        use_container_width=True,
                        on_click=lambda p=process: st.session_state.rules[rule][selected_dataset].remove(p)
                    )
                if operation_type == 'Cast':
                    st.session_state.rules[rule][selected_dataset][i]['operation'] = 'Cast'
                    if 'column_map' not in st.session_state.rules[rule][selected_dataset][i]:
                        st.session_state.rules[rule][selected_dataset][i]['column_map'] = {}
                    col1, col2 = st.columns(2)
                    with col1:
                        column_to_cast = st.selectbox(
                            "Column",
                            options=dataset_columns,
                            key=f"cast_column_{i}",
                            help="Select a column to cast."
                        )
                    with col2:
                        key_name = f'cast_type_{column_to_cast}_{i}'
                        if key_name not in st.session_state.cast:
                            st.session_state.cast[key_name] = dtypes_map[st.session_state.datasets[st.session_state.selected_dataset].schema[column_to_cast].dataType] if column_to_cast else 'string'
                        cast_type = st.selectbox(
                            "Type",
                            options=list(dtypes_map.values()),
                            index=list(dtypes_map.values()).index(st.session_state.cast[key_name]) if key_name in st.session_state.cast else 0,
                            key=key_name,
                            help="Select a type to cast the column.",
                            on_change=lambda: update_cast_list(rule, selected_dataset, i, column_to_cast)
                        )
                elif operation_type == 'Filter':
                    st.session_state.cast = {}
                    st.session_state.rules[rule][selected_dataset][i]['operation'] = 'Filter'
                    if 'column_map' not in st.session_state.rules[rule][selected_dataset][i]:
                        st.session_state.rules[rule][selected_dataset][i]['column_map'] = {}
                st.write("---")
            
with d:
    st.write(st.session_state.rules)
