from time import time
import streamlit as st
from pyspark.sql import types
from pyspark.sql.functions import to_date, col
import random
import time

from pipeline_utils import PipelineUtils

pipeline_utils = PipelineUtils(st.session_state.spark, st.session_state.datasets, st.session_state.dataset_paths, st.session_state.views, st.session_state.temp_datasets)

        
def reset_views(dataset_name):
    st.session_state.temp_datasets[dataset_name] = st.session_state.datasets[dataset_name]
    view_name = st.session_state.views[dataset_name]["view_name"]
    st.session_state.cast = {}
    st.session_state.temp_datasets[dataset_name].createOrReplaceTempView(view_name)
    st.toast('Data reseted successfully!', icon="✅")
    time.sleep(.3)




dtypes_map = { types.IntegerType(): 'int', types.StringType(): 'string', types.DoubleType(): 'double', types.FloatType(): 'float', types.BooleanType(): 'bool', types.TimestampType(): 'timestamp', types.DateType(): 'date', types.LongType(): 'long', types.ShortType(): 'short', types.ByteType(): 'byte' }
reverse_dtypes_map = {v: k for k, v in dtypes_map.items()}

        
def test_rule(rules, rule):
    for dataset_name, processes in rules[rule].items():
        for process in processes:
            if process['operation'] == 'Cast':
                column_map = process.get('column_map', {})
                for column, cast_type in column_map.items():
                    if cast_type:
                        try:
                            if cast_type == 'date':
                                st.session_state.temp_datasets[dataset_name] = st.session_state.temp_datasets[dataset_name].withColumn(column, to_date(col(column), 'MM-dd-yy'))
                            else:
                                st.session_state.temp_datasets[dataset_name] = st.session_state.temp_datasets[dataset_name].withColumn(column, st.session_state.temp_datasets[dataset_name][column].cast(reverse_dtypes_map[cast_type]))
                            view_name = st.session_state.views[dataset_name]["view_name"]
                            st.session_state.temp_datasets[dataset_name].createOrReplaceTempView(view_name)
                        except Exception as e:
                            st.error(f"Error casting column {column} to {cast_type}: {e}")
    st.toast('Rule applied successfully!', icon="✅")
    time.sleep(.3)
    
if list(st.session_state.temp_datasets):
    col1, col2 = st.columns(2)
    with col1:
        selected_dataset = st.selectbox("Select Dataset",
            options=list(st.session_state.temp_datasets.keys()),
            key="selected_dataset",
            help="Select a dataset to process."
        )
        options = ["View", "Describe", "Schema", "Visualize"]
        selection = st.segmented_control(
            "Operations", options, selection_mode="single", default="View", key="selection"
        )

        def add_process(rule, selected_dataset):
            if selected_dataset not in st.session_state.temp_rules[rule]:
                st.session_state.temp_rules[rule][selected_dataset] = []
            st.session_state.temp_rules[rule][selected_dataset].append({'process_query': '', 'process_type': 'SQL'})

        match selection:
            case "View":
                st.session_state.views_query = f'SELECT * FROM {st.session_state.views[selected_dataset]["view_name"]}'
                query = st.text_input("Query", value=st.session_state.views_query, placeholder=st.session_state.views_query)
                if query:
                    df = st.session_state.spark.sql(query)
                    rows = df.count()
                    print(f"Rows in the dataset: {rows}", flush=True)
                    if rows > 10:
                        st.dataframe(df.limit(rows if rows < 60 else 60).toPandas(), height=350)
                    else:
                        print("Displaying all rows:", flush=True)
                        dataframe = df.limit(rows).toPandas()
                        st.dataframe(dataframe)
                    st.session_state.views_query = query
                else:
                    st.dataframe(st.session_state.temp_datasets[selected_dataset].limit(10).toPandas())
                
            case "Describe":
                st.write(st.session_state.temp_datasets[selected_dataset].describe().toPandas())
            case "Schema":
                st.write(f'**{st.session_state.views[selected_dataset]["view_name"]}**')
                data = []
                for field in st.session_state.temp_datasets[selected_dataset].schema:
                    data.append((field.name, dtypes_map[field.dataType], field.nullable))
                df = st.session_state.spark.createDataFrame(data, ["field", "type", "nullable"])
                if df.count() > 10:
                    st.dataframe(df.toPandas(), height=350)
                else:
                    st.dataframe(df.toPandas())
                
    with col2:
        rules = None
        view_name = st.session_state.views[st.session_state.selected_dataset]["view_name"]
        selected_dataset = st.session_state.selected_dataset
        # col1, col2, col3, col4, col5, col6 = st.columns([3.9, 3, 2.1, 2.2, 2, 1.8], vertical_alignment="bottom")
        rule_selection = st.segmented_control("**Rule builder**", ['Create rule', 'Load rule'], selection_mode="single", default='Create rule', key="rule_selection")
        rule = f'rule_{st.session_state.total_rules+1}'
        
        
        def update_cast_list(rule, selected_dataset, index, column_to_cast):
            st.session_state.cast[f'cast_type_{column_to_cast}_{index}'] = st.session_state[f'cast_type_{column_to_cast}_{index}']
            if column_to_cast not in st.session_state.temp_rules[rule][selected_dataset][index]['column_map']:
                st.session_state.temp_rules[rule][selected_dataset][index]['column_map'][column_to_cast] = ''
            st.session_state.temp_rules[rule][selected_dataset][index]['column_map'][column_to_cast] = st.session_state.cast[f'cast_type_{column_to_cast}_{index}']
        col1, col2, col3, col4, col5, col6 = st.columns([5, 1.8, 1.7, 1.9, 2.2, 0.1], vertical_alignment="bottom")
        with col2:
            if st.button("Save Rule", disabled=rule_selection == 'Load rule'):
                if st.session_state[f"{rule}"] != '':
                    temp = st.session_state.temp_rules[rule]
                    new_rule_name = st.session_state[f"{rule}"]
                    if new_rule_name not in st.session_state.rules:
                        del st.session_state.temp_rules[rule]
                        st.session_state.cast = {}
                        st.session_state.rules[new_rule_name] = temp
                        st.toast('Rule created successfully!', icon="✅")
                        time.sleep(.2)
                else:
                    st.toast('Rule name can\'t be empty.', icon="❌")
                    time.sleep(.2)
                
        with col1:
            if rule_selection != 'Create rule':
                st.selectbox("Select Rule", options=list(st.session_state.rules.keys()), index=0, key="selected_rule", disabled=rule_selection == 'Create rule', label_visibility = "collapsed", help="Select a rule to load.", on_change=lambda: st.session_state.temp_rules.update(st.session_state.rules[st.session_state.selected_rule]) if st.session_state.selected_rule in st.session_state.rules else None)
                rule = list(st.session_state.rules.keys())[0]
                rules = st.session_state.rules
            else:
                if rule not in st.session_state.temp_rules:
                    st.session_state.temp_rules[rule] = {}
                st.text_input("Rule Name", key=f"{rule}", disabled=rule_selection != 'Create rule', help="Enter a name for the rule.", label_visibility="collapsed", placeholder="Rule name")
                rules = st.session_state.temp_rules
        with col3:
            if st.button("Test Rule"):
                pipeline_utils.test_rule(rules[rule], from_ui=True)
                pass
        with col4:
            if st.button("Reset Data"):
                reset_views(selected_dataset)
        with col5:
            if st.button("Add Process", disabled=rule_selection == 'Load rule'):
                add_process(rule, selected_dataset)
                st.toast('Process added successfully!', icon="✅")
                time.sleep(.3)

        with st.container(height=462):
            dataset_columns = st.session_state.temp_datasets[st.session_state.selected_dataset].columns
            print(rules)
            for i, process in enumerate(rules[rule][selected_dataset] if rule in rules and selected_dataset in rules[rule] else []):
                col1, col3 = st.columns([2, 1], vertical_alignment="bottom")
                operation_type = col1.selectbox("Operation", options=['Cast', 'Filter', ''], index=0, key=f"operation_type_{i}", disabled=rule_selection == 'Load rule', help="Select an operation to perform on the dataset.")

                with col3:
                    st.button(
                        "🗑️",  # delete icon
                        help="Delete this process",
                        key=f'delete_process_{i}',
                        use_container_width=True,
                        disabled=rule_selection == 'Load rule',
                        on_click=lambda p=process: rules[rule][selected_dataset].remove(p)
                    )
                if operation_type == 'Cast':
                    rules[rule][selected_dataset][i]['operation'] = 'Cast'
                    if 'column_map' not in rules[rule][selected_dataset][i]:
                        rules[rule][selected_dataset][i]['column_map'] = {}
                    col1, col2 = st.columns(2)
                    with col1:
                        column_to_cast = st.selectbox(
                            "Column",
                            options=dataset_columns,
                            key=f"cast_column_{i}",
                            help="Select a column to cast.",
                            
                        )
                    with col2:
                        key_name = f'cast_type_{column_to_cast}_{i}'
                        if key_name not in st.session_state.cast:
                            st.session_state.cast[key_name] = dtypes_map[st.session_state.temp_datasets[st.session_state.selected_dataset].schema[column_to_cast].dataType] if column_to_cast else 'string'
                        if rule_selection == 'Load rule' and column_to_cast in rules[rule][selected_dataset][i]['column_map']:
                            st.session_state.cast[key_name] = rules[rule][selected_dataset][i]['column_map'][column_to_cast]
                        cast_type = st.selectbox(
                            "Type",
                            options=list(dtypes_map.values()),
                            index=list(dtypes_map.values()).index(st.session_state.cast[key_name]) if key_name in st.session_state.cast else 0,
                            key=key_name,
                            help="Select a type to cast the column.",
                            on_change=lambda: update_cast_list(rule, selected_dataset, i, column_to_cast) if rule_selection != 'Load rule' else None
                        )
                elif operation_type == 'Filter':
                    st.session_state.cast = {}
                    rules[rule][selected_dataset][i]['operation'] = 'Filter'
                    if 'column_map' not in rules[rule][selected_dataset][i]:
                        rules[rule][selected_dataset][i]['column_map'] = {}
                st.write("---")
else:
    st.warning('No data found to process', icon="⚠️")
                

    
            
# with d:
#     st.write(st.session_state.temp_rules)
