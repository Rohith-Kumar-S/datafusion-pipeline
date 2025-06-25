import streamlit as st
from pyspark.sql import types
import random

col1, col2 = st.columns(2)

def reset_temp_views():
    st.session_state.temp_views = st.session_state.views.copy()

if list(st.session_state.datasets):
    if "temp_views" not in st.session_state:
        reset_temp_views()

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

        dtypes_map = { types.IntegerType(): 'int', types.StringType(): 'string', types.DoubleType(): 'double', types.FloatType(): 'float', types.BooleanType(): 'bool', types.TimestampType(): 'timestamp', types.DateType(): 'date', types.LongType(): 'long', types.ShortType(): 'short', types.ByteType(): 'byte' }

        def add_process(state):
            print('Adding process to state:', state in st.session_state.rules)
            st.session_state.rules[state].append({'process_name': random.randint(1, 20), 'process_query': '', 'process_type': 'SQL'})

        match selection:
            case "View":
                st.session_state.views_query = f'SELECT * FROM {st.session_state.temp_views[selected_dataset]["view_name"]}'
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
                st.write(f'**{st.session_state.temp_views[selected_dataset]["view_name"]}**')
                data = []
                for field in st.session_state.datasets[selected_dataset].schema:
                    data.append((field.name, dtypes_map[field.dataType], field.nullable))
                df = st.session_state.spark.createDataFrame(data, ["field", "type", "nullable"])
                if df.count() > 10:
                    st.dataframe(df.toPandas(), height=350)
                else:
                    st.dataframe(df.toPandas())
            
with col2:
    col1, col2, col3, col4 = st.columns([5, 3, 2, 2], gap="small", vertical_alignment="bottom")
    rule_selection = col1.segmented_control("Rule builder", ['Create rule', 'Load rule'], selection_mode="single", default='Create rule', key="rule_selection")
    state = f'rule_{st.session_state.total_rules+1}'
    if rule_selection == 'Create rule':
        if state not in st.session_state.rules:
            print('Creating new rule')
            st.session_state.rules[state] = []
            # st.session_state.total_rules += 1
    col2.selectbox("Select Rule", options=list(st.session_state.rules.keys()), key="selected_rule", disabled=rule_selection == 'Create rule', help="Select a rule to edit or view.")
    with col3:
        if st.button("Add Process", disabled= rule_selection == 'Load rule'):
            add_process(state)
    with col4:
            if st.button("Test Rule"):
                pass
     
    with st.container(height=520):
        for i, process in enumerate(st.session_state.rules[state]):
            col1, col2, col3 = st.columns([8, 2, 1], vertical_alignment="bottom")
            process_type = col2.selectbox("Process Type", options=['SQL', 'Python'], index=0, key=f"process_type_{i}")
            col1.text_input("Process Query", value=process['process_query'], key=f"process_query_{i}")
            with col3:
                st.button(
                    "🗑️",  # delete icon
                    help="Delete this process",
                    key=f'delete_process_{i}',
                    use_container_width=True,
                    on_click=lambda p=process: st.session_state.rules[state].remove(p) if i!=0 else None
                )    
            st.write("---")
