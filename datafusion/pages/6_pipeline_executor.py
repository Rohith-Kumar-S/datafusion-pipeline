import streamlit as st
import copy
from pipeline_engine import Pipeline

selected_pipeline = st.selectbox('Select Pipeline', options=list(st.session_state.pipelines.keys()), index=0, key="selected_pipeline")

progress_text = "Operation in progress. Please wait."
my_bar = st.progress(0, text=progress_text)

st.button("Execute Pipeline", disabled=selected_pipeline == '' or selected_pipeline == None, on_click=lambda: execute_pipeline(selected_pipeline, my_bar), help="Execute the selected pipeline.", key="execute_pipeline")

def execute_pipeline(selected_pipeline, my_bar):
    pipeline_data = copy.deepcopy(st.session_state.pipelines[selected_pipeline])
    for i, layer in enumerate(pipeline_data):
        if layer['layer_type'] == 'Source':
            layer['layer_selection'] = st.session_state.inputs[layer['layer_selection']]
        elif layer['layer_type'] == 'Processor':
            layer['layer_selection'] = st.session_state.rules[layer['layer_selection']]
        elif layer['layer_type'] == 'Fusion':
            layer['layer_selection'] = st.session_state.fusions[layer['layer_selection']]
        elif layer['layer_type'] == 'Target':
            layer['layer_selection'] = st.session_state.targets[layer['layer_selection']]
    engine = Pipeline(st.session_state.spark)
    print(pipeline_data)
    my_bar.progress(10, text=progress_text)
    exported_paths = engine.execute(pipeline_data, my_bar)
    
    with st.container():
        for path in exported_paths:
            st.download_button(label=f"Download {path}", data=open(path, "rb"), file_name=path)
    my_bar.progress(100, text="Pipeline executed successfully.")
    st.toast('Pipeline executed successfully!', icon="✅")
    print('exported_paths: ',exported_paths)