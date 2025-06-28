import streamlit as st
import time
import copy



def add_layer(pipeline):
    st.session_state.temp_pipelines[pipeline].append({
        'layer_type': '',
        'layer_selection': '',
    })

    
def update_layer_type(i, pipeline, option):
    if option != '-- Select an option --':
        st.session_state.temp_pipelines[pipeline][i]['layer_type'] = option


def update_layer_selection(i, pipeline, layer_selection):
    if layer_selection != '-- Select an option --':
        st.session_state.temp_pipelines[pipeline][i]['layer_selection'] = layer_selection





pipelines = None
pipeline_selection = st.segmented_control("**Pipeline Builder**", ['Create pipeline', 'Load pipeline'], default='Create pipeline', key='pipeline_control')
pipeline = f'pipeline_{st.session_state.total_fusions+1}'

col1, col2, col5, col4, col3 = st.columns([4, 2, 4, 12, 2], vertical_alignment="bottom")
with col2:
    if st.button("Save pipeline", disabled=pipeline_selection == 'Load pipeline'):
        if st.session_state[f"{pipeline}"] != '':
            temp = st.session_state.temp_pipelines[pipeline]
            new_pipeline_name = st.session_state[f"{pipeline}"]
            if new_pipeline_name not in st.session_state.pipelines:
                del st.session_state.temp_pipelines[pipeline]
                st.session_state.pipelines[new_pipeline_name] = temp
                st.toast('pipeline created successfully!', icon="✅")
                time.sleep(.2)
        else:
            st.toast('pipeline name can\'t be empty.', icon="❌")
            time.sleep(.2)


        
with col1:
    if pipeline_selection != 'Create pipeline': 
        pipeline = st.selectbox("Select pipeline", options=list(st.session_state.pipelines.keys()), index=0, key="selected_pipeline", disabled=pipeline_selection == 'Create pipeline', label_visibility="collapsed", help="Select a pipeline to load.", placeholder="Select pipeline")
        pipelines = st.session_state.pipelines
    else:
        if pipeline not in st.session_state.temp_pipelines:
            st.session_state.temp_pipelines[pipeline] = [{'layer_type': 'Source',
'layer_selection': ''}]
        st.text_input("pipeline Name", key=f"{pipeline}", disabled=pipeline_selection != 'Create pipeline', help="Enter a name for the pipeline.", label_visibility="collapsed", placeholder="pipeline name")
        pipelines = st.session_state.temp_pipelines

with col5:
    if st.button("Delete pipeline", disabled=pipeline_selection != 'Load pipeline'):
        del st.session_state.pipelines[pipeline]
        st.toast('pipeline deleted successfully!', icon="✅")
        time.sleep(.2)
        

with col3:
    if st.button("Add Layer", disabled=pipeline_selection == 'Load pipeline'):
        pass
        add_layer(pipeline)
        st.toast('Layer added successfully!', icon="✅")
        time.sleep(.3)

with st.container(height=462):

    for i, process in enumerate(pipelines[pipeline] if pipeline in pipelines else []):
        col1, col2, col3 = st.columns([4, 2, 1], vertical_alignment="bottom")
        options = ["-- Select an option --"]
        if i > 0:
            options.extend(['Processor', 'Fusion', 'Target'])
        else:
            options.extend(['Source'])

        col1.selectbox(
            "Layer type",
            options=options,
            index=options.index(pipelines[pipeline][i]['layer_type']) if pipelines[pipeline][i]['layer_type']!='' else 0,
            key=f'layer_type_{i}',
            on_change=lambda i=i: update_layer_type(i, pipeline, st.session_state[f'layer_type_{i}']),
            disabled=pipeline_selection == 'Load pipeline',
        )
        
        options = ["-- Select an option --"]
        if st.session_state[f'layer_type_{i}'] == 'Source':
            options.extend(['bleh'])
        elif st.session_state[f'layer_type_{i}'] == 'Processor':
            options.extend(list(st.session_state.rules.keys()))
        elif st.session_state[f'layer_type_{i}'] == 'Fusion':
            options.extend(list(st.session_state.fusions.keys()))
        elif st.session_state[f'layer_type_{i}'] == 'Target':
            options.extend(list(st.session_state.targets.keys()))
        col2.selectbox(
            "Select layer",
            options=options,
            index=options.index(pipelines[pipeline][i]['layer_selection']) if pipelines[pipeline][i]['layer_selection']!='' else 0,
            key=f'layer_selection_{i}',
            on_change=lambda i=i: update_layer_selection(i, pipeline, st.session_state[f'layer_selection_{i}']),
            disabled=pipeline_selection == 'Load pipeline',
        )
        
        with col3:
            st.button(
                "🗑️",  # delete icon
                help="Delete this process",
                key=f'delete_process_{i}',
                use_container_width=True,
                disabled=pipeline_selection == 'Load pipeline',
                on_click=lambda p=process: pipelines[pipeline].remove(p) 
            )

        st.write("---")
        if pipeline_selection == 'Load pipeline':
            print(pipeline)
            pipeline_data = copy.deepcopy(st.session_state.pipelines[pipeline])
            for i, layer in enumerate(pipeline_data):
                if layer['layer_type'] == 'Source':
                    pass
                elif layer['layer_type'] == 'Processor':
                    layer['layer_selection'] = st.session_state.rules[layer['layer_selection']]
                elif layer['layer_type'] == 'Fusion':
                    layer['layer_selection'] = st.session_state.fusions[layer['layer_selection']]
                elif layer['layer_type'] == 'Target':
                    layer['layer_selection'] = st.session_state.targets[layer['layer_selection']]
            print(pipeline_data)
                

# with side:
#     st.write(st.session_state.temp_pipelines)
