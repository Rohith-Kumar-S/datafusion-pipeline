from time import time
import streamlit as st
from pyspark.sql import types
from pyspark.sql.functions import to_date, col
import random
import time

# a, b = st.columns([2, 8])


def add_process(fusion):
    st.session_state.temp_fusions[fusion].append({
        'datasets_to_fuse': [],
        'fuse_by': 'columns',
        'fusable_columns': []
    })

    
def get_fusable_columns(datasets):
    dataset = datasets[0]
    fusable_columns = []
    for dtype in st.session_state.temp_datasets[dataset].dtypes:
        dtype_present = True
        for i in range(1, len(datasets)):
            if dtype not in st.session_state.temp_datasets[datasets[i]].dtypes:
                dtype_present = False
                break
        if dtype_present:
            fusable_columns.append(dtype[0])
    return fusable_columns


def update_datasets_to_fuse(i, datasets_to_fuse):
    st.session_state.temp_fusions[fusion][i]['datasets_to_fuse'] = datasets_to_fuse


def update_fusion_type(i, option):
    st.session_state.temp_fusions[fusion][i]['fuse_by'] = option


def update_fusable_columns(i, fusable_columns):
    st.session_state.temp_fusions[fusion][i]['fusable_columns'] = fusable_columns

if list(st.session_state.temp_datasets):
    fusions = None
    fusion_selection = st.segmented_control("**Fusion builder**", ['Create fusion', 'Load fusion'], selection_mode="single", default='Create fusion', key="fusion_selection")
    fusion = f'fusion_{st.session_state.total_fusions+1}'
    
    col1, col2, col4, col3 = st.columns([4,2,12,2], vertical_alignment="bottom")
    with col2:
        if st.button("Save fusion", disabled=fusion_selection == 'Load fusion'):
            if st.session_state[f"{fusion}"] != '':
                temp = st.session_state.temp_fusions[fusion]
                new_fusion_name = st.session_state[f"{fusion}"]
                if new_fusion_name not in st.session_state.fusions:
                    del st.session_state.temp_fusions[fusion]
                    st.session_state.fusions[new_fusion_name] = temp
                    st.toast('fusion created successfully!', icon="✅")
                    time.sleep(.2)
            else:
                st.toast('fusion name can\'t be empty.', icon="❌")
                time.sleep(.2)
            
    with col1:
        if fusion_selection != 'Create fusion':
            st.selectbox("Select fusion", options=list(st.session_state.fusions.keys()), index=0, key="selected_fusion", disabled=fusion_selection == 'Create fusion', label_visibility="collapsed", help="Select a fusion to load.", on_change=lambda: st.session_state.temp_fusions.update(st.session_state.fusions[st.session_state.selected_fusion]) if st.session_state.selected_fusion in st.session_state.fusions else None)
            fusion = list(st.session_state.fusions.keys())[0]
            fusions = st.session_state.fusions
        else:
            if fusion not in st.session_state.temp_fusions:
                st.session_state.temp_fusions[fusion] = []
            st.text_input("fusion Name", key=f"{fusion}", disabled=fusion_selection != 'Create fusion', help="Enter a name for the fusion.", label_visibility="collapsed", placeholder="fusion name")
            fusions = st.session_state.temp_fusions

    with col3:
        if st.button("Add Process", disabled=fusion_selection == 'Load fusion'):
            pass
            add_process(fusion)
            st.toast('Process added successfully!', icon="✅")
            time.sleep(.3)

    with st.container(height=462):

        for i, process in enumerate(fusions[fusion]):
            col1, col3 = st.columns([7, 1], vertical_alignment="bottom")
            datasets_to_fuse = col1.multiselect('Datasets to fuse',
                options=list(st.session_state.temp_datasets.keys()),
                default=fusions[fusion][i]['datasets_to_fuse'],
                key=f'datasets_to_fuse_{i}',
                help="Select datasets to fuse.",
                on_change=lambda: update_datasets_to_fuse(i, st.session_state[f'datasets_to_fuse_{i}']),
                disabled=fusion_selection == 'Load fusion'
            )

            with col3:
                st.button(
                    "🗑️",  # delete icon
                    help="Delete this process",
                    key=f'delete_process_{i}',
                    use_container_width=True,
                    disabled=fusion_selection == 'Load fusion',
                    on_click=lambda p=process: fusions[fusion].remove(p)
                )
            col1, col2 = st.columns([2, 13])
            if len(datasets_to_fuse) > 1:
                options = ['columns', 'rows']
                with col1:
                    option = st.selectbox(label='Fuse by', options=options, index=options.index(fusions[fusion][i]['fuse_by']), key=f'fuse_by_{i}', on_change=lambda: update_fusion_type(i, st.session_state[f'fuse_by_{i}']), disabled=fusion_selection == 'Load fusion', help="Select how to fuse the datasets.")
                options = get_fusable_columns(datasets_to_fuse)
                if fusion_selection != 'Load fusion':
                    update_fusable_columns(i, options)
                if option == 'columns':
                    with col2:
                        st.multiselect('Fusable columns',
                        options=options,
                        default=options,
                        key=f'fusable_columns_{i}',
                        help="Select columns to fuse.",
                        on_change=lambda: update_fusable_columns(i, st.session_state[f'fusable_columns_{i}']),
                        disabled=fusion_selection == 'Load fusion'
                    )
            st.write("---")
else:
    st.warning('No data found to fuse', icon="⚠️")
            
# with a:
#     st.write(st.session_state.temp_fusions)
