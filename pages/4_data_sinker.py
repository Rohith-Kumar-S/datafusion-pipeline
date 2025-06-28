import streamlit as st


def add_to_conversion(target_name, dataset, format_key):
    """
    Add the dataset to the conversion list with the specified format.
    """
    if 'conversions' not in st.session_state.temp_targets[target_name]:
        print('helo', target_name)
        st.session_state.temp_targets[target_name]['conversions'] = {}
    st.session_state.temp_targets[target_name]['conversions'][dataset] = st.session_state[format_key]
    st.toast(f'Dataset {dataset} added for conversion to {st.session_state[format_key]} format.', icon="✅")




st.segmented_control("**Target Builder**", ['Create target', 'Load target'], default='Create target', key='target_control')

if st.session_state.target_control == 'Create target':
    target_name = f'target_{st.session_state.total_targets + 1}'
    if target_name not in st.session_state.temp_targets:
            st.session_state.temp_targets[target_name] = {}
    conversion_options = ['parquet', 'csv']
    col1, col2, col3 = st.columns(3)
    with col1:
        st.selectbox('Target type', options=['external'], key='target_type')
    with col2:
        st.text_input('Target name', key='target_name')
    with col3:
        st.text_input('Target path', key='target_path', placeholder='e.g. s3://my-bucket/my-data/')
    col1, col2, col3 = st.columns(3)
    with col1:
        st.multiselect('Datasets to be sent to the target', options=list(st.session_state.datasets.keys()), key='target_datasets')
    with col2:
        if st.session_state.target_datasets:
            target_dataset = st.selectbox('Convert dataset', options=list(st.session_state.target_datasets), key='convert_dataset', help="Select a dataset to convert before saving.")
    with col3:
        if st.session_state.target_datasets:
            st.selectbox('Select target format', index=conversion_options.index(st.session_state.convert_dataset.split('.')[1]), options=conversion_options, key=f'{target_dataset}_format', on_change=lambda: add_to_conversion(target_name, target_dataset, f'{target_dataset}_format'), help="Select the format to convert the dataset before saving.")

    if st.button('Save target'):
        if st.session_state.target_name != '':
            temp = st.session_state.temp_targets[target_name]
            new_target_name = st.session_state.target_name
            target_path = st.session_state.target_path
            target_type = st.session_state.target_type
            target_datasets = st.session_state.target_datasets
            st.session_state.targets[new_target_name] = temp
            del st.session_state.temp_targets[target_name]
            st.session_state.targets[new_target_name]['type'] = target_type
            if target_path and target_path != '':
                st.session_state.targets[new_target_name]['path'] = target_path
            st.session_state.targets[new_target_name]['datasets'] = target_datasets
            st.toast(f'Target {new_target_name} created successfully!', icon="✅")
        else:
            st.toast('Target name and path cannot be empty.', icon="❌")
else:
    st.selectbox("Select target", options=list(st.session_state.targets.keys()), key="selected_target", help="Select a target to load.")
    if st.session_state.selected_target:
        target = st.session_state.targets[st.session_state.selected_target]
        st.write(f"**Target Name:** {st.session_state.selected_target}")
        st.write(f"**Target Type:** {target['type']}")
        if 'path' in target:
            st.write(f"**Target Path:** {target['path']}")
        if 'datasets' in target:
            st.write(f"**Datasets:** {', '.join(target['datasets'])}")
        if 'conversions' in target:
            st.write(f"**Conversions:** {', '.join(target['conversions'])}")
            st.write(f'Converts to: {", ".join(list(target["conversions"].values()))}')

