import streamlit as st
import os
import findspark
from pyspark.sql import SparkSession

st.set_page_config(
    page_title="Data Fusion Pipeline",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
    
)

# Initialize the session state key if it doesn't exist
if "user_input" not in st.session_state:
    st.session_state.user_input = ""
if "datasets" not in st.session_state:
    st.session_state.datasets = {}
    st.session_state.dataset_paths = {}

if 'temp_datasets' not in st.session_state:
    st.session_state.temp_datasets = {}
    
if "views" not in st.session_state:
    st.session_state.views = {}

if "views_query" not in st.session_state:
    st.session_state.views_query = ''

if "spark" not in st.session_state:
    st.session_state.spark = None
    
if "temp_rules" not in st.session_state:
    st.session_state.temp_rules = {}

if "rules" not in st.session_state:
    st.session_state.rules = {}

if "total_rules" not in st.session_state:
    st.session_state.total_rules = 0
    
if "temp_fusions" not in st.session_state:
    st.session_state.temp_fusions = {}

if "fusions" not in st.session_state:
    st.session_state.fusions = {}

if "total_fusions" not in st.session_state:
    st.session_state.total_fusions = 0

if "cast" not in st.session_state:
    st.session_state.cast = {}
    
if "temp_targets" not in st.session_state:
    st.session_state.temp_targets = {}

if "targets" not in st.session_state:
    st.session_state.targets = {}
    
if "total_targets" not in st.session_state:
    st.session_state.total_targets = 0
    
if "temp_pipelines" not in st.session_state:
    st.session_state.temp_pipelines = {}
    
if "pipeline_layer_selections" not in st.session_state:
    st.session_state.pipeline_layer_selections = {}

if "pipelines" not in st.session_state:
    st.session_state.pipelines = {}
    
if "total_pipelines" not in st.session_state:
    st.session_state.total_pipelines = 0

    
# # Initialize Spark
@st.cache_resource
def Spark_Data_Fusion():
    findspark.init()
    findspark.find()    
    return SparkSession.builder.appName("DataFusion").getOrCreate()


st.session_state.spark = Spark_Data_Fusion()

st.title("Data Fusion Pipeline")
pages = {
    "": [
        st.Page(os.path.join('pages', "1_data_injestion.py"), title="Injest Data"),
        st.Page(os.path.join('pages', "2_data_processor.py"), title="Process Data"),
        st.Page(os.path.join('pages', "3_data_unifier.py"), title="Fuse Data"),
        st.Page(os.path.join('pages', "4_data_sinker.py"), title="Data Target"),
        st.Page(os.path.join('pages', "5_pipeline_builder.py"), title="Build Pipeline"),
    ]
}

pg = st.navigation(pages)
pg.run()

