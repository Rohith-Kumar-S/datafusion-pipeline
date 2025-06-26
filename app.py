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
    
if "rules" not in st.session_state:
    st.session_state.rules = {}

if "total_rules" not in st.session_state:
    st.session_state.total_rules = 0
    
if "cast" not in st.session_state:
    st.session_state.cast = {}

    
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
        st.Page(os.path.join('pages', "2_data_processor.py"), title="Data Processor"),
    ]
}

pg = st.navigation(pages)
pg.run()

