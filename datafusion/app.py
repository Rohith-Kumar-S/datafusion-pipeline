import streamlit as st
import os
from pyspark.sql import SparkSession
from pymongo import MongoClient


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

if "temp_datasets" not in st.session_state:
    st.session_state.temp_datasets = {}

if "views" not in st.session_state:
    st.session_state.views = {}

if "views_query" not in st.session_state:
    st.session_state.views_query = ""

if "spark" not in st.session_state:
    st.session_state.spark = None

if "temp_inputs" not in st.session_state:
    st.session_state.temp_inputs = {}

if "inputs" not in st.session_state:
    st.session_state.inputs = {}

if "total_inputs" not in st.session_state:
    st.session_state.total_inputs = 0

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

if "imported_data" not in st.session_state:
    st.session_state.imported_data = []

if "fusion_dataset_names" not in st.session_state:
    st.session_state.fusion_dataset_names = []

if "stream_data" not in st.session_state:
    st.session_state.stream_data = {}

if "part_of_stream" not in st.session_state:
    st.session_state.part_of_stream = []

if "input_loaded" not in st.session_state:
    st.session_state.input_loaded = ""
    
if "pipeline_db" not in st.session_state:
    st.session_state.pipeline_db = None

if "input_key" not in st.session_state:
    st.session_state.input_key = ""
    
if "rules_key" not in st.session_state:
    st.session_state.rules_key = ""

if "fusions_key" not in st.session_state:
    st.session_state.fusions_key = ""

if "targets_key" not in st.session_state:
    st.session_state.targets_key = ""
    
if "db_key" not in st.session_state:
    st.session_state.db_key = ""

if "main_thread_running" not in st.session_state:
    st.session_state.main_thread_running = False

if "child_threads" not in st.session_state:
    st.session_state.child_threads = []
    
if "exported_paths" not in st.session_state:
    st.session_state.exported_paths = {}
    
if "query" not in st.session_state:
    st.session_state.query = {}


# # Initialize Spark
@st.cache_resource
def Spark_Data_Fusion():
    jar_path = os.path.join("dependencies", "jars", "postgresql-42.7.7.jar")
    return (
        SparkSession.builder.config("spark.streaming.stopGracefullyOnShutdown", True)
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        )
        .config("spark.jars", jar_path)
        .config("spark.sql.shuffle.partitions", 8)
        .appName("DataFusion")
        .getOrCreate()
    )

if st.session_state.pipeline_db is None:
    client = MongoClient(os.environ["MONGO_URI"] + "/?authSource=admin")
    print("MongoDB connection established ", client.list_database_names(), flush=True)
    db = client["datafusion"]
    st.session_state.pipeline_db = db

st.session_state.spark = Spark_Data_Fusion()
print("Spark version:", st.session_state.spark.version)

st.title("Data Fusion Pipeline")
pages = {
    "": [
        st.Page(os.path.join("pages", "1_data_injestion.py"), title="Injest Data"),
        st.Page(os.path.join("pages", "2_data_processor.py"), title="Process Data"),
        st.Page(os.path.join("pages", "3_data_unifier.py"), title="Fuse Data"),
        st.Page(os.path.join("pages", "4_data_sinker.py"), title="Data Target"),
        st.Page(os.path.join("pages", "5_pipeline_builder.py"), title="Build Pipeline"),
        st.Page(
            os.path.join("pages", "6_pipeline_executor.py"), title="Execute Pipeline"
        ),
    ]
}

pg = st.navigation(pages)
pg.run()
