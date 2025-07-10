from kagglehub import kagglehub
import re
from urllib.parse import urlparse
import requests
import os
import random
import string
import streamlit as st
import time
from pyspark.sql import types
import pyspark.sql.functions as F

import os

class PipelineUtils:
    """Utility class for handling data ingestion and processing in a data pipeline."""
    
    def __init__(self, spark_session=None, datasets_state={}, datasetpath_state={}, views_state={},
                 temp_datasets_state={}):
        self.datasets_state = datasets_state
        self.datasetpath_state = datasetpath_state
        self.views_state = views_state
        self.spark_session_state = spark_session
        self.temp_datasets_state = temp_datasets_state
        self.dtypes_map = { types.IntegerType(): 'int', types.StringType(): 'string', types.DoubleType(): 'double', types.FloatType(): 'float', types.BooleanType(): 'bool', types.TimestampType(): 'timestamp', types.DateType(): 'date', types.LongType(): 'long', types.ShortType(): 'short', types.ByteType(): 'byte' }
        self.reverse_dtypes_map = {v: k for k, v in self.dtypes_map.items()}

    
        
    def get_datasets_state(self):
        """Return the current state of datasets."""
        return self.datasets_state
    
    def get_datasetpath_state(self):
        """Return the current state of dataset paths."""
        return self.datasetpath_state
    def get_views_state(self):
        """Return the current state of views."""
        return self.views_state
    def get_temp_datasets_state(self):
        """Return the current state of temporary datasets."""
        return self.temp_datasets_state
    def get_user_input_state(self):
        """Return the current user input state."""
        return self.user_input_state
    

    def add_dataset(self, file_name, dataset_path):
        dataset_name = file_name.lower().replace(' ', '_')
        self.datasets_state[dataset_name] = None
        self.datasetpath_state[dataset_name] = dataset_path

    def find_files(self, directory, extensions=['.csv', '.json', '.xlsx']):
        """Find all files in the directory with the specified extensions."""
        files = []
        for root, dirs, filenames in os.walk(directory):
            for filename in filenames:
                if any(filename.endswith(ext) for ext in extensions):
                    self.add_dataset(filename, os.path.join(root, filename))
                    files.append(os.path.join(root, filename))
        return files

    def import_kaggle_data(self, dataset):
        """Import a dataset from Kaggle and return the DataFrame."""
        print(f"Importing dataset: {dataset}")
        path = kagglehub.dataset_download(dataset)
        self.find_files(path)
        print(f"Dataset downloaded to: {path}")
        return path

    def import_github_raw(self, url):
        """Import a raw file from GitHub and return the DataFrame."""
        file_name = url.split("/")[-1]
        req = requests.get(url)
        url_content = req.content
        os.makedirs('data', exist_ok=True)
        csv_file_name = os.path.join('data', file_name)
        with open(csv_file_name, 'wb') as csv_file:
            csv_file.write(url_content)
        self.add_dataset(file_name, csv_file_name)
        return csv_file_name

    def detect_source_type(self, url):
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if 'kaggle' in host.strip() and 'datasets' in path.strip():
            dataset_name = path.strip().split('datasets/')[1]
            self.import_kaggle_data(dataset_name)
        elif re.compile(r"raw\.githubusercontent\.com").search(host.strip()):
            self.import_github_raw(url)
        else:
            print('not a kaggle link:', url)

    def random_table_name(self, prefix='table_', length=8):
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
        return prefix + suffix
    
    def update_data_state_variables(self, dataset_name, data, from_ui=True):
        view_name = "_".join(dataset_name.split('.')[0].lower().split(' ')).replace("-", "_").replace(".", "_")
        view_name = "view_" + view_name
        self.views_state[dataset_name] = {"view_name": view_name }
        self.datasets_state[dataset_name] = data
        self.temp_datasets_state[dataset_name] = self.datasets_state[dataset_name]
        if from_ui:
            self.temp_datasets_state[dataset_name].cache()
            self.temp_datasets_state[dataset_name].createOrReplaceTempView(view_name)

    def import_data(self, value, import_type, from_ui=True):
        if import_type == "link": 
            urls = re.findall(r'https?://[^\s]+|http?://[^\s]+', value)
            for url in urls:
                if url == "":
                    continue
                self.detect_source_type(url)
            for dataset_name, dataset_path in self.datasetpath_state.items():
                if dataset_path is None:
                    print(f"Dataset {dataset_name} could not be imported.")
                else:
                    print(f"Importing dataset: {dataset_name} from {dataset_path}")
                    data = None
                    if dataset_path.endswith('.csv'):
                        data = self.spark_session_state.read.csv(dataset_path, header=True, inferSchema=True, nullValue='NA')
                    self.update_data_state_variables(dataset_name, data, from_ui=True)
        elif import_type == "stream":
            if value['stream_format'] == 'kafka':
                try:
                    kafka_df = (
                        st.session_state.spark
                        .read
                        .format('kafka')
                        .option('kafka.bootstrap.servers', value['bootstrap_server'])
                        .option('subscribe', value['topic'])
                        .option('startingOffsets', 'earliest')
                        .load()
                    )
                    print('Stream data imported successfully', flush=True)
                    kafka_df = kafka_df.withColumn('value', F.expr('cast(value as string)'))
                    kafka_df = kafka_df.filter(kafka_df.value.like("{%"))
                    json_sample = kafka_df.select('value').head()[0]  # Get the first row's value
                    schema = F.schema_of_json(json_sample)
                    kafka_df = kafka_df.withColumn("parsed_json", F.from_json(F.col("value"), schema)).selectExpr('parsed_json.*')
                    self.update_data_state_variables(value['dataset_name'], kafka_df, from_ui=True)
                except Exception as e:
                    print(f"Error importing stream data: {e}", flush=True)
                    st.error(f"Error importing stream data")
                    return False
        return True
        
            
    

        
    def test_rule(self, rule, from_ui=True):
        for dataset_name, processes in rule.items():
            for process in processes:
                column_map = process.get('column_map', {})
                if process['operation'] == 'Cast':
                    for column, cast_type in column_map.items():
                        if cast_type:
                            try:
                                if cast_type == 'date':
                                    self.temp_datasets_state[dataset_name] = self.temp_datasets_state[dataset_name].withColumn(column, F.to_date(F.col(column), 'MM-dd-yy'))
                                else:
                                    self.temp_datasets_state[dataset_name] = self.temp_datasets_state[dataset_name].withColumn(column, self.temp_datasets_state[dataset_name][column].cast(self.reverse_dtypes_map[cast_type]))
                                if from_ui:
                                    view_name = self.views_state[dataset_name]["view_name"]
                                    self.temp_datasets_state[dataset_name].createOrReplaceTempView(view_name)
                                    st.toast('Rule applied successfully!', icon="✅")
                                    time.sleep(.3)
                            except Exception as e:
                                if from_ui:
                                    st.error(f"Error casting column {column} to {cast_type}: {e}")
                                    time.sleep(.3)
                elif process['operation'] == 'Explode':
                    column_name = column_map.get('new_column_name', '')
                    data_source = column_map.get('column_data_source', '')
                    print(f"Exploding column {column_name} from data source {data_source}", flush=True)
                    try:
                        df = st.session_state.temp_datasets[dataset_name]
                        df = df.withColumn(column_name, F.explode(data_source))
                        st.session_state.temp_datasets[dataset_name] = df
                        view_name = st.session_state.views[dataset_name]["view_name"]
                        st.session_state.temp_datasets[dataset_name].createOrReplaceTempView(view_name)
                        st.toast('Rule applied successfully!', icon="✅")
                        time.sleep(.3)
                    except Exception as e:
                        st.error(f"Error exploding column {column_name}: {e}")
                                    
    def get_fusable_columns(self, datasets):
        dataset = datasets[0]
        fusable_columns = []
        for dtype in self.temp_datasets_state[dataset].dtypes:
            dtype_present = True
            for i in range(1, len(datasets)):
                if dtype not in self.temp_datasets_state[datasets[i]].dtypes:
                    dtype_present = False
                    break
            if dtype_present:
                fusable_columns.append(dtype[0])
        return fusable_columns
    
    def fuse_datasets(self, fusion_name, datasets_to_fuse, fuse_by='columns', fusable_columns=[]):
        dataset_1 =  self.temp_datasets_state[datasets_to_fuse[0]]
        for dataset in datasets_to_fuse[1:]:
            if fuse_by == 'columns':
                dataset_1 = dataset_1.select(fusable_columns).union(self.temp_datasets_state[dataset].select(fusable_columns))
        self.temp_datasets_state[fusion_name] = dataset_1
           
    def export_datasets(self, export_data):
        print(f"Exporting datasets: ###################################################")
        datasets = export_data['datasets']
        paths = []
        if export_data['type'] == 'external':
            for dataset in datasets:
                if dataset.endswith('.parquet'):
                    self.temp_datasets_state[dataset].write.parquet(dataset)
                elif  dataset.endswith('.csv'):
                    
                    self.temp_datasets_state[dataset].toPandas().to_csv(dataset, index=False)
                paths.append(dataset)
        return paths

    def test_spark(self):
        print(self.spark_session_state)
