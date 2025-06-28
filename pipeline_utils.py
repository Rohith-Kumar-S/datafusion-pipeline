from kagglehub import kagglehub
import re
from urllib.parse import urlparse
import requests
import os
import random
import string
import streamlit as st


class PipelineUtils:
    """Utility class for handling data ingestion and processing in a data pipeline."""
    
    def __init__(self, spark_session=None, datasets_state={}, datasetpath_state={}, views_state={},
                 temp_datasets_state={}, user_input_state=""):
        self.datasets_state = datasets_state
        self.datasetpath_state = datasetpath_state
        self.views_state = views_state
        self.spark_session_state = spark_session
        self.temp_datasets_state = temp_datasets_state
        self.user_input_state = user_input_state
        self.HOST_PATTERNS = {
            "kaggle": {'matcher': re.compile(r"kaggle\.com"), 'function': self.import_kaggle_data},
            "github_raw": {'matcher': re.compile(r"raw\.githubusercontent\.com"), 'function': self.import_github_raw}
        }

        self.EXTENSIONS = {
            ".csv": "direct_file",
            ".json": "direct_file",
            ".xlsx": "direct_file",
            ".nc": "direct_file",
            # add more as needed
        }

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

    def import_data(self, links, from_ui=True):
        if not links == "": 
            urls = re.findall(r'https?://[^\s]+|http?://[^\s]+', links)
            for url in urls:
                if url == "":
                    continue
                self.detect_source_type(url)
            for dataset_name, dataset_path in self.datasetpath_state.items():
                if dataset_path is None:
                    print(f"Dataset {dataset_name} could not be imported.")
                else:
                    print(f"Importing dataset: {dataset_name} from {dataset_path}")
                    view_name = "_".join(dataset_name.split('.')[0].lower().split(' ')).replace("-", "_").replace(".", "_")
                    view_name = "view_" + view_name
                    self.views_state[dataset_name] = {"view_name": view_name }
                    self.datasets[dataset_name] = self.spark_session_state.read.csv(dataset_path, header=True, inferSchema=True, nullValue='NA')
                    self.temp_datasets[dataset_name] = self.datasets[dataset_name]
                    if from_ui:
                        self.temp_datasets[dataset_name].createOrReplaceTempView(view_name)
            self.user_input = links
