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

    def __init__(
        self,
        spark_session=None,
        datasets_state={},
        datasetpath_state={},
        views_state={},
        temp_datasets_state={},
        pipeline_db=None,
    ):
        self.datasets_state = datasets_state
        self.datasetpath_state = datasetpath_state
        self.views_state = views_state
        self.spark_session_state = spark_session
        self.temp_datasets_state = temp_datasets_state
        self.dtypes_map = {
            types.IntegerType(): "int",
            types.StringType(): "string",
            types.DoubleType(): "double",
            types.FloatType(): "float",
            types.BooleanType(): "bool",
            types.TimestampType(): "timestamp",
            types.DateType(): "date",
            types.LongType(): "long",
            types.ShortType(): "short",
            types.ByteType(): "byte",
        }
        self.reverse_dtypes_map = {v: k for k, v in self.dtypes_map.items()}
        self.pipeline_db = pipeline_db
        self.inputs = None
        self.rules = None
        self.fusions = None
        self.targets = None
        self.pipelines = None
        self.active_streams = None

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

    def get_inputs(self):
        """Return the current state of inputs."""
        return self.inputs
    
    def get_rules(self):
        """Return the current state of rules."""
        return self.rules
    
    def get_fusions(self):
        """Return the current state of fusions."""
        return self.fusions
    
    def get_targets(self):
        """Return the current state of targets."""
        return self.targets
    
    def get_pipelines(self):
        """Return the current state of pipelines."""
        return self.pipelines

    def get_active_streams(self):
        """Return the current state of active streams."""
        return self.active_streams

    def add_dataset(self, file_name, dataset_path):
        dataset_name = file_name.lower().replace(" ", "_")
        self.datasetpath_state[dataset_name] = dataset_path

    def find_files(self, directory, extensions=[".csv", ".json", ".xlsx"]):
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
        os.makedirs("data", exist_ok=True)
        csv_file_name = os.path.join("data", file_name)
        with open(csv_file_name, "wb") as csv_file:
            csv_file.write(url_content)
        self.add_dataset(file_name, csv_file_name)
        return csv_file_name

    def detect_source_type(self, url):
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if "kaggle" in host.strip() and "datasets" in path.strip():
            dataset_name = path.strip().split("datasets/")[1]
            self.import_kaggle_data(dataset_name)
        elif re.compile(r"raw\.githubusercontent\.com").search(host.strip()):
            self.import_github_raw(url)
        else:
            print("not a kaggle link:", url)

    def random_table_name(self, prefix="table_", length=8):
        suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=length)
        )
        return prefix + suffix

    def update_data_state_variables(self, dataset_name, data, from_ui=False):
        view_name = (
            "_".join(dataset_name.lower().split(" "))
            .replace("-", "_")
            .replace(".", "_")
        )
        view_name = "view_" + view_name
        self.views_state[dataset_name] = {"view_name": view_name}
        self.datasets_state[dataset_name] = data
        self.temp_datasets_state[dataset_name] = self.datasets_state[dataset_name]
        if from_ui:
            self.temp_datasets_state[dataset_name].cache()
            self.temp_datasets_state[dataset_name].createOrReplaceTempView(view_name)

    def import_data(self, value, import_type, from_ui=True):
        dataframe_names = []
        if import_type == "Link":
            urls = re.findall(r"https?://[^\s]+|http?://[^\s]+", value)
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
                    if dataset_path.endswith(".csv"):
                        data = self.spark_session_state.read.csv(
                            dataset_path, header=True, inferSchema=True, nullValue="NA"
                        )

                    self.update_data_state_variables(
                        dataset_name.split(".")[0], data, from_ui
                    )
                    dataframe_names.append(dataset_name.split(".")[0])
        elif import_type == "Stream":
            if value["stream_from"] == "kafka":
                try:
                    kafka_df = None
                    if from_ui:
                        kafka_df = (
                            self.spark_session_state.read.format("kafka")
                            .option("kafka.bootstrap.servers", value["server_spec"])
                            .option("subscribe", value["topic_to_subscribe"])
                            .option("startingOffsets", "earliest")
                            .load()
                        )
                    else:
                        kafka_df = (
                            self.spark_session_state.readStream.format("kafka")
                            .option("kafka.bootstrap.servers", value["server_spec"])
                            .option("subscribe", value["topic_to_subscribe"])
                            .option("startingOffsets", "earliest")
                            .load()
                        )
                    print("Stream data imported successfully", flush=True)
                    kafka_df = kafka_df.withColumn(
                        "value", F.expr("cast(value as string)")
                    )
                    kafka_df = kafka_df.filter(kafka_df.value.like("{%"))
                    json_sample = value["schema_skeleton"]
                    schema = F.schema_of_json(json_sample)
                    kafka_df = kafka_df.withColumn(
                        "parsed_json", F.from_json(F.col("value"), schema)
                    ).selectExpr("parsed_json.*")
                    self.update_data_state_variables(
                        value["dataframe_name"], kafka_df, from_ui
                    )
                    dataframe_names.append(value["dataframe_name"])
                except Exception as e:
                    print(f"Error importing stream data: {e}", flush=True)
                    if from_ui:
                        st.error(f"Error importing stream data: {e}")
                    return False
        return True, dataframe_names

    def reset_views(self, dataset_name):
        self.temp_datasets_state[dataset_name] = self.datasets_state[dataset_name]

    def test_rule(self, rule, from_ui=True):
        for dataset_name, processes in rule.items():
            self.reset_views(dataset_name)
            for process in processes:
                data_map = process.get("data_map", {})
                if not data_map:
                    return False
                match process["operation"]:
                    case "Cast":
                        column_to_cast = data_map.get("column_to_cast", None)
                        cast_type = data_map.get("cast_type", None)
                        if cast_type and column_to_cast:
                            try:
                                if cast_type == "date":
                                    self.temp_datasets_state[
                                        dataset_name
                                    ] = self.temp_datasets_state[
                                        dataset_name
                                    ].withColumn(
                                        column_to_cast, F.to_date(F.col(column_to_cast), "MM-dd-yy")
                                    )
                                else:
                                    self.temp_datasets_state[
                                        dataset_name
                                    ] = self.temp_datasets_state[
                                        dataset_name
                                    ].withColumn(
                                        column_to_cast,
                                        self.temp_datasets_state[dataset_name][
                                            column_to_cast
                                        ].cast(self.reverse_dtypes_map[cast_type]),
                                    )
                                if from_ui:
                                    view_name = self.views_state[dataset_name][
                                        "view_name"
                                    ]
                                    self.temp_datasets_state[
                                        dataset_name
                                    ].createOrReplaceTempView(view_name)
                                    st.toast(
                                        "Cast applied successfully!", icon="✅"
                                    )
                                    time.sleep(0.3)
                            except Exception as e:
                                if from_ui:
                                    st.error(
                                        f"Error casting column {column_to_cast} to {cast_type}: {e}"
                                    )
                                    time.sleep(0.3)
                        else:
                            if not from_ui:
                                print(
                                    "Column to cast and cast type cannot be empty.",
                                    flush=True,
                                )
                    case "Delete":
                        drop_by = data_map.get("drop_by", "")
                        data_reference = data_map.get("data_reference", "")
                        if drop_by == "column":
                            self.temp_datasets_state[dataset_name] = (
                                self.temp_datasets_state[dataset_name].drop(
                                    data_reference
                                )
                            )
                        if from_ui:
                            view_name = self.views_state[dataset_name]["view_name"]
                            self.temp_datasets_state[
                                dataset_name
                            ].createOrReplaceTempView(view_name)
                            st.toast("Delete applied successfully!", icon="✅")
                            time.sleep(0.3)
                    case "Explode":
                        column_name = data_map.get("new_column_name", "")
                        data_source = data_map.get("data_reference", "")
                        print(
                            f"Exploding column {column_name} from data source {data_source}",
                            flush=True,
                        )
                        try:
                            df = self.temp_datasets_state[dataset_name]
                            df = df.withColumn(column_name, F.explode(data_source))
                            self.temp_datasets_state[dataset_name] = df
                            if from_ui:
                                view_name = self.views_state[dataset_name]["view_name"]
                                self.temp_datasets_state[
                                    dataset_name
                                ].createOrReplaceTempView(view_name)
                                st.toast("Explode applied successfully!", icon="✅")
                                time.sleep(0.3)
                        except Exception as e:
                            if from_ui:
                                st.error(f"Error exploding column {column_name}: {e}")
                            else:
                                print(f"Error exploding column {column_name}: {e}", flush=True)
                    case "Flatten":
                        column_name = data_map.get("new_column_name", "")
                        data_source = data_map.get("data_reference", "")
                        print(
                            f"Flattening column {column_name} from data source {data_source}",
                            flush=True,
                        )
                        try:
                            df = self.temp_datasets_state[dataset_name]
                            df = df.withColumn(column_name, F.col(data_source))
                            self.temp_datasets_state[dataset_name] = df
                            if from_ui:
                                view_name = self.views_state[dataset_name]["view_name"]
                                self.temp_datasets_state[
                                    dataset_name
                                ].createOrReplaceTempView(view_name)
                                st.toast("Flatten applied successfully!", icon="✅")
                                time.sleep(0.3)
                        except Exception as e:
                            if from_ui:
                                st.error(f"Error flattening column {column_name}: {e}")
                            else:
                                print(f"Error flattening column {column_name}: {e}", flush=True)
                    case "Save":
                        new_dataframe_name = data_map.get("save_as", "")
                        if new_dataframe_name not in self.temp_datasets_state:
                            self.temp_datasets_state[new_dataframe_name] = (
                                self.temp_datasets_state[dataset_name]
                            )
                            self.views_state[new_dataframe_name] = {
                                "view_name": self.random_table_name(new_dataframe_name)
                            }
                            if from_ui:
                                view_name = self.views_state[dataset_name]["view_name"]
                                self.temp_datasets_state[
                                    dataset_name
                                ].createOrReplaceTempView(view_name)
                                st.toast("Save applied successfully!", icon="✅")
                                time.sleep(0.3)
        return True

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

    def fuse_datasets(
        self, fusion_name, datasets_to_fuse, fuse_by=None, fuse_how="inner"
    ):
        dataset_1 = self.temp_datasets_state[datasets_to_fuse[0]]
        for dataset in datasets_to_fuse[1:]:
            dataset_1 = dataset_1.join(
                self.temp_datasets_state[dataset], on=fuse_by, how=fuse_how
            )
        self.temp_datasets_state[fusion_name] = dataset_1

    def export_datasets(self, export_data):
        print(
            f"Exporting datasets: ###################################################",
            flush=True,
        )
        datasets = export_data["datasets"]
        paths = []
        query = None
        if export_data["type"] == "download":
            for dataset in datasets:
                requested_conversion = export_data["conversions"][dataset]
                is_part_of_stream = dataset in export_data["part_of_stream"]
                if requested_conversion == "parquet":
                    self.temp_datasets_state[dataset].write.parquet(dataset)

                elif requested_conversion == "csv":
                    # self.temp_datasets_state[dataset].write.csv(
                    #     dataset, header=True, mode="overwrite"
                    # )
                    if is_part_of_stream:
                        self.temp_datasets_state[dataset].writeStream.format(
                            "csv"
                        ).option("path", f"{dataset}.{requested_conversion}").start()
                    else:
                        self.temp_datasets_state[dataset].toPandas().to_csv(
                            f"{dataset}.{requested_conversion}", index=False
                        )
                paths.append(f"{dataset}.{requested_conversion}")
        elif export_data["type"] == "console":
            for dataset in datasets:
                is_part_of_stream = dataset in export_data["part_of_stream"]
                if is_part_of_stream:
                    query = (
                        self.temp_datasets_state[dataset]
                        .writeStream.format("console")
                        .outputMode("append")
                        .option("checkpointLocation", "checkpoint_dir")
                        # .start()
                    )
                # try:
                #     query.awaitTermination()
                # except KeyboardInterrupt:
                #     query.stop()
        elif export_data["type"] == "jdbc":
            for dataset in datasets:
                is_part_of_stream = dataset in export_data["part_of_stream"]

                def device_data_output(df, batch_id):
                    print("Batch id :", str(batch_id))

                    (
                        df.write.mode("append")
                        .format("jdbc")
                        .option("driver", "org.postgresql.Driver")
                        .option("url", export_data["jdbc_url"])
                        .option("dbtable", export_data["jdbc_table_name"])
                        .option("user", export_data["jdbc_username"])
                        .option("password", export_data["jdbc_password"])
                        .save()
                    )

                    print(df.show())

                if is_part_of_stream:
                    print("Streaming data to JDBC#########################", flush=True)
                    query = (
                        self.temp_datasets_state[dataset]
                        .writeStream.foreachBatch(device_data_output)
                        .trigger(processingTime="10 seconds")
                        .option(
                            "checkpointLocation",
                            os.path.join("checkpoint_dir", dataset),
                        )
                        # .start()
                    )
                    # try:
                    #     query.awaitTermination()
                    # except Exception as e:
                    #     query.stop()
                else:
                    device_data_output(self.temp_datasets_state[dataset], 0)
        return paths, query

    def test_spark(self):
        print(self.spark_session_state)

    def load_input_sources(self, read_only=False):
        input_key = None
        input_sources = self.pipeline_db.input_sources
        if not read_only and not list(input_sources.find()):
            input_sources.insert_one({"value": {}})
        for input_source in input_sources.find():
            # print("Input Source:", input_source, flush=True)
            input_key = input_source["_id"]
            self.inputs = input_source["value"]
        return input_key, input_sources

    def load_rules(self, input_source=None, read_only=False):
        rules_key = None
        if input_source!=None:
            db_rules = self.pipeline_db.db_rules
            if not read_only and not list(
                db_rules.find({"input_source": input_source})
            ):
                db_rules.insert_one(
                    {"input_source": input_source, "value": {}}
                )
            for rules_in_db in db_rules.find(
                {"input_source": input_source}
            ):
                # print("rules_in_db:", rules_in_db, flush=True)
                rules_key = rules_in_db["_id"]
                self.rules = rules_in_db["value"]
            return rules_key, db_rules

    def load_fusions(self, input_source=None, read_only=False):
        fusions_key = None
        if input_source !=None:
            db_fusions = self.pipeline_db.db_fusions
            if not read_only and not list(
                db_fusions.find({"input_source": input_source})
            ):
                db_fusions.insert_one(
                    {"input_source": input_source, "value": {}}
                )
            for fusions_in_db in db_fusions.find(
                {"input_source": input_source}
            ):
                # print("fusions_in_db:", fusions_in_db, flush=True)
                fusions_key = fusions_in_db["_id"]
                self.fusions = fusions_in_db["value"]
            return fusions_key, db_fusions

    def load_targets(self, input_source=None, read_only=False):
        targets_key = None
        if input_source != None:
            db_targets = self.pipeline_db.db_targets
            if not read_only and not list(
                db_targets.find({"input_source": input_source})
            ):
                db_targets.insert_one(
                {"input_source": input_source, "value": {}}
            )
            for targets_in_db in db_targets.find(
                {"input_source": input_source}
            ):
                # print("targets_in_db:", targets_in_db, flush=True)
                targets_key = targets_in_db["_id"]
                self.targets = targets_in_db["value"]
            return targets_key, db_targets

    def load_pipelines(self, read_only=False):
        pipe_key = None
        db_pipes = self.pipeline_db.db_pipes
        if not read_only and not list(db_pipes.find()):
            db_pipes.insert_one({"value": {}})
        for pipe in db_pipes.find():
            # print("Pipeline:", pipe, flush=True)
            pipe_key = pipe["_id"]
            self.pipelines = pipe["value"]
        return pipe_key, db_pipes

    def load_active_streams(self):
        active_pipes = None
        db_active_pipe_streams = self.pipeline_db.db_active_pipe_streams
        if not list(db_active_pipe_streams.find()):
            db_active_pipe_streams.insert_one({"value": {}})
        for pipe in db_active_pipe_streams.find():
            # print("Active Pipeline:", pipe, flush=True)
            active_pipes = pipe["value"]
        return active_pipes
    
    
