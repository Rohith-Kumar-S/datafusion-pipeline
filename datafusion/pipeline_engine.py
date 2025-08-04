from pipeline_utils import PipelineUtils
from pyspark.sql import SparkSession


def Spark_Data_Fusion():  
    return SparkSession.builder.appName("DataFusion").getOrCreate()


class Pipeline:

    def __init__(self, spark=None):
        self.pipeline = []
        self.spark = spark
        if self.spark is None:
            self.spark = Spark_Data_Fusion()
        self.datasets = {}
        self.dataset_paths = {}
        self.views = {}
        self.temp_datasets = {}
        self.pipeline_utils = PipelineUtils(spark,
                                self.datasets,
                                self.dataset_paths,
                                self.views,
                                self.temp_datasets)
        self.exported_paths = []
        self.query = None
        
    def execute(self, pipeline, j, progress_data):
        """Execute the pipeline by running each layer in sequence."""
        self.pipeline = pipeline
        self.run_pipeline(j, progress_data)
        # return self.exported_paths, self.query

    def get_pipeline(self):
        return self.pipeline
    
    def get_exported_paths(self):
        """Return the paths of the datasets exported by the pipeline."""
        return self.exported_paths
    
    def get_query(self):
        """Return the query object for the pipeline."""
        return self.query

    def run_pipeline(self, j, progress_data):
        """Run the pipeline by executing each layer in sequence."""
        pipeline = self.pipeline
        pipeline_utils = self.pipeline_utils
        user_interupt = False

        if not pipeline:
            raise ValueError("Pipeline is empty. Please add layers to the pipeline.")

        # Ensure the first layer is a Source
        if pipeline[0]["layer_type"] != "Source":
            raise ValueError("The first layer must be a Source layer.")

        # Process each layer in the pipeline
        for i, layer in enumerate(pipeline):
            if layer["layer_type"] == "Source":
                print(f"Executing Source layer", flush=True)
                if layer["layer_selection"]["Link"]:
                    pipeline_utils.import_data(layer["layer_selection"]["Link"]["source_links"], "Link", from_ui=False)
                if layer["layer_selection"]["Stream"]:
                    pipeline_utils.import_data(layer["layer_selection"]["Stream"], "Stream", from_ui=False)
                print(f"Source layer executed successfully with datasets", flush=True)
            elif layer["layer_type"] == "Processor":
                print(f"Executing Processor layer", flush=True)
                success = pipeline_utils.test_rule(layer["layer_selection"], from_ui=False)
                if success:
                    print(f"Processor layer executed successfully with datasets", flush=True)
                else:
                    print(f"Processor layer execution failed. Please check the rules.", flush=True)
                    break
                
            elif layer["layer_type"] == "Fusion":
                print(f"Executing Fusion layer", flush=True)
                pipeline_utils.fuse_datasets(layer["layer_selection"][0]["fused_dataset_name"],
                                    layer["layer_selection"][0]["datasets_to_fuse"],
                                    layer["layer_selection"][0]["fuse_by"],
                                    layer["layer_selection"][0].get("fuse_on", None),
                                    layer["layer_selection"][0].get("fuse_how", None))
                print(f"Fusion layer executed successfully with datasets", flush=True)
            elif layer["layer_type"] == "Target":
                print(f"Executing Target layer", flush=True)
                self.exported_paths, self.query = pipeline_utils.export_datasets(layer["layer_selection"])
                print(f"Target layer executed successfully with datasets exported to {self.exported_paths}", flush=True)
                
            progress = int(10 + (90 / len(pipeline)) + (i * (90 / len(pipeline))))
            print(f"Progress: {progress}%", flush=True)
            progress_data[j]['progress'] = progress
            progress_data[j]['status'] = f'Layer {layer["layer_type"]} executed successfully.'
