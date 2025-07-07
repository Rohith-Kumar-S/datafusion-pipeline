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
        
    def execute(self, pipeline, statusbar=None):
        """Execute the pipeline by running each layer in sequence."""
        self.pipeline = pipeline
        self.run_pipeline(statusbar)
        return self.exported_paths

    def get_pipeline(self):
        return self.pipeline
    
    def get_exported_paths(self):
        """Return the paths of the datasets exported by the pipeline."""
        return self.exported_paths

    def run_pipeline(self, statusbar=None):
        """Run the pipeline by executing each layer in sequence."""
        pipeline = self.pipeline
        pipeline_utils = self.pipeline_utils

        if not pipeline:
            raise ValueError("Pipeline is empty. Please add layers to the pipeline.")

        # Ensure the first layer is a Source
        if pipeline[0]["layer_type"] != "Source":
            raise ValueError("The first layer must be a Source layer.")

        # Process each layer in the pipeline
        for i, layer in enumerate(pipeline):
            if layer["layer_type"] == "Source":
                print(f"Executing Source layer")
                if layer["layer_selection"]["source_type"] == "Link":
                    pipeline_utils.import_data(layer["layer_selection"]["source_links"], from_ui=False)
                print(f"Source layer executed successfully with datasets")
            elif layer["layer_type"] == "Processor":
                print(f"Executing Processor layer")
                pipeline_utils.test_rule(layer["layer_selection"])
                print(f"Processor layer executed successfully with datasets")
            elif layer["layer_type"] == "Fusion":
                print(f"Executing Fusion layer")
                pipeline_utils.fuse_datasets(layer["layer_selection"][0]["fused_dataset_name"],
                                    layer["layer_selection"][0]["datasets_to_fuse"],
                                    layer["layer_selection"][0]["fuse_by"],
                                    layer["layer_selection"][0]["fusable_columns"])
                print(f"Fusion layer executed successfully with datasets")
            elif layer["layer_type"] == "Target":
                print(f"Executing Target layer")
                self.exported_paths = pipeline_utils.export_datasets(layer["layer_selection"])
                print(f"Target layer executed successfully with datasets exported to {self.exported_paths}")
            statusbar.progress(int(10 + (90 / len(pipeline)) + (i * (90 / len(pipeline)))), text=f'Layer {layer["layer_type"]} executed successfully.')
