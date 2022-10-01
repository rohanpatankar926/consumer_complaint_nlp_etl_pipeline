from collections import namedtuple
import os 
from datetime import datetime

URL = "https://files.consumerfinance.gov/ccdb/complaints.json.zip"
ROOT_DIR=os.path.join(os.getcwd(),"data")
PipelineConfig = namedtuple("PipelineConfig", ["pipeline_dir", "archive_dir", "failed_dir","outbox_dir"])
ExtractConfig=namedtuple("ExtractConfig",["download_url","download_dir","extract_dir"])
TransformConfig = namedtuple("TransformConfig", ["transform_dir"])
LoadConfig = namedtuple("LoadConfig", ["outbox_dir", "load_dir"])
ARCHIVE_DIR_NAME = "archive"
FAILED_DIR_NAME = "failed"
OUTBOX_DIR_NAME = "outbox"

class FinaceComplaintPipelineConfig:
    def __init__(self,name="consumer_complaint_finance_analysis"):
        pipeline_dir=os.path.join(ROOT_DIR,name)
        timestamp=datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        self.pipeline_config=PipelineConfig(pipeline_dir=os.path.join(pipeline_dir,timestamp),
                                            archive_dir=os.path.join(pipeline_dir,ARCHIVE_DIR_NAME)
                                            ,failed_dir=os.path.join(pipeline_dir,FAILED_DIR_NAME),
                                            outbox_dir=os.path.join(pipeline_dir,OUTBOX_DIR_NAME)
                                            )
        
    def get_pipeline_config(self)->PipelineConfig:
        print(f"The Pipeline Configuration is as follows:{self.pipeline_config}")
        return self.pipeline_config

    def get_extraction_config(self)->ExtractConfig:
        extract_folder_name=os.path.join(self.pipeline_config.pipeline_dir,"extract")
        config=ExtractConfig(download_url=URL,
                            download_dir=os.path.join(extract_folder_name,"downloaded_files"),
                            extract_dir=os.path.join(extract_folder_name,"extracted_files")
                            )
        print(f"The extraction configuration as follows:{config}")
        return config

    def get_transform_config(self)->TransformConfig:
        transform_dir=os.path.join(self.pipeline_config.pipeline_dir,"transform")
        config=TransformConfig(transform_dir=transform_dir)
        print(f"The transformation configuration as follows:{config}")

    def get_load_config(self)->LoadConfig:
        load_config = LoadConfig(outbox_dir=self.pipeline_config.outbox_dir,
                                 load_dir=os.path.join(self.pipeline_config.pipeline_dir, "load"))
        print(f"Load config: {load_config}")
        return load_config
