from collections import namedtuple
from datetime import datetime
data_source=namedtuple("data_source",["location_path"])

class CONSUMER_COMPLAINTS_ML_PIPELINE:
    def __init__(self,name="consumer_complaints_analysis_pipeline") -> None:
        self.data_source=data_source(location_path="hdfs:///finance.parquet")
    
    def get_data_source(self)->str:
        return self.data_source