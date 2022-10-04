from pyspark.sql import SparkSession
import sys
import yaml
sys.path.append("/home/dataguy/Downloads/consumer_complaint_nlp_etl_pipeline-main")
from config.config import data_source
from application_loggings import logging

spark=SparkSession.builder.master("local[*]").appName("consumerComplaintsProject").getOrCreate()

class GETDATA_FROM_HDFS:
    def __init__(self,data_source:data_source):
        self.data_path=data_source

    def read_data(self):
        try:
            logging.info("Reading the data from the hadoop distributed file system()hdfs) directory")
            data_source_hdfs=self.data_path.location_path
            self.read_data_=spark.read.parquet(data_source_hdfs)
            logging.info("Data read successfully")
        except Exception as e:
            logging.info(e)

GETDATA_FROM_HDFS(data_source="hdfs:///finance.parquet").read_data()