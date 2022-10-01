import json
import os
import sys
# sys.path.append("/home/dataguy/Downloads/consumer_complaint_nlp_etl_pipeline-main")
# from application_loggings import logging
from datetime import datetime
from zipfile import ZipFile
import pendulum
import shutil
from pyspark.sql import SparkSession
import logging
from six.moves import urllib
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
spark=SparkSession.builder.master("local[*]").appName("consumerComplaintsProject").getOrCreate()

URL_KEY="url"
DOWNLOAD_DIR_KEY="download_dir"
EXTRACT_DIR_KEY="extract_dir"
EXTRACT_FILE_PATH_KEY="extract_file_path"
HDFS_FILE_KEY="hdfs_file"
ZIP_FILE_PATH="zip_file_path"
FINANCE_CONFIG_INFO_KEY="finance_config_info"


class CONSUMER_COMPLAINT_ANALYSIS:
    def __init__(self):
        pass

    def download_data(self,url:str,download_dir:str)->str:
        # logging.info(f"removing {download_dir} if exists")
        if os.path.exists(download_dir):
            shutil.rmtree(download_dir)
        
        logging.info(f"creating {download_dir} directory")
        os.makedirs(download_dir,exist_ok=True)
        # logging.info(f"downloading file from {url}")
        self.file_name=os.path.basename(url).replace(".zip","")
        self.download_file_path=os.path.join(download_dir,self.file_name)
        urllib.request.urlretrieve(url,self.download_file_path)
        return self.download_file_path

    def extract_data(self,zip_file_path:str,extract_file_dir:str)->str:
        # logging.info(f"removing {extract_file_dir} if exists")
        if os.path.exists(extract_file_dir):
            shutil.rmtree(extract_file_dir)
        # logging.info(f"creating {extract_file_dir} directory")
        os.makedirs(extract_file_dir,exist_ok=True)
        with ZipFile(zip_file_path,"r") as zip_file:
            zip_file.extractall(extract_file_dir)
        
        self.file_name=os.listdir(extract_file_dir)[0]
        self.file_path=os.path.join(extract_file_dir,self.file_name)
        return self.file_path

    def is_hdfs_file_present(self,file_path:str)->None:
        jvm=spark._jvm
        jsc=spark._jsc
        hadoop_fs=jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
        return hadoop_fs.exists(jvm.org.apache.hadoop.fs.Path(file_path))

    def load_data_to_hdfs(self,source_file_path:str,hadoop_file_path:str)->None:
        self.ingested_date_col_name="ingested_date"
        self.local_file_start_path="file:///"
        if not source_file_path.startswith("/"):
            source_file_path=source_file_path[1:]
        source_file_path=f"{self.local_file_start_path}{source_file_path}"
        print(source_file_path) 
        self.dataframe=spark.read.json(source_file_path)
        print(f"Number of record in downloaded file: {self.dataframe.select('complaint_id').count()}")
        self.current_complaint_df = self.dataframe.drop(self.dataframe._corrupt_record)
        self.current_complaint_df=self.current_complaint_df.where(self.current_complaint_df.complaint_id.isNotNull())
        if self.is_hdfs_file_present(hadoop_file_path):
            existing_complaints_data=spark.read.parquet(hadoop_file_path)
            print(f"Number of records in data we have is:{existing_complaints_data.select('complaint_id').count()}")
            self.selected_compalint_id=self.dataframe.join(existing_complaints_data,self.dataframe.complaint_id==existing_complaints_data.complaint_id,how="left").select(self.dataframe.complaint_id)
            self.current_complaint_df=self.current_complaint_df.join(self.selected_compalint_id,self.current_complaint_df.complaint_id==self.selected_compalint_id.complaint_id,how="inner").drop(self.selected_compalint_id.complaint_id)
            print(f"Number of records in current data is:{self.current_complaint_df.select('complaint_id').count()}")
            self.current_complaint_df.write.mode("append").parquet(hadoop_file_path)
        else:
            self.current_complaint_df.write.parquet(hadoop_file_path)
        print(f"Number of records in data we have is:{existing_complaints_data.select('complaint_id').count()}")


defullt_args={
    "owner":"airflow",
    "start_date": datetime(2021, 8, 1),
    "retries":5,
    "depends_on_past":False,
    "email_on_failure":True,
    "email_on_retry":True,
    "email":"rohanpatankar926@gmail.com",
    "catchup":False
}

with DAG(
    dag_id="consumer_complaints_analysis",
    default_args=defullt_args,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=["consumer_complaints_analysis"]
) as dag:
   
    def finance_config(**kwargs):
        finance_config_data=kwargs["ti"]
        url="https://files.consumerfinance.gov/ccdb/complaints.json.zip"
        download_dir="/consumer_complaint_analysis/finance/data/zip"
        extract_dir="/consumer_complaint_analysis/finance/data/json"
        hdfs_file="/user/root/consumer_complaints_analysis/finance_data.parquet"

        config={URL_KEY:url}
        config[DOWNLOAD_DIR_KEY]=download_dir
        config[EXTRACT_DIR_KEY]=extract_dir
        config[HDFS_FILE_KEY]=hdfs_file

        finance_config_data.xcom_push(key=FINANCE_CONFIG_INFO_KEY,value=config)
    
    def download_data(**kwargs):
        finance_config_data=kwargs["ti"]
        config=finance_config_data.xcom_pull(task_ids="finance_config",key=FINANCE_CONFIG_INFO_KEY)
        url=config[URL_KEY]
        download_dir=config[DOWNLOAD_DIR_KEY]
        zip_file_path=CONSUMER_COMPLAINT_ANALYSIS().download_data(url,download_dir)
        config[ZIP_FILE_PATH]=zip_file_path
        finance_config_data.xcom_push(key=FINANCE_CONFIG_INFO_KEY,value=config)

    def extract_data(**kwargs):
        finance_config_data=kwargs["ti"]
        config=finance_config_data.xcom_pull(task_ids="download",key=FINANCE_CONFIG_INFO_KEY)
        zip_file_path=config[ZIP_FILE_PATH]
        extract_dir=config[EXTRACT_DIR_KEY]
        extract_file_path=CONSUMER_COMPLAINT_ANALYSIS().extract_data(zip_file_path,extract_dir)
        config[EXTRACT_FILE_PATH_KEY]=extract_file_path
        finance_config_data.xcom_push(key=FINANCE_CONFIG_INFO_KEY,value=config)

    def store_record(**kwargs):
        finance_config_data=kwargs["ti"]
        config=finance_config_data.xcom_pull(task_ids="extract",key=FINANCE_CONFIG_INFO_KEY)
        extract_file_path=config[EXTRACT_FILE_PATH_KEY]
        hdfs_file=config[HDFS_FILE_KEY]
        CONSUMER_COMPLAINT_ANALYSIS().load_data_to_hdfs(extract_file_path,hdfs_file)
    
    config_task=PythonOperator(
        task_id="finance_config",
        python_callable=finance_config,
    )
    download_task=PythonOperator(
        task_id="download",
        python_callable=download_data,
    )
    extract_task=PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )
    store_task=PythonOperator(
        task_id="store",
        python_callable=store_record,
    )
    success_task=BashOperator(
        task_id="success",
        bash_command="echo 'success'",
    )
    email=EmailOperator(
        task_id="email",
        to="rohanpatankar926@gmail.com",
        subject="consumer_complaints_analysis",
        html_content="consumer_complaints_analysis etl pipeline successfully completed",
    )
    config_task>>download_task>>extract_task>>store_task>>success_task>>email