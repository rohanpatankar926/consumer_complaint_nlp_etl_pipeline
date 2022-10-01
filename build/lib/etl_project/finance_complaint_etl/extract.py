from finance_complaint_config.config import ExtractConfig
from six.moves import urllib
import os
from zipfile import ZipFile
from collections import namedtuple

ExtractOutput=namedtuple("ExtractOutput",["extract_dir"])

class Extract:
    def __init__(self,extract_config:ExtractConfig):
        self.extract_config=extract_config

    def download_file(self):
        try:
            download_dir=self.extract_config.download_dir
            os.makedirs(download_dir,exist_ok=True)
            url=self.extract_config.download_url
            file_name=os.path.basename(url)
            download_file_path=os.path.join(download_dir,file_name)
            urllib.request.urlretrieve(url,download_file_path)
            return download_file_path
        except Exception as e:
            raise e
    
    def extract_file(self,zip_file):
        try:
            os.makedirs(self.extract_config.extract_dir,exist_ok=True)
            extract_file=self.extract_config.extract_dir
            with ZipFile(zip_file) as zip_file:
                zip_file.extractall(extract_file)
            extract_output=ExtractOutput(extract_dir=self.extract_config.extract_dir)
            return extract_output
        except Exception as e:
            raise e

    def extraction_start(self):
        try:
            download_file_path=self.download_file()
            return self.extract_file(zip_file=download_file_path)
        except Exception as e:
            raise e
            