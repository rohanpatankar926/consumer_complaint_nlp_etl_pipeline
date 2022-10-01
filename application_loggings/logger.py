import logging
import os
import sys
from datetime import datetime

LOG_DIR="logs"
MAIN_DIR_LOGS=os.path.join(os.getcwd(),LOG_DIR)

os.makedirs(MAIN_DIR_LOGS,exist_ok=True)
CURRENT_TIME_STAMP = f"{datetime.now().strftime('%Y_%m_%d_%H')}"
file_name = f"log_{CURRENT_TIME_STAMP}.log"

log_file_path = os.path.join(LOG_DIR, file_name)

logging.basicConfig(filename=log_file_path,
                    filemode='w',
                    format='[%(asctime)s] %(name)s - %(levelname)s - %(message)s',
                    level=logging.NOTSET)