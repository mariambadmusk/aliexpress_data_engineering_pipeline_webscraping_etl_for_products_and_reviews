import logging
import os
import glob
import re
from dotenv import load_dotenv


# setup logging
def setup_logging():
    logging.basicConfig(filenme='app.log', 
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d:  %(message)s'
    )

    return logging.getLogger(__name__)


# convert csv to df


# find raw data csv file

def find_rawdata_file(category):
    load_dotenv()
    timestamp_pattern = r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
    parent_dir = os.getenv("RAWDATA_DIR")
    path = os.path.join(parent_dir, f"rawdata-*{category}-{timestamp_pattern}.json")
    match_file = glob.glob(path)
    valid_file = [file for file in match_file
                  if re.search(timestamp_pattern, os.path.basename(file))
                ]
   
    return valid_file

