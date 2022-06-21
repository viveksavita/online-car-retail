import json
import os
import pandas as pd
import logging
import pytest
from src.dataprocessing.ingestion import data_ingestion_orders
from src.dataprocessing.preprocessing import data_partition
from src.analysis.reports import cancelled_onhold_orders, product_line_count,review_discount

logging.basicConfig(filename='logname.txt',filemode='a',level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

with open('config.json','r') as f:
    config = json.load(f) 


parquet_partition_path =os.path.join(os.getcwd(),config['parquet_partition_path'])
parquet_partition_on =config['parquet_partition_on']
test_dir = config['test_path']
review_prod = config['review_prod_line']

if __name__ == "__main__" :
    
    try:
        logger.info("---------------------------------------")
        logger.info("INFO: Data ingestion step is started ")
        data  = data_ingestion_orders()
        logger.info("INFO: Data ingestion step is completed ") 
    except:
        logger.error("ERROR: Error found in the data ingestion step. Please check logs")

    try:
        logger.info("INFO: Data pre-processing and partitioning step is started")
        data_partition(data, parquet_partition_path, parquet_partition_on)
        logger.info("INFO: Data pre-processing and partitioning step is completed")
        
    except:
        logger.error("ERROR: Error found in the data pre-processing step. Please check logs")

    try:
        logger.info("INFO: Analysis and reporting step is started")
        cancelled_onhold_orders(data)
        product_line_count(data)
    except:
        logger.error("ERROR: Error found in reporting step. Please check logs")
    pytest.main(args=['-s', os.path.join(os.getcwd(),test_dir,"testing.py")])
    review_discount(data, list(review_prod))




