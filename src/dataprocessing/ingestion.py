import pandas as pd
import numpy as np
import os
import json
import logging
from datetime import datetime
import dask.dataframe as dd
import pyarrow as pa


logging.basicConfig(filename='logname.txt',filemode='a',level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


#############Load config.json and get input and output paths
try:
    with open('config.json','r') as f:
        config = json.load(f) 
    logger.info("INFO: Initializing the path variables from the config file")

    input_path =os.path.join(os.getcwd(),config['input_folder_path'])
    output_path =os.path.join(os.getcwd(),config['output_folder_path'])
    columns = list(config['column_order'])
except FileNotFoundError:
    logger.error("ERROR: Config.json file not found in the root directory")


#############Function for data ingestion
def data_ingestion_orders() -> pd.DataFrame:
    final_dataframe = pd.DataFrame()
    try:
        filenames = os.listdir(input_path)
        logger.info("File reading intialized")
        
        for each_filename in filenames:
            with open(os.path.join(input_path,each_filename),'r') as f:
                data = json.loads(f.read())
            
            df = pd.json_normalize(data, record_path =['attributes'],meta=list(config['meta_columns']))
            df = df[columns]
            df["ORDERDATE"] = pd.to_datetime(df["ORDERDATE"])
            final_dataframe = pd.concat([final_dataframe,df])
            logger.info("INFO: File ingestion completed :" + str(each_filename) +"| Record ingested :"+ str(len(df)) )
    
    except FileNotFoundError:
        logger.error ("ERROR : data directory does not exist in the root directory")

   
    final_dataframe.to_csv( os.path.join(output_path,"curateddata.csv"), index=False)
    logger.info("INFO: Final dataset is created with row count of "+str(len(final_dataframe)))

    return final_dataframe


