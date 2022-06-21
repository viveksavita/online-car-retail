from asyncio.log import logger
import os
import json
import logging
import pyarrow as pa
import pandas as pd
import numpy as np
from datetime import datetime
import dask.dataframe as dd


def data_partition(pandas_dataframe, parquet_partition_path, parquet_partition_on):
    SCHEMA = pa.schema([
        ("ORDERNUMBER", pa.int64()),
        ("PRODUCTCODE", pa.string()),
        ("QUANTITYORDERED",pa.int64()),
        ("PRICEEACH", pa.float64()),
        ("SALES", pa.float64()),
       ("ORDERDATE", pa.date64()), 
        ("STATUS", pa.string()), 
         ("PRODUCTLINE", pa.string()), 
         ("MSRP", pa.int64())
         ])
    ddf = dd.from_pandas(pandas_dataframe, npartitions=1)
    try:
        ddf.to_parquet(os.path.join(parquet_partition_path), 
        engine="pyarrow", 
        partition_on=[parquet_partition_on],
        schema=SCHEMA)
    except ValueError:
        logger.error("ERROR: Partitioning on non-existent column")