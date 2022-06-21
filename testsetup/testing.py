import os
import json
import pytest
import logging
import pandas as pd
import numpy as np




try:
    with open('config.json','r') as f:
        config = json.load(f) 
except FileNotFoundError:
    logger.error("ERROR: Config.json file not found in the root directory")

@pytest.fixture(scope='session')
def ingested_data(request):
    data_path = os.path.join(os.getcwd(),config['output_folder_path'],"curateddata.csv")
    if data_path is None:
        pytest.fail("You must provide the --csv option on the command line")

    data = pd.read_csv(data_path)

    return data

def test_folder_setup():
    root_path = os.getcwd()
    folders = ['data','ingesteddata','partition','src']
    for folder in folders:
        assert os.path.isdir(os.path.join(root_path, folder)) == True


def test_duplicate_data(ingested_data):
    duplicate_data = ingested_data[ingested_data.duplicated()]
    assert duplicate_data.empty
   


def test_column_names(ingested_data):

    expected_colums = [
        "ORDERNUMBER",
        "PRODUCTCODE",
        "QUANTITYORDERED",
        "PRICEEACH",
        "SALES",
        "ORDERDATE",
        "STATUS",
        "PRODUCTLINE",
        "MSRP"
    ]

    these_columns = ingested_data.columns.values
    print(ingested_data.columns.values)

    # This also enforces the same order
    assert list(expected_colums) == list(these_columns)

def test_status_values(ingested_data):

    known_values = ['Shipped', 'Resolved', 'On Hold', 'Disputed', 'In Process',
       'Cancelled']

    status = set(ingested_data['STATUS'].unique())
    assert set(known_values) == set(status)

def test_product_line_values(ingested_data):

    known_values = ['Motorcycles', 'Classic Cars', 'Vintage Cars', 'Trucks and Buses',
       'Trains', 'Planes', 'Ships']

    productline = set(ingested_data['PRODUCTLINE'].unique())
    assert set(known_values) == set(productline)


