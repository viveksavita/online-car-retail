<<<<<<HEAD
Online car retail data pipeline repository- This project contains the data pipeline frame work and modules to transform the source data in JSON format to analytics. This repository explains the key feature and description of the structure of pipleline

# Environment Set up
* Download and install conda if you don’t have it already.
    * Use the supplied requirements file to create a new environment, or
    * conda create -n [envname] "python=3.8" pandas pyarrow pytest numpy matplotlib seaborn dask 

## Setup

* Uzip the provided the zip folder into the local machine
* Open the terminal and browse to the root directory of the project which is online-car-retail-data-pipeline on your local machine

## Files and data description


1. data - This folder contains the source data which is in the JSON format, it can have multiple yearly files
    
2. images
    - This folder contains the analytics results in the form of the images which can be used to create report
3. ingesteddata
    - This folder contains the curated data file which is the merged files in the csv format from the JSON files
4. partition
    - This folder contains the parition of the processing merged files. Each file contains the data at date level in the paraquet format

5. src
   - analysis : this directory contains the report generating script
      - reports.py : This scrip contains the module to geenrate the insights using the merged data
   - dataprocessing: this directory contains the two scrips for data ingestion and transformation.
      - ingestion.py : this script contains the logic to ingest the JSON file from source directory and merge them in to the target dataframe and save into the ingested data directory
      - preprocessing.py: this scrip contains the logic to divide the merged data into the paraquet files at data
   - testsetup: this directory contains the test script for testing.
      - testing.py : this script perform the unit testing using pytest module which involves high level testing of data and environment setup
   
6. config.json : this is the configuration file to parameterize the key interaction points in the data pipeline process.
7. logname.txt: Log file with execution progress and error details.
8. README.md : This file contains the description fo the project and instriuctions to execute the project

## Execution step
* Open the terminal 
* Browse to the project folder 
* run command : python main.py

## Results

* Check log file for execution steps 
* Check image folder for the visualization
* Check ingesteddata directory for the merged dataset
* Check reporting data directory for aggregated data into csv format
