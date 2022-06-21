from operator import index
import os
import json
import numpy as np
import pandas as pd
import seaborn as sns
import logging
import matplotlib.pyplot as plt


logging.basicConfig(filename='logname.txt',filemode='a',level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()



try:
    with open('config.json','r') as f:
        config = json.load(f) 
        image_output = os.path.join(os.getcwd(),config['image_analysis'])
        report_path =  os.path.join(os.getcwd(),config['report_data'])
except FileNotFoundError:
    logger.error("ERROR: Unable to read Config.json file in the root directory")



def ingested_data(request):
    data_path = os.path.join(os.getcwd(),config['output_folder_path'],"curateddata.csv")
    try:
        data = pd.read_csv(data_path)
    except FileNotFoundError:
        logger.error("ERROR: Unable to read dataset for reporting")
    return data



def cancelled_onhold_orders(data):
    try:
        data["year"] = data["ORDERDATE"].dt.year
        agg_data = data.groupby(["STATUS","year"])["SALES"].sum().reset_index(name = "value")
        agg_data.to_csv(os.path.join(report_path,"cancelled_onhold_order.csv"), index= False)
        agg_data = agg_data[agg_data["STATUS"].isin(["Cancelled","On Hold"])]
        g = sns.catplot(
        data=agg_data, kind="bar",
        x="year", y="value", hue="STATUS",
        ci="sd", palette="dark", alpha=.4, height=4
        )
        g.despine(left=True)
        g.set_axis_labels("Year", "Sales value")
        plt.savefig(os.path.join(image_output,'cancelled_onhold_analysis.png'))
        logger.info ("INFO: cancelled and on hold orders report is generate in the image folder")
    except:
        logger.error("ERROR: Unable to generate reports for cancelled and onhold orders")




def product_line_count(data):
    try:
        plt.figure(figsize=(10,10))
        agg_data = data.groupby(["PRODUCTLINE"])["PRODUCTCODE"].nunique().reset_index(name = "value")
        agg_data.to_csv(os.path.join(report_path,"products_in_productlines.csv"), index=False)
        _=sns.barplot(data = agg_data , y = "PRODUCTLINE" ,x = "value",palette="dark")
        plt.savefig(os.path.join(image_output,'products in productline.png'))
        logger.info ("INFO: Product line analysis  is generate in the image folder")
    except:
        logger.error("ERROR: Unable to generate productline reports due error in data")





def review_discount(data, product_line):
    try:
        logger.info("INFO: Reveiwing the discounted on the selected productline")
        filt_data =  data[data["PRODUCTLINE"].isin(product_line)]
        conditions = [
            ( filt_data["QUANTITYORDERED"] >= 0) & (filt_data["QUANTITYORDERED"] <=30),
            ( filt_data["QUANTITYORDERED"] >= 31) & (filt_data["QUANTITYORDERED"] <=60 ),
            ( filt_data["QUANTITYORDERED"] >= 61) & (filt_data["QUANTITYORDERED"] <=80 ),
            ( filt_data["QUANTITYORDERED"] >= 81) & (filt_data["QUANTITYORDERED"] <=100 ),
            ( filt_data["QUANTITYORDERED"] > 100)
        ]

        values = [0, 0.025, 0.04,0.06,0.1]

        filt_data["DISCOUNT"] = np.select(conditions, values)
        filt_data["LOGIC_DISCOUNT"] = filt_data["PRICEEACH"] - (filt_data["PRICEEACH"] * filt_data["DISCOUNT"])
        filt_data = filt_data[filt_data["PRICEEACH"] != filt_data["LOGIC_DISCOUNT"]]
        filt_data.to_csv(os.path.join(report_path,"Data quality issue - review discount.csv"), index=False)
    except:
        logger.error("ERROR: Error occured in reviewing the discount logic function")

        
