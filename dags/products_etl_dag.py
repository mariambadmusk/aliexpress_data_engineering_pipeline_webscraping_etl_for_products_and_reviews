from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airfow.operators.python import PythonOperator
from airflow.decorators import task
from extract import Aliexpress_products_scraper
from transform import mainTransformAliexpressProductsDetails
from utils import setup_logging, find_rawdata_file
from datetime import datetime, timedelta
import pandas as pd
import os


# intialise logging
logger = setup_logging()


# define default arguments
default_args = {
    "owner": "kike",
    "depends_on_past":False,
    "retries": 1,
    "retries_delay": timedelta(minutes=2),
    "start_date":datetime(), #add date

}

# define functions
def start_log():
    logger.info(f"ETL pipeline for {category} category started")

def scrape_products(category):
    logger.info(f"Extracting products listings for category: {category}"),
    products = Aliexpress_products_scraper()
    products.extract_products_listings(category)

def transform_data(ti, categoryID, category):
    logger.info(f"Consuming rawdata file for category: {category}")
    path = ti.xcom_pull(task_ids=f"consume_rawdata_file_{category}")
    logger.info(f"Transforming data for category: {category}")
    mainTransformAliexpressProductsDetails(path, category, categoryID)
    

def end_log():
    logger.info(f"ETL pipeline for {category} category completed")


# define helper functions for reading and writing processed categories
def read_processed_categories():
    if not os.path.exists("processed_category_log.log"):
        return []
    else:
        with open("processed_category_log.log", "r") as file:
            processed_categories =  file.read().splitlines()
        return processed_categories

def write_processed_categories(category):
    with open("processed_categories_log.log", "a") as file:
        file.write(f"{category}\n")
    
        

# Read category mapping and processed categories log
category_mapping = pd.read_csv("category_mapping.csv")
processed_categories = read_processed_categories() 


# define DAG

with DAG(
    dag_id = "aliexpress_products_pipeline",
    default_args=default_args,
    description ="ETL pipeline for Aliexpress product data",
    schedule_interval=timedelta(days=2),
    catchup=False,
) as dag:
    

    start_task = DummyOperator(task_id="start_task")
    end_task = DummyOperator(task_id="end_task")


    start_log_task = PythonOperator(
        task_id = "start_log",
        python_callable=start_log,
    ) 


     
    for categoryID, category in category_mapping.items():

        if category in processed_categories:
            continue
      
        extract_task = PythonOperator(
            task_id = f"scraping_products_{category}",
            python_callable=scrape_products,                                   
            op_args=[category],
        )


        # find extracted data for the catefory in rawdata directory
        find_rawdata_file_task = PythonOperator(
            task_id = f"find_rawdata_file_{category}",
            python_callable=find_rawdata_file,
            op_args=[category],
        )


        # Transform the extracted data
        transform_data_task = PythonOperator(
                        task_id = f"transform_task_{category}",
                        python_callable=transform_data,
                        op_args=[category, categoryID],
                 )
        

        # log end of task
        end_log_task = PythonOperator(
            task_id = "end_log_task",
            python_callable=end_log,
            op_args=[category],
        )

        write_processed_categories(category)      

    # Task dependencies
    start_task >> start_log_task >> find_rawdata_file_task >> transform_data_task >> end_task >> end_log_task






    

        
            
