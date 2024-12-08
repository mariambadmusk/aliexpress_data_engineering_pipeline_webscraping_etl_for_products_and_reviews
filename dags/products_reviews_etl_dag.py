from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airfow.operators.python import PythonOperator
from extract import Aliexpress_reviews_scraper
from transform import mainTransformAliexpressProductReviews
from utils import setup_logging
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
    "retries_delay": timedelta(minutes=30),
    "start_date":datetime(), #add date

}


# define helper functions for reading and writing processed categories
def read_processed_product_ids():
    if not os.path.exists("processed_ product_id_log.log"):
        return []
    else:
        with open("processed_ product_id_log.log", "r") as file:
            processed_product_ids =  file.read().splitlines()
        return processed_product_ids

def write_processed_product_ids(category, product_id):
    with open("processed_product_ids_log.log", "a") as file:
        file.write(f"{category}:{product_id}\n")
    
        
# Read processed categories log
processed_product_ids = read_processed_product_ids() 


# define functions
def start_log():
    logger.info(f"ETL pipeline for product_id {product_id} in category {category} started")

 
def scrape_reviews(product_id):  #edit
    logger.info(f"Extracting reviews reviews for product_id: { product_id} in category {category}"),
    reviews = Aliexpress_reviews_scraper()
    reviews.extract_product_reviews(product_id)


def transform_data(category):
    logger.info(f"Transforming data for  product_id: {product_id} in category {category}")
    transformed_dir = os.getenv("TRANSFORMED_DIR")
    full_path = os.path.join(transformed_dir, f"{category}-productIds.csv" )
    mainTransformAliexpressProductReviews(full_path, category)
    

def end_log():
    logger.info(f"ETL pipeline for product_id {product_id} review completed")



# define DAG
with DAG(
    dag_id = "aliexpress_reviews_pipeline",
    default_args=default_args,
    description ="ETL pipeline for Aliexpress product reviews",
    schedule_interval=timedelta(days=2),
    catchup=False,
) as dag:
    

    start_task = DummyOperator(task_id="start_task")
    end_task = DummyOperator(task_id="end_task")


    start_log_task = PythonOperator(
        task_id = "start_log",
        python_callable=start_log,
    ) 


     
    for index, row in product_df.iterrows():  
        product_id = row["productId"]
        category = row["category"]

        if category and product_id in processed_product_ids:
            continue
    
        extract_task = PythonOperator(
            task_id = f"scraping_reviews_{product_id}",
            python_callable=scrape_reviews,                                   
            op_args=[product_id],
        )


        # Transform the extracted data
        transform_data_task = PythonOperator(
                        task_id = f"transform_task_{product_id}",
                        python_callable=transform_data,
                        op_args=[category],
                )
        

        # log end of task
        end_log_task = PythonOperator(
            task_id = "end_log_task",
            python_callable=end_log,
            op_args=[product_id],
        )

        write_processed_product_ids(product_id)      

        # Task dependencies
        start_task >> start_log_task >> transform_data_task >> end_task >> end_log_task






    

        
            
