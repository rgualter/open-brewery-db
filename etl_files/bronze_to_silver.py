import logging
from dotenv import load_dotenv
from common_resources import read_data_from_s3, write_data_to_s3, create_spark_session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv('/opt/airflow/config/.env')

def process_bronze_to_silver():
    spark = create_spark_session()
    df = read_data_from_s3(spark= spark, format= "delta", bucket_name= "open-brewerie-db", layer="bronze", file_name="list-breweries")
    write_data_to_s3(df = df, spark= spark, format= "delta", bucket_name= "open-brewerie-db", layer="silver", file_name="list-breweries",  partition="country", mode= "append")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_bronze_to_silver()
