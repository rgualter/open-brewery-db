import logging
from datetime import datetime
from common_resources import read_data_from_s3, write_data_to_s3, create_spark_session
from dotenv import load_dotenv

load_dotenv('/opt/airflow/config/.env')

logger = logging.getLogger(__name__)

date=datetime.now().date()
def process_raw_to_bronze():
    spark = create_spark_session()
    df = read_data_from_s3(spark=spark, format="json", bucket_name="open-brewerie-db", layer="raw", date=date)
    write_data_to_s3(df=df, spark=spark, format="delta", bucket_name="open-brewerie-db", layer="bronze", file_name="list-breweries", mode="append")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_raw_to_bronze()
