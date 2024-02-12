import logging
from pyspark.sql.functions import count
from delta import *
from common_resources import read_data_from_s3, write_data_to_s3, create_spark_session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_aggregate_view(df):
    #logger.info("Counting IDs grouped by brewery_type and country")
    count_df = df.groupBy("brewery_type", "country").agg(count("id").alias("count_id"))
    
    logger.info("Counting IDs completed successfully")
    count_df.show()
    return count_df

def process_silver_to_gold():
    spark = create_spark_session()
    df = read_data_from_s3(spark= spark, format= "delta", bucket_name= "open-brewerie-db", layer="silver", file_name="list-breweries-country")
    aggregate = create_aggregate_view(df)
    write_data_to_s3(df = aggregate, spark= spark, format= "delta", bucket_name= "open-brewerie-db", layer="gold", file_name="aggregate-list-breweries", mode= "append")

if __name__ == "__main__":
    process_silver_to_gold()