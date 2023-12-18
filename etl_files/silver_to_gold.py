import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

bucket_name = "open-brewerie-db"
silver_object_key = f"silver/extracted_at={datetime.now().date()}"
gold_object_key = f"gold/extracted_at={datetime.now().date()}"

def read_parquet_from_s3(spark, bucket_name, object_key):
    file_path = f"s3a://{bucket_name}/{object_key}/list-breweries*.parquet"
    logger.info(f"Reading Parquet file from: {file_path}")

    df = spark.read.parquet(file_path)
    logger.info("Parquet file read successfully")
    return df

def create_aggregate_view(df):
    logger.info("Counting IDs grouped by brewery_type and country")
    count_df = df.groupBy("brewery_type", "country").agg(count("id").alias("count_id"))
    
    logger.info("Counting IDs completed successfully")
    count_df.show()
    return count_df

def write_parquet_to_gold_s3(df, bucket_name, object_key):
    file_path = f"s3a://{bucket_name}/{object_key}/aggregated-breweries.parquet"
    logger.info(f"Writing DataFrame to Parquet: {file_path}")
    
    df.write.parquet(file_path, mode="overwrite")
    logger.info("DataFrame written to Parquet successfully")

#df = read_parquet_from_s3(spark, bucket_name, silver_object_key)
#aggregated_df = create_aggregate_view(df)
#write_parquet_to_gold_s3(aggregated_df, bucket_name, gold_object_key)

def process_silver_to_gold(spark, bucket_name, silver_object_key, gold_object_key):
    df = read_parquet_from_s3(spark, bucket_name, silver_object_key)
    aggregated_df = create_aggregate_view(df)
    write_parquet_to_gold_s3(aggregated_df, bucket_name, gold_object_key)

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()
    spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
)
    bucket_name = "open-brewerie-db"
    silver_object_key = f"silver/extracted_at={datetime.now().date()}"
    gold_object_key = f"gold/extracted_at={datetime.now().date()}"

    process_silver_to_gold(spark, bucket_name, silver_object_key, gold_object_key)