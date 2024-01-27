import logging
from datetime import datetime
from delta import *
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def read_json_from_raw_s3(spark, bucket_name, object_key):
    path = f"s3a://{bucket_name}/{object_key}/*.json"
    logger.info(f"Reading JSON files from: {path}")
    df = spark.read.option("multiLine", "true").json(path)
    df.printSchema()
    logger.info("JSON files read successfully")
    return df

def read_parquet_from_s3(spark, bucket_name, object_key):
    file_path = f"s3a://{bucket_name}/{object_key}/list-breweries*.parquet"
    logger.info(f"Reading Parquet file from: {file_path}")
    df = spark.read.parquet(file_path)
    logger.info("Parquet file read successfully")
    return df

def read_parquet_from_gold_s3(spark, bucket_name, object_key):
    file_path = f"s3a://{bucket_name}/{object_key}/aggregated-breweries*.parquet"
    logger.info(f"Reading Parquet file from: {file_path}")
    df = spark.read.parquet(file_path)
    logger.info("Parquet file read successfully")
    return df

spark = SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()
spark._jsc.hadoopConfiguration().set(
"fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
)

def read_delta_from_gold_s3(spark, bucket_name, object_key):
    file_path = f"s3a://{bucket_name}/{object_key}/aggregated-breweries.delta"
    logger.info(f"Reading Parquet file from: {file_path}")
    df = spark.read.format("delta").load(file_path)
    #df = spark.read.format("delta").option("versionAsOf", 1).load(file_path)
    logger.info("Delta file read successfully")
    return df


spark = SparkSession.builder\
    .master("local[3]")\
    .appName("SparkETL")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
spark._jsc.hadoopConfiguration().set(
"fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
)


bucket_name = "open-brewerie-db"
raw_object_key = f"raw/extracted_at={datetime.now().date()}"
silver_object_key = f"silver/extracted_at={datetime.now().date()}"
gold_object_key = f"gold/extracted_at={datetime.now().date()}"


#raw_df = read_json_from_raw_s3(spark, bucket_name, raw_object_key)
#silver_df = read_parquet_from_s3(spark, bucket_name, silver_object_key)
gold_df = read_delta_from_gold_s3(spark, bucket_name, gold_object_key)

#raw_df.show()
#silver_df.show()
#gold_df.show()

delatable = DeltaTable.forPath(spark, "s3a://open-brewerie-db/gold/extracted_at=2024-01-27/aggregated-breweries.delta")
fullHistoryDF = delatable.history() # get full history of the table
lastOperationDF = delatable.history(0) # get the last operation

gold_df.show()
fullHistoryDF.show()
lastOperationDF.show()