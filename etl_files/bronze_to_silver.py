import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession
from pyspark import SparkConf, SparkContext


from dotenv import load_dotenv
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv('/opt/airflow/config/.env')

today_date = datetime.now().date()

def read_json_from_raw_s3(spark, date):
    object_key = f"raw/extracted_at={date}"
    bucket_name = "open-brewerie-db"
    path = f"s3a://{bucket_name}/{object_key}/*.json"
    
    df = spark.read.option("multiLine", "true").json(path)
    df.printSchema()
    return df

def read_delta_from_bronze_s3(spark):
    bucket_name = "open-brewerie-db"
    object_key = "bronze"
    file_path = f"s3a://{bucket_name}/{object_key}/list-breweries"
    logger.info(f"Reading Delta file from: {file_path}")
    df = spark.read.format("delta").load(file_path)
    #df = spark.read.format("delta").option("versionAsOf", 1).load(file_path)
    logger.info("Delta file read successfully")
    return df

def save_parquet_to_silver_s3(df, date):
    bucket_name = "open-brewerie-db"
    object_key = f"silver/extracted_at={date}"
    file_path = f"s3a://{bucket_name}/{object_key}/list-breweries_{date}.parquet"

    logger.info(f"Writing DataFrame to Parquet: {file_path}")
    df.write.partitionBy("country").parquet(file_path)
    logger.info("DataFrame written to Parquet successfully")

def save_delta_to_silver_s3(df):
    bucket_name = "open-brewerie-db"
    object_key = "silver"
    file_path = f"s3a://{bucket_name}/{object_key}/list-breweries"
    file_path_ = f"s3a://{bucket_name}/{object_key}/list-breweries-table"

    logger.info(f"Writing DataFrame to Delta: {file_path}")
    df.write.format("delta").mode("append").save(file_path)
    df.write.option("path", file_path_).mode("append").saveAsTable("breweries")
    logger.info("DataFrame written to Delta successfully")




def process_raw_to_silver():
    today_date = datetime.now().date()
    spark = SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()
    spark._jsc.hadoopConfiguration().set("spark.driver.extraClassPath", "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar")
    spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    df = read_json_from_raw_s3(spark, today_date)
    save_parquet_to_silver_s3(df, today_date)

def process_bronze_to_silver():
    spark = SparkSession.builder.master("local[3]").appName("SparkETL")\
    .config("fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config('spark.delta.logStore.class','org.apache.spark.sql.delta.storage.S3SingleDriverLogStore') \
    .config("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.3.1') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .getOrCreate()

    spark._jsc.hadoopConfiguration().set("spark.driver.extraClassPath", "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar")
    spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    df = read_delta_from_bronze_s3(spark)
    save_delta_to_silver_s3(df)

if __name__ == "__main__":
    process_bronze_to_silver()
