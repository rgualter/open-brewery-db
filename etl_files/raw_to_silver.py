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
#spark = SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()
#spark._jsc.hadoopConfiguration().set(
#    "fs.s3a.aws.credentials.provider",
#    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
#)

def read_json_from_raw_s3(spark, date):
    object_key = f"raw/extracted_at={date}"
    bucket_name = "open-brewerie-db"
    path = f"s3a://{bucket_name}/{object_key}/*.json"
    
    df = spark.read.option("multiLine", "true").json(path)
    df.printSchema()
    return df


def save_parquet_to_silver_s3(df, date):
    bucket_name = "open-brewerie-db"
    object_key = f"silver/extracted_at={date}"
    file_path = f"s3a://{bucket_name}/{object_key}/list-breweries_{date}.parquet"

    logger.info(f"Writing DataFrame to Parquet: {file_path}")
    df.write.partitionBy("country").parquet(file_path)
    logger.info("DataFrame written to Parquet successfully")

#df = read_json_from_raw_s3(spark, today_date)

#save_parquet_to_silver_s3(df, today_date)

def process_raw_to_silver():
    today_date = datetime.now().date()
    spark = SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()
    #spark._jsc.hadoopConfiguration().set(
    #    "fs.s3a.aws.credentials.provider",
    #    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    #)
    #spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    spark._jsc.hadoopConfiguration().set("spark.driver.extraClassPath", "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar")
    spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    df = read_json_from_raw_s3(spark, today_date)
    save_parquet_to_silver_s3(df, today_date)

if __name__ == "__main__":
    process_raw_to_silver()
