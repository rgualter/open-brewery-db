import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

""" 
The `construct_file_path` function constructs the file path based on the provided bucket name, layer, file name, and date.

Args:
    bucket_name (str): The name of the S3 bucket.
    layer (str): The layer for which to construct the file path.
    file_name (str): The name of the file. Required for non-raw layers.
    date (str): The date for the "raw" layer. Required for "raw" layer and when file name is not provided.

Returns:
    str: The constructed file path.

Raises:
    ValueError: If the file name is not provided for non-raw layers.
"""
def construct_file_path(bucket_name, layer, file_name, date):
    if layer == "raw" and file_name is None:
        layer = f"{layer}/extracted_at={date}"
        return f"s3a://{bucket_name}/{layer}/*.json"
    else:
        if file_name is None:
            raise ValueError("File name is required for non-raw layers")
        return f"s3a://{bucket_name}/{layer}/{file_name}"
    

"""
The `read_data_from_s3` function reads data from S3 based on the provided format, bucket name, layer, file name, date, and version.

Args:
    spark: The SparkSession instance.
    format (str): The format of the file to be read (e.g., "parquet", "delta", "json").
    bucket_name (str): The name of the S3 bucket.
    layer (str): The layer from which to read the file (e.g., "raw", "bronze", "silver").
    file_name (str, optional): The name of the file to be read. Required for non-raw layers.
    date (str, optional): The date for the "raw" layer. Required for "raw" layer and "json" format.
    version (int, optional): The version of the Delta file to read. Required for "delta" format.

Returns:
    pyspark.sql.DataFrame: The DataFrame containing the data read from the file.

Raises:
    ValueError: If an unsupported format is provided or if the version is not an integer.

"""
def read_data_from_s3(spark, format, bucket_name, layer, file_name=None, date=None, version= None):
    file_path = construct_file_path(bucket_name, layer, file_name, date)

    logger.info(f"Reading {format} file(s) from {file_path}")

    if format == "parquet":
        df = spark.read.parquet(file_path)
        
    elif format == "delta":
        if version is not None and not isinstance(version, int):
            raise ValueError("Version must be an integer")
        df = spark.read.format("delta").option("versionAsOf", 0 if version is None else version).load(file_path)

    elif format == "json":
        df = spark.read.option("multiLine", "true").json(file_path)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(f"{format} file(s) read successfully from {file_path}")
    return df
    

"""
The `write_data_to_s3` function writes a DataFrame to S3 storage in the specified format and layer.

Args:
    df: The DataFrame to be written.
    spark: The SparkSession object.
    format: The format in which the DataFrame should be written (either "parquet", "delta", or "delta-table").
    bucket_name: The name of the S3 bucket.
    layer: The layer in which the data should be written (e.g., "silver", "gold").
    file_name: The name of the file to be written.
    date (optional): The date associated with the data (default: None).
    mode (optional): The write mode (append, overwrite) for the DataFrame  (default: None).
    partition (optional): The column(s) to partition the data by (default: None).

Returns:
    None

Raises:
    ValueError: If an unsupported format is specified.

Examples:
    # Write DataFrame to Parquet format
    write_data_to_s3(df, spark, "parquet", "my-bucket", "silver", mode="overwrite")

    # Write DataFrame to Delta format with partitioning
    write_data_to_s3(df, spark, "delta", "my-bucket", "silver", mode="append", partition="country")

    # Write DataFrame as Delta table
    write_data_to_s3(df, spark, "delta-table", "my-bucket", "silver", "data", mode="overwrite")

"""
def write_data_to_s3(df, spark, format, bucket_name, layer, file_name, date=None, mode=None, partition=None ):
    file_path = construct_file_path(bucket_name, layer, file_name, date)

    logger.info(f"Writing DataFrame to {format}: {file_path}")

    writer = df.write if partition is None else df.write.partitionBy(partition)

    if format == "parquet":
        writer.parquet(file_path, mode=mode)

    elif format == "delta":
        if partition is None:
            writer.format("delta").mode(mode).save(file_path)
        else:
            writer.partitionBy(partition).format("delta").mode(mode).save(f"{file_path}-{partition}")

    elif format == "delta-table":
        if partition is None:
            writer.option("path", f"{file_path}-table").mode(mode).saveAsTable("breweries")
        else:
            writer.format("delta").option("path", f"{file_path}-table-{partition}").mode(mode).saveAsTable("breweries")

    else:
        raise ValueError(f"Unsupported format: {format}. Only 'parquet', 'delta' or 'delta-table' formats are supported.")

    logger.info(f"DataFrame written to {format}: {file_path} successfully")



"""
Creates a SparkSession with the specified configurations.
Returns:
    SparkSession: The created SparkSession.
Args:
    None
"""
def create_spark_session():

    return (
        SparkSession.builder.master("local[3]")
        .appName("SparkETL")
        .config(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            'spark.delta.logStore.class',
            'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore',
        )
        .config("spark.jars.packages", 
                'org.apache.hadoop:hadoop-aws:3.3.1')
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.driver.extraClassPath",
            "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar:/opt/airflow/spark/jars/delta-core_2.12-2.3.0.jar:/opt/airflow/spark/jars/delta-storage-2.3.0.jar",
        )
        .getOrCreate()
    )