from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from api_request import get_api_request


ARGS = {
    "owner": "ricardogualter",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

dag = DAG(
    dag_id="obdb-pipeline",
    default_args=ARGS,
    description="",
    schedule_interval= None, #"0 0 * * *" # cron for every day at 12:00 AM,
    dagrun_timeout=timedelta(minutes=20)
)



dag = DAG(
    dag_id="upload_to_s3_dag",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None
)

ingest_api_to_s3 = PythonOperator(
    task_id="upload_to_s3_task",
    python_callable=get_api_request,
    dag=dag
)
raw_to_silver_task = SparkSubmitOperator(
    task_id="raw_to_silver_task",
    application="/opt/airflow/etl_files/raw_to_silver.py",
    conn_id="spark_default",
    conf={
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.driver.extraClassPath": "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.jars.packages": 'org.apache.hadoop:hadoop-aws:3.3.1',
        "spark.hadoop.fs.s3a.path.style.access": "true"
    },
    dag=dag
)

silver_to_gold_task = SparkSubmitOperator(
    task_id="silver_to_gold_task",
    application="/opt/airflow/etl_files/silver_to_gold.py",
    conn_id="spark_default",
    conf={
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.driver.extraClassPath": "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.jars.packages": 'org.apache.hadoop:hadoop-aws:3.3.1',
        "spark.hadoop.fs.s3a.path.style.access": "true"
    },
    dag=dag
)

ingest_api_to_s3 >> raw_to_silver_task >> silver_to_gold_task