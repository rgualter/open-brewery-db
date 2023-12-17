



from airflow import DAG
from airflow.operators.python import PythonOperator
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


# Define the DAG
dag = DAG(
    dag_id="upload_to_s3_dag",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None
)

# Define the PythonCallable task
ingest_api_to_s3 = PythonOperator(
    task_id="upload_to_s3_task",
    python_callable=get_api_request,
    dag=dag
)





# transform apartments data 
#transformation = SparkSubmitOperator(
#    task_id="transformation",
#    application="/opt/airflow/spark/transformation.py",
#    name="vivareal_transformation",
#    conn_id="spark_default",
#    conf={
#        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
#        "spark.driver.extraClassPath": "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
#        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#        "spark.hadoop.fs.s3a.path.style.access": "true"
#    },
#    application_args=[
#        join(S3_BUCKET.format(stage="raw"), "extracted_date={{ ds }}/*.json"),
#        join(S3_BUCKET.format(stage="processed"), "extracted_date={{ ds }}/"),
#    ],
#    dag=dag
#)

# verify file existence
#s3_sensor = S3KeySensor(
#    task_id="verify_s3",
#    bucket_key="extracted_date={{ ds }}/*.parquet",
#    bucket_name="YOURBUCKET", # only the name of the bucket
#    aws_conn_id="s3_connection",
#    wildcard_match=True,
#    poke_interval=15,
#    timeout=60,
#    dag=dag
#)

# add columns to DataFrame and partition by neighborhood
#curated = SparkSubmitOperator(
#   task_id="curated",
#   application="/opt/airflow/spark/curated.py",
#   name="vivareal_curated",
#   conn_id="spark_default",
#   conf={
#       "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
#       "spark.driver.extraClassPath": "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
#       "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#       "spark.hadoop.fs.s3a.path.style.access": "true"
#   },
#   application_args=[
#       join(S3_BUCKET.format(stage="processed"), "extracted_date={{ ds }}/*.parquet"),
#       join(S3_BUCKET.format(stage="curated"), "extracted_date={{ ds }}/"),
#   ],
#   dag=dag
#)

ingest_api_to_s3