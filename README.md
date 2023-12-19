# AB-InBev Data Challenge

[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

## About

Data Chalenge Documentation.

The project is an ELTL pipeline, orchestrated with Apache Airflow inside a Docker Container.

The test is to consume data from an API, persist into a data lake architecture with three layers with the first one being raw data, the second one curated and partitioned by location and the third one having analytical aggregated data. 

## Architecture 

![alt text](/images/pipeline.png)


## Prerequisites

- [AWS S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Spark](https://spark.apache.org/docs/latest/)

## Setup

Clone the project to your desired location:

    $ git clone https://github.com/rgualter/open-brewery-db.git

Execute the following command that will create the .env file containig the Airflow UID needed by docker-compose:

    $ echo -e "AIRFLOW_UID=$(id -u)" > .env


Build Docker:

    $ docker-compose build 

Initialize Airflow database:

    $ docker-compose up airflow-init

Start Containers:

    $ docker-compose up -d


When everything is done, you can check all the containers running:

    $ docker ps

## Airflow Interface

Now you can access Airflow web interface by going to http://localhost:8080 with the default user which is in the docker-compose.yml. **Username/Password: airflow**

With your AWS S3 user and bucket created, you can store your credentials in the **connections** in Airflow. And we can store which port Spark is exposed when we submit our jobs:

![alt text](/images/airflow_spark.png)
![alt text](/images/airflow_aws.png)

Now, you can see my effort to make the dag work lol.

![alt text](/images/airflow_dag.png)

And finally, check the S3 bucket if our partitioned data is in the right place.

![alt text](/images/aws_raw.png)
![alt text](/images/aws_silver.png)
![alt text](/images/aws_gold.png)

## Shut down or restart Airflow

If you need to make changes or shut down:

    $ docker-compose down

## References 

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
- [Spark by examples](https://sparkbyexamples.com/pyspark-tutorial/)
- [Spark s3 integration](https://spark.apache.org/docs/latest/cloud-integration.html)
- [Airflow and Spark with Docker](https://medium.com/data-arena/building-a-spark-and-airflow-development-environment-with-docker-f0b9b625edd8)
- [Working with data files from S3 in your local pySpark environment] (https://blog.revolve.team/2023/05/02/data-files-from-s3-in-local-pyspark-environment/)
## License

This project is licensed under the terms of the **MIT** license.