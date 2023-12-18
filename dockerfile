FROM apache/airflow:2.7.3-python3.8

LABEL maintainer="Ricardo Gualter"

USER root:root

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow

WORKDIR /opt/airflow

RUN pip install -r requirements.txt

