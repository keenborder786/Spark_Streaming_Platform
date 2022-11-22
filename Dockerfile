FROM continuumio/miniconda3:latest

WORKDIR /opt

## Updating the current conda environment with desired packages
COPY environment.yml .
RUN conda update -n base conda && \
    conda env update --name base -f environment.yml

# Install base utilities
RUN apt-get update && \
    apt-get install -y build-essential  && \
    apt-get install -yq curl wget jq vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

## Install java and spark
RUN apt update && \
    apt install -y default-jdk && \
    wget https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.1-bin-hadoop3.tgz && \
    mv spark-3.3.1-bin-hadoop3 /opt/spark
RUN export SPARK_HOME=/opt/spark
RUN export PATH=$PATH:$SPARK_HOME/bin


## Setting up my code
COPY streaming/ /opt/poc_kafka_delta/streaming/
COPY main.py /opt/poc_kafka_delta


## Execute the final shell script to run our job
CMD /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.1,com.amazonaws:aws-java-sdk:1.12.341,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4 --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore /opt/poc_kafka_delta/main.py





