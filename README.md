# Spark_Streaming_Platform

# Architecture of Spark Cluster:

![Spark-Architecture](https://github.com/keenborder786/poc_kafka_delta/blob/main/diagrams/Spark_Architecture.drawio.png)

## How to test run the pipeline?

### Method-1: Docker Compose

#### Step:0 Create minio credentials:

<p> Create two files named minio_user.txt and minio_password.txt , store them in a folder called secrets </p>


#### Step:1 Set following Config Variables and put in an .env.compose file (If already not created)

```console

S3_USER=user
S3_PASSWORD=password
S3_END_POINT=172.18.0.5:9000
S3_SOURCE_BUCKET=test
KAFKA_SERVER=172.18.0.4:9092
KAFKA_TOPIC_NAME='{"customer":"customer_topics" , "transaction":"trans_topics"}'
KAFKA_CONSUMER_CONFIG='{"failOnDataLoss":"false"}'
DELTA_LAKE_TABLE_CONFIG='{"Delta_Lake_Tables": {"customer": {"delta.appendOnly": "false","delta.enableChangeDataFeed": "true","delta.deletedFileRetentionDuration": "interval 7 days"}}}'
DELTA_LAKE_TABLE_SCHEMAS='{"Delta_Lake_Tables": {"customer": {"fields": [{"metadata": {},"name": "id","nullable": true,"type": "string"},{"metadata": {},"name": "status","nullable": true,"type": "string"},{"metadata": {},"name": "status_metadata","nullable": true,"type": "string"},{"metadata": {},"name": "creator","nullable": true,"type": "string"},{"metadata": {},"name": "created","nullable": true,"type": "timestamp"},{"metadata": {},"name": "creator_type","nullable": true,"type": "string"},{"metadata": {},"name": "updater","nullable": true,"type": "string"},{"metadata": {},"name": "updated","nullable": true,"type": "timestamp"},{"metadata": {},"name": "updater_type","nullable": true,"type": "string"}]},"transaction": {"fields": [{"metadata": {},"name": "id","nullable": true,"type": "string"},{"metadata": {},"name": "date","nullable": true,"type": "timestamp"},{"metadata": {},"name": "value","nullable": true,"type": "float"}]}}}'
MASTER_HOST_NAME=spark://172.18.0.6:7077
DRIVER_IP=172.18.0.8
DRIVER_HOST=172.18.0.8
DRIVER_PORT=40207
DRIVER_BLOCK_MANAGER_PORT=40208
EXECUTOR_CORES="1"
EXECUTOR_MEMORY=500m
MIN_EXECUTOR_NO="0"
MAX_EXECUTOR_NO="2"
SHUFFLE_PARTITION="2"

```
<p> KafkaConsumerConfig options can be seen from: https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html </p>


#### Step:2 Start the app through docker compose file

```console

docker-compose up

```
<p> NOTE: The setting up of services might take some time for the first time since spark need to download the packages from internet. However, next time it will store the packages on the mounted volumes </p>

#### Step:3 Run simulate_kafka.py

```console

docker exec -it sparkclient bash

```
<p> Once in the spark container shell, cd to poc_kafka_delta since you will be already in opt folder and then run simulate_kafka.py

```console

cd poc_kafka_delta
python simulate_kafka.py -p 50 -i 150

```

### Method-2: Helm Chart [Click Here](https://github.com/keenborder786/poc_kafka_delta/tree/main/spark-application)


## How to set up dev environment for contributing?

  - Install [mamba](https://github.com/conda-forge/miniforge#mambaforge) for conda package management on your local machine. More [Instruction](https://mamba.readthedocs.io/en/latest/installation.html).
  - Once mamba is set up, clone the repo and run the following commands in the root directory: 
  ```console
  
  mamba env create -f environment.yml
  conda activate spark_streaming

  ```
  - Now you can start developing in the repo.
  - Once you are done with the changes, run the following command:
  
  ```console

  pre-commit install

  ```
  - This will install a pre-commit github hook to format your code according to pep 8 standards. For the packages we are using to format the code refer   to .pre-commit-config.yaml
   - You can now commit your code.
