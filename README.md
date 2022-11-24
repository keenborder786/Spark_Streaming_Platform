# poc_kafka_delta

## To run the spark streaming job:

### Spark and Hadoop Version:
<p> Spark-3.3.1 and Hadoop3 </p>

### Python Version:
Python 3.9.13


#### Step:0 Create minio credentials:

## First Option (Run the spark job on local machine) 
<p> Create two files named minio_user.txt and minio_password.txt , store them in a folder called secrets </p>

### Setting up conda environment:

```console
conda env create -f environment.yml

```

#### Set following Config Variables as per your desire and put in an .env file.
```console
S3USER='user'
S3Password='password'
S3EndPoint='127.0.0.1:9000'
SourceBucket='test'
KafkaServer='localhost:9092'
TopicName='cdc_test_topics'
KafkaConsumerConfig='{"startingOffsets":"latest","failOnDataLoss":"false","minOffsetsPerTrigger":60000,"maxTriggerDelay":"1m"}'
TableName='cdc_table'
TypeJob='append'
DeltaTableConfig='{"delta.appendOnly":"true"}'
Source_Schema='{"type":"","fields":""}' 

```
<p> KafkaConsumerConfig options can be seen from: https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html </p>
<p> DeltaTableConfig (for Raw and Customer Tables) options can be seen from: https://docs.delta.io/latest/table-properties.html </p>
<p> Source_Schema can be generated from StructType.jsonValue() </p>

#### Step:2 Build the docker image for spark_job

```console

docker build . --tag spark_job:2.0

```

#### Step:3 Start the app through docker compose file

```console

docker-compose up

```

<p> You can also build a spark job image as well in order to deploy it on a pod. </p>


## Second Option (Build the Image and run on docker compose or k8) 

#### Build Spark Job Image

```console

docker build . --tag spark_job:1.0

```
#### Step: 4 Run simulate_kafka.py on host machine to send messages for processing and uploading to minio
