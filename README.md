# poc_kafka_delta

## To run the spark streaming job:

### Spark and Hadoop Version:
<p> Spark-3.3.1 and Hadoop3 </p>

### Python Version:
Python 3.9.13


#### Step:0 Create minio credentials:

<p> Create two files named minio_user.txt and minio_password.txt , store them in a folder called secrets </p>

#### Step:1 Set following Config Variables and put in an .env file.

```console
S3USER=user # as per your secret file
S3Password=password # as per your secret file
S3EndPoint=quay.io/minio/minio://minio:9000
SourceBucket=test
KafkaServer=wurstmeister/kafka://kafka:9092
TopicName=cdc_test_topics
KafkaConsumerConfig='{"startingOffsets":"latest","failOnDataLoss":"false"}'
TableName=DimCustomer
RawEventTableConfig='{"delta.appendOnly":"true","enableChangeDataFeed":"true","deletedFileRetentionDuration":"interval 7 days"}'
CustomerTableConfig='{"delta.appendOnly":"false","enableChangeDataFeed":"true","deletedFileRetentionDuration":"interval 7 days"}'
Source_Schema='{"type":"","fields":""}' 

```
<p> KafkaConsumerConfig options can be seen from: https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html </p>
<p> DeltaTableConfig options can be seen from: https://docs.delta.io/latest/table-properties.html </p>
<p> Source_Schema can be generated from StructType.jsonValue() </p>

#### Step:2 Build the docker image for spark_job

```console

docker build . --tag spark_job:2.0

```

#### Step:3 Start the app through docker compose file

```console

docker-compose up

```

#### Step: 4 Run simulate_kafka.py on host machine to send messages for processing and uploading to minio
