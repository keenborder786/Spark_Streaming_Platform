# poc_kafka_delta

## How to test run the pipeline?

### Method-1: Docker Compose

#### Step:0 Create minio credentials:

<p> Create two files named minio_user.txt and minio_password.txt , store them in a folder called secrets </p>


#### Step:1 Set following Config Variables and put in an .env file.

```console

S3USER=user ## same as what you stored in secret file
S3Password=password ## same as what you stored in secret file
S3EndPoint=172.18.0.5:9000
SourceBucket=test
KafkaServer=172.18.0.4:9092
TopicName=cdc_test_topics
KafkaConsumerConfig='{"failOnDataLoss":"false"}'
RawEventTableConfig='{"delta.appendOnly":"true","delta.enableChangeDataFeed":"true","delta.deletedFileRetentionDuration":"interval 7 days"}'
CustomerTableConfig='{"delta.appendOnly":"false","delta.enableChangeDataFeed":"true","delta.deletedFileRetentionDuration":"interval 7 days"}'

```
<p> KafkaConsumerConfig options can be seen from: https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html </p>
<p> DeltaTableConfig (for Raw and Customer Tables) options can be seen from: https://docs.delta.io/latest/table-properties.html </p>

#### Step:2 Build the docker images for sparkbase,sparkmaster,sparkworker & sparkclient

```console

docker build -f Dockerfile_base --tag sparkbase:1.0 . \
docker build -f Dockerfile_master --tag sparkmaster:1.0 . \
docker build -f Dockerfile_worker --tag sparkworker:1.0 . \
docker build -f Dockerfile_client --tag sparkclient:1.0 .

```

#### Step:3 Start the app through docker compose file

```console

docker-compose up

```
<p> NOTE: The setting up of services might take some time for the first time since spark need to download the packages from internet. However, next time it will store the packages on the mounted volumes </p>

#### Step:4 Run simulate_kafka.py

```console

docker exec -it sparkclient bash

```
<p> Once in the spark container shell, cd to poc_kafka_delta since you will be already in opt folder and then run simulate_kafka.py

```console

cd poc_kafka_delta
python simulate_kafka.py

```

### Method-2: Helm Chart [Click Here](https://github.com/keenborder786/poc_kafka_delta/tree/helm_testing/spark-application)


