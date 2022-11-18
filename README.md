# poc_kafka_delta

## To run the spark streaming job:

### Spark and Hadoop Version:
<p> Spark-3.3.1 and Hadoop3 </p>

### Python Version:
Python 3.9.13

### Setting up conda environment:
``console
conda env create -f environment.yml
```

#### Set Config Variables
```console

export S3USER=user
export S3Password=password
export S3EndPoint=127.0.0.1:9000
export SourceBucket=test
export TopicName=cdc_test_topics
export KafkaServer='localhost:9092'
export TypeJob = 'append'

```
#### Start Kafka and MinIO Containers


```console

docker-compose -f docker_yaml/kafka.yaml up -d

docker run -d   -p 9000:9000    -p 9090:9090    --name minio    -v ~/minio/data:/data    -e "MINIO_ROOT_USER=user"    -e "MINIO_ROOT_PASSWORD=password"    quay.io/minio/minio server /data --console-address ":9090"

```

#### Start Spark Job



```console

./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.1,com.amazonaws:aws-java-sdk:1.12.341,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4 --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore /home/$USER/poc_kafka_delta/spark_kafka.py

```
