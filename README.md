# poc_kafka_delta

## To run the spark streaming job:

```console

export S3USER=<S3User>
export S3Password=<S3password>
export S3EndPoint=127.0.0.1:9000
export SourceBucket=test
export TopicName=cdc_test_topics
export KafkaServer='localhost:9092'

./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.1,com.amazonaws:aws-java-sdk:1.12.341,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4 --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore /home/$USER/poc_kafka_delta/spark_kafka.py

```
