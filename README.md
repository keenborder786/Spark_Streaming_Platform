# poc_kafka_delta

## To run the spark streaming job:

### Spark and Hadoop Version:
<p> Spark-3.3.1 and Hadoop3 </p>

### Python Version:
Python 3.9.13

### Conda Settings

```yaml
          conda version : 22.9.0
    conda-build version : 3.22.0
         python version : 3.9.13.final.0
       virtual packages : __linux=5.15.0=0
                          __glibc=2.35=0
                          __unix=0=0
                          __archspec=1=x86_64
  conda av metadata url : None
           channel URLs : https://repo.anaconda.com/pkgs/main/linux-64
                          https://repo.anaconda.com/pkgs/main/noarch
                          https://repo.anaconda.com/pkgs/free/linux-64
                          https://repo.anaconda.com/pkgs/free/noarch
                          https://repo.anaconda.com/pkgs/r/linux-64
                          https://repo.anaconda.com/pkgs/r/noarch
                          https://conda.anaconda.org/conda-forge/linux-64
                          https://conda.anaconda.org/conda-forge/noarch
                          https://conda.anaconda.org/pypi/linux-64
                          https://conda.anaconda.org/pypi/noarch
               platform : linux-64
             user-agent : conda/22.9.0 requests/2.28.1 CPython/3.9.13 Linux/5.15.0-52-generic ubuntu/22.04.1 glibc/2.35
             netrc file : None  
           offline mode : False
```


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
<p> DeltaTableConfig options can be seen from: https://docs.delta.io/latest/table-properties.html </p>
<p> Source_Schema can be generated from StructType.jsonValue() </p>

#### Start Kafka and MinIO Containers


```console

docker-compose -f docker_yaml/kafka.yaml up -d

docker run -d   -p 9000:9000    -p 9090:9090    --name minio    -v ~/minio/data:/data    -e "MINIO_ROOT_USER=user"    -e "MINIO_ROOT_PASSWORD=password"    quay.io/minio/minio server /data --console-address ":9090"

```
d
#### Start Spark Job


<p> Download the spark from following link:https://www.apache.org/dyn/closer.lua/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz </p>
<p> Navigate to spark-3.3.1-bin-hadoop3 and run the following command </p>

```console

./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.1,com.amazonaws:aws-java-sdk:1.12.341,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4 --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore /home/$USER/poc_kafka_delta/main.py

```
