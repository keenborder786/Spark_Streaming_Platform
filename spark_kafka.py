import json
import os
import pyspark
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col,from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from typing import Dict,Optional
from delta import *
from pathlib import Path
from pyspark import SparkConf

#To run: ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.1,com.amazonaws:aws-java-sdk:1.12.341,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4 --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore /home/mohtashimkhan/poc_kafka_delta/spark_kafka.py
# docker-compose -f docker_yaml/kafka.yaml -d
# docker run -d   -p 9000:9000    -p 9090:9090    --name minio    -v ~/minio/data:/data    -e "MINIO_ROOT_USER=user"    -e "MINIO_ROOT_PASSWORD=password"    quay.io/minio/minio server /data --console-address ":9090"

s3accessKeyAws = "user"
s3secretKeyAws = "password"
connectionTimeOut = "600000"
s3endPointLoc= "127.0.0.1:9000"
sourceBucket = "test"

def get_spark_session(app_name:str , master_name:str , config:Optional[Dict] = {}) -> SparkSession:
    """
    Start the spark session.

    Parameters:
    ---------------------------
    app_name(str): The name given to the instance of spark session

    master_name(str): The url for the spark cluster
    
    config(dict , optional): The dictionary which contains the configuration for spark session

    Returns:
    ---------------------------
    SparkSession: Instance of spark session
    
    """
    if config == {}:
        spark = (
            SparkSession.builder.appName("Kafka_Lake_Spark")
            .master("local[*]")
            .getOrCreate()
        )
        return 
    else:
        spark = (
            SparkSession.builder.appName("Kafka_Lake_Spark")
            .master("local[*]")
            .getOrCreate()
        )
        configuration = config.items()
        spark.sparkContext._conf.setAll(configuration)
        spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3endPointLoc)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3accessKeyAws)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3secretKeyAws)
        spark.sparkContext._jsc.hadoopConfiguration().set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
        spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark.sparkContext.setLogLevel('Error')
        return spark


def read_kafka_stream(spark_session:SparkSession , kafka_bootstrap_server:str , topic_name:str , starting_offset:str) -> DataFrame:
    """
    
    Reads the kafka stream from the given topic and cluster
    
    Parameters:
    ---------------------------
    spark_session(SparkSession): Instance of spark session.

    kafka_bootstrap_server(str): The IP address for the kafka cluster
    
    topic_name(str): The topic to which the spark is reading the stream from

    starting_offset(str): Should we read the message in the given topic from start or end

    cdcSchema(structtype): The streaming schema in structtype

    Returns:
    ---------------------------
    DataFrame: Structured Spark DataFrame

    """

    df = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_server)
        .option("subscribe", topic_name)
        .option("startingOffsets", starting_offset)
        .option("failOnDataLoss","false")
        .load()
    )
    return df


if __name__ == '__main__':

    ## Setting up the spark session
    spark = get_spark_session('kafka_delta' , 'local[*]' , {"spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
                                                        "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                        "spark.executor.extraJavaOptions":"-Dcom.amazonaws.services.s3.enableV4=true",
                                                        "spark.driver.extraJavaOptions":"-Dcom.amazonaws.services.s3.enableV4=true"
                                                        })
    ## The schema for the CDC coming from Debezium connector
    cdc_schema = StructType([StructField('schema',StructType([
                                                            StructField('type',StringType()),
                                                            StructField('fields',ArrayType(
                                                                                    StructType([StructField('type',StringType()),
                                                                                                StructField('fields',ArrayType(
                                                                                                    StructType([StructField('type',StringType()),
                                                                                                                StructField('fields',ArrayType(
                                                                                                                    StructType([StructField('type',StringType()),
                                                                                                                                StructField('optional',BooleanType()),
                                                                                                                                StructField('field',StringType())
                                                                                                                                ])                                       
                                                                                                                                            )
                                                                                                                                ),
                                                                                                                StructField('optional',BooleanType()),
                                                                                                                StructField('name',StringType()),
                                                                                                                StructField('field',StringType())])
                                                                                                                                )
                                                                                                            ),
                                                                                                StructField('optional',StringType()),
                                                                                                StructField('name',StringType()),
                                                                                                StructField('field',StringType())
                                                                                                ]
                                                                                                )
                                                                                            )
                                                                        ),
                                                            StructField('optional',BooleanType()),
                                                            StructField('name',StringType())
                                                                ]
                                                            )
                                        ),
                            StructField('payload',StructType([StructField('before',IntegerType()),
                                                              StructField('after',StructType([
                                                                                            StructField('id',StructType([
                                                                                                            StructField('value',IntegerType()),
                                                                                                            StructField('set',StringType())   
                                                                                                                        ]
                                                                                                                        )
                                                                                                        ),
                                                                                            StructField('pin',StringType()),
                                                                                            StructField('status',StringType()),
                                                                                            StructField('created',StringType()),
                                                                                            StructField('creator_type',StringType()),
                                                                                            StructField('creator',StructType([
                                                                                                            StructField('value',IntegerType()),
                                                                                                            StructField('set',StringType())   
                                                                                                                        ]
                                                                                                                        )
                                                                                                        ),
                                                                                            StructField('updated',StringType()),
                                                                                            StructField('updator_type',StringType()),
                                                                                            StructField('updator',StringType())
                                                                                            ])
                                                                            ),
                                                                StructField('source',StructType([
                                                                                            StructField('version',StringType()),
                                                                                            StructField('connector',StringType()),
                                                                                            StructField('name',StringType()),
                                                                                            StructField('ts_ms',IntegerType()),
                                                                                            StructField('snapshot',StringType()),
                                                                                            StructField('db',StringType()),
                                                                                            StructField('sequence',StringType()),
                                                                                            StructField('schema',StringType()),
                                                                                            StructField('table',StringType()),
                                                                                            StructField('txId',StringType()),
                                                                                            StructField('lsn',StringType()),
                                                                                            StructField('xmin',StringType())
                                                                                            ])
                                                                            ),
                                                                StructField('op',StringType()),
                                                                StructField('ts_ms',IntegerType()),
                                                                StructField('transaction',IntegerType())
                                                                ]
                                                                )
                                            )])

    #reading kafka stream
    df = read_kafka_stream(spark , 'localhost:9092', 'cdc_test_topics' , 'latest')
    df = df.withColumn('value', from_json(col('value').cast('string'), cdc_schema)).select('value','timestamp')
    
    ##Writing the stream to the delta lake
    # df.writeStream.format('console').start().awaitTermination()
    df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "s3a://test/checkpoint/").start("s3a://test/").awaitTermination()
    