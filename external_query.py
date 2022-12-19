from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from streaming.deltalake_engine import DeltaLakeInteraction

load_dotenv(find_dotenv(), override=True)


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.1,com.amazonaws:aws-java-sdk:1.12.341,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4 /home/mohtashimkhan/poc_kafka_delta/external_query.py

if __name__ == "__main__":

    spark = SparkSession.builder.appName(
        "test").master("local[*]").getOrCreate()
    spark.sparkContext.setSystemProperty(
        "com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.setLogLevel("Error")
    hadoop_config = {
        "fs.s3a.endpoint": "127.0.0.1:9000",
        "fs.s3a.access.key": "user",
        "fs.s3a.secret.key": "password",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "com.amazonaws.services.s3.enableV4": "true",
        "fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }
    if hadoop_config != {}:
        for k, v in hadoop_config.items():
            spark.sparkContext._jsc.hadoopConfiguration().set(k, v)
    customer_table_deltalake_instance = DeltaLakeInteraction(
        spark, "test", "customer")
    customer_table_deltalake_instance.query_latest_table().show(100)
