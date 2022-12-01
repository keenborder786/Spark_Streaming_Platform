import pyspark

from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv() , override = True)
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import * 
from streaming.spark_engine import SparkProcessing
from streaming.deltalake_engine import DeltaLakeInteraction
from pyspark.sql import SparkSession
from streaming.config import (
                    hadoop_config,
                    kafka_server,
                    topic_name,
                    sourceBucket,
                    kafka_config,
                    customer_table_config,
                    customer_fields_map,
                    customer_write_row_schema)

if __name__ == '__main__':

    spark = (
        SparkSession.builder.appName('test')\
        .master('local[*]')
        .getOrCreate()
    )
    spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.setLogLevel('Error')
    hadoop_config={"fs.s3a.endpoint":'127.0.0.1:9000',
                "fs.s3a.access.key":'user',
                "fs.s3a.secret.key":'password',
                "spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.path.style.access":"true",
                "com.amazonaws.services.s3.enableV4":"true",
                "fs.s3a.connection.ssl.enabled":"false",
                "spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem"
                }
    if hadoop_config != {}:
        for k,v in hadoop_config.items():
            spark.sparkContext._jsc.hadoopConfiguration().set(k, v)
    customer_table_deltalake_instance = DeltaLakeInteraction(spark, sourceBucket , 'DimCustomer')
    customer_table_deltalake_instance.query_latest_table().write.csv('temp')
