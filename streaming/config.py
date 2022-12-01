import os
import json
from pyspark.sql.types import *

## All of the configs needed to run the streaming pipeline
s3accessKeyAws = os.environ['S3USER']
s3secretKeyAws = os.environ['S3Password']
s3endPointLoc= os.environ['S3EndPoint']
sourceBucket = os.environ['SourceBucket']
kafka_server = os.environ['KafkaServer']
kafka_config = json.loads(os.environ['KafkaConsumerConfig'])
topic_name = os.environ['TopicName']
customer_table_config = json.loads(os.environ['CustomerTableConfig'])
customer_write_row_schema = StructType([StructField('time_event', FloatType(), True),
                              StructField('op',StringType(),True),
                              StructField('id', StringType(), True), 
                              StructField('status', StringType(), True), 
                              StructField('status_metadata', StringType(), True), 
                              StructField('creator', StringType(), True), 
                              StructField('created', TimestampType(), True), 
                              StructField('creator_type', StringType(), True), 
                              StructField('updater', StringType(), True), 
                              StructField('updated', TimestampType(), True), 
                              StructField('updater_type', StringType(), True)])
customer_fields_map = {'id':'update_table.id',
                        'status':'update_table.status',
                        'status_metadata':'update_table.status_metadata',
                        'creator':'update_table.creator',
                        'created':'update_table.created',
                        'creator_type':'update_table.creator_type',
                        'updater':'update_table.updater',
                        'updated':'update_table.updated',
                        'updater_type':'update_table.updater_type'
                        }
hadoop_config={"fs.s3a.endpoint":s3endPointLoc,
                "fs.s3a.access.key":s3accessKeyAws,
                "fs.s3a.secret.key":s3secretKeyAws,
                "spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.path.style.access":"true",
                "com.amazonaws.services.s3.enableV4":"true",
                "fs.s3a.connection.ssl.enabled":"false",
                "spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem"
                }