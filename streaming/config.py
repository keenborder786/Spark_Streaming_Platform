import os
import json
from pyspark.sql.types import *

# ## All of the configs needed to run the streaming pipeline
s3accessKeyAws = os.environ['S3USER']
s3secretKeyAws = os.environ['S3Password']
s3endPointLoc= os.environ['S3EndPoint']
sourceBucket = os.environ['SourceBucket']
kafka_server = os.environ['KafkaServer']
kafka_config = json.loads(os.environ['KafkaConsumerConfig'])
topic_name = os.environ['TopicName']
customer_table_config = json.loads(os.environ['CustomerTableConfig'])

spark_to_python_types = {'string':StringType(),
                    'integer':IntegerType(),
                    'timestamp':TimestampType(),
                    'float':FloatType()}

hadoop_config={"fs.s3a.endpoint":s3endPointLoc,
                "fs.s3a.access.key":s3accessKeyAws,
                "fs.s3a.secret.key":s3secretKeyAws,
                "spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.path.style.access":"true",
                "com.amazonaws.services.s3.enableV4":"true",
                "fs.s3a.connection.ssl.enabled":"false",
                "spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem"
                }

debeziumSourceSchema = StructType([
                                StructField('version',StringType()),
                                StructField('connector',StringType()),
                                StructField('name',StringType()),
                                StructField('ts_ms',LongType()),
                                StructField('snapshot',StringType()),
                                StructField('db',StringType()),
                                StructField('sequence',StringType()),
                                StructField('schema',StringType()),
                                StructField('table',StringType()),
                                StructField('txId',StringType()),
                                StructField('lsn',StringType()),
                                StructField('xmin',StringType())
                                ])

## Customer Schemas:
##  a) Source Schema:## This can be made less complex if the values are not recieved in StructType/Dict
## This is the source scema. How data is coming from debzium
customer_cdc_schema = StructType([
                              StructField('id', StructType().add(StructField('value',StringType())).add(StructField('set',BooleanType()))), 
                              StructField('status', StructType().add(StructField('value',StringType())).add(StructField('set',BooleanType()))), 
                              StructField('status_metadata', StructType().add(StructField('value',StringType())).add(StructField('set',BooleanType()))), 
                              StructField('creator', StructType().add(StructField('value',StringType())).add(StructField('set',BooleanType()))), 
                              StructField('created', StructType().add(StructField('value',LongType())).add(StructField('set',BooleanType()))), 
                              StructField('creator_type', StructType().add(StructField('value',StringType())).add(StructField('set',BooleanType()))), 
                              StructField('updater', StructType().add(StructField('value',StringType())).add(StructField('set',BooleanType()))), 
                              StructField('updated', StructType().add(StructField('value',LongType())).add(StructField('set',BooleanType()))), 
                              StructField('updater_type',StructType().add(StructField('value',StringType())).add(StructField('set',BooleanType())))])

debeziumCustomerEventSchema =  StructType([StructField("op", StringType()),
                                        StructField("ts_ms", LongType()),
                                        StructField("transaction",StringType()),
                                        StructField("source",debeziumSourceSchema),
                                        StructField('before',customer_cdc_schema),
                                        StructField('after',customer_cdc_schema)])
##  b) Sink Schema
customer_schema  = [StructField('id', StringType(), True), 
                              StructField('status', StringType(), True), 
                              StructField('status_metadata', StringType(), True), 
                              StructField('creator', StringType(), True), 
                              StructField('created', TimestampType(), True), 
                              StructField('creator_type', StringType(), True), 
                              StructField('updater', StringType(), True), 
                              StructField('updated', TimestampType(), True), 
                              StructField('updater_type', StringType(), True)]

## b-1) The final schema of the table on delta lake
customer_write_schema = StructType(customer_schema)

##  b-2) Sink Schema with operation + timestamp of the event needed to update the delta lake in foreachbatch function
customer_cdc_delta_schema = StructType(customer_schema + [
                                       StructField("op", StringType()),
                                       StructField("time_event", FloatType())])

##  c) LookUp table for batch function
customer_fields_map = { 'id':'update_table.id',
                        'status':'update_table.status',
                        'status_metadata':'update_table.status_metadata',
                        'creator':'update_table.creator',
                        'created':'update_table.created',
                        'creator_type':'update_table.creator_type',
                        'updater':'update_table.updater',
                        'updated':'update_table.updated',
                        'updater_type':'update_table.updater_type'
                        }