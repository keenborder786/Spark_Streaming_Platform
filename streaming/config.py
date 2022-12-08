import os
import json
from pyspark.sql.types import *

# ## All of the configs needed to run the streaming pipeline
s3accessKeyAws = os.environ['S3_USER']
s3secretKeyAws = os.environ['S3_PASSWORD']
s3endPointLoc= os.environ['S3_END_POINT']
sourceBucket = os.environ['SOURCE_BUCKET']
kafka_server = os.environ['KAFKA_SERVER']
kafka_config = json.loads(os.environ['KAFKA_CONSUMER_CONFIG'])
topic_name = os.environ['TOPIC_NAME']
customer_table_config = json.loads(os.environ['CUSTOMER_TABLE_CONFIG'])
customer_schema_config = json.loads(os.environ['CUSTOMER_SCHEMA'])['fields']

spark_to_python_types = {'string':StringType(),
                    'integer':IntegerType(),
                    'long':LongType(),
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

## Generating Table Schemas: Right now for one table but can be made generic by using hash_map at this stage(#TO-DO#)
customer_cdc_schema = StructType()## Source CDC-Schema(including the set & value fields)
customer_sink_schema = []## Sink Schema for the table
customer_fields_map = {} ## Fields map for the final update on delta lake table

for field in customer_schema_config:##Populating the above fields from the given schema for the table

    field_cdc_type = (spark_to_python_types['long'] if field['type'] == 'timestamp' else spark_to_python_types[field['type']]) ## Convert the type to spark type, if type is of timestamp then cdc_type will be long because thats how we recieved timestamp data in our payload
    spark_type = StructField(field['name'],StructType([StructField('value',field_cdc_type),StructField('set',BooleanType())])) ## Spark Type of one field for our table as per the cdc_payload
    customer_cdc_schema.add(spark_type) ## Updating the cdc_schema to parse the kafka message
    customer_sink_schema.append(StructField.fromJson(field)) ## Updating the sink schema
    customer_fields_map[field['name']] = 'update_table.' + field['name'] ## Updating the field mapping table needed to write data on delta lake

debeziumCustomerEventSchema =  StructType([StructField("op", StringType()),
                                        StructField("ts_ms", LongType()),
                                        StructField("transaction",StringType()),
                                        StructField("source",debeziumSourceSchema),
                                        StructField('before',customer_cdc_schema),
                                        StructField('after',customer_cdc_schema)])
## The final schema of the table on delta lake
customer_write_schema = StructType(customer_sink_schema)
## Sink Schema with operation + timestamp of the event needed to update the delta lake in foreachbatch function
customer_cdc_delta_schema = StructType(customer_sink_schema + [
                                       StructField("op", StringType()),
                                       StructField("time_event", FloatType())])

