"""
The main configuration module which loads all of the env variables
needed to run our streaming pipeline.

"""

import json
import os

from dotenv import find_dotenv, load_dotenv
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.utils import parse_json

# loading the schemas and config for delta lake tables
load_dotenv(find_dotenv(".env.schema"), override=True)
s3accessKeyAws = os.environ["S3_USER"]
s3secretKeyAws = os.environ["S3_PASSWORD"]
s3endPointLoc = os.environ["S3_END_POINT"]
sourceBucket = os.environ["S3_SOURCE_BUCKET"]
kafka_server = os.environ["KAFKA_SERVER"]
kafka_config = json.loads(os.environ["KAFKA_CONSUMER_CONFIG"])
topic_name = os.environ["KAFKA_TOPIC_NAME"]
delta_lake_tables_config = json.loads(os.environ["DELTA_LAKE_TABLE_CONFIG"])[
    "Delta_Lake_Tables"]
delta_lake_schemas = json.loads(os.environ["DELTA_LAKE_TABLE_SCHEMAS"])[
    "Delta_Lake_Tables"]

spark_to_python_types = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "timestamp": TimestampType(),
    "float": FloatType(),
}

hadoop_config = {
    "fs.s3a.endpoint": s3endPointLoc,
    "fs.s3a.access.key": s3accessKeyAws,
    "fs.s3a.secret.key": s3secretKeyAws,
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "com.amazonaws.services.s3.enableV4": "true",
    "fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

debeziumsourceschema = StructType(
    [
        StructField("version", StringType()),
        StructField("connector", StringType()),
        StructField("name", StringType()),
        StructField("ts_ms", LongType()),
        StructField("snapshot", StringType()),
        StructField("db", StringType()),
        StructField("sequence", StringType()),
        StructField("schema", StringType()),
        StructField("table", StringType()),
        StructField("txId", StringType()),
        StructField("lsn", StringType()),
        StructField("xmin", StringType()),
    ]
)
final_schemas = {}
for table in delta_lake_schemas:
    final_schemas[table] = {}
    (
        table_write_schema,
        table_fields_map,
        table_cdc_delta_schema,
        debeziumTableEventSchema,
    ) = parse_json(delta_lake_schemas[table]["fields"], spark_to_python_types, debeziumsourceschema)
    final_schemas[table][f"{table}_write_schema"] = table_write_schema
    final_schemas[table][f"{table}_fields_map"] = table_fields_map
    final_schemas[table][f"{table}_cdc_delta_schema"] = table_cdc_delta_schema
    final_schemas[table][f"{table}_debeziumTableEventSchema"] = debeziumTableEventSchema
