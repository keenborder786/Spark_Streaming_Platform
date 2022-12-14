"""

This modules will contain the business logic for processing multiple streams
that are coming from kafka.
    - Raw_Messages_IDs
    - Customers
    - Transcations

"""

import json
from typing import List

import pyspark
from pyspark.sql.functions import col, from_json, get_json_object, when
from pyspark.sql.types import FloatType, StringType, StructType

from streaming.spark_engine import SparkJob


class SparkProcessing(SparkJob):
    """
    The main class which start our SparkSession and reads the data from our kafka streaming source
    """

    def __init__(self, app_name, hadoop_config):
        super().__init__(app_name, hadoop_config)

    def event_processing(
        self, df: pyspark.sql.DataFrame, debeziumsourceschema: StructType()
    ) -> pyspark.sql.DataFrame:
        """

        This function extracts the payload from raw value column coming from kafka.

        Parameters:
        -----------------------------------------------------------------
        df(pyspark.sql.DataFrame): Raw Events Dataframe loaded from kafka that needs to be processed

        Returns:
        ------------------------------------------------------------------
        pyspark.sql.DataFrame

        """

        df = df.withColumn("value", col("value").cast("string"))
        df = df.filter(
            (df.value.isNotNull()) & (df.value.like("%payload%"))
        )  # Safety Check: Payload should be present. Accountng for any null values.
        df = df.withColumn("payload", get_json_object(df.value, "$.payload"))
        df = df.withColumn("source", get_json_object(
            df.value, "$.payload.source"))
        df = df.withColumn("source", col("source").cast(StringType()))
        df = df.withColumn("source", from_json(
            col("source"), debeziumsourceschema))
        return df.select("payload", "source")

    def table_processing(
        self,
        df: pyspark.sql.DataFrame,
        source_schema: StructType,
        sink_schema: List[str],
        spark_to_python_types,
    ) -> pyspark.sql.DataFrame:
        """
        This function will parse the payload in the streaming df and
        take the desired columns from payload for the any given table.
        Parameters:
        -----------------------------------------------------------------
        df(pyspark.sql.DataFrame): Streaming data which consist of payload column
        that needs to be used to update the table

        source_schema(StructType): This consist of schema of the payload coming from debezium connector

        sink_schema(StructType): This consist of schema which needs to be written to the delta lake.

        Returns:
        ------------------------------------------------------------------
        pyspark.sql.DataFrame
        """
        df = df.withColumn("payload", col("payload").cast(StringType()))
        df = df.withColumn("payload", from_json(col("payload"), source_schema))
        df = df.withColumn("op", col("payload.op"))
        df = df.withColumn("time_event", col("payload.ts_ms") / 1000.0)
        df = df.withColumn("time_event", col("time_event").cast(FloatType()))

        # Get the sink schema as json
        fields = json.loads(sink_schema.json())["fields"]
        all_names_fields = []  # This is needed to select the relevant columns
        for field in fields:
            name, schema = (
                field["name"],
                field["type"],
            )  # Get the name of column and type of column
            df = df.withColumn(
                name,
                when(df.op != "d", (col(f"payload.after.{name}.value"))).otherwise(
                    col(f"payload.before.{name}.value")
                ),
            )  # If operation is u or c then take after values otherwise take the before values
            if schema == "timestamp":
                df = df.withColumn(
                    name, col(name) / 1000.0
                )  # We need to  avoid the ValueError: year is out of range
            df = df.withColumn(
                name, col(name).cast(spark_to_python_types[schema])
            )  # Cast to sink schema, where spark_to_python_types convert python types to Spark Types

            all_names_fields.append(name)

        all_names_fields = all_names_fields + ["op", "time_event"]
        return df.select(*all_names_fields)
