from typing import Callable, Dict, List, Tuple

import pyspark
from pyspark.sql.types import BooleanType, FloatType, LongType, StringType, StructField, StructType
from pyspark.storagelevel import StorageLevel

from streaming.spark_engine import SparkProcessing


def bindFunction(
    table_name: str,
    spark_processor: SparkProcessing,
    delta_lake_builder,
    cdc_schema: StructType(),
    fields_map: Dict[str, str],
) -> Callable:
    """
    The bindFunction creates a batch processing function for the given deltalake table with the desired schema.
    We call bindFunction for each of the tables given in our final schema.

    Parameters:
    ----------------
    spark_processor(streaming.spark_engine.SparkProcessing): The underlying class called SparkProcessing in spark_engine module.

    table_name(String): The table name for which we are updating the data for.

    delta_lake_builder(DeltaTableBuilder): The delta lake builder needed to modify the table on delta lake

    cdc_schema(StrucType): The schema for the given table on delta lake in spark struct type including op and time_event columns.

    fields_map(Dict): The columns for the given table for which you want to insert or update the values from the payload.

    Returns:
    -------------
    func(Callable): Batch Processing Function with desired parameters

    """

    def func(micro_df: pyspark.sql.DataFrame, batch_id: int):
        """
        This is the batch processing function which is created for each of our tables and is used by ForEachBatch Spark
        Stream processing.

        This function updates, deletes and inserts the data from the CDC payload for the given table on delta lake
        After every 100 batches, we run the optimize command to compact the parquet files created by delta lake.


        Parameters:
        ----------------
        micro_df(pyspark.sql.types.Row): The current batch from our streaming data frame

        batch_id(int): The number of batch that we are processing for our pipeline

        Returns
        -----------

        """

        print(
            f"===========Processing micro-batch {batch_id} for {table_name} Table===========")
        if batch_id % 10 == 0:  # Compact the files into one file after every 10 batch
            delta_lake_builder.optimize().executeCompaction()

        # Latest Window Approach
        # latestChangesDF = micro_df.withColumn("row_num", row_number().over(Window.partitionBy("id").orderBy(col("time_event").desc()))).where("row_num == 1")

        # Operations
        # Delete the Row: If the latest event is of delete type & customer_id is in the delta table
        # Update the Row: If the latest event is of not delete type & customer id is in the delta table
        # Insert the row: If the latest event is of not delete type & customer id is not in the delta table

        each_row_data = (
            micro_df.orderBy("id", "time_event")
            .persist(StorageLevel.MEMORY_AND_DISK_DESER)
            .collect()
        )
        for (
            row
        ) in (
            each_row_data
        ):  # Iterating over each event and updating the delta lake table. This is needed to keep track of all events.
            latestChangesDF = spark_processor.spark_session.createDataFrame(
                [row], schema=cdc_schema
            )  # Need to convert each rowback to dataframe type

            delta_lake_builder.alias("main_table").merge(
                latestChangesDF.alias(
                    "update_table"), "main_table.id = update_table.id"
            ).whenMatchedDelete(condition="update_table.op = 'd'").whenMatchedUpdate(
                condition="update_table.op != 'd'", set=fields_map
            ).whenNotMatchedInsert(
                condition="update_table.op != 'd'", values=fields_map
            ).execute()

    func.__name__ = table_name
    return func


def parse_json(
    table_schema_config: List[Dict[str, str]],
    spark_to_python_types: Dict[str, str],
    debeziumSourceSchema: StructType(),
) -> Tuple:
    """
    This will parse the json which consist of our schema for a particular table.

    Parameters:
    ------------------------------------------------------------------------------
    table_schema_config (List): A List of dictionary which consist of information about each field in the table. The field consist of the following information:
        - metadata
        - name
        - nullable
        - type

    spark_to_python_types (Dict): A Dictionary which converts types in table_schema_config to spark types

    debeziumSourceSchema (StructType): The schema in spark of the CDC payload's source


    Returns:
    -------------------------------------------------------------------------------

    A tuple consisting of:

    - table_write_schema: The final sink schema of the table
    - table_fields_map: Field mapping to update our delta table
    - table_cdc_delta_schema: table_write_schema including op and time_event column
    - debeziumTableEventSchema: The final cdc payload schema for the table


    """
    # Generating Table Schemas: Right now for one table but can be made generic by using hash_map at this stage(#TO-DO#)
    # Source CDC-Schema(including the set & value fields)
    table_cdc_schema = StructType()
    table_sink_schema = []  # Sink Schema for the table
    table_fields_map = {}  # Fields map for the final update on delta lake table

    for (
        field
    ) in table_schema_config:  # Populating the above fields from the given schema for the table

        field_cdc_type = (
            spark_to_python_types["long"]
            if field["type"] == "timestamp"
            else spark_to_python_types[field["type"]]
        )  # Convert the type to spark type, if type is of timestamp then cdc_type will be long because thats how we recieved timestamp data in our payload
        spark_type = StructField(
            field["name"],
            StructType(
                [
                    StructField("value", field_cdc_type),
                    StructField("set", BooleanType()),
                ]
            ),
        )  # Spark Type of one field for our table as per the cdc_payload
        # Updating the cdc_schema to parse the kafka message
        table_cdc_schema.add(spark_type)
        # Updating the sink schema
        table_sink_schema.append(StructField.fromJson(field))
        table_fields_map[field["name"]] = (
            "update_table." + field["name"]
        )  # Updating the field mapping table needed to write data on delta lake

    debeziumTableEventSchema = StructType(
        [
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField("transaction", StringType()),
            StructField("source", debeziumSourceSchema),
            StructField("before", table_cdc_schema),
            StructField("after", table_cdc_schema),
        ]
    )
    # The final schema of the table on delta lake
    table_write_schema = StructType(table_sink_schema)
    # Sink Schema with operation + timestamp of the event needed to update the delta lake in foreachbatch function
    table_cdc_delta_schema = StructType(
        table_sink_schema
        + [StructField("op", StringType()),
           StructField("time_event", FloatType())]
    )

    return (
        table_write_schema,
        table_fields_map,
        table_cdc_delta_schema,
        debeziumTableEventSchema,
    )
