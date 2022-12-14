"""
This is the main module which will run our streaming pipeline
according to the configuration provided in config.py
loading from all env variables

"""


from dotenv import find_dotenv, load_dotenv

from streaming.config import (
    debeziumSourceSchema,
    delta_lake_tables_config,
    final_schemas,
    hadoop_config,
    kafka_config,
    kafka_server,
    sourceBucket,
    spark_to_python_types,
    topic_name,
)
from streaming.deltalake_engine import DeltaLakeInteraction
from streaming.spark_engine import SparkProcessing
from streaming.utils import bindFunction

load_dotenv(find_dotenv(), override=True)

if __name__ == "__main__":
    # Setting up the spark session
    # Spark Session for the streaming jobs.
    spark_processor = SparkProcessing("kafka_delta", hadoop_config)
    # This will store the streaming dataframes for each of our tables.
    hash_map_variables_streams = {}
    # This list will consist of created batch_processing function
    # for each of tables and the table names.
    all_batch_processing_funcitons = []

    for table, _ in final_schemas.items():  # Iterate over each table.
        # Create the desired table if it does not exists
        table_deltalake_instance = DeltaLakeInteraction(
            spark_processor.spark_session, sourceBucket, table
        )
        delta_lake_table = table_deltalake_instance.create_delta_table(
            final_schemas[table][f"{table}_write_schema"],
            delta_lake_tables_config.get(table, {}),
        )

        # Create the batch_processing_function for the given tables with its schema and
        # parameters and append to the all_batch_processing_funcitons with the table name.
        all_batch_processing_funcitons.append(
            (
                bindFunction(
                    table,
                    spark_processor,
                    delta_lake_table,
                    final_schemas[table][f"{table}_cdc_delta_schema"],
                    final_schemas[table][f"{table}_fields_map"],
                ),
                table,
            )
        )

    # Iterate over each batch_processing_function created for each table
    for func, table in all_batch_processing_funcitons:

        # Reading kafka stream
        hash_map_variables_streams["df_" + str(table)] = spark_processor.read_kafka_stream(
            kafka_server, topic_name, "latest", kafka_config
        )

        # Processing the raw_events coming from kafka.
        # Extracting payload which contains the events for our table.
        hash_map_variables_streams["raw_events_" + str(table)] = spark_processor.event_processing(
            hash_map_variables_streams["df_" +
                                       str(table)], debeziumSourceSchema
        )

        # Filtering out the events that we need for the table
        hash_map_variables_streams["raw_events_" + str(table)] = hash_map_variables_streams[
            "raw_events_" + str(table)
        ].filter((hash_map_variables_streams["raw_events_" + str(table)].source.table == table))

        # Processing the table data from payload.
        hash_map_variables_streams["table_update_" + str(table)] = spark_processor.table_processing(
            hash_map_variables_streams["raw_events_" + str(table)],
            final_schemas[table][f"{table}_debeziumTableEventSchema"],
            final_schemas[table][f"{table}_write_schema"],
            spark_to_python_types,
        )

        # Updating the table data on delta lake from our new events
        # func is the batch_processing_function
        # which we have created by calling bindFunction in above loop.
        hash_map_variables_streams["table_update_" + str(table)].writeStream.foreachBatch(
            func
        ).option("checkpointLocation", f"s3a://{sourceBucket}/{table}/_checkpoint").outputMode(
            "update"
        ).start()

    # Waiting for any of the streams' termination
    spark_processor.spark_session.streams.awaitAnyTermination()
