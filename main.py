## This is the main module which will run our streaming pipeline according to the configuration provided in config.py
## loading from all env variables



from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv() , override = True)
from pyspark.sql.types import *
from pyspark.sql.functions import * 
from streaming.spark_engine import SparkProcessing
from streaming.deltalake_engine import DeltaLakeInteraction
from streaming.utils import batch_function_processing
from streaming.config import (
                    spark_to_python_types,
                    hadoop_config,
                    kafka_server,
                    topic_name,
                    sourceBucket,
                    kafka_config,
                    delta_lake_tables_config,
                    final_schemas)


if __name__ == '__main__':
    #Setting up the spark session
    spark_processor = SparkProcessing('kafka_delta' ,hadoop_config)
    
    
    for table in final_schemas:## Start streaming for each table
        #Reading kafka stream
        df = spark_processor.read_kafka_stream(kafka_server, topic_name, 'latest', kafka_config)
        
        ##### Processing the raw_events coming from kafka. Extracting payload which contains the events for our table.
        raw_events = spark_processor.event_processing(df)
        
        ###### Processing the customer data from payload.
        table_update = spark_processor.table_processing(raw_events , final_schemas[table][f'{table}_debeziumTableEventSchema']  , 
                                                        final_schemas[table][f'{table}_write_schema'],
                                                        spark_to_python_types)
        
        ####### Create the customer table if it does not exists
        table_deltalake_instance = DeltaLakeInteraction(spark_processor.spark_session, sourceBucket , table)
        delta_lake_table = table_deltalake_instance.create_delta_table(final_schemas[table][f'{table}_write_schema'] , delta_lake_tables_config[table])

        ######## Updating the customer table data on delta lake from our new events
        table_update.writeStream.foreachBatch(lambda micro_df,epochId: batch_function_processing(micro_df, epochId, 
            spark_processor,delta_lake_table, final_schemas[table][f'{table}_cdc_delta_schema'] , 
                                              final_schemas[table][f'{table}_fields_map'])).option("checkpointLocation", "s3a://{}/{}/_checkpoint".format(sourceBucket,table)) \
            .outputMode("update") \
            .start()
    spark_processor.spark_session.streams.awaitAnyTermination()

