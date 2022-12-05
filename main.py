## This is the main module which will run our streaming pipeline according to the configuration provided in config.py
## loading from all env variables



from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv() , override = True)
from pyspark.sql.types import *
from pyspark.sql.functions import * 
from streaming.spark_engine import SparkProcessing
from streaming.deltalake_engine import DeltaLakeInteraction
from streaming.batch_writing import batch_function_processing
from streaming.config import (
                    hadoop_config,
                    kafka_server,
                    topic_name,
                    sourceBucket,
                    kafka_config,
                    customer_table_config,
                    customer_write_schema,
                    customer_fields_map,
                    customer_cdc_delta_schema,
                    debeziumCustomerEventSchema)


if __name__ == '__main__':
    #Setting up the spark session
    spark_processor = SparkProcessing('kafka_delta' ,hadoop_config)
    
    #Reading kafka stream
    df = spark_processor.read_kafka_stream(kafka_server, topic_name, 'latest', kafka_config)
    
    #### Processing the raw_events coming from kafka. Extracting payload which contains the events for our table.
    raw_events = spark_processor.event_processing(df)
   
    ##### Processing the customer data from payload.
    customer_update = spark_processor.table_processing(raw_events , debeziumCustomerEventSchema , customer_write_schema)
    
    # ###### Create the customer table if it does not exists
    customer_table_deltalake_instance = DeltaLakeInteraction(spark_processor.spark_session, sourceBucket , 'DimCustomer')
    customer_table = customer_table_deltalake_instance.create_delta_table(customer_write_schema , customer_table_config)

    # # ###### Updating the customer table data on delta lake from our new events
    final_streaming = customer_update.writeStream.foreachBatch(lambda micro_df,epochId: batch_function_processing(micro_df, epochId, 
        spark_processor,customer_table,customer_cdc_delta_schema , customer_fields_map)).option("checkpointLocation", "s3a://{}/{}/_checkpoint".format(sourceBucket,'DimCustomer')) \
        .outputMode("update") \
        .start()
    spark_processor.spark_session.streams.awaitAnyTermination()

