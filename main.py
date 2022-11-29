## This is the main module which will run our streaming pipeline according to the configuration provided in config.py
## loading from all env variables


import pyspark

from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv() , override = True)
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import *
from streaming.spark_engine import SparkProcessing
from streaming.deltalake_engine import DeltaLakeInteraction
from streaming.config import (spark_config,
                    hadoop_config,
                    kafka_server,
                    topic_name,
                    sourceBucket,
                    kafka_config,
                    customer_table_config,
                    fact_hash_id_table_config)



def batch_function_raw_events(micro_df:pyspark.sql.types.Row, batch_id:int) -> None:
    """

    This functions insert every message consisting of CDC payload as it on Delta Lake. Making sure
    that each message is only written once.
    After every 100 batches, we run the optimize command to compact the parquet files created by delta lake
    

    Parameters:
    ----------------
    micro_df(pyspark.sql.types.Row): The current batch from our streaming data frame

    batch_id(int): The number of batch that we are processing for our pipeline

            
    Returns
    -----------  
        
    """
    print(f"===========Processing micro-batch {batch_id} for Raw Events===========")
    if batch_id % 100 == 0:## Compact the files into one file after every 10 batch & delete the files greater than the retention period not needed by delta lake 
        raw_events_table.optimize().executeCompaction()
    
    ## If the message is already written to raw events table then don't write the message.
    raw_events_table.alias('events') \
    .merge(micro_df.alias('updates').persist(StorageLevel.MEMORY_AND_DISK_DESER) , "events.unique_message_id = updates.unique_message_id") \
    .whenNotMatchedInsertAll().execute()

    
    
def batch_function_customer_processing(micro_df:pyspark.sql.DataFrame, batch_id:int):
    """
    This function either inserts the micro_df to delta table otherwise update the data from micro_df
    After every 100 batches, we run the optimize command to compact the parquet files created by delta lake
    
    Parameters:
    ----------------
    micro_df(pyspark.sql.types.Row): The current batch from our streaming data frame

    batch_id(int): The number of batch that we are processing for our pipeline

            
    Returns
    -----------  
    
    """
    print(f"===========Processing micro-batch {batch_id} for Customer Table===========")
    if batch_id % 100 == 0:## Compact the files into one file after every 10 batch & delete the files greater than the retention period not needed by delta lake 
        customer_table.optimize().executeCompaction()
    
    customer_table \
    .alias("main_table") \
    .merge(micro_df.alias("update_table").persist(StorageLevel.MEMORY_AND_DISK_DESER), "main_table.id = update_table.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

if __name__ == '__main__':

    #Setting up the spark session
    spark_processor = SparkProcessing('kafka_delta' , 'local[*]',spark_config,hadoop_config)
    raw_events_deltalake_instance = DeltaLakeInteraction(spark_processor.spark_session, sourceBucket , 'FactHashIDs')

    #Reading kafka stream
    df = spark_processor.read_kafka_stream(kafka_server, topic_name, 'latest',kafka_config)
    

    #### Processing the raw_events coming from kafka. Taking out the unique_message_id and payload which contains the values for our table.
    raw_events = spark_processor.event_processing(df)
    
    ##### Create the FactHashID tables if it does not exists. FactHashID will consist of hashes of messages_ids that have been processed so far.
    raw_events_deltalake_instance = DeltaLakeInteraction(spark_processor.spark_session, sourceBucket , 'FactHashIDs')
    raw_events_table = raw_events_deltalake_instance.create_delta_table(raw_events.select('unique_message_id').schema, fact_hash_id_table_config)
    
    ##### Processing the customer data from payload.
    customer_update = spark_processor.customer_table_processing(raw_events.select('after_payload'))

    #### Create the customer table if it does not exists
    customer_table_deltalake_instance = DeltaLakeInteraction(spark_processor.spark_session, sourceBucket , 'DimCustomer')
    customer_table = customer_table_deltalake_instance.create_delta_table(customer_update.schema, customer_table_config)

    #### Writing the raw_events on delta lake and ensuring each message is only written once by using a unique identifier
    ### We only write the hash values to our FactHashIDs table. 
    raw_event_streaming = raw_events.select(['unique_message_id']).repartition(1).writeStream.foreachBatch(batch_function_raw_events).outputMode("append") \
        .option("checkpointLocation", "s3a://{}/{}/_checkpoint".format(sourceBucket,'FactHashIDs')) \
        .start()
    
    ##### Updating the customer table data on delta lake from our new messages
    final_streaming = customer_update.repartition(1).writeStream.foreachBatch(batch_function_customer_processing).outputMode("update") \
        .option("checkpointLocation", "s3a://{}/{}/_checkpoint".format(sourceBucket,'DimCustomer')) \
        .start()
    spark_processor.spark_session.streams.awaitAnyTermination()
    