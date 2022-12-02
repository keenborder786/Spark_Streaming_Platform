## This is the main module which will run our streaming pipeline according to the configuration provided in config.py
## loading from all env variables


import pyspark

from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv() , override = True)
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import * 
from streaming.spark_engine import SparkProcessing
from streaming.deltalake_engine import DeltaLakeInteraction

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


def batch_function_customer_processing(micro_df:pyspark.sql.DataFrame , batch_id:int):
    """
    This function updates, deletes and inserts the customer data from the CDC payload
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
    


    ## Operations
        ## Delete the Row: If the latest event is of delete type & customer_id is in the delta table
        ## Update the Row: If the latest event is of not delete type & customer id is in the delta table 
        ## Insert the row: If the latest event is of not delete type & customer id is not in the delta table 
    each_row_data = micro_df.orderBy('id','time_event').persist(StorageLevel.MEMORY_AND_DISK_DESER).collect()
    for row in each_row_data:##########  Iterating over each event and updating the delta lake table. This is needed to keep track of all events.
        latestChangesDF = spark_processor.spark_session.createDataFrame([row],schema = customer_cdc_delta_schema)## Need to convert each rowback to dataframe type
        
        customer_table \
        .alias("main_table") \
        .merge(latestChangesDF.alias("update_table"), "main_table.id = update_table.id") \
        .whenMatchedDelete(condition = "update_table.op = 'd'") \
        .whenMatchedUpdate(condition = "update_table.op != 'd'" , set  = customer_fields_map) \
        .whenNotMatchedInsert(condition = "update_table.op != 'd'" , values = customer_fields_map) \
        .execute()

if __name__ == '__main__':
    #Setting up the spark session
    spark_processor = SparkProcessing('kafka_delta' ,hadoop_config)
    
    #Reading kafka stream
    df = spark_processor.read_kafka_stream(kafka_server, topic_name, 'latest',kafka_config)
    
    #### Processing the raw_events coming from kafka. Extracting payload which contains the events for our table.
    raw_events = spark_processor.event_processing(df)
   
    ##### Processing the customer data from payload.
    customer_update = spark_processor.table_processing(raw_events , debeziumCustomerEventSchema , customer_write_schema)

    ###### Create the customer table if it does not exists
    customer_table_deltalake_instance = DeltaLakeInteraction(spark_processor.spark_session, sourceBucket , 'DimCustomer')
    customer_table = customer_table_deltalake_instance.create_delta_table(customer_write_schema , customer_table_config)

    # # ###### Updating the customer table data on delta lake from our new events
    final_streaming = customer_update.writeStream.foreachBatch(batch_function_customer_processing).outputMode("update") \
        .start()
    spark_processor.spark_session.streams.awaitAnyTermination()

