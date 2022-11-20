## This is the main module which will run our streaming pipeline according to the configuration provided in config.py
## loading from all env variables


import pyspark
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv() , override = True)
from pyspark.storagelevel import StorageLevel
from delta.tables import DeltaTable
from streaming.spark_engine import SparkProcessing
from streaming.deltalake_engine import DeltaLakeInteraction
from streaming.config import (spark_config,
                    delta_table_config,
                    hadoop_config,
                    kafka_server,
                    topic_name,
                    sourceBucket,
                    table_name,
                    cdc_schema,
                    kafka_config,
                    type_job)


def batch_function_append(micro_df:pyspark.sql.types.Row, batch_id:int) -> None:
    """

    This function inserts every streaming micro_df to the delta table.
    After every 10 batches, we run the optimize command to compact the parquet files created by delta lake
    

    Parameters:
    ----------------
    micro_df(pyspark.sql.types.Row): The current batch from our streaming data frame

    batch_id(int): The number of batch that we are processing for our pipeline

            
    Returns
    -----------  
        
    """
    print(f"===========Processing micro-batch {batch_id}===========")
    if batch_id % 100 == 0:## Compact the files into one file after every 10 batch & delete the files greater than the retention period not needed by delta lake 
        deltatable.optimize().executeCompaction()
        deltatable.vacuum()
    ## TxnAppID  & TxnVersions makes the delta lake indempotent in order to hold exactly once semantics
    micro_df.persist(StorageLevel.MEMORY_AND_DISK_DESER).write \
            .option("txnAppId", table_name) \
            .option("txnVersion", batch_id) \
            .mode('append') \
            .format('delta') \
            .save("s3a://{}/{}".format(sourceBucket,table_name))
    
def batch_function_upsert(micro_df:pyspark.sql.Row, batch_id:int):
    """
    This function either inserts the micro_df to delta table otherwise update the data from micro_df
    After every 10 batches, we run the optimize command to compact the parquet files created by delta lake
    
    Parameters:
    ----------------
    micro_df(pyspark.sql.types.Row): The current batch from our streaming data frame

    batch_id(int): The number of batch that we are processing for our pipeline

            
    Returns
    -----------  
    
    """
    print(f"===========Processing micro-batch {batch_id}===========")
    if batch_id % 100 == 0:## Compact the files into one file after every 10 batch & delete the files greater than the retention period not needed by delta lake 
        deltatable.optimize().executeCompaction()
        deltatable.vacuum()
    deltatable \
    .alias("main_table") \
    .merge(micro_df.alias("update_table").persist(StorageLevel.MEMORY_AND_DISK_DESER), "main_table.customer_id = update_table.customer_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

if __name__ == '__main__':

    #Setting up the spark session
    spark_processor = SparkProcessing('kafka_delta' , 'local[*]',spark_config,hadoop_config)

    #Reading kafka stream
    df = spark_processor.read_kafka_stream(kafka_server, topic_name, 'latest',kafka_config)
    df = spark_processor.event_processing(df , cdc_schema) ## What type of processing do we need to do?
    ### Business-wise processing can come here!

    ## Create the delta table if not exists. This will create the delta table only once.
    ## Remeber this will create the  desired delta table according to the desired table configuration.
    ## This does not need to be here eventually since this is not part of the main streaming pipeline
    
    deltalake_instance = DeltaLakeInteraction(spark_processor.spark_session, sourceBucket , table_name)
    deltatable = deltalake_instance.create_delta_table(df.schema, delta_table_config)
    ## Writing the stream to the delta lake and decide whether to append only or upsert
    if type_job == 'append':
        df.repartition(1).writeStream.foreachBatch(batch_function_append) \
            .option("checkpointLocation", "s3a://{}/{}/_checkpoint/".format(sourceBucket,table_name)) \
            .start().awaitTermination()
    else:
        # df.repartition(1).writeStream.format('console').start().awaitTermination()
        df.repartition(1).writeStream.foreachBatch(batch_function_upsert) \
            .option("checkpointLocation", "s3a://{}/{}/_checkpoint/".format(sourceBucket,table_name)) \
            .start().awaitTermination()
