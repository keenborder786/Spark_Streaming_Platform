import pyspark
import delta
from streaming.spark_engine import SparkProcessing

from typing import Dict
from pyspark.storagelevel import StorageLevel

def batch_function_processing(micro_df:pyspark.sql.DataFrame , batch_id:int , 
                             spark_processor:SparkProcessing,
                             delta_lake_builder:delta.tables.DeltaTableBuilder , 
                             cdc_schema:pyspark.sql.types.StructType,
                             fields_map:Dict[str,str]):
    """
    This function updates, deletes and inserts the data from the CDC payload for the given table on delta lake
    After every 100 batches, we run the optimize command to compact the parquet files created by delta lake
    
    Parameters:
    ----------------
    micro_df(pyspark.sql.types.Row): The current batch from our streaming data frame
    
    batch_id(int): The number of batch that we are processing for our pipeline

    spark_processor(streaming.spark_engine.SparkProcessing): The underlying class called SparkProcessing in spark_engine module.
    
    delta_lake_builder(DeltaTableBuilder): The delta lake builder needed to modify the table on delta lake
    
    cdc_schema(StrucType): The schema for the given table on delta lake in spark struct type including op and time_event columns.
            
    fields_map(Dict): The columns for the given table for which you want to insert or update the values from the payload.
    
    Returns
    -----------  
    
    """
    print(f"===========Processing micro-batch {batch_id} for Customer Table===========")
    if batch_id % 100 == 0:## Compact the files into one file after every 10 batch & delete the files greater than the retention period not needed by delta lake 
        delta_lake_builder.optimize().executeCompaction()
    


    ## Operations
        ## Delete the Row: If the latest event is of delete type & customer_id is in the delta table
        ## Update the Row: If the latest event is of not delete type & customer id is in the delta table 
        ## Insert the row: If the latest event is of not delete type & customer id is not in the delta table 
    
    each_row_data = micro_df.orderBy('id','time_event').persist(StorageLevel.MEMORY_AND_DISK_DESER).collect()
    for row in each_row_data:##########  Iterating over each event and updating the delta lake table. This is needed to keep track of all events.
        latestChangesDF = spark_processor.spark_session.createDataFrame([row],schema = cdc_schema)## Need to convert each rowback to dataframe type
        
        delta_lake_builder \
        .alias("main_table") \
        .merge(latestChangesDF.alias("update_table"), "main_table.id = update_table.id") \
        .whenMatchedDelete(condition = "update_table.op = 'd'") \
        .whenMatchedUpdate(condition = "update_table.op != 'd'" , set  = fields_map) \
        .whenNotMatchedInsert(condition = "update_table.op != 'd'" , values = fields_map) \
        .execute()