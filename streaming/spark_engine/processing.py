## This modules will contain the business logic for processing multiple streams that are coming from kafka.
    ## Transcations and Events
    ## Customers
from streaming.spark_engine import SparkJob
from streaming.deltalake_engine  import DeltaLakeInteraction
from pyspark.sql.functions import from_json,col,when,sha2, concat_ws
from streaming.config import sourceBucket
import pyspark


class SparkProcessing(SparkJob):
    def __init__(self,app_name,master_name,config,hadoop_config):
        super().__init__(app_name,master_name,config,hadoop_config)
    
    def event_processing(self ,df:pyspark.sql.DataFrame , schema:pyspark.sql.types.StructType) -> pyspark.sql.DataFrame:
        """
        
        Parameters:
        -----------------------------------------------------------------
        df(pyspark.sql.DataFrame): Raw Events Dataframe loaded from kafka that needs to be processed
        
        schema(pyspark.sql.types.StructType): The schema for raw event cdc coming from kafka. This needs to carefully matched with json otherwise
        stream will start giving nulls!!!
        

        Returns:    
        ------------------------------------------------------------------
        pyspark.sql.DataFrame
        
        """
        
        df = df.withColumn('value_json', from_json(col('value').cast('string'), schema))
        df = df.withColumn('unique_message_id',sha2(concat_ws('||',col('value_json.payload.source.version'),
                                                    col('value_json.payload.source.connector'),
                                                    col('value_json.payload.source.name'),
                                                    col('value_json.payload.source.ts_ms'),
                                                    col('value_json.payload.source.snapshot'),
                                                    col('value_json.payload.source.db'),
                                                    col('value_json.payload.source.sequence'),
                                                    col('value_json.payload.source.schema'),
                                                    col('value_json.payload.source.table'),
                                                    col('value_json.payload.source.txId'),
                                                    col('value_json.payload.source.lsn'),
                                                    col('value_json.payload.source.xmin')),256)).withColumn('meta_data',col('value_json.schema')) \
                .withColumn('payload',col('value_json.payload')).select('unique_message_id','value_json','meta_data','payload','timestamp')
   
        return df

    def customer_processing(self , df:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        This function will parse the payload in the streaming df and take the desired columns from payload for the customer table.

        Parameters:
        -----------------------------------------------------------------
        df(pyspark.sql.DataFrame): Streaming data which consist of payload column that needs to be used to update the customer table
        
        

        Returns:    
        ------------------------------------------------------------------
        pyspark.sql.DataFrame

        
        """

        ## We need to decide on the payload schema
        df = df.select('payload')
        df = df.withColumn('new_data',col('payload.after'))
        df = df.withColumn('id',col('new_data.id'))
        df = df.withColumn('status',col('new_data.status'))
        df = df.withColumn('status_metadata',col('new_data.status_metadata'))
        df = df.withColumn('creator',col('new_data.creator'))
        df = df.withColumn('created',col('new_data.created'))
        df = df.withColumn('creator_type',col('new_data.creator_type'))
        df = df.withColumn('updater',col('new_data.updater'))
        df = df.withColumn('updated',col('new_data.updated'))
        df = df.withColumn('updater_type',col('new_data.updater_type'))
        df = df.select('id','status','status_metadata','creator','creator_type','updater','updated','updater_type')
        return df

        
    