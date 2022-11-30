## This modules will contain the business logic for processing multiple streams that are coming from kafka.
    ## Raw_Messages_IDs
    ## Customers
    ## Transcations

from streaming.spark_engine import SparkJob
from pyspark.sql.types import StringType,TimestampType,IntegerType
from pyspark.sql.functions import col,udf,lit
import pyspark
import json


class SparkProcessing(SparkJob):

    def __init__(self,app_name,hadoop_config):
        super().__init__(app_name,hadoop_config)
    
    def event_processing(self ,df:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        
        This function extracts the payload from raw value column coming from kafka and also form of a unique_message_id
        to identify each message that has been processed.
        Parameters:
        -----------------------------------------------------------------
        df(pyspark.sql.DataFrame): Raw Events Dataframe loaded from kafka that needs to be processed
                
        Returns:    
        ------------------------------------------------------------------
        pyspark.sql.DataFrame
        
        """
        ### User Defined Functions to parse the json string coming from kafka
        @udf
        def to_json(s):
            """
            Converts coming kafka json string to JSON type in the dataframe.
            
            """
            
            s=json.loads(s)
            return s
        
        @udf
        def extract_time(s):
            """
            
            Extracting the time at which Debezium processed the event. This will be needed to get the latest update, if multiple events are coming
            for a id in the same micro batch
            
            """
            return s['payload']['ts_ms']
        @udf
        def extract_operation(s):
            return s['payload']['op']

        df = df.withColumn('value', col('value').cast('string'))
        df = df.filter((df.value.isNotNull()) & (df.value.like('%payload%'))) ## Safety Check: Payload should be present. Accountng for any null values.
        df = df.withColumn('cdc_event',to_json(col('value')))## Making the StructType Column out of Value Column
        df = df.withColumn('time_event',extract_time('cdc_event'))
        df = df.withColumn('op',extract_operation('cdc_event'))
        df = df.select(['cdc_event','time_event','op'])## Sending in the updated,before values and the type of operation that was done
        return df

    def customer_table_processing(self , df:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        This function will parse the payload in the streaming df and take the desired columns from payload for the given table.
        Parameters:
        -----------------------------------------------------------------
        df(pyspark.sql.DataFrame): Streaming data which consist of payload column that needs to be used to update the table
        
        Returns:    
        ------------------------------------------------------------------
        pyspark.sql.DataFrame
        """
        ## Due to restriction in pyspark straming we are extracting values from the given field in way. We might need to find a viable alternative for the long-term
        ## Can't use the for loops, it gives an error. There is a way to pass column name in argument in pyspark in udf but in streaming it gives an error.
        
        ##### Customer Fields  Extraction Functions ###########
        @udf
        def customer_id(field):
            
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['id']['value'] if type(payload['after']['id']) == dict else payload['after']['id'])
            else:
                return (payload['before']['id']['value'] if type(payload['before']['id']) == dict else payload['before']['id'])

        @udf
        def customer_status(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['status']['value'] if type(payload['after']['status']) == dict else payload['after']['status'])
            else:
                return (payload['before']['status']['value'] if type(payload['before']['status']) == dict else payload['before']['status'])
        @udf
        def customer_status_metadata(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['status_metadata']['value'] if type(payload['after']['status_metadata']) == dict else payload['after']['status_metadata'])
            else:
                return (payload['before']['status_metadata']['value'] if type(payload['before']['status_metadata']) == dict else payload['before']['status_metadata'])
        @udf
        def customer_creator(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['creator']['value'] if type(payload['after']['creator']) == dict else payload['after']['creator'])
            else:
                return (payload['before']['creator']['value'] if type(payload['before']['creator']) == dict else payload['before']['creator'])
        @udf
        def customer_created(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['created']['value'] if type(payload['after']['created']) == dict else payload['after']['created'])
            else:
                return (payload['before']['created']['value'] if type(payload['before']['created']) == dict else payload['before']['created'])
        @udf
        def customer_creator_type(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['creator_type']['value'] if type(payload['after']['creator_type']) == dict else payload['after']['creator_type'])
            else:
                return (payload['before']['creator_type']['value'] if type(payload['before']['creator_type']) == dict else payload['before']['creator_type'])
        @udf
        def customer_updater(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['updater']['value'] if type(payload['after']['updater']) == dict else payload['after']['updater'])
            else:
                return (payload['before']['updater']['value'] if type(payload['before']['updater']) == dict else payload['before']['updater'])
        @udf
        def customer_updated(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['updated']['value'] if type(payload['after']['updated']) == dict else payload['after']['updated'])
            else:
                return (payload['before']['updated']['value'] if type(payload['before']['updated']) == dict else payload['before']['updated'])
        @udf
        def customer_updater_type(field):
            payload = field['payload']
            if payload['op'] != 'd':## If operation is not of delete type then take the after payload
                return (payload['after']['updater_type']['value'] if type(payload['after']['updater_type']) == dict else payload['after']['updater_type'])
            else:
                return (payload['before']['updater_type']['value'] if type(payload['before']['updater_type']) == dict else payload['before']['updater_type'])
            
       
        
        ############ Customer Fields #######################################################
        df = df.withColumn('id',customer_id(col('cdc_event')).cast(StringType())) \
        .withColumn('status',customer_status(col('cdc_event')).cast(StringType())) \
        .withColumn('status_metadata',customer_status_metadata(col('cdc_event')).cast(StringType())) \
        .withColumn('creator',customer_creator(col('cdc_event')).cast(StringType())) \
        .withColumn('created',customer_created(col('cdc_event')).cast(TimestampType())) \
        .withColumn('creator_type',customer_creator_type(col('cdc_event')).cast(StringType())) \
        .withColumn('updater',customer_updater(col('cdc_event')).cast(StringType())) \
        .withColumn('updated',customer_updated(col('cdc_event')).cast(TimestampType())) \
        .withColumn('updater_type',customer_updater_type(col('cdc_event')).cast(StringType()))\
        .withColumn('time_event',col('time_event')/1000)
        ####################################################################################

        df = df.drop('cdc_event') ## We don't need the payload column any longer

        return df
