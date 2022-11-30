## This modules will contain the business logic for processing multiple streams that are coming from kafka.
    ## Raw_Messages_IDs
    ## Customers
    ## Transcations

from streaming.spark_engine import SparkJob
from pyspark.sql.types import StringType,TimestampType,IntegerType
from pyspark.sql.functions import col,sha2,udf
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
        def extract_payload(s):
            """
            Extracting the payload from the value_json column
            """

            return s['payload']
        
        @udf
        def extract_time(s):
            """
            
            Extracting the time at which Debezium processed the event. This will be needed to get the latest update, if multiple events are coming
            for a id in the same micro batch
            
            """
            return s['ts_ms']

        df = df.withColumn('value', col('value').cast('string'))
        df = df.filter((df.value.isNotNull()) & (df.value.cast(StringType()).like('%payload%'))) ## Safety Check: Payload should be present. Accountng for any null values.
        df = df.withColumn('value_json',to_json(col('value')))## Making the StructType Column out of Value Column
        df = df.withColumn('payload',extract_payload(col('value_json'))) ## Extracted the Main Payload
        df = df.withColumn('ts_ms',extract_time('payload'))
        df = df.select(['payload','ts_ms'])## Sending in the updated,before values and the type of operation that was done
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
        
        ##### Customer Fields ###########
        @udf
        def customer_id(field):
            if field['op'] != 'd':## If operation is not of delete type then take the after payload
                return (field['after']['id']['value'] if type(['after']['id']) == dict else field['after']['id'])
            else:
                return (field['before']['id']['value'] if type(['before']['id']) == dict else field['before']['id'])

        @udf
        def customer_status(field):
            if field['op'] != 'd':## If operation is not of delete type then take the after payload
                return (field['after']['status']['value'] if type(['after']['status']) == dict else field['after']['status'])## This accounts for the fact that if the value for field is null then it won't be of dict type
            else:
                return (field['before']['status']['value'] if type(['before']['status']) == dict else field['before']['status'])
        @udf
        def customer_status_metadatafield(field):
            if field['op'] != 'd':## If operation is not of delete type then take the after payload
                return (field['after']['status_metadata']['value'] if type(['after']['status_metadata']) == dict else field['after']['status_metadata'])
            else:
                return (field['before']['status_metadata']['value'] if type(['before']['status_metadata']) == dict else field['before']['status_metadata'])
        @udf
        def customer_creator(field):
            if field['op'] != 'd':
                return (field['after']['creator']['value'] if type(['after']['creator']) == dict else field['after']['creator'])
            else:
                return (field['before']['creator']['value'] if type(['before']['creator']) == dict else field['before']['creator'])
        @udf
        def customer_created(field):
            if field['op'] != 'd':
                return (field['after']['created']['value'] if type(['after']['created']) == dict else field['after']['created'])
            else:
                return (field['before']['created']['value'] if type(['before']['created']) == dict else field['before']['created'])
        @udf
        def customer_creator_type(field):
            if field['op'] != 'd':
                return (field['after']['creator_type']['value'] if type(['after']['creator_type']) == dict else field['after']['creator_type'])
            else:
                return (field['before']['creator_type']['value'] if type(['before']['creator_type']) == dict else field['before']['creator_type'])
        @udf
        def customer_updater(field):
            if field['op'] != 'd':
                return (field['after']['updater']['value'] if type(['after']['updater']) == dict else field['after']['updater'])
            else:
                return (field['before']['updater']['value'] if type(['before']['updater']) == dict else field['before']['updater'])
        @udf
        def customer_updated(field):
            if field['op'] != 'd':
                return (field['after']['updated']['value'] if type(['after']['updated']) == dict else field['after']['updated'])
            else:
                return (field['before']['updated']['value'] if type(['before']['updated']) == dict else field['before']['updated'])
        @udf
        def customer_updater_type(field):
            if field['op'] != 'd':
                return (field['after']['updater_type']['value'] if type(['after']['updater_type']) == dict else field['after']['updater_type'])
            else:
                return (field['before']['updater_type']['value'] if type(['before']['updater_type']) == dict else field['before']['updater_type'])
            
        df = df.filter(col('payload').like('%id%'))## ID NEEDS TO BE THEIR otherwise it is a wrong input
        
        ############ Customer Fields #####################################
        df = df.withColumn('id',customer_id(col('payload')).cast(StringType())) \
        .withColumn('status',customer_status(col('payload')).cast(StringType())) \
        .withColumn('status_metadata',customer_status_metadatafield(col('payload')).cast(StringType())) \
        .withColumn('creator',customer_creator(col('payload')).cast(StringType())) \
        .withColumn('created',customer_created(col('payload')).cast(TimestampType())) \
        .withColumn('creator_type',customer_creator_type(col('payload')).cast(StringType())) \
        .withColumn('updater',customer_updater(col('payload')).cast(StringType())) \
        .withColumn('updated',customer_updated(col('payload')).cast(TimestampType())) \
        .withColumn('updater_type',customer_updater_type(col('payload')).cast(StringType())) \
        .withColumn('time_event',col('ts_ms').cast(IntegerType())/1000)
        #####################################################################

        df = df.drop('payload','ts_ms') ## We don't need the payload column any longer

        return df