## This modules will contain the business logic for processing multiple streams that are coming from kafka.
    ## Raw_Messages_IDs
    ## Customers
    ## Transcations
from json.decoder import JSONDecodeError
from pyspark.sql.functions import lit
from streaming.spark_engine import SparkJob
from pyspark.sql.types import StringType,TimestampType
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
        def extract_source(s):
            """
            Extracting the source from the payload
            
            """
            
            return s['source']
            
        @udf
        def extract_unique_identifier(s):
            """
            Creating the unique identifier from source of payload
            
            """
            
            return s['db'] + '||' + s['sequence'] + '||' +s['table'] + '||' + s['txId']
            
        @udf
        def extract_payload_after(payload_data):
            """
            Extracting the updated value from the payload
            
            """
            
            return payload_data['after']
           
                

        df = df.withColumn('value', col('value').cast('string'))
        df = df.filter((df.value.isNotNull()) & (df.value.cast(StringType()).like('%payload%'))) ## Safety Check: Payload should be present
        df = df.withColumn('value_json',to_json(col('value')))## Making the StructType Column out of Value Column
        df = df.withColumn('payload',extract_payload(col('value_json'))) ## Extracted the Main Payload
        df = df.withColumn('source',extract_source(col('payload')))## Extracted the Main Source
        df = df.withColumn('after_payload',extract_payload_after(col('payload')))## Extracted the Updated Values from Payload
        df = df.withColumn('unique_message_id',extract_unique_identifier(col('source')))## Created the Unique Message ID from source fields
        df = df.withColumn('unique_message_id',sha2(col('unique_message_id'),256))
        df = df.select(['unique_message_id','after_payload'])## Sending in the updated values
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
        
        @udf
        def customer_id(field):
            return (field['id']['value'] if type(field['id'])==dict else field['id'])
        @udf
        def customer_status(field):
            return (field['status']['value'] if type(field['status'])==dict else field['status'])
        @udf
        def customer_status_metadatafield(field):
            return (field['status_metadata']['value'] if type(field['status_metadata'])==dict else field['status_metadata'])
        @udf
        def customer_creator(field):
            return (field['creator']['value'] if type(field['creator'])==dict else field['creator'])
        @udf
        def customer_created(field):
            return (field['created']['value'] if type(field['created'])==dict else field['created'])
        @udf
        def customer_creator_type(field):
            return (field['creator_type']['value'] if type(field['creator_type'])==dict else field['creator_type'])
        @udf
        def customer_updater(field):
            return (field['updater']['value'] if type(field['updater'])==dict else field['updater'])
        @udf
        def customer_updated(field):
            return (field['updated']['value'] if type(field['updated'])==dict else field['updated'])
        @udf
        def customer_updater_type(field):
            return (field['updater_type']['value'] if type(field['updater_type'])==dict else field['updater_type'])
            
        df = df.filter(df.after_payload.like('%id%'))## Safety Check: if id is present or not. If not then filter them out.
        
        ############ Customer Fields #####################################
        df = df.withColumn('id',customer_id(col('after_payload')).cast(StringType())) \
        .withColumn('status',customer_status(col('after_payload')).cast(StringType())) \
        .withColumn('status_metadata',customer_status_metadatafield(col('after_payload')).cast(StringType())) \
        .withColumn('creator',customer_creator(col('after_payload')).cast(StringType())) \
        .withColumn('created',customer_created(col('after_payload')).cast(TimestampType())) \
        .withColumn('creator_type',customer_creator_type(col('after_payload')).cast(StringType())) \
        .withColumn('updater',customer_updater(col('after_payload')).cast(StringType())) \
        .withColumn('updated',customer_updated(col('after_payload')).cast(TimestampType())) \
        .withColumn('updater_type',customer_updater_type(col('after_payload')).cast(StringType())).dropDuplicates()
        #####################################################################
        df = df.drop('after_payload')
        return df