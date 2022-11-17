## This modules will contain the business logic for processing multiple streams that are coming from kafka.
    ## Transcations and Events
    ## Customers
from streaming.spark_engine import SparkJob
from pyspark.sql.functions import from_json,col
import pyspark
class SparkProcessing(SparkJob):
    def __init__(self,app_name,master_name,config,hadoop_config):
        super().__init__(app_name,master_name,config,hadoop_config)
    
    def event_processing(self ,df:pyspark.sql.DataFrame , schema:pyspark.sql.types.StructType) -> pyspark.sql.DataFrame:
        """
        A functions which converts the value(cdc json) column which is in bytes coming from kafka into a struct type column.
        The output of this function can be used to process different types of tables e.g consumer tables.

        Parameters:
        -----------------------------------------------------------------
        df(pyspark.sql.DataFrame): Dataframe loaded from kafka that needs to be processed
        
        schema(pyspark.sql.types.StructType): The schema for the json column coming from kafka
        

        Returns:    

        pyspark.sql.DataFrame
        
        """
        
        df = df.withColumn('value_json', from_json(col('value').cast('string'), schema))
        df = df.withColumn('meta_data',col('value_json.schema')).withColumn('payload',col('value_json.payload')).select('value_json','meta_data','payload','timestamp')
        return df

    