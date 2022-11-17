
## This module is to interact with the delta:
    ## Creating New Tables
    ## Restoring Table Versions
    ## Querying Table from Specific Timestamp

import pyspark
from pyspark.sql import DataFrame
from streaming.config import spark_config,hadoop_config
from delta import DeltaTable,tables

class DeltaLakeInteraction:

    def __init__(self,spark , sourcebucket,table_name):
        self.spark_session = spark
        self.bucket = sourcebucket
        self.table_name = table_name
    
    def create_delta_table(self ,schema,*partition_col ,**table_properties):    
        
        delta_table = DeltaTable.createIfNotExists(self.spark_session) \
            .tableName(self.table_name) \
            .addColumns(schema) \
            .location("s3a://{}/{}".format(self.bucket,self.table_name))
        
        for key,val in table_properties.items():
            delta_table = delta_table.property('delta.'+key,val)
       
        if partition_col == ():
            delta_table = delta_table.execute()
        else:
            delta_table = delta_table.partitionedBy(*partition_col).execute()

        return delta_table

    def restore_table_version(self ,version_number:int):
        """
        Parameters:
        ----------------------------
        version_number: The version_number to which you want to restore the table to

        Restore the DeltaTable to an older version of the table specified by version number.
        
        """
        deltaTable = DeltaTable.forPath(self.spark_session, "s3a://{}/{}".format(self.bucket,self.table_name))
        deltaTable.restoreToVersion(version_number)
        print('Table has been restored to the following version {}'.format(version_number))


    def restore_table_version(self , time_period:str):
        """
        Parameters:
        ----------------------------
        time_period: The time_period to which you want to restore the table to
        
        
        Returns
        ---------------------------
        None

        """
        deltaTable = DeltaTable.forPath(self.spark, "s3a://{}/{}".format(self.bucket,self.table_name))
        deltaTable.restoreToTimestamp(time_period)
        print('Table has been restored to the following timestamp {}'.format(self.time_period))

    def query_table_time_period(self,time_period:str) -> pyspark.sql.DataFrame:
        """
        
        This will load the snapshot of our dataframe as of specific time period  from our testing delta lake

        Parameters:
        -------------------------------------------------------
        spark_session(pyspark.sql.SparkSession): Instance of spark session.

        time_period(str): The timeperiod for which you want to load the dataframe for
        
        Returns:
        ---------------------------------------------------------
        pyspark.sql.DataFrame
        
        """

        df = self.spark_session.read.format('delta').option("timestampAsOf", time_period).load("s3a://{}/{}".format(self.bucket,self.table_name))
        return df


    def query_table_time_version(self , version:int) -> pyspark.sql.DataFrame:
        """
        
        This will load the snapshot of our dataframe as of specific time period  from our testing delta lake

        Parameters:
        -------------------------------------------------------
        spark_session(pyspark.sql.SparkSession): Instance of spark session.

        time_period(str): The timeperiod for which you want to load the dataframe for
        
        Returns:
        ---------------------------------------------------------
        pyspark.sql.DataFrame
        
        """

        df = self.spark_session.read.format('delta').option("versionAsOf", version).load("s3a://{}/{}".format(self.bucket,self.table_name))
        return df


