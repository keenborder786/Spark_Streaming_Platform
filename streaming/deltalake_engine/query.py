
## This module is to interact with the delta:
    ## Creating New Tables
    ## Restoring Table Versions
    ## Querying Table from Specific Timestamp

from delta import DeltaTable
from typing import Dict
import pyspark
import delta

class DeltaLakeInteraction:

    def __init__(self,spark , sourcebucket, table_name):
        self.spark_session = spark
        self.bucket = sourcebucket
        self.table_name = table_name

    def create_delta_table(self ,schema:pyspark.sql.types.StructType, table_properties:Dict ,*partition_col) -> delta.tables.DeltaTableBuilder:
        """
        This will create a delta table on the delta lake if it does not exist given the configuration.

        Parameters:
        --------------------------------------------------------
        schema(StructType): The schema which is being used to create the delta table

        table_properties(Dict): The delta table properties that you would like to use

        partition_col(list): The columns which you need to use for partitioning your delta table
        
        Returns
        ---------------------------------------------------------
        delta.tables.DeltaTableBuilder
    
        """  
        delta_table = DeltaTable.createIfNotExists(self.spark_session) \
            .tableName(self.table_name) \
            .addColumns(schema) \
            .location("s3a://{}/{}".format(self.bucket,self.table_name))
        
        for key,val in table_properties.items():
            delta_table = delta_table.property(key,val)
       
        if partition_col == ():
            delta_table = delta_table.execute()
        else:
            delta_table = delta_table.partitionedBy(*partition_col).execute()

        return delta_table

    def restore_table_version(self ,version_number:int) -> None:
        """

        Restore the DeltaTable to an older version of the table specified by version number.
        
        Parameters:
        ----------------------------
        version_number: The version_number to which you want to restore the table to


        Returns
        ------------------------------
        None

        """
        deltaTable = DeltaTable.forPath(self.spark_session, "s3a://{}/{}".format(self.bucket,self.table_name))
        deltaTable.restoreToVersion(version_number)
        print('Table has been restored to the following version {}'.format(version_number))


    def restore_table_time_period(self , time_period:str) -> None:
        """
        Restore the DeltaTable to an snapshot of older timestamp.

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
    def query_latest_table(self) -> pyspark.sql.DataFrame:
        """
        
        This will return the latest version of our detla lake table
        
        
        """
        df = self.spark_session.read.format('delta').load("s3a://{}/{}".format(self.bucket,self.table_name))
        return df