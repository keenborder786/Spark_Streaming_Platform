### This is a testing module in order to query the table from delta lake and test the time line functionality
import pyspark
from pyspark.sql import DataFrame
from config import sourceBucket,kafka_server,topic_name,spark_config,hadoop_config,table_name,payload_schema,cdc_schema
from spark_kafka import get_spark_session

def query_table_time_period(spark_session:pyspark.sql.SparkSession , time_period:str) -> pyspark.sql.DataFrame:
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

    df = spark_session.read.format('delta').option("timestampAsOf", time_period).load("s3a://{}/{}".format(sourceBucket,table_name))
    return df


if __name__ == '__main__':
    spark = get_spark_session('kafka_delta' , 'local[*]',spark_config,hadoop_config)
    df = query_table_time_period(spark , '2022-11-16')

