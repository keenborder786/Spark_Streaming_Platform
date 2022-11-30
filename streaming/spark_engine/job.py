## This module will run our streaming pipeline


import pyspark


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col,from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from typing import Dict,Optional



class SparkJob:
    def __init__(self,app_name,hadoop_config):
        self.spark_session = self.get_spark_session(app_name,hadoop_config)


    def get_spark_session(self,app_name:str , 
                        hadoop_config:Optional[Dict] = {}) -> pyspark.sql.SparkSession:
        """
        Start the spark session.

        Parameters:
        ---------------------------
        app_name(str): The name given to the instance of spark session

        hadoop_config(dict,optional): The hadoop configuration for our spark session

        Returns:
        ---------------------------
        SparkSession: Instance of spark session
        
        """
        
        spark = (
            SparkSession.builder.appName(app_name)\
            .getOrCreate()
        )
        spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        spark.sparkContext.setLogLevel('Error')
           
        if hadoop_config != {}:
            for k,v in hadoop_config.items():
                spark.sparkContext._jsc.hadoopConfiguration().set(k, v)

        return spark


    def read_kafka_stream(self , 
                        kafka_bootstrap_server:str , 
                        topic_name:str , 
                        starting_offset:str,
                        kafka_config:Optional[Dict[str,str]] = {}) -> pyspark.sql.DataFrame:
        """
        
        Reads the kafka stream from the given topic and cluster
        
        Parameters:
        ---------------------------
        kafka_bootstrap_server(str): The IP address for the kafka cluster
        
        topic_name(str): The topic to which the spark is reading the stream from

        starting_offset(str): Should we read the message in the given topic from start or end

        kafka_config(Dict,Optional): The configurations for reading streaming from kafka

        Returns:
        ---------------------------
        DataFrame: Structured Spark DataFrame

        """
        
        df = self.spark_session.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_server) \
                                                          .option("subscribe", topic_name) \
                                                          .option("starting_offset",starting_offset)
        for k,v in kafka_config.items(): df = df.option(k,v) ## Loading the other specific config
        df = df.load()
        
        return df





    
    