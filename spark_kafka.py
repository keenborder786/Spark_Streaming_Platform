from config import sourceBucket,kafka_server,topic_name,spark_config,hadoop_config,cdc_schema,table_name
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col,from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark import StorageLevel
from typing import Dict,Optional
from delta import DeltaTable
def get_spark_session(app_name:str , master_name:str, config:Optional[Dict] = {}, hadoop_config:Optional[Dict] = {}) -> SparkSession:
    """
    Start the spark session.

    Parameters:
    ---------------------------
    app_name(str): The name given to the instance of spark session

    master_name(str): The url for the spark cluster
    
    config(dict , optional): The dictionary which contains the configuration for spark session

    hadoop_config(dict,optional): The hadoop configuration for our spark session

    Returns:
    ---------------------------
    SparkSession: Instance of spark session
    
    """
    if config == {}:
        spark = (
            SparkSession.builder.appName(app_name)
            .master(master_name)
            .getOrCreate()
        )
        return 
    else:
        spark = (
            SparkSession.builder.appName(app_name)
            .master(master_name)
            .getOrCreate()
        )
        configuration = config.items()
        spark.sparkContext._conf.setAll(configuration)
        spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        spark.sparkContext.setLogLevel('Error')
        if hadoop_config != {}:
            for k,v in hadoop_config.items():
                spark.sparkContext._jsc.hadoopConfiguration().set(k, v)

        return spark


def read_kafka_stream(spark_session:SparkSession , kafka_bootstrap_server:str , topic_name:str , starting_offset:str) -> DataFrame:
    """
    
    Reads the kafka stream from the given topic and cluster
    
    Parameters:
    ---------------------------
    spark_session(SparkSession): Instance of spark session.

    kafka_bootstrap_server(str): The IP address for the kafka cluster
    
    topic_name(str): The topic to which the spark is reading the stream from

    starting_offset(str): Should we read the message in the given topic from start or end

    cdcSchema(structtype): The streaming schema in structtype

    Returns:
    ---------------------------
    DataFrame: Structured Spark DataFrame

    """

    df = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_server)
        .option("subscribe", topic_name)
        .option("startingOffsets", starting_offset)
        .option("failOnDataLoss","false")
        .option('minOffsetsPerTrigger',60000) ##60000 offset approximate to 1MB
        .option('maxTriggerDelay','1m')## The trigger can be delayed maximum by 3 minutes
        .load()
    )
    return df


if __name__ == '__main__':
    
    ## Setting up the spark session
    spark = get_spark_session('kafka_delta' , 'local[*]',spark_config,hadoop_config)
    
    # #reading kafka stream
    df = read_kafka_stream(spark , kafka_server, topic_name, 'latest')
    df = df.withColumn('value', from_json(col('value').cast('string'), cdc_schema)).dropna(subset = 'value').select('value','timestamp')
    
    ## Create the delta table if not exists. This will create the delta table only once.
    deltaTable = DeltaTable.createIfNotExists(spark) \
        .tableName(table_name) \
        .addColumns(df.schema) \
        .location("s3a://{}/{}".format(sourceBucket,table_name)) \
        .execute()
    
    # # ##Writing the stream to the delta lake
    df.coalesce(1).writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://{}/{}/_checkpoint/".format(sourceBucket,table_name)) \
        .toTable(tableName = table_name).awaitTermination()
    
    