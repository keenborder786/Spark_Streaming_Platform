import pyspark

from config import sourceBucket,kafka_server,topic_name,spark_config,hadoop_config,table_name,payload_schema,cdc_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col,from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.storagelevel import StorageLevel
from typing import Dict,Optional
from delta import DeltaTable



def get_spark_session(app_name:str , 
                    master_name:str, config:Optional[Dict] = {}, 
                    hadoop_config:Optional[Dict] = {}) -> pyspark.sql.SparkSession:
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


def read_kafka_stream(spark_session:pyspark.sql.SparkSession , 
                    kafka_bootstrap_server:str , 
                    topic_name:str , 
                    starting_offset:str) -> pyspark.sql.DataFrame:
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
        # .option('minOffsetsPerTrigger',60000) ##60000 offset approximate to 1MB
        # .option('maxTriggerDelay','1m') ## The trigger can be delayed maximum by 3 minutes
        .load()
    )
    return df

def batch_function(micro_df:pyspark.sql.types.Row, batch_id:int) -> None:
  """
  Parameters:

  ----------------

  micro_df(pyspark.sql.types.Row): The current batch from our streaming data frame

  batch_id(int): The number of batch that we are processing for our pipeline

  This function processes the micro batch for our spark pipeline and writes the data to delta lake.
  After every 10 batches, we run the optimize command to compact the parquet files created by delta lake
    
    
  Returns
  -----------  
    
  """
  print(f"Processing micro-batch {batch_id}")
  if batch_id % 10 == 0:## Compact the files into one file after every 10 batch.
    deltaTable.optimize().executeCompaction()
    
 ## TxnAppID  & TxnVersions makes the delta lake indempotent in order to hold exactly once semantics
  micro_df.persist(StorageLevel.MEMORY_AND_DISK_DESER).write \
        .option("txnAppId", "cdc_streaming") \
        .option("txnVersion", batch_id) \
        .mode('append') \
        .format('delta') \
        .save("s3a://{}/{}".format(sourceBucket,table_name))
  

if __name__ == '__main__':
    
    #Setting up the spark session
    spark = get_spark_session('kafka_delta' , 'local[*]',spark_config,hadoop_config)
    
    #Reading kafka stream
    df = read_kafka_stream(spark , kafka_server, topic_name, 'latest')
    df = df.withColumn('value_json', from_json(col('value').cast('string'), cdc_schema))
    df = df.withColumn('meta_data',col('value_json.schema')).withColumn('payload',col('value_json.payload')).select('value_json','meta_data','payload','timestamp')

    # # #Create the delta table if not exists. This will create the delta table only once.
    deltaTable = DeltaTable.createIfNotExists(spark) \
        .tableName(table_name) \
        .addColumns(df.schema) \
        .location("s3a://{}/{}".format(sourceBucket,table_name)) \
        .execute()
    
    # # #Writing the stream to the delta lake
    df.repartition(1).writeStream.foreachBatch(batch_function) \
        .option("checkpointLocation", "s3a://{}/{}/_checkpoint/".format(sourceBucket,table_name)) \
        .start().awaitTermination()
    
    