import os
from pyspark.sql.types import DataType,IntegerType, StringType, StructField, StructType,ArrayType,BooleanType,LongType,BinaryType,AtomicType,NullType

s3accessKeyAws = os.environ['S3USER']
s3secretKeyAws = os.environ['S3Password']
connectionTimeOut = "600000"
s3endPointLoc= os.environ['S3EndPoint']
sourceBucket = os.environ['SourceBucket']
kafka_server = os.environ['KafkaServer']
topic_name = os.environ['TopicName']
table_name = 'cdc_table'
spark_config = {"spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.executor.extraJavaOptions":"-Dcom.amazonaws.services.s3.enableV4=true",
                "spark.driver.extraJavaOptions":"-Dcom.amazonaws.services.s3.enableV4=true",
                "spark.streaming.receiver.writeAheadLog.enable":"true",
                "spark.streaming.driver.writeAheadLog.closeFileAfterWrite":"true",
                "spark.streaming.receiver.writeAheadLog.closeFileAfterWrite":"true"
                }
hadoop_config = {"fs.s3a.endpoint":s3endPointLoc,
                "fs.s3a.access.key":s3accessKeyAws,
                "fs.s3a.secret.key":s3secretKeyAws,
                'spark.hadoop.fs.s3a.aws.credentials.provider':'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
                "spark.hadoop.fs.s3a.path.style.access":"true",
                "com.amazonaws.services.s3.enableV4":"true",
                "fs.s3a.connection.ssl.enabled":"false",
                "spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem"
                }
    
    
cdc_schema = StructType([StructField('schema',StructType([
                                                            StructField('type',StringType()),
                                                            StructField('fields',ArrayType(
                                                                                    StructType([StructField('type',StringType()),
                                                                                                StructField('fields',ArrayType(
                                                                                                    StructType([StructField('type',StringType()),
                                                                                                                StructField('fields',ArrayType(
                                                                                                                    StructType([StructField('type',StringType()),
                                                                                                                                StructField('optional',BooleanType()),
                                                                                                                                StructField('field',StringType())
                                                                                                                                ])                                       
                                                                                                                                            )
                                                                                                                                ),
                                                                                                                StructField('optional',BooleanType()),
                                                                                                                StructField('name',StringType()),
                                                                                                                StructField('field',StringType())])
                                                                                                                                )
                                                                                                            ),
                                                                                                StructField('optional',StringType()),
                                                                                                StructField('name',StringType()),
                                                                                                StructField('field',StringType())
                                                                                                ]
                                                                                                )
                                                                                            )
                                                                        ),
                                                            StructField('optional',BooleanType()),
                                                            StructField('name',StringType())
                                                                ]
                                                            )
                                        ),
                        StructField('payload',StructType([StructField('before',IntegerType()),
                                                              StructField('after',StructType([
                                                                                            StructField('id',StructType([
                                                                                                            StructField('value',IntegerType()),
                                                                                                            StructField('set',BooleanType())   
                                                                                                                        ]
                                                                                                                        )
                                                                                                        ),
                                                                                            StructField('pin',StringType()),
                                                                                            StructField('status',StringType()),
                                                                                            StructField('created',StringType()),
                                                                                            StructField('creator_type',StringType()),
                                                                                            StructField('creator',StructType([
                                                                                                            StructField('value',StringType()),
                                                                                                            StructField('set',BooleanType())   
                                                                                                                        ]
                                                                                                                        )
                                                                                                        ),
                                                                                            StructField('updated',StringType()),
                                                                                            StructField('updator_type',StringType()),
                                                                                            StructField('updator',StringType())
                                                                                            ])
                                                                            ),
                                                                StructField('source',StructType([
                                                                                            StructField('version',StringType()),
                                                                                            StructField('connector',StringType()),
                                                                                            StructField('name',StringType()),
                                                                                            StructField('ts_ms',LongType()),
                                                                                            StructField('snapshot',StringType()),
                                                                                            StructField('db',StringType()),
                                                                                            StructField('sequence',StringType()),
                                                                                            StructField('schema',StringType()),
                                                                                            StructField('table',StringType()),
                                                                                            StructField('txId',StringType()),
                                                                                            StructField('lsn',StringType()),
                                                                                            StructField('xmin',StringType())
                                                                                            ])
                                                                            ),
                                                                StructField('op',StringType()),
                                                                StructField('ts_ms',LongType()),
                                                                StructField('transaction',StringType())
                                                                ]
                                                                )
                                            )])

payload_schema = StructType([StructField('payload',StructType([StructField('before',IntegerType()),
                                                              StructField('after',StructType([
                                                                                            StructField('id',StructType([
                                                                                                            StructField('value',IntegerType()),
                                                                                                            StructField('set',BooleanType())   
                                                                                                                        ]
                                                                                                                        )
                                                                                                        ),
                                                                                            StructField('pin',StringType()),
                                                                                            StructField('status',StringType()),
                                                                                            StructField('created',StringType()),
                                                                                            StructField('creator_type',StringType()),
                                                                                            StructField('creator',StructType([
                                                                                                            StructField('value',StringType()),
                                                                                                            StructField('set',BooleanType())   
                                                                                                                        ]
                                                                                                                        )
                                                                                                        ),
                                                                                            StructField('updated',StringType()),
                                                                                            StructField('updator_type',StringType()),
                                                                                            StructField('updator',StringType())
                                                                                            ])
                                                                            ),
                                                                StructField('source',StructType([
                                                                                            StructField('version',StringType()),
                                                                                            StructField('connector',StringType()),
                                                                                            StructField('name',StringType()),
                                                                                            StructField('ts_ms',LongType()),
                                                                                            StructField('snapshot',StringType()),
                                                                                            StructField('db',StringType()),
                                                                                            StructField('sequence',StringType()),
                                                                                            StructField('schema',StringType()),
                                                                                            StructField('table',StringType()),
                                                                                            StructField('txId',StringType()),
                                                                                            StructField('lsn',StringType()),
                                                                                            StructField('xmin',StringType())
                                                                                            ])
                                                                            ),
                                                                StructField('op',StringType()),
                                                                StructField('ts_ms',LongType()),
                                                                StructField('transaction',StringType())
                                                                ]
                                                                )
                                            )])


