<!--- app-name: Apache Kafka -->

# Spark Application Helm Chart

## Introduction

This chart bootstraps our Spark Application deployment (Streaming Job Processing CDC Payload Packages from Kafka and updating delta lake tables) on a [Kubernetes](https://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites

- K8 cluster
- Kafka Cluster (You need to have a running kafka server with atleast one topic and one broker)
- S3 Bucket (You need to have a delta lake compliant S3 bucket)
- Helm 3.2.0+

## Installing the Chart

First you will have to make sure that the ***kafka cluster*** from where the cdc_packages are coming in and ***s3 bucket(delta lake)*** are up and running. 
Afterwards, make sure that following parameters have been set up in the values.yaml file (details in [Parameters](#parameters)):

  - KAFKA_SERVER
  - S3_USER
  - S3_PASSWORD
  - S3_END_POINT
  - SOURCE_BUCKET
  - TOPIC_NAME
  - KAFKA_CONSUMER_CONFIG


```console
helm package spark-application
helm install spark-application-<version_no>.tgz
```

These commands deploy Spark on the Kubernetes cluster in the default configuration. The [Parameters](#parameters) section lists the parameters that can be configured during installation.


## Uninstalling the Chart

To uninstall/delete the `spark-application` deployment:

```console
helm delete spark-application
```
The command removes all the Spark Application components associated with the chart and deletes the release.


## Test only for Dev Purposes:

If you want to test run the spark job then follow the given step:

- Step-0: Set up minikube cluster by following the [instruction] (https://minikube.sigs.k8s.io/docs/start/)
- Step-1: Install [Helm](https://helm.sh/docs/intro/install/)  
- Step-2: Install kafka cluster chart by [bitnami](https://github.com/bitnami/charts/tree/main/bitnami/kafka) and deploy on minikube with default values.
- Step-3: Set up dev minio chart provided [here](https://github.com/keenborder786/poc_kafka_delta/tree/main/minio) and deploy on minikube.
- Step-4: Build all of the images for all the docker files from [here](https://github.com/keenborder786/poc_kafka_delta/tree/main)
- Step-5: Now run the spark helm chart and deploy on minikube with default values.
- Step-6: Open the shell of sparkclient pod running on minikube and run the following script ***/opt/poc_kafka_delta/simulate_kafka.py*** to test run a job for customer table.

## Parameters

### Kafka Cluster parameters

| Name                      | Description                                       | Value                                              |
| ------------------------- | -----------------------------------------------   | -------------------------------------------------  |
| `kafka.kafkaServer`       | Server of IP where the broker(s) are running      | `my-release-kafka.default.svc.cluster.local:9092`  |
| `kafka.topic_name`        | Topic Name from where the cdc_packages are coming | `cdc_test_topics`                                  |
| `kafka.consumer_config`   | Spark Config for Kafka as a consumer              | `'{"failOnDataLoss":"false"}'`                     |


### S3 Parameters

| Name                     | Description                                                                             | Value           |
| ------------------------ | --------------------------------------------------------------------------------------- | --------------- |
| `s3.endPointLoc`         | End Point for S3                                                                        | `10.97.0.3:9000`|
| `s3.user`                | Username for S3                                                                         | `user`          |
| `s3.password`            | Password for S3                                                                         | `password`      |
| `s3.source_bucket`       | Bucket Name where your delta lake tables are.                                           | `test`          |


### Delta Lake Tables Parameters

| Name                                       | Description                                                                             | Value           |
| -------------------------------------------| --------------------------------------------------------------------------------------- | --------------- |
| `delta_lake_tables.customer_table_config`  | [Delta Lake Table Config](https://docs.delta.io/latest/table-properties.html) for customer table. You need to povide the configuration in form json where '{table-property:value}'.                                                                         |`'{"delta.appendOnly":"false","delta.enableChangeDataFeed":"true","delta.deletedFileRetentionDuration":"interval 7 days"}'`|  
| `delta_lake_tables.customer_schema`       | The schema of the customer table. Should be provided in the following format: {fields:[{"metadata":{},"name":"col_name","nullable":true/false,"type":"string"/"integer"/"timestamp"/"float"}]}                                           | `'{"fields":[{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"status","nullable":true,"type":"string"},{"metadata":{},"name":"status_metadata","nullable":true,"type":"string"},{"metadata":{},"name":"creator","nullable":true,"type":"string"},{"metadata":{},"name":"created","nullable":true,"type":"timestamp"},{"metadata":{},"name":"creator_type","nullable":true,"type":"string"},{"metadata":{},"name":"updater","nullable":true,"type":"string"},{"metadata":{},"name":"updated","nullable":true,"type":"timestamp"},{"metadata":{},"name":"updater_type","nullable":true,"type":"string"}]}'`|

### Spark Cluster Parameters


#### Docker Images for Spark Application

| Name                            | Description                                                    | Value                |
| ------------------------        | -----------------------------------                            | ---------------      |
| `spark.master_image.registry    | Spark Master Image registry                                    | `docker.io`          |
| `spark.master_image.repository  | Spark Master Image repository                                  | `library/sparkmaster`|
| `spark.master_image.tag         | Spark Master Image tag (immutable tags are recommended)        | `1.0`                |
| `spark.master_image.digest      | Spark Master Image digest                                      | `""`                 |
| `spark.worker_image.registry    | Spark Worker Image registry                                    | `docker.io`          |
| `spark.worker_image.repository  | Spark Worker Image repository                                  | `library/sparkmaster`|
| `spark.worker_image.tag         | Spark Worker Image tag (immutable tags are recommended)        | `1.0`                |
| `spark.worker_image.digest      | Spark Worker Image digest                                      | `""`                 |
| `spark.client_image.registry    | Spark Client Image registry                                    | `docker.io`          |
| `spark.client_image.repository  | Spark Client Image repository                                  | `library/sparkmaster`|
| `spark.client_image.tag         | Spark Client Image tag (immutable tags are recommended)        | `1.0`                |
| `spark.client_image.digest      | Spark Client Image digest                                      | `""`                 |

#### Master Config
| Name                            | Description                                                    | Value                |
| ------------------------        | -----------------------------------                            | -------------------- |
| `spark.master_config.ip         | IP Address for Master Service                                  | `10.97.0.4`          |

#### Worker1 Config
| Name                             | Description                                                    | Value                |
| ------------------------         | -----------------------------------                            | -------------------- |
| `spark.worker1_config.ip         | IP Address for Worker 1 Service                                | `10.97.0.5`          |
| `spark.worker1_config.cores      | Cores for Worker 1                                             | `2`                  |
| `spark.worker1_config.memory     | Memory for Worker 1                                            | `1G`                 |


