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
| `nameOverride`           | String to partially override common.names.fullname                                      | `""`            |
| `fullnameOverride`       | String to fully override common.names.fullname                                          | `""`            |
| `clusterDomain`          | Default Kubernetes cluster domain                                                       | `cluster.local` |
| `commonLabels`           | Labels to add to all deployed objects                                                   | `{}`            |
| `commonAnnotations`      | Annotations to add to all deployed objects                                              | `{}`            |
| `extraDeploy`            | Array of extra objects to deploy with the release                                       | `[]`            |
| `diagnosticMode.enabled` | Enable diagnostic mode (all probes will be disabled and the command will be overridden) | `false`         |
| `diagnosticMode.command` | Command to override all containers in the statefulset                                   | `["sleep"]`     |
| `diagnosticMode.args`    | Args to override all containers in the statefulset                                      | `["infinity"]`  |




Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```console
helm install my-release \
  --set replicaCount=3 \
  my-repo/kafka
```
