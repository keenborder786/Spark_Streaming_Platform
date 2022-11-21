FROM ubuntu:latest

# Install base utilities
RUN apt-get update && \
    apt-get install -y build-essential  && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install miniconda
COPY --from=continuumio/miniconda3:latest /opt/conda /opt/conda
ENV PATH=/opt/conda/bin:$PATH

## Install java and spark
RUN apt update
RUN apt install -y default-jdk
RUN wget https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz   
RUN tar -xzf spark-3.3.1-bin-hadoop3.tgz
RUN mv spark-3.3.1-bin-hadoop3 /opt/spark
RUN export SPARK_HOME=/opt/spark
RUN export PATH=$PATH:$SPARK_HOME/bin


## Creating the conda environment to run the code
COPY environment.yml .
RUN conda env create -f environment.yml
RUN echo "conda activate spark_streaming" >> ~/.bashrc
SHELL ["/bin/bash", "--login", "-c"]


## Setting up my code
RUN mkdir /opt/poc_kafka_delta
WORKDIR /opt/poc_kafka_delta
COPY streaming/ streaming/
COPY main.py .
COPY entrypoint.sh ./

## Execute the final shell script to run our job
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]




