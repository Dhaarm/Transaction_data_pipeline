FROM apache/airflow:2.8.1

USER root

# Install Java
RUN apt-get update && apt-get install -y openjdk-17-jdk curl

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    | tar -xz -C /opt/

RUN mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

# Python dependencies
RUN pip install --no-cache-dir \
    faker \
    pandas \
    cassandra-driver \
    delta-spark