FROM apache/airflow:3.0.6

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV HADOOP_HOME /opt/hadoop-3.3.6
ENV HADOOP_COMMON_LIB_NATIVE_DIR /opt/hadoop-3.3.6/lib/native
ENV HADOOP_OPTS " -Djava.library.path=/opt/hadoop-3.3.6/lib/native"
ENV LD_LIBRARY_PATH /opt/hadoop-3.3.6/lib/native

USER root

RUN set -eux; \
    apt-get update && \
    apt-get install --yes --no-install-recommends \
    wget \
    openjdk-17-jdk \
    python3.11 python3.11-venv python3.11-dev

WORKDIR /opt

# JARs necessários para acessar o MinIO via Hadoop AWS
RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar


USER airflow

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt