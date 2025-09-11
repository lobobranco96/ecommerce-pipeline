FROM bitnami/spark:3.5.3

WORKDIR /app

USER root

# Instala pacotes adicionais do sistema
RUN install_packages curl wget python3.11 python3.11-venv python3.11-dev

# Atualiza pip e instala great_expectations no Python nativo do Spark
# (Bitnami Spark usa /opt/bitnami/python/bin/python3 por padrão)
RUN /opt/bitnami/python/bin/pip install --upgrade pip \
 && /opt/bitnami/python/bin/pip install great_expectations

# versão adequada do Guava (necessária para o Hadoop AWS)
RUN rm /opt/bitnami/spark/jars/guava-14.0.1.jar \
 && wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar

# JARs necessários para acessar o MinIO via Hadoop AWS
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 /opt/bitnami/spark/jars/*.jar

# Configura o classpath do Spark para incluir os JARs adicionais
ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*"

# Configura permissões para a pasta de JARs do Spark
RUN chmod -R 777 /opt/bitnami/spark/jars

# Garante que o Spark utilize Python do próprio Spark
ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3

USER 1001
