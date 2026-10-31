# Spark 3.5 + Delta Lake + Kafka source + S3A, with every jar baked in at build
# time so jobs start offline and reproducibly (no Ivy resolution at runtime).
FROM apache/spark:3.5.7-java17-python3

ARG MAVEN=https://repo1.maven.org/maven2
ARG SPARK_VERSION=3.5.7
ARG DELTA_VERSION=3.3.2

USER root
RUN cd /opt/spark/jars && for path in \
      io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
      io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
      org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    ; do curl -fsSLO "${MAVEN}/${path}"; done

COPY log4j2.properties /opt/spark/conf/log4j2.properties
COPY requirements.txt /tmp/requirements.txt
# pyspark ships with the base image; --no-deps stops delta-spark from reinstalling it.
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && pip install --no-cache-dir --no-deps delta-spark==${DELTA_VERSION}

WORKDIR /opt/tidewater
COPY tidewater_spark ./tidewater_spark
COPY jobs ./jobs
COPY tests ./tests
ENV PYTHONPATH=/opt/tidewater:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER spark
