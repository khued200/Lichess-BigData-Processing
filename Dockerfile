# Use Python 3.11.6 as the base image
FROM python:3.11.6-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the entire application code (both producer and consumer)
COPY . .

# Optionally install Spark and Java if needed for consumer
RUN apt-get update && \
    apt-get install -y openjdk-11-jre && \
    apt-get clean

# Download Spark binaries
ENV SPARK_VERSION=3.5.4
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark &&\
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# Expose necessary ports for Spark or other services
EXPOSE 4040 7077

# Default command (can be overridden via docker-compose or command-line)
CMD ["python", "Producer.py"]
