from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from pyspark.sql.types import *
import os
import time
from pyspark.sql.functions import from_json, col

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .master("local[*]")\
    .getOrCreate()

# Kafka parameters
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'chess_games'

kafka_input_config= {
    "kafka.bootstrap.servers":'localhost:9092',
    "subscribe":kafka_topic,
    
}
checkpoint_dir = "C:\\Users\\LamNH\\Desktop\\assignment\\2024-1\\Bigdata\\kafka\\kafka_2.12-3.8.0\\tmp\\kafka-checkpoint"

kafka_output_config= {
    "kafka.bootstrap.servers":'localhost:9092',
    "topic":"output",
    "failOnDataLoss":"false",
    "checkpointLocation": "/tmp/kafka-checkpoint"
}


# Parse the JSON data
chess_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Round", StringType(), True),
    StructField("White", StringType(), True),
    StructField("Black", StringType(), True),
    StructField("TimeControl", StringType(), True),
    StructField("Result", StringType(), True),
    StructField("WhiteElo", StringType(), True),
    StructField("BlackElo", StringType(), True),
    StructField("Moves", ArrayType(StringType()), True),
    StructField("Variant", StringType(), True)
])
      # Adjust this based on the actual structure of your JSON data

# Parse the JSON using the defined schema
# df = spark\
#     .readStream\
#     .format("kafka")\
#     .options(**kafka_input_config)\
#     .load()\
#     .select(F.from_json(F.col("value").cast("string"),df_schema).alias("json_data"))\
#     .select("json_data.*")

chess_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "chess_games") \
    .option("startingOffsets", "earliest") \
    .load()

chess_stream_df = chess_stream_df.selectExpr("CAST(value AS STRING) as json_value")

chess_data_df = chess_stream_df.withColumn("parsed_value", from_json(col("json_value"), chess_schema)).select("parsed_value.*")

# output_df = df.select(F.to_json(F.struct(*df.columns)).alias("value"))
# Process the data (for demonstration, just print to console)
# write = output_df.writeStream.outputMode("append").format("console").start()

query = chess_data_df.writeStream \
    .outputMode("append") \
    .format("console")\
    .start()
# Await termination
time.sleep(10)
query.awaitTermination()