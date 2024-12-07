from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import time
from pyspark.sql.functions import from_json, col, to_date, expr
from pyspark.sql.types import *

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
checkpoint_dir = "C:\\Users\\LamNH\\Desktop\\assignment\\2024-1\\Bigdata\\kafka\\kafka_2.12-3.8.0\\tmp\\kafka-checkpoint"

# Create Spark session
spark = SparkSession.builder \
    .appName("SparkToCassandra") \
    .master("local[*]") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# Kafka parameters
kafka_bootstrap_servers = 'localhost:29092'
kafka_topic = 'chess-games'

# Define schema for JSON data
chess_schema = StructType([
    StructField("GameID", StringType(), True),
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



# Read data from Kafka
chess_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

chess_stream_df = chess_stream_df.selectExpr("CAST(value AS STRING) as json_value")

# Parse the JSON data
chess_data_df = chess_stream_df.withColumn("parsed_value", from_json(col("json_value"), chess_schema)).select("parsed_value.*")

chess_data_df = chess_data_df.select(
    col("GameID").alias("gameid"),
    to_date(F.regexp_replace(col("Date"), "\\.", "-"), "yyyy-MM-dd").alias("date"),
    col("Round").alias("round"),
    col("White").alias("white"),
    col("Black").alias("black"),
    col("TimeControl").alias("timecontrol"),  # Check casing
    col("Result").alias("result"),            # Check casing
    col("WhiteElo").alias("whiteelo"),        # Check casing
    col("BlackElo").alias("blackelo"),        # Check casing
    col("Moves").alias("moves"),              # Check casing
    col("Variant").alias("variant")           # Check casing
)
chess_data_df = chess_data_df.filter(col("gameid").isNotNull())

# Write to Cassandra
query = chess_data_df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "chess_keyspace") \
    .option("table", "chess_game") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start()

# Await termination
query.awaitTermination()
