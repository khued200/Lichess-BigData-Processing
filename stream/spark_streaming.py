from pyspark.sql import SparkSession
from pyspark.sql.functions import  from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType
import uuid

spark = SparkSession.builder \
    .appName("kafka_to_cassandra") \
    .master('spark://spark-master:7077') \
    .config("spark.cores.max", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.cassandra.connection.host","localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3," \
                                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3," \
                                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    .getOrCreate()

checkpoint_dir = "data/spark-master/checkpoint" 
spark.sparkContext.setCheckpointDir(checkpoint_dir)

spark.sparkContext.setLogLevel("WARN")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "chess") \
    .option("startingOffsets", "earliest") \
    .load()

chess_schema = StructType([
        StructField("Event", StringType(), True),
        StructField("Site", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Round", StringType(), True),
        StructField("White", StringType(), True),
        StructField("Black", StringType(), True),
        StructField("Result", StringType(), True),
        StructField("UTCDate", StringType(), True),
        StructField("UTCTime", StringType(), True),
        StructField("WhiteElo", StringType(), True),
        StructField("BlackElo", StringType(), True),
        StructField("WhiteRatingDiff", StringType(), True),
        StructField("BlackRatingDiff", StringType(), True),
        StructField("TimeControl", StringType(), True),
        StructField("Termination", StringType(), True),
        StructField("Variant", StringType(), True),
        StructField("Moves", StringType(), True)
    ])


kafka_values = kafka_stream.selectExpr("CAST(value AS STRING)")

chess_stream = kafka_values.select(from_json(col("value"), chess_schema).alias("data"))

chess_df = chess_stream.select("data.*")

for column in chess_df.columns:
    chess_df = chess_df.withColumnRenamed(column, column.lower())

uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# Thêm cột GameID vào DataFrame
chess_df = chess_df.withColumn("gameid", uuid_udf())

chess_df.printSchema()

print("Chuan bi gui du lieu.....................")

query = chess_df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "lichess") \
    .option("table", "games") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start()

print("Da luu xong du lieu")

query.awaitTermination()
