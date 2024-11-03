from bcrypt import kdf
from pyspark.sql import SparkSession
from pyspark.sql.functions import  from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType
import uuid

scala_version = '2.12'
spark_version = '3.5.3'

# package = (
#     f'org.apache.spark:spark-streaming-kafka-0-10_{scala_version}:{spark_version}',
#     f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
# )

# Initialize Spark Session with the necessary packages
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master('local[*]') \
    .config("spark.cassandra.connection.host","localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3," \
                                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3," \
                                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    .getOrCreate()


# Thiết lập checkpoint directory
checkpoint_dir = "data/spark-master/checkpoint"  # Hoặc một đường dẫn cục bộ
spark.sparkContext.setCheckpointDir(checkpoint_dir)

# Set the log level to avoid excessive log output
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
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

# Select the value field (assuming messages are in bytes)
kafka_values = kafka_stream.selectExpr("CAST(value AS STRING)")

# Parse the JSON string into a DataFrame using the defined schema
chess_stream = kafka_values.select(from_json(col("value"), chess_schema).alias("data"))

# Select the fields from the parsed DataFrame
chess_df = chess_stream.select("data.*")

# Đổi tên các cột thành chữ thường
for column in chess_df.columns:
    chess_df = chess_df.withColumnRenamed(column, column.lower())

# Tạo UDF để sinh UUID dưới dạng chuỗi
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# Thêm cột GameID vào DataFrame
chess_df = chess_df.withColumn("gameid", uuid_udf())

# Preview schema or data
chess_df.printSchema()

print("Chuan bi gui du lieu.....................")

# Start the query to write to the console
# query = chess_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# Ghi DataFrame vào Cassandra
query = chess_df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "lichess") \
    .option("table", "games") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start()

print("Da luu xong du lieu")

# Await termination of the query
query.awaitTermination()
