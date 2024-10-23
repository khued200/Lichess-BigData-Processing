from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json, col
from pyspark.sql.types import StructType, StructField, StringType

scala_version = '2.12'
spark_version = '3.5.3'

package = (
    f'org.apache.spark:spark-streaming-kafka-0-10_{scala_version}:{spark_version}',
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
)

# Initialize Spark Session with the necessary packages
spark = SparkSession.builder \
    .master('local[*]') \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Set the log level to avoid excessive log output
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "chess") \
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

# Start the query to write to the console
query = chess_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the query
query.awaitTermination()
