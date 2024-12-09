from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import from_json, col, to_date, expr
from pyspark.sql.types import *
import logging
from cassandra.cluster import Cluster

def create_keyspace(session):
    keyspace_query = """
            CREATE KEYSPACE IF NOT EXISTS chess_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
    """
    session.execute(keyspace_query)
    print("Keyspace created successfully!")


def create_table(session):
    table_query = """
        CREATE TABLE IF NOT EXISTS chess_keyspace.chess_game (
                gameid text,
                date date,
                time time,
                round text,
                white text,
                black text,
                timecontrol text,
                result text,
                whiteelo int,
                blackelo int,
                moves text,
                variant text,
                PRIMARY KEY (gameid)
        );
    """
    session.execute(table_query)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    gameid = kwargs.get('gameid')
    black = kwargs.get('black')
    blackelo = kwargs.get('blackelo')
    date = kwargs.get('date')
    moves = kwargs.get('moves')
    result = kwargs.get('result')
    round = kwargs.get('round')
    time = kwargs.get('time')
    timecontrol = kwargs.get('timecontrol')
    variant = kwargs.get('variant')
    white = kwargs.get('white')
    whiteelo = kwargs.get('whiteelo')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(gameid, black, blackelo, date, moves, 
                result, round, time, timecontrol, variant, white, whiteelo)
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (gameid, black, blackelo, date, moves, 
                result, round, time, timecontrol, variant, white, whiteelo))
        print(f"Data inserted for {gameid}")

    except Exception as e:
        print(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName("SparkToCassandra") \
            .master("local[*]") \
            .config("spark.jars.packages",  "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"\
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1," \
                                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")\
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()
        # checkpoint_dir = "/opt/spark-data/tmp"
        # s_conn.sparkContext.setCheckpointDir(checkpoint_dir)
        # s_conn.sparkContext.setLogLevel("WARN")
        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:29092') \
            .option('subscribe', 'chess-games') \
            .option('startingOffsets', 'earliest') \
            .load()
        print("kafka dataframe created successfully")
    except Exception as e:
        print(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        print(f"Could not create cassandra connection due to {e}")
        return None
    

def create_selection_df_from_kafka(spark_df):
    chess_schema = StructType([
        StructField("GameID", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
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

    chess_stream_df = spark_df.selectExpr("CAST(value AS STRING) as json_value")

    # Parse the JSON data
    chess_data_df = chess_stream_df.withColumn("parsed_value", from_json(col("json_value"), chess_schema)).select("parsed_value.*")

    chess_data_df = chess_data_df.select(
        col("GameID").alias("gameid"),
        to_date(F.regexp_replace(col("Date"), "\\.", "-"), "yyyy-MM-dd").alias("date"),
        col("Time").alias("time"),
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

    return chess_data_df

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()
        create_keyspace(session)
        create_table(session)
        if session is not None:
            logging.info("Streaming is being started...")
            print("Streaming is being started...")
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .outputMode("append") 
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'chess_keyspace')
                               .option('table', 'chess_game')
                               .start())

            streaming_query.awaitTermination()