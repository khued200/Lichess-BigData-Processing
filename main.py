from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("SimpleSparkJob") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
result = rdd.map(lambda x: x * x).collect()

# In kết quả
print("Result: ", result)

# Dừng Spark session
spark.stop()
