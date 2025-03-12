from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("RouteOptimization") \
    .getOrCreate()

# Schema for reading data from Kafka
schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("traffic_condition", StringType(), True),
    StructField("weather_condition", StringType(), True)
])

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "route_data") \
    .load()

# Convert the data to JSON and process it
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write the data to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
