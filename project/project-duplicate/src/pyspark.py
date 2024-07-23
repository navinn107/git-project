import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Function to fetch data from the API
def fetch_data():
    url = "https://api.yourservice.com/data"  # Replace with your API endpoint
    response = requests.get(url)
    return response.json()

# Create a Spark Session
spark = SparkSession.builder \
    .appName("RealTimeAPIDataProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for the incoming data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("observationDateTime", TimestampType(), True),
    StructField("airTemperature", StructType([StructField("instValue", IntegerType(), True)])),
    StructField("ambientNoise", StructType([StructField("instValue", IntegerType(), True)])),
    StructField("co2", StructType([StructField("instValue", IntegerType(), True)])),
    StructField("o3", StructType([StructField("instValue", IntegerType(), True)])),
    StructField("pm10", StructType([StructField("instValue", IntegerType(), True)])),
])

# Generate a stream of data by fetching from the API
def api_data_generator():
    while True:
        data = fetch_data()
        yield data
        time.sleep(5)  # Fetch data every 5 seconds

# Create a DataFrame from the stream of data
df = spark.createDataFrame(api_data_generator(), schema)

# Define a streaming DataFrame with a schema
streaming_df = df.select(
    col("id"),
    col("observationDateTime"),
    col("airTemperature.instValue").alias("airTemperature"),
    col("ambientNoise.instValue").alias("ambientNoise"),
    col("co2.instValue").alias("co2"),
    col("o3.instValue").alias("o3"),
    col("pm10.instValue").alias("pm10")
)

# Perform some transformations
transformed_df = streaming_df \
    .withColumn("airTemperatureFahrenheit", col("airTemperature") * 9 / 5 + 32) \
    .withColumn("co2_log", expr("log(co2)")) \
    .filter(col("pm10") > 50)

# Write the transformed data to the console
query = transformed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
