from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# Define schema for incoming events
event_schema = StructType([
    StructField("timestamp", LongType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("model", StringType()),
    StructField("color", StringType()),
    StructField("engine", StringType()),
    StructField("interior", StringType()),
    StructField("price", DoubleType()),
    StructField("step", StringType())
])

# Create Spark session
spark = SparkSession.builder \
    .appName("ConfiguratorETL") \
    .master("local[*]") \
    .getOrCreate()

# Read from Kafka topic
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "configurator-events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value from binary to string
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")) \
    .select("data.*")

# Write as Parquet files (partitioned by model)
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "output_samples/parquet") \
    .option("checkpointLocation", "output_samples/checkpoints") \
    .partitionBy("model") \
    .outputMode("append") \
    .start()

query.awaitTermination()
