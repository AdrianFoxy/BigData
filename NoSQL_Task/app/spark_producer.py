from pyspark.sql import SparkSession
from trip_schema import schema

# Define Spark session
spark = SparkSession.builder.appName("Top Route Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read CSV files using structured streaming
df = spark.readStream.format("parquet") \
    .option("header", True) \
    .option("timestampFormat", "M/d/y H:m") \
    .schema(schema) \
    .load("../resources/parquet")

# Prepare the result DataFrame to send to Kafka
kafka_df = df.selectExpr("to_json(struct(*)) AS value")

# Send the result to a Kafka topic
kafka_df.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("topic", "trip_topic") \
    .option("checkpointLocation", "checkpoints") \
    .start() \
    .awaitTermination()


