from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, count
from trip_schema import schema

# Define Spark session
spark = SparkSession.builder.appName("Top Routes from Kafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Define schema for the Kafka message


# Read input data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "trip_topic") \
    .option("startingOffsets", "earliest") \
    .load()

clean_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# The number of records that were transferred will be 669959
# count_df = clean_df.agg(count("*").alias("count"))

# top routers
routes_df = clean_df.groupBy("start_station_name", "end_station_name") \
                     .agg(count("*").alias("trips_num")) \
                     .orderBy("trips_num", ascending=False)

# Write output to the console
console_output = routes_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start()

console_output.awaitTermination()
