from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, count
from trip_schema import schema


# Define Spark session
spark = SparkSession.builder \
    .appName("Top Route from Kafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.mongodb.spark:mongo-spark-connector:10.0.2") \
    .getOrCreate()


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

routes_df = clean_df.groupBy("start_station_name", "end_station_name") \
                     .agg(count("*").alias("trips_num")) \
                     .orderBy("trips_num", ascending=False)

# Write to MongoDB
mongo_uri = "mongodb://root:rootpassword@localhost:27017"
mongo_collection = "top_trips"

routes_df.writeStream \
    .format("mongodb") \
    .option("uri", mongo_uri) \
    .option("database", "my_database") \
    .option("collection", mongo_collection) \
    .option("checkpointLocation", "checkpoints") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()


