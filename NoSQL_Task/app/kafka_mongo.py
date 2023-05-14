from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from trip_schema import schema
from dotenv import load_dotenv
import os

spark = SparkSession.builder \
    .appName("KafkaToMongoDB") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,org.mongodb.spark:mongo-spark-connector:10.0.0") \
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

load_dotenv()
MONGODB_USERNAME = os.getenv("MONGODB_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")

def write_row(batch_df , batch_id):
    batch_df.write\
        .format("mongodb")\
        .mode("append")\
        .option("spark.mongodb.connection.uri", f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@localhost:27017") \
        .option("spark.mongodb.database", "bike_database") \
        .option("spark.mongodb.collection", "top_trips_1") \
        .save()
    pass

# Write to MongoDB
query = routes_df \
    .writeStream \
    .foreachBatch(write_row) \
    .outputMode("complete") \
    .start()\
    .awaitTermination(60)


