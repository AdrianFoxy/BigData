from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

spark = SparkSession.builder \
    .master('local') \
    .appName("Top Routes") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2,"
                                   "org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0") \
    .config("es.net.ssl.cert.allow.self.signed", "true") \
    .getOrCreate()

load_dotenv()
MONGODB_USERNAME = os.getenv("MONGODB_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")

df = spark.read.format("mongodb") \
    .option("spark.mongodb.connection.uri", f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@localhost:27017") \
    .option("spark.mongodb.database", "bike_database") \
    .option("spark.mongodb.collection", "top_trips_1") \
    .load()

df.printSchema()
df.show()

es_write_conf = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.nodes.wan.only": "true",
    "es.mapping.exclude": "_id"
}

df.write.format("es") \
    .mode("overwrite") \
    .options(**es_write_conf) \
    .save("top_trips_1")