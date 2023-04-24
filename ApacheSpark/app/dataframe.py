from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType

# Define Spark session
spark = SparkSession.builder.master("local").appName("Top Bike by Day").getOrCreate()

# Define CSV schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("duration", FloatType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("bike_id", StringType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("zip_code", StringType(), True)
])

# Read CSV file
df = spark.read.format("csv") \
             .option("header", True) \
             .option("timestampFormat", "M/d/y H:m") \
             .schema(schema) \
             .load("resources/input/trip.csv")

# Top routes
# group by start and end station, and count num of trips
result_df_1 = df.groupBy("start_station_name", "end_station_name") \
                        .agg(count("*").alias("trips_num")) \
                        .orderBy("trips_num", ascending=False)

# Save result of top routers as CSV file
result_df_1.repartition(10).write.format("csv") \
             .option("header", True) \
             .mode("overwrite")\
             .save("resources/output")
