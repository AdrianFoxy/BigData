from pyspark.sql.types import StructType, StructField, StringType, LongType

# Define CSV schema
schema = StructType([
    StructField("id", LongType(), True),
    StructField("duration", LongType(), True),
    StructField("start_date", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", LongType(), True),
    StructField("end_date", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", LongType(), True),
    StructField("bike_id", LongType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("zip_code", StringType(), True)
])