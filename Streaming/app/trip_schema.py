from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

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