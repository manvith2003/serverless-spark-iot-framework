"""
Basic Spark Structured Streaming Job for IoT Data
Consumes from Kafka and performs real-time processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session(app_name="IoT-Streaming"):
    """Create Spark session with Kafka support"""

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark

def define_smart_city_schema():
    """Define schema for Smart City IoT data"""
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True),
        StructField("metrics", StructType([
            StructField("vehicle_count", IntegerType(), True),
            StructField("average_speed_kmh", DoubleType(), True),
            StructField("congestion_level", StringType(), True),
            StructField("lane_occupancy_percent", DoubleType(), True),
            StructField("aqi", IntegerType(), True),
            StructField("pm25", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("co2_ppm", IntegerType(), True),
            StructField("temperature_celsius", DoubleType(), True),
            StructField("humidity_percent", DoubleType(), True),
            StructField("total_spots", IntegerType(), True),
            StructField("occupied_spots", IntegerType(), True),
            StructField("available_spots", IntegerType(), True),
            StructField("occupancy_rate", DoubleType(), True)
        ]), True)
    ])

def read_kafka_stream(spark, kafka_brokers="localhost:9092", topic="iot-smartcity-raw"):
    """Read streaming data from Kafka"""

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    return df

def process_smart_city_stream(df, schema):
    """Process Smart City IoT stream"""

    parsed_df = df.select(
        col("key").cast("string").alias("sensor_key"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    flattened_df = parsed_df.select(
        col("sensor_key"),
        col("data.sensor_id"),
        col("data.sensor_type"),
        col("data.city"),
        to_timestamp(col("data.timestamp")).alias("event_timestamp"),
        col("data.location.latitude"),
        col("data.location.longitude"),
        col("data.metrics.*"),
        col("kafka_timestamp")
    )

    return flattened_df

def add_watermark_and_aggregate(df, watermark_duration="10 seconds", window_duration="30 seconds"):
    """Add watermark and perform windowed aggregations"""

    watermarked_df = df.withWatermark("event_timestamp", watermark_duration)

    aggregated_df = watermarked_df \
        .groupBy(
            window(col("event_timestamp"), window_duration),
            col("sensor_type"),
            col("city")
        ) \
        .agg(
            count("*").alias("message_count"),
            approx_count_distinct("sensor_id").alias("unique_sensors"),
            avg("vehicle_count").alias("avg_vehicle_count"),
            max("vehicle_count").alias("max_vehicle_count"),
            avg("average_speed_kmh").alias("avg_speed"),
            avg("aqi").alias("avg_aqi"),
            max("aqi").alias("max_aqi"),
            avg("pm25").alias("avg_pm25"),
            avg("pm10").alias("avg_pm10"),
            avg("occupancy_rate").alias("avg_parking_occupancy"),
            max("occupancy_rate").alias("max_parking_occupancy")
        )

    return aggregated_df

def main():
    """Main Spark Streaming application"""

    print(" Starting Spark Structured Streaming for IoT Data\n")

    print("Creating Spark session...")
    spark = create_spark_session()
    print(" Spark session created\n")

    schema = define_smart_city_schema()

    print(" Reading from Kafka topic: iot-smartcity-raw")
    raw_stream = read_kafka_stream(spark, topic="iot-smartcity-raw")
    print(" Connected to Kafka\n")

    print("  Processing stream...")
    processed_stream = process_smart_city_stream(raw_stream, schema)

    print(" Starting raw data console output...")
    raw_query = processed_stream \
        .select(
            "sensor_id",
            "sensor_type",
            "event_timestamp",
            "vehicle_count",
            "average_speed_kmh",
            "congestion_level",
            "aqi",
            "pm25",
            "occupancy_rate"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .trigger(processingTime="5 seconds") \
        .start()

    print(" Raw data query started\n")

    print(" Starting aggregation query...")
    aggregated_stream = add_watermark_and_aggregate(
        processed_stream,
        watermark_duration="10 seconds",
        window_duration="30 seconds"
    )

    agg_query = aggregated_stream \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "sensor_type",
            "city",
            "message_count",
            "unique_sensors",
            "avg_vehicle_count",
            "avg_speed",
            "avg_aqi",
            "avg_pm25",
            "avg_parking_occupancy"
        ) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(" Aggregation query started\n")

    print("="*60)
    print(" All streaming queries running!")
    print("="*60)
    print("\n Monitoring IoT data stream...")
    print("Press Ctrl+C to stop\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n Stopping streaming queries...")
        raw_query.stop()
        agg_query.stop()
        spark.stop()
        print(" Spark streaming stopped")

if __name__ == "__main__":
    main()
