"""
Multi-topic Spark Streaming for All IoT Data Types
Processes Smart City, Healthcare, and Industrial data simultaneously
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("IoT-Multi-Stream") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints") \
        .getOrCreate()

def process_all_streams(spark):
    """Process all three IoT data types"""

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "iot-smartcity-raw,iot-healthcare-raw,iot-industrial-raw") \
        .option("startingOffsets", "latest") \
        .load()

    query = df \
        .selectExpr(
            "topic",
            "CAST(key AS STRING) as sensor_id",
            "CAST(value AS STRING) as data_preview"
        ) \
        .withColumn("data_length", length(col("data_preview"))) \
        .select("topic", "sensor_id", "data_length") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()

    return query

def main():
    print(" Multi-Stream IoT Processing\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    query = process_all_streams(spark)

    print(" Processing all IoT topics!")
    print("   - iot-smartcity-raw")
    print("   - iot-healthcare-raw")
    print("   - iot-industrial-raw")
    print("\nPress Ctrl+C to stop\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()

if __name__ == "__main__":
    main()
