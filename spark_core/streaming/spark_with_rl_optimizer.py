"""
Spark Streaming with RL-Based Resource Optimizer
Uses trained PPO agent to dynamically allocate resources
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import sys
import os

# Add optimization module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../optimization/resource_allocation'))

from ppo_agent import ResourceAllocator


class SparkStreamingWithRL:
    """Spark Streaming with RL-based resource optimization"""
    
    def __init__(self):
        self.spark = None
        self.rl_optimizer = ResourceAllocator()
        self.current_metrics = {
            'workload_rate': 0,
            'data_volume': 0,
            'cpu_util': 50,
            'mem_util': 50,
            'latency': 500,
            'cost_rate': 5
        }
        
    def create_spark_session(self, num_executors=4, memory_per_executor=2):
        """Create Spark session with dynamic configuration"""
        
        self.spark = SparkSession.builder \
            .appName("IoT-Streaming-RL-Optimized") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.executor.instances", num_executors) \
            .config("spark.executor.memory", f"{memory_per_executor}g") \
            .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        return self.spark
    
    def load_rl_model(self):
        """Load trained RL model"""
        try:
            self.rl_optimizer.load("final_model")
            print("✅ RL Optimizer loaded successfully\n")
        except FileNotFoundError:
            print("⚠️  No trained model found. Using default configuration.")
            self.rl_optimizer = None
    
    def get_rl_recommendation(self):
        """Get resource allocation recommendation from RL agent"""
        
        if self.rl_optimizer is None or self.rl_optimizer.model is None:
            return None
        
        # Create state vector from current metrics
        state = np.array([
            self.current_metrics['workload_rate'],
            self.current_metrics['data_volume'],
            self.current_metrics['cpu_util'],
            self.current_metrics['mem_util'],
            self.current_metrics['latency'],
            self.current_metrics['cost_rate']
        ], dtype=np.float32)
        
        # Get recommendation
        action, info = self.rl_optimizer.predict(state)
        
        return info
    
    def update_metrics(self, batch_df):
        """Update current metrics from batch processing"""
        # This would be implemented with actual Spark metrics
        # For now, simulate metrics
        self.current_metrics['workload_rate'] = np.random.uniform(10, 500)
        self.current_metrics['data_volume'] = np.random.uniform(10, 500)
    
    def process_stream(self):
        """Main streaming processing with RL optimization"""
        
        print("🚀 Starting Spark Streaming with RL Optimization\n")
        
        # Load RL model
        self.load_rl_model()
        
        # Get initial recommendation
        if self.rl_optimizer:
            rec = self.get_rl_recommendation()
            print("🤖 RL Optimizer Recommendation:")
            print(f"   Executors: {rec['num_executors']}")
            print(f"   Memory per Executor: {rec['memory_per_executor_gb']} GB")
            print(f"   Storage Tier: {rec['storage_tier_name']}")
            print(f"   Total Memory: {rec['total_memory_gb']} GB\n")
            
            # Create Spark with recommended config
            self.create_spark_session(
                num_executors=rec['num_executors'],
                memory_per_executor=rec['memory_per_executor_gb']
            )
        else:
            # Use defaults
            self.create_spark_session()
        
        # Define schema
        schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("sensor_type", StringType(), True),
            StructField("city", StringType(), True),
            StructField("timestamp", StringType(), True),
        ])
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "iot-smartcity-raw") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse and process
        parsed_df = df.select(
            col("key").cast("string").alias("sensor_id"),
            col("value").cast("string").alias("data"),
            col("timestamp")
        )
        
        # Write to console
        query = parsed_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "5") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print("✅ Streaming started with RL-optimized configuration!")
        print("\n📊 Processing IoT data with optimized resources...")
        print("Press Ctrl+C to stop\n")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n🛑 Stopping...")
            query.stop()
            self.spark.stop()


def main():
    streaming_app = SparkStreamingWithRL()
    streaming_app.process_stream()


if __name__ == "__main__":
    main()
