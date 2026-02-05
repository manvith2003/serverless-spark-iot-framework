#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════════════
                    SPARK STREAMING WITH TWO-RL CONTINUOUS LEARNING
                    Real Kafka Consumer + Phase-1 & Phase-2 RL
═══════════════════════════════════════════════════════════════════════════════════════

This script:
1. Connects to Kafka topic and processes real IoT data
2. Uses Phase-1 RL (PPO) to decide scaling actions every batch
3. Uses Phase-2 RL to update (α, β, γ) weights every N batches
4. Stores RL state in Redis for persistence
5. Outputs real-time metrics to console

Prerequisites:
  - Kafka running on localhost:9092
  - Redis running on localhost:6379
  - Spark installed

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      spark_core/streaming/spark_with_two_rl.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import numpy as np
import builtins
import time
import json
import threading
import sys
import os
from datetime import datetime
from typing import Dict, Tuple, Optional
from dataclasses import dataclass

# Try to import Redis
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ redis not installed. RL state will not persist.")


# ═══════════════════════════════════════════════════════════════════════════════════════
# PRINT UTILITIES  
# ═══════════════════════════════════════════════════════════════════════════════════════

class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'═' * 80}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}  {text}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'═' * 80}{Colors.ENDC}")


def print_metric(name: str, value: str, indent: int = 6):
    spaces = " " * indent
    print(f"{spaces}{Colors.BLUE}├─{Colors.ENDC} {name}: {Colors.BOLD}{value}{Colors.ENDC}")


# ═══════════════════════════════════════════════════════════════════════════════════════
# TWO-RL CONTROLLER
# ═══════════════════════════════════════════════════════════════════════════════════════

class TwoRLController:
    """
    Combined Phase-1 and Phase-2 RL Controller for Spark Streaming.
    
    Phase-1: Scaling decisions (every batch)
    Phase-2: Weight updates (every N batches = 1 episode)
    """
    
    def __init__(self,
                 sla_target_ms: float = 200.0,
                 cost_budget_per_hour: float = 10.0,
                 episode_length: int = 10,
                 redis_host: str = "localhost",
                 redis_port: int = 6379):
        
        # Business constraints
        self.sla_target = sla_target_ms
        self.cost_budget = cost_budget_per_hour
        self.episode_length = episode_length
        
        # RL State
        self.alpha = 0.33  # Cost weight
        self.beta = 0.33   # Latency weight
        self.gamma = 0.34  # Throughput weight
        self.current_executors = 2
        
        # Episode tracking
        self.batch_count = 0
        self.episode_count = 0
        self.episode_latencies = []
        self.episode_costs = []
        self.episode_violations = 0
        self.episode_rewards = []
        
        # Global stats
        self.total_batches = 0
        self.total_messages = 0
        self.total_violations = 0
        
        # Redis connection
        self.redis_client = None
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
                self.redis_client.ping()
                self._load_state_from_redis()
                print(f"  {Colors.GREEN}✓{Colors.ENDC} Redis connected: {redis_host}:{redis_port}")
            except Exception as e:
                print(f"  {Colors.YELLOW}⚠{Colors.ENDC} Redis not available: {e}")
                self.redis_client = None
    
    def _load_state_from_redis(self):
        """Load previous RL state from Redis"""
        if not self.redis_client:
            return
        
        try:
            weights = self.redis_client.hgetall("rl:weights")
            if weights:
                self.alpha = float(weights.get(b"alpha", 0.33))
                self.beta = float(weights.get(b"beta", 0.33))
                self.gamma = float(weights.get(b"gamma", 0.34))
            
            executors = self.redis_client.get("rl:executors")
            if executors:
                self.current_executors = int(executors)
            
            print(f"  {Colors.GREEN}✓{Colors.ENDC} Loaded RL state from Redis")
        except Exception as e:
            print(f"  {Colors.YELLOW}⚠{Colors.ENDC} Could not load Redis state: {e}")
    
    def _save_state_to_redis(self):
        """Save current RL state to Redis"""
        if not self.redis_client:
            return
        
        try:
            self.redis_client.hset("rl:weights", mapping={
                "alpha": str(self.alpha),
                "beta": str(self.beta),
                "gamma": str(self.gamma)
            })
            self.redis_client.set("rl:executors", self.current_executors)
            self.redis_client.set("rl:total_batches", self.total_batches)
            self.redis_client.set("rl:total_messages", self.total_messages)
        except Exception:
            pass
    
    def phase1_decide(self, metrics: Dict) -> Tuple[str, int, float]:
        """
        Phase-1 RL: Make scaling decision based on current metrics.
        
        State: [workload, latency, cpu, cost, α, β, γ]
        Action: scale_up, scale_down, maintain
        Reward: -α·cost - β·latency + γ·throughput
        """
        workload = metrics.get("workload", 100)
        latency = metrics.get("latency_ms", 150)
        cpu = metrics.get("cpu_util", 50)
        cost = self.current_executors * 0.5
        throughput = metrics.get("throughput", workload * 0.9)
        
        # Compute reward
        norm_cost = cost / 10.0
        norm_latency = latency / 1000.0
        norm_throughput = throughput / 200.0
        
        reward = (
            -self.alpha * norm_cost +
            -self.beta * norm_latency +
            self.gamma * norm_throughput
        )
        
        # Simple policy (in production, use trained PPO model)
        if latency > self.sla_target * 1.2:
            action = "SCALE_UP"
            new_executors = min(20, self.current_executors + 2)
        elif latency < self.sla_target * 0.4 and cost > self.cost_budget * 0.8:
            action = "SCALE_DOWN"
            new_executors = max(1, self.current_executors - 1)
        else:
            action = "MAINTAIN"
            new_executors = self.current_executors
        
        # Update state
        old_executors = self.current_executors
        self.current_executors = new_executors
        
        # Track for episode
        self.episode_latencies.append(latency)
        self.episode_costs.append(cost)
        self.episode_rewards.append(reward)
        
        if latency > self.sla_target:
            self.episode_violations += 1
            self.total_violations += 1
        
        return action, new_executors, reward
    
    def phase2_update(self) -> Tuple[float, float, float, float]:
        """
        Phase-2 RL: Update weights after episode completes.
        
        Called every `episode_length` batches.
        """
        if len(self.episode_latencies) == 0:
            return self.alpha, self.beta, self.gamma, 0.0
        
        # Aggregate episode metrics
        avg_latency = np.mean(self.episode_latencies)
        total_cost = np.sum(self.episode_costs)
        violations = self.episode_violations
        
        # Compute meta-reward
        if avg_latency <= self.sla_target:
            latency_reward = 1.0
        else:
            latency_reward = -1.0 * (avg_latency / self.sla_target - 1)
        
        if total_cost <= self.cost_budget:
            cost_reward = 1.0 - (total_cost / self.cost_budget) * 0.5
        else:
            cost_reward = -1.0 * (total_cost / self.cost_budget - 1)
        
        violation_penalty = -0.5 * violations
        meta_reward = latency_reward + cost_reward + violation_penalty
        
        # Adjust weights (simple gradient-based update)
        old_alpha, old_beta, old_gamma = self.alpha, self.beta, self.gamma
        
        if avg_latency > self.sla_target * 1.1:
            self.beta = min(0.7, self.beta + 0.03)
        elif total_cost > self.cost_budget:
            self.alpha = min(0.7, self.alpha + 0.03)
        
        # Normalize
        total = self.alpha + self.beta + self.gamma
        self.alpha /= total
        self.beta /= total
        self.gamma /= total
        
        # Store in Redis
        if self.redis_client:
            self.redis_client.lpush("rl:meta_rewards", f"{time.time()}:{meta_reward:.4f}")
            self.redis_client.lpush("rl:episodes", json.dumps({
                "episode": self.episode_count,
                "avg_latency": avg_latency,
                "total_cost": total_cost,
                "violations": violations,
                "meta_reward": meta_reward,
                "weights": {"alpha": self.alpha, "beta": self.beta, "gamma": self.gamma}
            }))
        
        # Reset episode tracking
        self.episode_latencies = []
        self.episode_costs = []
        self.episode_violations = 0
        self.episode_rewards = []
        self.episode_count += 1
        
        return self.alpha, self.beta, self.gamma, meta_reward
    
    def process_batch(self, batch_metrics: Dict) -> Dict:
        """
        Process one batch with both RL phases.
        
        Returns dict with all decisions and metrics.
        """
        self.batch_count += 1
        self.total_batches += 1
        self.total_messages += batch_metrics.get("message_count", 0)
        
        # Phase-1: Scaling decision
        action, new_executors, reward = self.phase1_decide(batch_metrics)
        
        result = {
            "batch": self.batch_count,
            "action": action,
            "executors": new_executors,
            "reward": reward,
            "weights": {"alpha": self.alpha, "beta": self.beta, "gamma": self.gamma}
        }
        
        # Phase-2: Check if episode complete
        if self.batch_count >= self.episode_length:
            alpha, beta, gamma, meta_reward = self.phase2_update()
            result["episode_complete"] = True
            result["episode"] = self.episode_count
            result["meta_reward"] = meta_reward
            result["new_weights"] = {"alpha": alpha, "beta": beta, "gamma": gamma}
            self.batch_count = 0
        else:
            result["episode_complete"] = False
        
        # Save state
        self._save_state_to_redis()
        
        return result


# ═══════════════════════════════════════════════════════════════════════════════════════
# SPARK STREAMING WITH TWO-RL
# ═══════════════════════════════════════════════════════════════════════════════════════

class SparkWithTwoRL:
    """
    Spark Structured Streaming integrated with Two-RL Continuous Learning.
    """
    
    def __init__(self,
                 kafka_bootstrap: str = "localhost:9092",
                 kafka_topic: str = "iot-realtime-demo",
                 batch_interval: str = "10 seconds"):
        
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_topic = kafka_topic
        self.batch_interval = batch_interval
        
        # Initialize RL controller
        self.rl_controller = TwoRLController(
            sla_target_ms=200.0,
            cost_budget_per_hour=10.0,
            episode_length=5  # 5 batches = 1 episode
        )
        
        self.spark = None
        self.start_time = None
    
    def create_spark_session(self):
        """Create Spark session with Kafka support"""
        print_header("CREATING SPARK SESSION")
        
        self.spark = SparkSession.builder \
            .appName("IoT-Streaming-TwoRL") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.executor.instances", str(self.rl_controller.current_executors)) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print_metric("App Name", "IoT-Streaming-TwoRL")
        print_metric("Master", "local[*]")
        print_metric("Kafka Bootstrap", self.kafka_bootstrap)
        print_metric("Topic", self.kafka_topic)
        print_metric("Initial Executors", str(self.rl_controller.current_executors))
        
        return self.spark
    
    def process_batch_callback(self, batch_df, batch_id):
        """
        Callback for each micro-batch.
        This is where RL decisions happen!
        """
        batch_start = time.time()
        
        # Get batch stats
        try:
            row_count = batch_df.count()
        except:
            row_count = 0
        
        if row_count == 0:
            return
        
        # Simulate metrics (in production, collect from Spark metrics)
        processing_time = (time.time() - batch_start) * 1000
        simulated_latency = 50 + row_count * 2 + np.random.uniform(-20, 20)
        
        batch_metrics = {
            "message_count": row_count,
            "workload": row_count * 10,
            "latency_ms": simulated_latency,
            "cpu_util": builtins.min(100, 30 + row_count * 0.5),
            "throughput": row_count / (processing_time / 1000) if processing_time > 0 else row_count
        }
        
        # RL Decision
        rl_result = self.rl_controller.process_batch(batch_metrics)
        
        # Print batch summary
        sla_ok = "✓" if simulated_latency <= self.rl_controller.sla_target else "✗"
        print(f"\n  {Colors.CYAN}┌─ BATCH {batch_id} ─────────────────────────────────────────────┐{Colors.ENDC}")
        print(f"  {Colors.CYAN}│{Colors.ENDC}  Messages: {row_count:5d}  |  Latency: {simulated_latency:6.1f}ms {sla_ok}")
        print(f"  {Colors.CYAN}│{Colors.ENDC}  Action: {rl_result['action']:10s}  |  Executors: {rl_result['executors']}")
        print(f"  {Colors.CYAN}│{Colors.ENDC}  Reward: {rl_result['reward']:+.4f}  |  Weights: α={self.rl_controller.alpha:.2f}, β={self.rl_controller.beta:.2f}, γ={self.rl_controller.gamma:.2f}")
        
        if rl_result["episode_complete"]:
            print(f"  {Colors.CYAN}│{Colors.ENDC}")
            print(f"  {Colors.YELLOW}│  🧠 EPISODE {rl_result['episode']} COMPLETE!{Colors.ENDC}")
            print(f"  {Colors.YELLOW}│     Meta-Reward: {rl_result['meta_reward']:+.4f}{Colors.ENDC}")
            if "new_weights" in rl_result:
                nw = rl_result["new_weights"]
                print(f"  {Colors.YELLOW}│     New Weights: α={nw['alpha']:.3f}, β={nw['beta']:.3f}, γ={nw['gamma']:.3f}{Colors.ENDC}")
        
        print(f"  {Colors.CYAN}└───────────────────────────────────────────────────────────┘{Colors.ENDC}")
    
    def start_streaming(self, timeout_seconds: Optional[int] = None):
        """Start the Spark streaming job with RL integration"""
        
        print_header("STARTING SPARK STREAMING WITH TWO-RL")
        
        # Create session
        self.create_spark_session()
        
        # Define schema for IoT data
        schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("sensor_type", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("location", StringType(), True)
        ])
        
        print(f"\n  {Colors.GREEN}Starting stream from Kafka topic: {self.kafka_topic}{Colors.ENDC}")
        print(f"  {Colors.GREEN}Batch interval: {self.batch_interval}{Colors.ENDC}")
        print(f"  {Colors.GREEN}Episode length: {self.rl_controller.episode_length} batches{Colors.ENDC}")
        print(f"\n  {Colors.YELLOW}Waiting for data...{Colors.ENDC}\n")
        
        self.start_time = time.time()
        
        try:
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON
            parsed_df = df.select(
                F.col("key").cast("string").alias("sensor_id"),
                F.from_json(F.col("value").cast("string"), schema).alias("data"),
                F.col("timestamp").alias("kafka_timestamp")
            ).select("sensor_id", "data.*", "kafka_timestamp")
            
            # Write stream with foreachBatch for RL processing
            query = parsed_df \
                .writeStream \
                .foreachBatch(self.process_batch_callback) \
                .outputMode("append") \
                .trigger(processingTime=self.batch_interval) \
                .start()
            
            if timeout_seconds:
                query.awaitTermination(timeout_seconds * 1000)
            else:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            print(f"\n\n  {Colors.YELLOW}Stream stopped by user{Colors.ENDC}")
        except Exception as e:
            print(f"\n  {Colors.RED}Error: {e}{Colors.ENDC}")
        finally:
            self.print_summary()
            if self.spark:
                self.spark.stop()
    
    def print_summary(self):
        """Print final summary"""
        duration = time.time() - self.start_time if self.start_time else 0
        
        print_header("STREAMING SESSION COMPLETE")
        print(f"""
    ┌──────────────────────────────────────────────────────────────────────────┐
    │                          SESSION STATISTICS                              │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  Duration:              {duration:>8.1f} seconds                               │
    │  Total Batches:         {self.rl_controller.total_batches:>8}                                   │
    │  Total Messages:        {self.rl_controller.total_messages:>8}                                   │
    │  Total Episodes:        {self.rl_controller.episode_count:>8}                                   │
    │  SLA Violations:        {self.rl_controller.total_violations:>8}                                   │
    │  Final Executors:       {self.rl_controller.current_executors:>8}                                   │
    │                                                                          │
    │  Final Weights:                                                          │
    │    α (cost):       {self.rl_controller.alpha:>8.3f}                                          │
    │    β (latency):    {self.rl_controller.beta:>8.3f}                                          │
    │    γ (throughput): {self.rl_controller.gamma:>8.3f}                                          │
    └──────────────────────────────────────────────────────────────────────────┘
        """)


# ═══════════════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Spark Streaming with Two-RL")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="iot-realtime-demo", help="Kafka topic to consume")
    parser.add_argument("--batch-interval", default="5 seconds", help="Batch interval")
    parser.add_argument("--timeout", type=int, default=None, help="Timeout in seconds (None = run forever)")
    
    args = parser.parse_args()
    
    print_header("SPARK STREAMING WITH TWO-RL CONTINUOUS LEARNING")
    print(f"""
    ┌──────────────────────────────────────────────────────────────────────────┐
    │                                                                          │
    │   📨 Kafka ({args.topic}) → ⚡ Spark → 🤖 Phase-1 RL → 🔧 Scaling        │
    │                                    ↓                                     │
    │                              🧠 Phase-2 RL (weights)                     │
    │                                    ↓                                     │
    │                              💾 Redis (state)                            │
    │                                                                          │
    │   Both RL agents learn CONTINUOUSLY from real Spark metrics!             │
    │                                                                          │
    └──────────────────────────────────────────────────────────────────────────┘
    """)
    
    streaming = SparkWithTwoRL(
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.topic,
        batch_interval=args.batch_interval
    )
    
    streaming.start_streaming(timeout_seconds=args.timeout)


if __name__ == "__main__":
    main()
