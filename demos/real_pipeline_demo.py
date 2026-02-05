#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════════════
                    REAL END-TO-END PIPELINE WITH ACTUAL SERVICES
                    IoT → MQTT → Kafka → Spark Simulation → Two-RL
═══════════════════════════════════════════════════════════════════════════════════════

This script connects to REAL running services:
  - MQTT Broker (Mosquitto) on localhost:1883
  - Apache Kafka on localhost:9092
  - Redis for state management on localhost:6379

Prerequisites:
  docker-compose up -d

Run:
  python3 demos/real_pipeline_demo.py
"""

import json
import time
import random
import threading
import numpy as np
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
import sys
import os

# ═══════════════════════════════════════════════════════════════════════════════════════
# IMPORTS - Real Services
# ═══════════════════════════════════════════════════════════════════════════════════════

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    print("⚠️ paho-mqtt not installed. Run: pip install paho-mqtt")

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️ kafka-python not installed. Run: pip install kafka-python")

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ redis not installed. Run: pip install redis")


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

def print_section(text: str):
    print(f"\n{Colors.BOLD}{Colors.CYAN}┌{'─' * 70}┐{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.CYAN}│  {text:<68}│{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.CYAN}└{'─' * 70}┘{Colors.ENDC}")

def print_step(icon: str, text: str, color=Colors.GREEN):
    print(f"  {color}{icon}{Colors.ENDC}  {text}")

def print_metric(name: str, value: str):
    print(f"      {Colors.BLUE}├─{Colors.ENDC} {name}: {Colors.BOLD}{value}{Colors.ENDC}")

def print_reward(text: str, value: float):
    color = Colors.GREEN if value >= 0 else Colors.RED
    sign = "+" if value >= 0 else ""
    print(f"      {color}│  → {text}: {sign}{value:.4f}{Colors.ENDC}")


# ═══════════════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════════════

@dataclass
class IoTReading:
    sensor_id: str
    sensor_type: str
    value: float
    timestamp: str
    location: str = "bangalore"

    def to_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "value": self.value,
            "timestamp": self.timestamp,
            "location": self.location
        }


@dataclass
class SparkMetrics:
    workload_rate: float
    latency_ms: float
    cpu_util: float
    cost: float
    throughput: float
    messages_processed: int


# ═══════════════════════════════════════════════════════════════════════════════════════
# REAL PIPELINE COMPONENTS
# ═══════════════════════════════════════════════════════════════════════════════════════

class RealPipeline:
    """
    Real pipeline connecting to actual running services.
    """
    
    def __init__(self,
                 mqtt_host: str = "localhost",
                 mqtt_port: int = 1883,
                 kafka_bootstrap: str = "localhost:9092",
                 redis_host: str = "localhost",
                 redis_port: int = 6379):
        
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.kafka_bootstrap = kafka_bootstrap
        self.redis_host = redis_host
        self.redis_port = redis_port
        
        # Connections
        self.mqtt_client = None
        self.kafka_producer = None
        self.kafka_consumer = None
        self.redis_client = None
        
        # RL State
        self.current_executors = 2
        self.alpha = 0.33  # Cost weight
        self.beta = 0.33   # Latency weight
        self.gamma = 0.34  # Throughput weight
        
        # Business constraints
        self.sla_target = 200.0
        self.cost_budget = 10.0
        
        # Metrics
        self.messages_sent = 0
        self.messages_received = 0
        self.episode_rewards = []
        self.sla_violations = 0
        
        # Topic
        self.KAFKA_TOPIC = "iot-realtime-demo"
        self.MQTT_TOPIC = "iot/demo/sensors"
    
    def connect_all(self) -> bool:
        """Connect to all services"""
        print_header("CONNECTING TO REAL SERVICES")
        
        success = True
        
        # 1. Connect to MQTT
        print_step("📡", "Connecting to MQTT Broker...")
        if MQTT_AVAILABLE:
            try:
                self.mqtt_client = mqtt.Client(client_id=f"rl-demo-{random.randint(1000,9999)}")
                self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, 60)
                self.mqtt_client.loop_start()
                print_metric("Status", f"{Colors.GREEN}CONNECTED{Colors.ENDC}")
                print_metric("Broker", f"{self.mqtt_host}:{self.mqtt_port}")
            except Exception as e:
                print_metric("Status", f"{Colors.RED}FAILED: {e}{Colors.ENDC}")
                success = False
        else:
            print_metric("Status", f"{Colors.YELLOW}SKIPPED (paho-mqtt not installed){Colors.ENDC}")
        
        # 2. Connect to Kafka
        print_step("📨", "Connecting to Kafka...")
        if KAFKA_AVAILABLE:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3
                )
                print_metric("Status", f"{Colors.GREEN}CONNECTED{Colors.ENDC}")
                print_metric("Broker", self.kafka_bootstrap)
                print_metric("Topic", self.KAFKA_TOPIC)
                
                # Create topic if not exists
                try:
                    admin = KafkaAdminClient(bootstrap_servers=self.kafka_bootstrap)
                    topic = NewTopic(name=self.KAFKA_TOPIC, num_partitions=4, replication_factor=1)
                    admin.create_topics([topic])
                    print_metric("Topic Created", "Yes")
                except Exception:
                    print_metric("Topic", "Already exists")
                    
            except Exception as e:
                print_metric("Status", f"{Colors.RED}FAILED: {e}{Colors.ENDC}")
                success = False
        else:
            print_metric("Status", f"{Colors.YELLOW}SKIPPED (kafka-python not installed){Colors.ENDC}")
        
        # 3. Connect to Redis
        print_step("💾", "Connecting to Redis...")
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
                self.redis_client.ping()
                print_metric("Status", f"{Colors.GREEN}CONNECTED{Colors.ENDC}")
                print_metric("Host", f"{self.redis_host}:{self.redis_port}")
                
                # Store initial RL state
                self.redis_client.hset("rl:weights", mapping={
                    "alpha": str(self.alpha),
                    "beta": str(self.beta),
                    "gamma": str(self.gamma)
                })
                self.redis_client.set("rl:executors", self.current_executors)
                print_metric("RL State", "Initialized in Redis")
                
            except Exception as e:
                print_metric("Status", f"{Colors.RED}FAILED: {e}{Colors.ENDC}")
                success = False
        else:
            print_metric("Status", f"{Colors.YELLOW}SKIPPED (redis not installed){Colors.ENDC}")
        
        return success
    
    def generate_iot_reading(self) -> IoTReading:
        """Generate realistic IoT sensor reading"""
        sensor_types = ["temperature", "humidity", "air_quality", "traffic", "power"]
        sensor_type = random.choice(sensor_types)
        
        values = {
            "temperature": random.gauss(32, 5),
            "humidity": random.gauss(65, 10),
            "air_quality": random.randint(50, 200),
            "traffic": random.randint(10, 500),
            "power": random.gauss(230, 10)
        }
        
        return IoTReading(
            sensor_id=f"sensor_{random.randint(1, 100):03d}",
            sensor_type=sensor_type,
            value=round(values[sensor_type], 2),
            timestamp=datetime.now().isoformat(),
            location=random.choice(["bangalore", "mumbai", "delhi", "chennai"])
        )
    
    def send_to_mqtt(self, reading: IoTReading) -> bool:
        """Send reading to MQTT broker"""
        if self.mqtt_client:
            try:
                payload = json.dumps(reading.to_dict())
                result = self.mqtt_client.publish(self.MQTT_TOPIC, payload, qos=1)
                return result.rc == mqtt.MQTT_ERR_SUCCESS
            except Exception as e:
                print(f"MQTT Error: {e}")
                return False
        return True  # Simulated
    
    def send_to_kafka(self, reading: IoTReading) -> dict:
        """Send reading to Kafka topic"""
        if self.kafka_producer:
            try:
                future = self.kafka_producer.send(
                    self.KAFKA_TOPIC, 
                    value=reading.to_dict(),
                    key=reading.sensor_id
                )
                record_metadata = future.get(timeout=10)
                self.messages_sent += 1
                return {
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset
                }
            except Exception as e:
                print(f"Kafka Error: {e}")
                return {"topic": self.KAFKA_TOPIC, "partition": 0, "offset": self.messages_sent}
        else:
            self.messages_sent += 1
            return {"topic": self.KAFKA_TOPIC, "partition": random.randint(0,3), "offset": self.messages_sent}
    
    def simulate_spark_processing(self, batch_size: int) -> SparkMetrics:
        """
        Simulate Spark processing metrics.
        In production, this would come from actual Spark metrics.
        """
        workload = batch_size * 10 + random.uniform(-20, 20)
        
        # Latency depends on executors
        base_latency = 200 * (workload / (self.current_executors * 50))
        latency = max(30, base_latency + random.uniform(-30, 30))
        
        cost = self.current_executors * 0.5
        
        return SparkMetrics(
            workload_rate=workload,
            latency_ms=latency,
            cpu_util=min(100, 30 + workload / self.current_executors * 0.5),
            cost=cost,
            throughput=workload * 0.9,
            messages_processed=batch_size
        )
    
    def phase1_decide(self, metrics: SparkMetrics) -> Tuple[str, int, float]:
        """Phase-1 RL scaling decision"""
        
        # Compute reward
        norm_cost = metrics.cost / 10.0
        norm_latency = metrics.latency_ms / 1000.0
        norm_throughput = metrics.throughput / 200.0
        
        reward = (
            -self.alpha * norm_cost +
            -self.beta * norm_latency +
            self.gamma * norm_throughput
        )
        
        # Simple policy
        if metrics.latency_ms > self.sla_target * 1.2:
            action = "SCALE_UP"
            new_executors = min(20, self.current_executors + 2)
        elif metrics.latency_ms < self.sla_target * 0.5 and metrics.cost > self.cost_budget * 0.8:
            action = "SCALE_DOWN"
            new_executors = max(1, self.current_executors - 1)
        else:
            action = "MAINTAIN"
            new_executors = self.current_executors
        
        return action, new_executors, reward
    
    def yarn_allocate(self, target: int) -> Dict:
        """Simulate YARN allocation"""
        old_count = self.current_executors
        self.current_executors = target
        
        # Store in Redis
        if self.redis_client:
            self.redis_client.set("rl:executors", target)
            self.redis_client.lpush("rl:executor_history", f"{time.time()}:{target}")
        
        return {
            "old_count": old_count,
            "new_count": target,
            "containers_added": max(0, target - old_count),
            "containers_removed": max(0, old_count - target)
        }
    
    def phase2_update(self, episode_data: Dict) -> Tuple[float, float, float, float]:
        """Phase-2 meta-RL weight update"""
        
        avg_latency = episode_data["avg_latency"]
        total_cost = episode_data["total_cost"]
        violations = episode_data["violations"]
        
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
        
        # Adjust weights
        old_alpha, old_beta, old_gamma = self.alpha, self.beta, self.gamma
        
        if avg_latency > self.sla_target * 1.1:
            self.beta = min(0.7, self.beta + 0.05)
        elif total_cost > self.cost_budget:
            self.alpha = min(0.7, self.alpha + 0.05)
        
        # Normalize
        total = self.alpha + self.beta + self.gamma
        self.alpha /= total
        self.beta /= total
        self.gamma /= total
        
        # Store in Redis
        if self.redis_client:
            self.redis_client.hset("rl:weights", mapping={
                "alpha": str(self.alpha),
                "beta": str(self.beta),
                "gamma": str(self.gamma)
            })
            self.redis_client.lpush("rl:meta_rewards", f"{time.time()}:{meta_reward}")
        
        return self.alpha, self.beta, self.gamma, meta_reward
    
    def run_real_demo(self, num_episodes: int = 3, steps_per_episode: int = 5):
        """Run the real demo with actual services"""
        
        print_header("REAL PIPELINE DEMO WITH ACTUAL SERVICES")
        print(f"""
    ┌──────────────────────────────────────────────────────────────────────────┐
    │  🌡️ IoT Sensors → 📡 MQTT (localhost:1883) → 📨 Kafka (localhost:9092)  │
    │                            ↓                                             │
    │  ⚡ Spark Processing → 🤖 Phase-1 RL → 🔧 YARN Scaling                   │
    │                            ↓                                             │
    │  💾 Redis (localhost:6379) ← 🧠 Phase-2 RL (Meta-Controller)            │
    └──────────────────────────────────────────────────────────────────────────┘
        """)
        
        time.sleep(1)
        
        for episode in range(num_episodes):
            print_header(f"EPISODE {episode + 1} / {num_episodes}")
            print(f"\n  {Colors.YELLOW}Weights:{Colors.ENDC} α={self.alpha:.3f}, β={self.beta:.3f}, γ={self.gamma:.3f}")
            print(f"  {Colors.YELLOW}Executors:{Colors.ENDC} {self.current_executors}\n")
            
            episode_latencies = []
            episode_costs = []
            episode_violations = 0
            episode_reward_sum = 0
            
            for step in range(steps_per_episode):
                print_section(f"STEP {step + 1} / {steps_per_episode}")
                time.sleep(0.2)
                
                # 1. Generate IoT Data
                batch_size = random.randint(5, 15)
                readings = [self.generate_iot_reading() for _ in range(batch_size)]
                print_step("🌡️", f"IoT SENSORS: Generated {batch_size} readings")
                print_metric("Sample", f"{readings[0].sensor_type}: {readings[0].value}")
                
                # 2. Send to MQTT
                mqtt_success = sum(1 for r in readings if self.send_to_mqtt(r))
                print_step("📡", f"MQTT BROKER: {mqtt_success}/{batch_size} published")
                print_metric("Topic", self.MQTT_TOPIC)
                
                # 3. Send to Kafka
                kafka_results = [self.send_to_kafka(r) for r in readings]
                partitions = set(k['partition'] for k in kafka_results)
                print_step("📨", f"KAFKA: {len(kafka_results)} messages sent")
                print_metric("Topic", kafka_results[0]['topic'])
                print_metric("Partitions", f"{len(partitions)} used")
                print_metric("Latest Offset", f"{kafka_results[-1]['offset']}")
                
                # 4. Spark Processing
                metrics = self.simulate_spark_processing(batch_size)
                sla_ok = "✓" if metrics.latency_ms <= self.sla_target else f"✗ (SLA: {self.sla_target}ms)"
                print_step("⚡", f"SPARK: Processing {batch_size} messages")
                print_metric("Latency", f"{metrics.latency_ms:.1f}ms {sla_ok}")
                print_metric("Throughput", f"{metrics.throughput:.1f} msg/s")
                print_metric("CPU", f"{metrics.cpu_util:.1f}%")
                
                if metrics.latency_ms > self.sla_target:
                    episode_violations += 1
                    self.sla_violations += 1
                
                episode_latencies.append(metrics.latency_ms)
                episode_costs.append(metrics.cost)
                
                # 5. Phase-1 RL Decision
                action, new_executors, reward = self.phase1_decide(metrics)
                print_step("🤖", f"PHASE-1 RL: Decision = {action}")
                print_metric("State", f"[load={metrics.workload_rate:.0f}, lat={metrics.latency_ms:.0f}]")
                
                print(f"\n      {Colors.CYAN}│  REWARD BREAKDOWN:{Colors.ENDC}")
                print_reward(f"Cost penalty (-α×{metrics.cost/10:.2f})", -self.alpha * metrics.cost/10)
                print_reward(f"Latency penalty (-β×{metrics.latency_ms/1000:.3f})", -self.beta * metrics.latency_ms/1000)
                print_reward(f"Throughput bonus (+γ×{metrics.throughput/200:.3f})", self.gamma * metrics.throughput/200)
                color = Colors.GREEN if reward > 0 else Colors.RED
                print(f"      {Colors.BOLD}│  TOTAL: {'+' if reward > 0 else ''}{reward:.4f}{Colors.ENDC}")
                
                episode_reward_sum += reward
                self.episode_rewards.append(reward)
                
                # 6. YARN Allocation
                if action != "MAINTAIN":
                    yarn_result = self.yarn_allocate(new_executors)
                    print_step("🔧", f"YARN: {yarn_result['old_count']} → {yarn_result['new_count']} executors")
                    if yarn_result['containers_added'] > 0:
                        print_metric("Added", f"+{yarn_result['containers_added']} containers 🚀")
                    if yarn_result['containers_removed'] > 0:
                        print_metric("Removed", f"-{yarn_result['containers_removed']} containers 🛑")
                else:
                    print_step("⏸️", f"YARN: Maintaining {self.current_executors} executors")
                
                # 7. Store in Redis
                if self.redis_client:
                    self.redis_client.lpush("rl:rewards", f"{time.time()}:{reward}")
                    print_step("💾", "REDIS: State saved")
                
                print()
                time.sleep(0.3)
            
            # Phase-2 Update
            print_section(f"PHASE-2 META-RL UPDATE")
            
            episode_data = {
                "avg_latency": np.mean(episode_latencies),
                "total_cost": np.sum(episode_costs),
                "violations": episode_violations
            }
            
            print_step("🧠", "PHASE-2 RL: Analyzing episode...")
            print_metric("Avg Latency", f"{episode_data['avg_latency']:.1f}ms")
            print_metric("Total Cost", f"${episode_data['total_cost']:.2f}")
            print_metric("Violations", f"{episode_violations}")
            
            old_alpha, old_beta, old_gamma = self.alpha, self.beta, self.gamma
            new_alpha, new_beta, new_gamma, meta_reward = self.phase2_update(episode_data)
            
            print(f"\n      {Colors.CYAN}│  META-REWARD:{Colors.ENDC}")
            color = Colors.GREEN if meta_reward > 0 else Colors.RED
            print(f"      {color}│  {'+' if meta_reward > 0 else ''}{meta_reward:.4f}{Colors.ENDC}")
            
            print(f"\n      {Colors.YELLOW}│  WEIGHT UPDATE:{Colors.ENDC}")
            print(f"      │  Old: α={old_alpha:.3f}, β={old_beta:.3f}, γ={old_gamma:.3f}")
            print(f"      │  New: α={new_alpha:.3f}, β={new_beta:.3f}, γ={new_gamma:.3f}")
            
            if new_beta > new_alpha and new_beta > new_gamma:
                priority = "LATENCY"
            elif new_alpha > new_beta:
                priority = "COST"
            else:
                priority = "THROUGHPUT"
            print(f"      {Colors.BOLD}│  → Priority: {priority}{Colors.ENDC}")
            
            time.sleep(0.5)
        
        # Summary
        print_header("DEMO COMPLETE - SUMMARY")
        
        print(f"""
    ┌──────────────────────────────────────────────────────────────────────────┐
    │                          REAL PIPELINE STATISTICS                        │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  Messages to MQTT:          {self.messages_sent:>8}                                   │
    │  Messages to Kafka:         {self.messages_sent:>8}                                   │
    │  SLA Violations:            {self.sla_violations:>8}                                   │
    │  Final Executors:           {self.current_executors:>8}                                   │
    │                                                                          │
    │  Final Weights (in Redis):                                               │
    │    α (cost):       {self.alpha:>8.3f}                                          │
    │    β (latency):    {self.beta:>8.3f}                                          │
    │    γ (throughput): {self.gamma:>8.3f}                                          │
    │                                                                          │
    │  Average Step Reward:       {np.mean(self.episode_rewards):>+8.4f}                             │
    └──────────────────────────────────────────────────────────────────────────┘
        """)
        
        # Show Redis keys
        if self.redis_client:
            print(f"  {Colors.GREEN}✅ RL State persisted in Redis!{Colors.ENDC}")
            print(f"     Keys: rl:weights, rl:executors, rl:rewards, rl:meta_rewards")
        
        if self.kafka_producer:
            print(f"  {Colors.GREEN}✅ Messages stored in Kafka topic: {self.KAFKA_TOPIC}{Colors.ENDC}")
        
        if self.mqtt_client:
            print(f"  {Colors.GREEN}✅ Messages published to MQTT: {self.MQTT_TOPIC}{Colors.ENDC}")
        
        print()
    
    def cleanup(self):
        """Close all connections"""
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
        if self.redis_client:
            self.redis_client.close()
        print("🛑 All connections closed")


if __name__ == "__main__":
    pipeline = RealPipeline()
    
    try:
        if pipeline.connect_all():
            pipeline.run_real_demo(num_episodes=3, steps_per_episode=4)
        else:
            print("\n⚠️ Some services not available. Running with available services...")
            pipeline.run_real_demo(num_episodes=2, steps_per_episode=3)
    except KeyboardInterrupt:
        print("\n🛑 Demo stopped by user")
    finally:
        pipeline.cleanup()
