#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════════════
                    IoT DATA PRODUCER FOR SPARK STREAMING TEST
═══════════════════════════════════════════════════════════════════════════════════════

This script sends continuous IoT data to Kafka for the Spark streaming job to consume.

Run in one terminal:
  python3 demos/iot_kafka_producer.py

Run in another terminal:
  python3 spark_core/streaming/spark_with_two_rl.py
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import sys


class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def create_iot_reading():
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
    
    return {
        "sensor_id": f"sensor_{random.randint(1, 100):03d}",
        "sensor_type": sensor_type,
        "value": round(values[sensor_type], 2),
        "timestamp": datetime.now().isoformat(),
        "location": random.choice(["bangalore", "mumbai", "delhi", "chennai"])
    }


def main():
    kafka_bootstrap = "localhost:9092"
    topic = "iot-realtime-demo"
    
    print(f"\n{Colors.BOLD}{'═' * 60}{Colors.ENDC}")
    print(f"{Colors.BOLD}  IoT DATA PRODUCER FOR SPARK STREAMING{Colors.ENDC}")
    print(f"{Colors.BOLD}{'═' * 60}{Colors.ENDC}\n")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all'
        )
        print(f"  {Colors.GREEN}✓{Colors.ENDC} Connected to Kafka: {kafka_bootstrap}")
        print(f"  {Colors.GREEN}✓{Colors.ENDC} Topic: {topic}")
    except Exception as e:
        print(f"  ❌ Failed to connect to Kafka: {e}")
        print(f"  Make sure Kafka is running: docker-compose up -d")
        sys.exit(1)
    
    print(f"\n  {Colors.YELLOW}Sending IoT data... (Ctrl+C to stop){Colors.ENDC}\n")
    
    message_count = 0
    batch_count = 0
    
    try:
        while True:
            # Send a batch of messages
            batch_size = random.randint(5, 20)
            batch_count += 1
            
            print(f"  {Colors.CYAN}Batch {batch_count}:{Colors.ENDC} Sending {batch_size} messages...", end=" ")
            
            for _ in range(batch_size):
                reading = create_iot_reading()
                producer.send(topic, value=reading, key=reading["sensor_id"])
                message_count += 1
            
            producer.flush()
            print(f"{Colors.GREEN}✓{Colors.ENDC} (Total: {message_count})")
            
            # Wait before next batch (simulates real IoT stream)
            wait_time = random.uniform(1, 3)
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print(f"\n\n  {Colors.YELLOW}Producer stopped.{Colors.ENDC}")
        print(f"  Total messages sent: {message_count}")
        print(f"  Total batches: {batch_count}\n")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
