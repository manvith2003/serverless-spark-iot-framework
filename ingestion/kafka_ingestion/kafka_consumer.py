"""
Kafka Consumer for IoT Data
Consumes messages from Kafka topics for testing
"""

import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import signal
import sys


class IoTKafkaConsumer:
    """Kafka consumer for IoT data"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092",
                 topics: list = None, group_id: str = "iot-consumer-group"):
        
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or ["iot-smartcity-raw", "iot-healthcare-raw", "iot-industrial-raw"]
        self.group_id = group_id
        self.consumer = None
        self.message_count = 0
        self.running = True
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        print("\n🛑 Shutting down consumer...")
        self.running = False
    
    def connect(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',  # Start from latest
                enable_auto_commit=True,
                consumer_timeout_ms=1000,  # 1 second timeout for polling
                max_poll_records=10,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            print(f"✅ Connected to Kafka broker at {self.bootstrap_servers}")
            print(f"📥 Subscribed to topics: {', '.join(self.topics)}")
            print(f"👥 Consumer group: {self.group_id}\n")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def consume_messages(self, max_messages: int = None):
        """Consume messages from Kafka"""
        if not self.consumer:
            raise ConnectionError("Consumer not connected")
        
        print("🎧 Listening for messages... (Press Ctrl+C to stop)\n")
        
        try:
            while self.running:
                # Poll for messages with timeout
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                if not message_batch:
                    # No messages, continue polling
                    continue
                
                # Process messages
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self.message_count += 1
                        
                        # Extract data
                        topic = message.topic
                        partition = message.partition
                        offset = message.offset
                        key = message.key
                        value = message.value
                        
                        # Get sensor info
                        sensor_type = (value.get('sensor_type') or 
                                      value.get('device_type') or 
                                      value.get('machine_type', 'unknown'))
                        sensor_id = (value.get('sensor_id') or 
                                    value.get('device_id') or 
                                    value.get('machine_id', 'unknown'))
                        
                        print(f"📨 [{self.message_count}] Topic: {topic}")
                        print(f"   Partition: {partition} | Offset: {offset}")
                        print(f"   Key: {key}")
                        print(f"   Type: {sensor_type} | ID: {sensor_id}")
                        print(f"   Timestamp: {value.get('timestamp', 'N/A')}")
                        
                        # Show sample metrics
                        if 'metrics' in value:
                            metrics = value['metrics']
                            metric_keys = list(metrics.keys())[:3]
                            metric_values = {k: metrics[k] for k in metric_keys}
                            print(f"   Metrics: {metric_values}")
                        elif 'vitals' in value:
                            vitals = value['vitals']
                            print(f"   Vitals: HR={vitals.get('heart_rate_bpm')}bpm, SpO2={vitals.get('oxygen_saturation_percent')}%")
                        elif 'sensors' in value:
                            sensors = value['sensors']
                            print(f"   Sensors: Temp={sensors.get('temperature_celsius')}°C, Vibration={sensors.get('vibration_mms')}mm/s")
                        
                        print("-" * 80)
                        
                        # Stop if max messages reached
                        if max_messages and self.message_count >= max_messages:
                            print(f"\n✅ Reached max messages ({max_messages})")
                            self.running = False
                            break
                    
                    if not self.running:
                        break
                        
        except KeyboardInterrupt:
            print("\n🛑 Consumer stopped by user")
        except Exception as e:
            print(f"❌ Error consuming messages: {e}")
            import traceback
            traceback.print_exc()
    
    def close(self):
        """Close the consumer"""
        if self.consumer:
            self.consumer.close()
            print(f"\n📊 Consumed {self.message_count} messages")
            print("🛑 Kafka consumer closed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka IoT Data Consumer")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker")
    parser.add_argument("--topics", nargs="+", 
                       default=["iot-smartcity-raw", "iot-healthcare-raw", "iot-industrial-raw"],
                       help="Topics to consume from")
    parser.add_argument("--group", default="iot-consumer-group", help="Consumer group ID")
    parser.add_argument("--max-messages", type=int, default=None, 
                       help="Maximum number of messages to consume")
    
    args = parser.parse_args()
    
    # Create consumer
    consumer = IoTKafkaConsumer(
        bootstrap_servers=args.broker,
        topics=args.topics,
        group_id=args.group
    )
    
    try:
        if consumer.connect():
            consumer.consume_messages(max_messages=args.max_messages)
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        consumer.close()
