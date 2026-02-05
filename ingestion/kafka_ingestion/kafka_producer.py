"""
Kafka Producer for IoT Data
Receives data from MQTT and publishes to Kafka topics
"""

import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mqtt_ingestion.mqtt_subscriber import MQTTSubscriber


class IoTKafkaProducer:
    """Kafka producer for IoT data with production-grade settings"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092",
                 compression_type: str = "gzip",
                 enable_idempotence: bool = True):
        self.bootstrap_servers = bootstrap_servers
        self.compression_type = compression_type
        self.enable_idempotence = enable_idempotence
        self.producer = None
        self.dlq_producer = None  # Dead letter queue producer
        self.message_count = 0
        self.failed_count = 0
        
    def connect(self):
        """Connect to Kafka broker with production settings"""
        try:
            print(f"   Connecting to Kafka at {self.bootstrap_servers}...")
            print(f"   Compression: {self.compression_type} | Idempotent: {self.enable_idempotence}")
            
            # Main producer with exactly-once semantics
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Production settings
                acks='all',  # Wait for all replicas
                retries=5,   # Retry on transient errors
                max_in_flight_requests_per_connection=1 if self.enable_idempotence else 5,
                enable_idempotence=self.enable_idempotence,  # Exactly-once semantics
                compression_type=self.compression_type,  # Compress messages
                linger_ms=5,  # Small batch optimization
                batch_size=32768,  # 32KB batch size
                request_timeout_ms=30000,
                api_version=(2, 5, 0)
            )
            
            # Separate producer for dead letter queue (simpler config)
            self.dlq_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                compression_type='gzip'
            )
            
            print(f"✅ Connected to Kafka broker at {self.bootstrap_servers}")
            print(f"   ├─ Main producer: idempotent={self.enable_idempotence}")
            print(f"   └─ DLQ producer: ready")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def send_message(self, topic: str, data: dict, key: str = None):
        """Send message to Kafka topic with DLQ fallback"""
        if not self.producer:
            raise ConnectionError("Producer not connected")
        
        try:
            # Send message
            future = self.producer.send(topic, value=data, key=key)
            
            # Wait for result
            record_metadata = future.get(timeout=10)
            
            self.message_count += 1
            
            return {
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }
            
        except KafkaError as e:
            print(f"❌ Kafka error: {e}")
            self._send_to_dlq(topic, data, key, str(e))
            return None
        except Exception as e:
            print(f"❌ Error sending message: {e}")
            self._send_to_dlq(topic, data, key, str(e))
            return None
    
    def _send_to_dlq(self, original_topic: str, data: dict, key: str, error: str):
        """Send failed message to dead letter queue"""
        if not self.dlq_producer:
            return
        
        try:
            dlq_message = {
                "original_topic": original_topic,
                "original_key": key,
                "original_data": data,
                "error": error,
                "failed_at": time.strftime("%Y-%m-%dT%H:%M:%S")
            }
            self.dlq_producer.send("iot-dead-letter", value=dlq_message, key=key)
            self.failed_count += 1
            print(f"   ↳ 🔄 Sent to dead-letter queue")
        except Exception as dlq_error:
            print(f"   ↳ ❌ DLQ also failed: {dlq_error}")
    
    def send_validated(self, data: dict, key: str = None):
        """Send validated data to the validated topic"""
        return self.send_message("iot-validated-data", data, key)
    
    def send_alert(self, data: dict, key: str = None, priority: str = "critical"):
        """Send alert to the appropriate alert topic"""
        topic = "iot-alerts-critical" if priority == "critical" else "iot-alerts-critical"
        alert_data = {
            **data,
            "alert_priority": priority,
            "alert_time": time.strftime("%Y-%m-%dT%H:%M:%S")
        }
        return self.send_message(topic, alert_data, key)
    
    def send_analytics_result(self, data: dict, key: str = None):
        """Send analytics results"""
        return self.send_message("iot-analytics-results", data, key)
    
    def close(self):
        """Close all producers"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
        if self.dlq_producer:
            self.dlq_producer.flush()
            self.dlq_producer.close()
        print(f"\n📊 Sent {self.message_count} messages to Kafka")
        if self.failed_count > 0:
            print(f"⚠️  {self.failed_count} messages sent to dead-letter queue")
        print("🛑 Kafka producer closed")


class MQTTToKafkaBridge:
    """Bridge between MQTT and Kafka"""
    
    def __init__(self, mqtt_broker: str = "localhost", mqtt_port: int = 1883,
                 kafka_broker: str = "localhost:9092", topics: list = None):
        
        self.kafka_producer = IoTKafkaProducer(bootstrap_servers=kafka_broker)
        
        self.mqtt_subscriber = MQTTSubscriber(
            broker_host=mqtt_broker,
            broker_port=mqtt_port,
            topics=topics or ["iot/#"]
        )
        
        # Topic mapping
        self.topic_mapping = {
            "iot/smartcity/sensors": "iot-smartcity-raw",
            "iot/healthcare/vitals": "iot-healthcare-raw",
            "iot/industrial/machines": "iot-industrial-raw"
        }
    
    def _mqtt_to_kafka_callback(self, mqtt_topic: str, data: dict):
        """Callback to forward MQTT messages to Kafka"""
        
        # Map MQTT topic to Kafka topic
        kafka_topic = self.topic_mapping.get(mqtt_topic, "iot-unknown-raw")
        
        # Use sensor/device ID as key
        key = (data.get('sensor_id') or 
               data.get('device_id') or 
               data.get('machine_id'))
        
        # Send to Kafka
        result = self.kafka_producer.send_message(kafka_topic, data, key)
        
        if result:
            print(f"  ↳ 📤 Forwarded to Kafka: {kafka_topic} (partition: {result['partition']}, offset: {result['offset']})")
    
    def start(self):
        """Start the bridge"""
        print("🌉 Starting MQTT → Kafka Bridge...\n")
        
        # Connect to Kafka first
        if not self.kafka_producer.connect():
            print("❌ Failed to connect to Kafka. Exiting.")
            return
        
        print()
        
        # Set callback before connecting to MQTT
        self.mqtt_subscriber.set_message_callback(self._mqtt_to_kafka_callback)
        
        # Connect to MQTT
        try:
            self.mqtt_subscriber.connect(retry_count=3, retry_delay=2.0)
            print()
            
            # Start listening
            self.mqtt_subscriber.start_loop()
            
        except ConnectionError as e:
            print(f"❌ Error: {e}")
            self.kafka_producer.close()
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            self.kafka_producer.close()
    
    def stop(self):
        """Stop the bridge"""
        self.mqtt_subscriber.disconnect()
        self.kafka_producer.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MQTT to Kafka Bridge")
    parser.add_argument("--mqtt-broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--kafka-broker", default="localhost:9092", help="Kafka broker")
    parser.add_argument("--topics", nargs="+", default=["iot/#"], 
                       help="MQTT topics to subscribe to")
    
    args = parser.parse_args()
    
    # Create bridge
    bridge = MQTTToKafkaBridge(
        mqtt_broker=args.mqtt_broker,
        mqtt_port=args.mqtt_port,
        kafka_broker=args.kafka_broker,
        topics=args.topics
    )
    
    try:
        bridge.start()
    except KeyboardInterrupt:
        print("\n🛑 Bridge stopped by user")
    finally:
        bridge.stop()
