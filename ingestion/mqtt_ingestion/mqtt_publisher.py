"""
MQTT Publisher for IoT Data
Publishes generated IoT data to MQTT broker
"""

import json
import time
import paho.mqtt.client as mqtt
from typing import Dict, Callable
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generators.smart_city_generator import SmartCityGenerator
from data_generators.healthcare_generator import HealthcareGenerator
from data_generators.industrial_generator import IndustrialGenerator


class MQTTPublisher:
    """Publish IoT data to MQTT broker"""
    
    def __init__(self, broker_host: str = "localhost", broker_port: int = 1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = mqtt.Client(client_id=f"iot_publisher_{int(time.time())}")
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_publish = self._on_publish
        
        self.connected = False
        self.message_count = 0
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when client connects to broker"""
        if rc == 0:
            self.connected = True
            print(f"✅ Connected to MQTT broker at {self.broker_host}:{self.broker_port}")
        else:
            print(f"❌ Failed to connect, return code: {rc}")
    
    def _on_publish(self, client, userdata, mid):
        """Callback for when message is published"""
        self.message_count += 1
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            
            # Wait for connection
            timeout = 10
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                raise ConnectionError("Could not connect to MQTT broker")
                
        except Exception as e:
            print(f"❌ Connection error: {e}")
            raise
    
    def publish(self, topic: str, data: Dict):
        """Publish data to specific topic"""
        if not self.connected:
            raise ConnectionError("Not connected to broker")
        
        payload = json.dumps(data)
        result = self.client.publish(topic, payload, qos=1)
        
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"❌ Failed to publish to {topic}")
        
        return result.rc == mqtt.MQTT_ERR_SUCCESS
    
    def disconnect(self):
        """Disconnect from broker"""
        self.client.loop_stop()
        self.client.disconnect()
        print(f"\n📊 Published {self.message_count} messages")
        print("🛑 Disconnected from MQTT broker")


def publish_smart_city(publisher: MQTTPublisher, interval: float = 1.0):
    """Publish smart city data"""
    generator = SmartCityGenerator(num_sensors=5)
    topic = "iot/smartcity/sensors"
    
    print(f"🚦 Publishing Smart City data to topic: {topic}")
    
    try:
        while True:
            batch = generator.generate_batch(batch_size=3)
            for data in batch:
                publisher.publish(topic, data)
                print(f"📤 Published: {data['sensor_type']} from {data['sensor_id']}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 Smart City publisher stopped")


def publish_healthcare(publisher: MQTTPublisher, interval: float = 2.0):
    """Publish healthcare data"""
    generator = HealthcareGenerator(num_patients=3)
    topic = "iot/healthcare/vitals"
    
    print(f"🏥 Publishing Healthcare data to topic: {topic}")
    
    try:
        while True:
            batch = generator.generate_batch(batch_size=2)
            for data in batch:
                publisher.publish(topic, data)
                print(f"📤 Published: {data['device_type']} for {data['patient_id']}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 Healthcare publisher stopped")


def publish_industrial(publisher: MQTTPublisher, interval: float = 1.0):
    """Publish industrial data"""
    generator = IndustrialGenerator(num_machines=3)
    topic = "iot/industrial/machines"
    
    print(f"🏭 Publishing Industrial data to topic: {topic}")
    
    try:
        while True:
            batch = generator.generate_batch(batch_size=2)
            for data in batch:
                publisher.publish(topic, data)
                print(f"📤 Published: {data['machine_type']} ({data['machine_id']})")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 Industrial publisher stopped")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MQTT IoT Data Publisher")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--type", choices=["smartcity", "healthcare", "industrial", "all"],
                       default="all", help="Type of data to publish")
    parser.add_argument("--interval", type=float, default=1.0, help="Publish interval in seconds")
    
    args = parser.parse_args()
    
    # Create publisher
    publisher = MQTTPublisher(broker_host=args.broker, broker_port=args.port)
    
    try:
        publisher.connect()
        
        if args.type == "smartcity":
            publish_smart_city(publisher, args.interval)
        elif args.type == "healthcare":
            publish_healthcare(publisher, args.interval)
        elif args.type == "industrial":
            publish_industrial(publisher, args.interval)
        else:
            # Publish all types (simplified - single thread)
            print("📡 Publishing all IoT data types...")
            publish_smart_city(publisher, args.interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Publisher stopped by user")
    finally:
        publisher.disconnect()
