"""
Multi-type MQTT Publisher
Publishes all IoT data types simultaneously (Smart City, Healthcare, Industrial)
"""

import sys
import os
import time
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mqtt_ingestion.mqtt_publisher import MQTTPublisher
from data_generators.smart_city_generator import SmartCityGenerator
from data_generators.healthcare_generator import HealthcareGenerator
from data_generators.industrial_generator import IndustrialGenerator


def publish_smartcity(interval: float = 2.0):
    """Publish smart city data"""
    publisher = MQTTPublisher()
    generator = SmartCityGenerator(num_sensors=5)
    
    try:
        publisher.connect()
        print("🚦 Smart City publisher started")
        
        while True:
            batch = generator.generate_batch(batch_size=2)
            for data in batch:
                publisher.publish("iot/smartcity/sensors", data)
            time.sleep(interval)
            
    except KeyboardInterrupt:
        pass
    finally:
        publisher.disconnect()


def publish_healthcare(interval: float = 3.0):
    """Publish healthcare data"""
    publisher = MQTTPublisher()
    generator = HealthcareGenerator(num_patients=3)
    
    try:
        publisher.connect()
        print("🏥 Healthcare publisher started")
        
        while True:
            batch = generator.generate_batch(batch_size=2)
            for data in batch:
                publisher.publish("iot/healthcare/vitals", data)
            time.sleep(interval)
            
    except KeyboardInterrupt:
        pass
    finally:
        publisher.disconnect()


def publish_industrial(interval: float = 1.5):
    """Publish industrial data"""
    publisher = MQTTPublisher()
    generator = IndustrialGenerator(num_machines=3)
    
    try:
        publisher.connect()
        print("🏭 Industrial publisher started")
        
        while True:
            batch = generator.generate_batch(batch_size=2)
            for data in batch:
                publisher.publish("iot/industrial/machines", data)
            time.sleep(interval)
            
    except KeyboardInterrupt:
        pass
    finally:
        publisher.disconnect()


if __name__ == "__main__":
    print("🚀 Starting Multi-Type IoT Data Publishers\n")
    
    # Create threads for each publisher
    threads = [
        threading.Thread(target=publish_smartcity, daemon=True, name="SmartCity"),
        threading.Thread(target=publish_healthcare, daemon=True, name="Healthcare"),
        threading.Thread(target=publish_industrial, daemon=True, name="Industrial")
    ]
    
    # Start all threads
    for t in threads:
        t.start()
        time.sleep(1)  # Stagger starts
    
    print("\n✅ All publishers running (Press Ctrl+C to stop)\n")
    print("📊 Publishing:")
    print("   🚦 Smart City: traffic, pollution, parking (every 2s)")
    print("   🏥 Healthcare: patient vitals, ECG (every 3s)")
    print("   🏭 Industrial: equipment sensors (every 1.5s)")
    print()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping all publishers...")
        time.sleep(2)
        print("✅ All publishers stopped")
