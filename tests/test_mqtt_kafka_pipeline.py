"""
Simple test for MQTT → Kafka pipeline
"""

import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.mqtt_ingestion.mqtt_publisher import MQTTPublisher
from ingestion.data_generators.smart_city_generator import SmartCityGenerator

def test_mqtt_publish():
    """Test MQTT publishing"""
    print("\n" + "="*60)
    print("Testing MQTT Publishing")
    print("="*60)
    
    try:
        # Create publisher
        publisher = MQTTPublisher(broker_host="localhost", broker_port=1883)
        
        # Connect
        print("Connecting to MQTT broker...")
        publisher.connect()
        
        if not publisher.connected:
            print("❌ Failed to connect to MQTT broker")
            return False
        
        print("✅ Connected successfully")
        
        # Generate and publish test data
        generator = SmartCityGenerator(num_sensors=2)
        
        print("\nPublishing 5 test messages...")
        for i in range(5):
            data = generator.generate_batch(batch_size=1)[0]
            success = publisher.publish("iot/smartcity/sensors", data)
            
            if success:
                print(f"  ✅ Message {i+1} published")
            else:
                print(f"  ❌ Message {i+1} failed")
            
            time.sleep(0.5)
        
        # Disconnect
        publisher.disconnect()
        
        print("\n✅ MQTT test passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ MQTT test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_mqtt_publish()
    sys.exit(0 if success else 1)
