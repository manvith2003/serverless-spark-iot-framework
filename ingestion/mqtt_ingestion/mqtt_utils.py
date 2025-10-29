"""
MQTT utility functions
"""

import time
import paho.mqtt.client as mqtt


def test_mqtt_connection(host="localhost", port=1883, timeout=10):
    """Test MQTT broker connection"""
    
    print(f"🔍 Testing MQTT connection to {host}:{port}...")
    
    connected = [False]
    
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            connected[0] = True
            print(f"✅ Successfully connected to MQTT broker")
        else:
            print(f"❌ Connection failed with reason code: {reason_code}")
    
    try:
        client = mqtt.Client(
            client_id=f"test_client_{int(time.time())}",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        client.on_connect = on_connect
        
        print(f"   Attempting connection...")
        client.connect(host, port, 60)
        client.loop_start()
        
        # Wait for connection
        start_time = time.time()
        while not connected[0] and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        
        client.loop_stop()
        client.disconnect()
        
        if connected[0]:
            print(f"✅ MQTT broker is accessible")
            return True
        else:
            print(f"❌ Could not connect within {timeout} seconds")
            return False
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False


if __name__ == "__main__":
    import sys
    test_mqtt_connection()
    sys.exit(0 if test_mqtt_connection() else 1)
