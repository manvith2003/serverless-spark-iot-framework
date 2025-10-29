"""
MQTT Subscriber for IoT Data
Subscribes to MQTT topics and forwards data to Kafka
"""

import json
import time
import paho.mqtt.client as mqtt
from typing import Callable, Optional
import signal
import sys


class MQTTSubscriber:
    """Subscribe to IoT data from MQTT broker"""
    
    def __init__(self, broker_host: str = "localhost", broker_port: int = 1883,
                 topics: list = None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topics = topics or ["iot/#"]
        
        # Use callback API version 2
        self.client = mqtt.Client(
            client_id=f"iot_subscriber_{int(time.time())}",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        self.connected = False
        self.message_count = 0
        self.message_callback = None
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for when client connects to broker"""
        if reason_code == 0:
            self.connected = True
            print(f"✅ Connected to MQTT broker at {self.broker_host}:{self.broker_port}")
            
            # Subscribe to topics
            for topic in self.topics:
                result = client.subscribe(topic, qos=1)
                if result[0] == mqtt.MQTT_ERR_SUCCESS:
                    print(f"📥 Subscribed to topic: {topic}")
                else:
                    print(f"❌ Failed to subscribe to: {topic}")
        else:
            print(f"❌ Failed to connect, reason code: {reason_code}")
    
    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received"""
        try:
            self.message_count += 1
            
            # Parse JSON payload
            data = json.loads(msg.payload.decode())
            
            # Get sensor info
            sensor_type = data.get('sensor_type') or data.get('device_type') or data.get('machine_type', 'unknown')
            sensor_id = data.get('sensor_id') or data.get('device_id') or data.get('machine_id', 'unknown')
            
            print(f"📨 [{self.message_count}] Received: {sensor_type} from {sensor_id} on {msg.topic}")
            
            # Call custom callback if provided
            if self.message_callback:
                self.message_callback(msg.topic, data)
                
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse message: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        """Callback for when client disconnects"""
        self.connected = False
        if reason_code != 0:
            print(f"⚠️  Unexpected disconnect (reason: {reason_code})")
    
    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        print("\n🛑 Shutting down subscriber...")
        self.disconnect()
        sys.exit(0)
    
    def set_message_callback(self, callback: Callable):
        """Set custom callback for processing messages"""
        self.message_callback = callback
    
    def connect(self, retry_count: int = 3, retry_delay: float = 2.0):
        """Connect to MQTT broker with retries"""
        for attempt in range(retry_count):
            try:
                print(f"   Attempt {attempt + 1}/{retry_count}: Connecting to {self.broker_host}:{self.broker_port}...")
                self.client.connect(self.broker_host, self.broker_port, 60)
                self.client.loop_start()
                
                # Wait for connection
                timeout = 10
                start_time = time.time()
                while not self.connected and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                if self.connected:
                    return True
                else:
                    print(f"   Timeout waiting for connection")
                    self.client.loop_stop()
                    if attempt < retry_count - 1:
                        print(f"   Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    
            except Exception as e:
                print(f"   Connection error: {e}")
                self.client.loop_stop()
                if attempt < retry_count - 1:
                    print(f"   Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        
        raise ConnectionError(f"Could not connect to MQTT broker after {retry_count} attempts")
    
    def start_loop(self):
        """Start the subscriber loop (blocking)"""
        if not self.connected:
            raise ConnectionError("Not connected to broker")
        
        print("🎧 Listening for messages... (Press Ctrl+C to stop)")
        try:
            # Keep the main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping subscriber...")
    
    def disconnect(self):
        """Disconnect from broker"""
        self.client.loop_stop()
        self.client.disconnect()
        print(f"\n📊 Received {self.message_count} messages")
        print("🛑 Disconnected from MQTT broker")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MQTT IoT Data Subscriber")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--topics", nargs="+", default=["iot/#"], 
                       help="Topics to subscribe to")
    
    args = parser.parse_args()
    
    # Create subscriber
    subscriber = MQTTSubscriber(
        broker_host=args.broker,
        broker_port=args.port,
        topics=args.topics
    )
    
    try:
        subscriber.connect()
        subscriber.start_loop()
    except KeyboardInterrupt:
        print("\n🛑 Subscriber stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        subscriber.disconnect()
