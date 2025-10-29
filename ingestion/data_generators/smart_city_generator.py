"""
Smart City IoT Data Generator
Simulates traffic sensors, pollution monitors, and parking sensors
"""

import json
import random
import time
from datetime import datetime
from typing import Dict, List
import uuid


class SmartCityGenerator:
    """Generate realistic smart city IoT sensor data"""
    
    def __init__(self, num_sensors: int = 10, city_name: str = "TechCity"):
        self.num_sensors = num_sensors
        self.city_name = city_name
        self.sensor_ids = [f"SC-{uuid.uuid4().hex[:8]}" for _ in range(num_sensors)]
        
        # Sensor locations (latitude, longitude)
        self.locations = self._generate_locations()
        
    def _generate_locations(self) -> List[Dict[str, float]]:
        """Generate random sensor locations within a city"""
        base_lat, base_lon = 40.7128, -74.0060  # New York as example
        return [
            {
                "latitude": base_lat + random.uniform(-0.1, 0.1),
                "longitude": base_lon + random.uniform(-0.1, 0.1)
            }
            for _ in range(self.num_sensors)
        ]
    
    def generate_traffic_data(self, sensor_id: str, location: Dict) -> Dict:
        """Generate traffic sensor data"""
        
        # Simulate traffic patterns (higher during rush hours)
        hour = datetime.now().hour
        is_rush_hour = (7 <= hour <= 9) or (17 <= hour <= 19)
        
        base_count = 100 if is_rush_hour else 30
        vehicle_count = random.randint(
            int(base_count * 0.8), 
            int(base_count * 1.5)
        )
        
        # Calculate average speed (slower during rush hour)
        avg_speed = random.uniform(15, 30) if is_rush_hour else random.uniform(40, 65)
        
        return {
            "sensor_id": sensor_id,
            "sensor_type": "traffic",
            "city": self.city_name,
            "timestamp": datetime.utcnow().isoformat(),
            "location": location,
            "metrics": {
                "vehicle_count": vehicle_count,
                "average_speed_kmh": round(avg_speed, 2),
                "congestion_level": self._calculate_congestion(vehicle_count, avg_speed),
                "lane_occupancy_percent": random.uniform(20, 95) if is_rush_hour else random.uniform(5, 40)
            }
        }
    
    def generate_pollution_data(self, sensor_id: str, location: Dict) -> Dict:
        """Generate air quality sensor data"""
        
        # Simulate pollution levels (higher during day and rush hours)
        hour = datetime.now().hour
        is_day = 6 <= hour <= 20
        
        base_aqi = 80 if is_day else 50
        
        return {
            "sensor_id": sensor_id,
            "sensor_type": "pollution",
            "city": self.city_name,
            "timestamp": datetime.utcnow().isoformat(),
            "location": location,
            "metrics": {
                "aqi": random.randint(base_aqi - 20, base_aqi + 40),
                "pm25": round(random.uniform(10, 150), 2),
                "pm10": round(random.uniform(15, 200), 2),
                "co2_ppm": random.randint(350, 450),
                "temperature_celsius": round(random.uniform(15, 35), 1),
                "humidity_percent": round(random.uniform(30, 80), 1)
            }
        }
    
    def generate_parking_data(self, sensor_id: str, location: Dict) -> Dict:
        """Generate parking sensor data"""
        
        total_spots = random.randint(50, 200)
        occupied = random.randint(0, total_spots)
        
        return {
            "sensor_id": sensor_id,
            "sensor_type": "parking",
            "city": self.city_name,
            "timestamp": datetime.utcnow().isoformat(),
            "location": location,
            "metrics": {
                "total_spots": total_spots,
                "occupied_spots": occupied,
                "available_spots": total_spots - occupied,
                "occupancy_rate": round((occupied / total_spots) * 100, 2)
            }
        }
    
    def _calculate_congestion(self, vehicle_count: int, avg_speed: float) -> str:
        """Calculate congestion level based on traffic metrics"""
        if vehicle_count > 80 and avg_speed < 25:
            return "HIGH"
        elif vehicle_count > 50 and avg_speed < 40:
            return "MEDIUM"
        else:
            return "LOW"
    
    def generate_batch(self, batch_size: int = 10) -> List[Dict]:
        """Generate a batch of mixed sensor data"""
        batch = []
        sensor_types = ["traffic", "pollution", "parking"]
        
        for _ in range(batch_size):
            sensor_idx = random.randint(0, self.num_sensors - 1)
            sensor_id = self.sensor_ids[sensor_idx]
            location = self.locations[sensor_idx]
            sensor_type = random.choice(sensor_types)
            
            if sensor_type == "traffic":
                data = self.generate_traffic_data(sensor_id, location)
            elif sensor_type == "pollution":
                data = self.generate_pollution_data(sensor_id, location)
            else:
                data = self.generate_parking_data(sensor_id, location)
            
            batch.append(data)
        
        return batch
    
    def stream_continuous(self, interval_seconds: float = 1.0, callback=None):
        """Continuously generate and stream data"""
        print(f"🚦 Starting Smart City data stream for {self.city_name}...")
        print(f"   Sensors: {self.num_sensors}")
        print(f"   Interval: {interval_seconds}s")
        
        try:
            while True:
                data = self.generate_batch(batch_size=5)
                
                if callback:
                    for record in data:
                        callback(record)
                else:
                    for record in data:
                        print(json.dumps(record, indent=2))
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n🛑 Smart City stream stopped")


# Test the generator
if __name__ == "__main__":
    generator = SmartCityGenerator(num_sensors=5, city_name="TechCity")
    
    print("Generating sample data...\n")
    batch = generator.generate_batch(batch_size=3)
    
    for record in batch:
        print(json.dumps(record, indent=2))
        print("-" * 60)
    
    # Uncomment to test continuous streaming
    # generator.stream_continuous(interval_seconds=2.0)
