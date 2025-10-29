"""
Industrial IoT Data Generator
Simulates manufacturing equipment sensors for predictive maintenance
"""

import json
import random
import time
from datetime import datetime
from typing import Dict, List
import uuid
import math


class IndustrialGenerator:
    """Generate realistic industrial equipment sensor data"""
    
    def __init__(self, num_machines: int = 10, factory_name: str = "Factory_01"):
        self.num_machines = num_machines
        self.factory_name = factory_name
        self.machine_ids = [f"MCH-{uuid.uuid4().hex[:8]}" for _ in range(num_machines)]
        
        # Machine states and wear levels
        self.machine_states = {}
        self._initialize_machines()
    
    def _initialize_machines(self):
        """Initialize machine states"""
        machine_types = ["CNC_Mill", "Lathe", "Press", "Welder", "Conveyor"]
        
        for machine_id in self.machine_ids:
            self.machine_states[machine_id] = {
                "type": random.choice(machine_types),
                "age_days": random.randint(30, 3650),
                "wear_level": random.uniform(0.1, 0.8),
                "last_maintenance": datetime.utcnow().isoformat(),
                "operational": True
            }
    
    def generate_sensor_data(self, machine_id: str) -> Dict:
        """Generate equipment sensor readings"""
        
        state = self.machine_states[machine_id]
        wear = state["wear_level"]
        
        # Gradually increase wear over time
        state["wear_level"] = min(1.0, wear + random.uniform(0, 0.001))
        
        # Higher wear = more vibration, heat, and power consumption
        base_vibration = 0.5 + (wear * 1.5)
        base_temperature = 60 + (wear * 30)
        base_power = 5 + (wear * 3)
        
        # Detect anomalies (simulate bearing failure, overheating, etc.)
        is_anomaly = random.random() < (wear * 0.1)  # Higher wear = more likely anomaly
        
        if is_anomaly:
            vibration = base_vibration * random.uniform(2, 4)
            temperature = base_temperature + random.uniform(20, 40)
            alert_level = "CRITICAL" if wear > 0.7 else "WARNING"
        else:
            vibration = base_vibration + random.uniform(-0.2, 0.2)
            temperature = base_temperature + random.uniform(-5, 5)
            alert_level = "NORMAL"
        
        # Operational status
        operational = state["operational"] and (wear < 0.95)
        
        return {
            "machine_id": machine_id,
            "machine_type": state["type"],
            "factory": self.factory_name,
            "timestamp": datetime.utcnow().isoformat(),
            "sensors": {
                "vibration_mms": round(vibration, 3),
                "temperature_celsius": round(temperature, 1),
                "pressure_bar": round(random.uniform(5, 15) + wear * 2, 2),
                "power_consumption_kw": round(base_power + random.uniform(-1, 1), 2),
                "rotation_speed_rpm": int(1200 + random.randint(-100, 100) - wear * 200),
                "acoustic_db": round(70 + wear * 20 + random.uniform(-5, 5), 1)
            },
            "status": {
                "operational": operational,
                "wear_level": round(wear, 3),
                "alert_level": alert_level,
                "estimated_remaining_life_hours": int((1 - wear) * 1000),
                "cycles_completed": random.randint(1000, 100000)
            }
        }
    
    def generate_production_data(self, machine_id: str) -> Dict:
        """Generate production metrics"""
        
        state = self.machine_states[machine_id]
        
        # Production efficiency decreases with wear
        efficiency = max(50, 100 - (state["wear_level"] * 30))
        
        return {
            "machine_id": machine_id,
            "machine_type": state["type"],
            "factory": self.factory_name,
            "timestamp": datetime.utcnow().isoformat(),
            "production": {
                "units_produced_hour": int(random.uniform(80, 120) * (efficiency / 100)),
                "efficiency_percent": round(efficiency, 1),
                "defect_rate_percent": round(state["wear_level"] * 5 + random.uniform(0, 2), 2),
                "downtime_minutes": int(state["wear_level"] * 30) if random.random() < 0.3 else 0,
                "quality_score": round(100 - state["wear_level"] * 20 + random.uniform(-5, 5), 1)
            }
        }
    
    def generate_batch(self, batch_size: int = 10) -> List[Dict]:
        """Generate a batch of machine data"""
        batch = []
        
        for _ in range(batch_size):
            machine_idx = random.randint(0, self.num_machines - 1)
            machine_id = self.machine_ids[machine_idx]
            
            # Mix sensor data and production data
            if random.random() < 0.7:
                data = self.generate_sensor_data(machine_id)
            else:
                data = self.generate_production_data(machine_id)
            
            batch.append(data)
        
        return batch
    
    def stream_continuous(self, interval_seconds: float = 1.0, callback=None):
        """Continuously generate and stream machine data"""
        print(f"🏭 Starting Industrial data stream for {self.factory_name}...")
        print(f"   Machines: {self.num_machines}")
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
            print("\n🛑 Industrial stream stopped")


# Test the generator
if __name__ == "__main__":
    generator = IndustrialGenerator(num_machines=3, factory_name="TechFactory_01")
    
    print("Generating sample industrial data...\n")
    batch = generator.generate_batch(batch_size=3)
    
    for record in batch:
        print(json.dumps(record, indent=2))
        print("-" * 60)
