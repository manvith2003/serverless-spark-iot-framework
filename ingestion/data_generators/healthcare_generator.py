"""
Healthcare IoT Data Generator
Simulates patient vital signs monitoring devices
"""

import json
import random
import time
from datetime import datetime
from typing import Dict, List
import uuid


class HealthcareGenerator:
    """Generate realistic healthcare IoT patient monitoring data"""
    
    def __init__(self, num_patients: int = 10, hospital_name: str = "General Hospital"):
        self.num_patients = num_patients
        self.hospital_name = hospital_name
        self.patient_ids = [f"PT-{uuid.uuid4().hex[:8]}" for _ in range(num_patients)]
        self.device_ids = [f"DEV-{uuid.uuid4().hex[:6]}" for _ in range(num_patients)]
        
        # Patient baseline vitals (for realistic variation)
        self.baselines = self._initialize_baselines()
    
    def _initialize_baselines(self) -> Dict:
        """Initialize baseline vitals for each patient"""
        baselines = {}
        for patient_id in self.patient_ids:
            baselines[patient_id] = {
                "heart_rate": random.randint(60, 80),
                "blood_pressure_sys": random.randint(110, 130),
                "blood_pressure_dia": random.randint(70, 85),
                "oxygen_saturation": random.uniform(95, 99),
                "temperature": random.uniform(36.5, 37.2),
                "respiratory_rate": random.randint(12, 18)
            }
        return baselines
    
    def generate_vital_signs(self, patient_id: str, device_id: str) -> Dict:
        """Generate realistic vital signs data"""
        
        baseline = self.baselines[patient_id]
        
        # Add random variation around baseline
        # Small chance of abnormal readings (for anomaly detection testing)
        is_abnormal = random.random() < 0.05
        
        if is_abnormal:
            # Generate concerning vitals
            heart_rate = random.randint(40, 50) if random.random() < 0.5 else random.randint(120, 160)
            spo2 = random.uniform(85, 92)
            temp = random.uniform(38.0, 39.5)
            alert_level = "HIGH"
        else:
            # Normal variation
            heart_rate = baseline["heart_rate"] + random.randint(-10, 10)
            spo2 = baseline["oxygen_saturation"] + random.uniform(-2, 1)
            temp = baseline["temperature"] + random.uniform(-0.5, 0.5)
            alert_level = "NORMAL"
        
        # Blood pressure
        sys_bp = baseline["blood_pressure_sys"] + random.randint(-15, 15)
        dia_bp = baseline["blood_pressure_dia"] + random.randint(-10, 10)
        
        # Respiratory rate
        resp_rate = baseline["respiratory_rate"] + random.randint(-3, 3)
        
        return {
            "patient_id": patient_id,
            "device_id": device_id,
            "device_type": "vital_signs_monitor",
            "hospital": self.hospital_name,
            "timestamp": datetime.utcnow().isoformat(),
            "vitals": {
                "heart_rate_bpm": max(40, min(180, heart_rate)),
                "blood_pressure": {
                    "systolic": max(80, min(200, sys_bp)),
                    "diastolic": max(50, min(120, dia_bp))
                },
                "oxygen_saturation_percent": round(max(85, min(100, spo2)), 1),
                "temperature_celsius": round(max(35.0, min(42.0, temp)), 1),
                "respiratory_rate_bpm": max(8, min(30, resp_rate))
            },
            "alert_level": alert_level,
            "battery_percent": random.randint(20, 100)
        }
    
    def generate_ecg_data(self, patient_id: str, device_id: str) -> Dict:
        """Generate simulated ECG waveform data"""
        
        # Simulate ECG wave points (simplified)
        ecg_points = [
            round(random.uniform(-0.5, 0.5) + 0.8 * (i % 10 == 5), 3)  # Spike every 10 points (R-wave)
            for i in range(100)
        ]
        
        return {
            "patient_id": patient_id,
            "device_id": device_id,
            "device_type": "ecg_monitor",
            "hospital": self.hospital_name,
            "timestamp": datetime.utcnow().isoformat(),
            "ecg_data": {
                "sample_rate_hz": 250,
                "lead": "Lead_II",
                "waveform": ecg_points[:20],  # Send subset to reduce size
                "heart_rate_bpm": self.baselines[patient_id]["heart_rate"] + random.randint(-5, 5),
                "qrs_duration_ms": random.randint(80, 120),
                "qt_interval_ms": random.randint(350, 450)
            }
        }
    
    def generate_batch(self, batch_size: int = 10) -> List[Dict]:
        """Generate a batch of patient data"""
        batch = []
        
        for _ in range(batch_size):
            patient_idx = random.randint(0, self.num_patients - 1)
            patient_id = self.patient_ids[patient_idx]
            device_id = self.device_ids[patient_idx]
            
            # Mostly vital signs, occasionally ECG
            if random.random() < 0.8:
                data = self.generate_vital_signs(patient_id, device_id)
            else:
                data = self.generate_ecg_data(patient_id, device_id)
            
            batch.append(data)
        
        return batch
    
    def stream_continuous(self, interval_seconds: float = 2.0, callback=None):
        """Continuously generate and stream patient data"""
        print(f"🏥 Starting Healthcare data stream for {self.hospital_name}...")
        print(f"   Patients: {self.num_patients}")
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
            print("\n🛑 Healthcare stream stopped")


# Test the generator
if __name__ == "__main__":
    generator = HealthcareGenerator(num_patients=3, hospital_name="St. Mary's Hospital")
    
    print("Generating sample healthcare data...\n")
    batch = generator.generate_batch(batch_size=3)
    
    for record in batch:
        print(json.dumps(record, indent=2))
        print("-" * 60)
