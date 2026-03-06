"""
IoT Pattern Feature Extractor
==============================

Lightweight module that maintains a sliding window of IoT traffic metrics
and extracts 7 temporal features for Phase-1 RL state augmentation.

This is NOT an RL agent - it's pure feature engineering that gives
Phase-1 PPO the pattern awareness it needs to predict traffic spikes
and issue pre_warm actions before cold starts happen.

Features extracted (7 dimensions):
    1. traffic_velocity      - Rate of change of workload (msg/s^2)
    2. traffic_acceleration  - Change in velocity (2nd derivative)
    3. rolling_mean_5min     - Smoothed recent trend
    4. rolling_std_5min      - Recent volatility / burstiness
    5. burst_probability     - Likelihood of upcoming spike (0-1)
    6. hour_sin              - Cyclical time encoding (sin component)
    7. hour_cos              - Cyclical time encoding (cos component)

Integration:
    IoT Devices → MQTT → Kafka → [PatternFeatureExtractor taps here]
                                         ↓
                                  7 features appended to
                                  Phase-1 PPO state vector
                                  (13 → 20 dimensions)
"""

import numpy as np
from collections import deque
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import time
import math
import logging

logger = logging.getLogger(__name__)


@dataclass
class TrafficSample:
    timestamp: float
    workload_rate: float
    kafka_lag: int = 0
    cpu_util: float = 0.0
    latency_ms: float = 0.0


class PatternFeatureExtractor:
    """
    Sliding window feature extractor for IoT traffic patterns.

    Taps into the MQTT/Kafka metrics stream and computes temporal
    features that help Phase-1 RL anticipate traffic spikes.

    Usage:
        extractor = PatternFeatureExtractor(window_size=300)

        # Each time new metrics arrive from Kafka/MQTT:
        extractor.update(workload_rate=150.0, kafka_lag=200, ...)

        # Get 7 pattern features for Phase-1 state:
        features = extractor.extract_features()
        # → np.array([velocity, acceleration, mean, std,
        #             burst_prob, hour_sin, hour_cos])
    """

    def __init__(self, window_size: int = 300,
                 burst_velocity_percentile: float = 90.0,
                 min_samples_for_stats: int = 5):
        self.window_size = window_size
        self.burst_velocity_percentile = burst_velocity_percentile
        self.min_samples = min_samples_for_stats

        self.buffer: deque[TrafficSample] = deque(maxlen=window_size)

        self.velocity_history: deque[float] = deque(maxlen=window_size)

        self._velocity_threshold: Optional[float] = None
        self._recalc_counter = 0
        self._recalc_interval = 50

    def update(self, workload_rate: float,
               kafka_lag: int = 0,
               cpu_util: float = 0.0,
               latency_ms: float = 0.0,
               timestamp: Optional[float] = None):
        ts = timestamp if timestamp is not None else time.time()

        sample = TrafficSample(
            timestamp=ts,
            workload_rate=workload_rate,
            kafka_lag=kafka_lag,
            cpu_util=cpu_util,
            latency_ms=latency_ms
        )

        if len(self.buffer) > 0:
            prev = self.buffer[-1]
            dt = ts - prev.timestamp
            if dt > 0:
                velocity = (workload_rate - prev.workload_rate) / dt
                self.velocity_history.append(velocity)

        self.buffer.append(sample)

        self._recalc_counter += 1
        if self._recalc_counter >= self._recalc_interval:
            self._update_velocity_threshold()
            self._recalc_counter = 0

    def extract_features(self) -> np.ndarray:
        if len(self.buffer) < self.min_samples:
            return np.zeros(7, dtype=np.float32)

        velocity = self._compute_velocity()
        acceleration = self._compute_acceleration()
        rolling_mean = self._compute_rolling_mean()
        rolling_std = self._compute_rolling_std()
        burst_prob = self._compute_burst_probability()
        hour_sin, hour_cos = self._compute_time_encoding()

        features = np.array([
            velocity,
            acceleration,
            rolling_mean,
            rolling_std,
            burst_prob,
            hour_sin,
            hour_cos
        ], dtype=np.float32)

        return features

    def _compute_velocity(self) -> float:
        if len(self.velocity_history) == 0:
            return 0.0
        return self.velocity_history[-1]

    def _compute_acceleration(self) -> float:
        if len(self.velocity_history) < 2:
            return 0.0
        return self.velocity_history[-1] - self.velocity_history[-2]

    def _compute_rolling_mean(self) -> float:
        if len(self.buffer) == 0:
            return 0.0

        window = min(300, len(self.buffer))
        recent = [self.buffer[-i - 1].workload_rate for i in range(window)]
        return float(np.mean(recent))

    def _compute_rolling_std(self) -> float:
        if len(self.buffer) < 2:
            return 0.0

        window = min(300, len(self.buffer))
        recent = [self.buffer[-i - 1].workload_rate for i in range(window)]
        return float(np.std(recent))

    def _compute_burst_probability(self) -> float:
        if len(self.velocity_history) < self.min_samples:
            return 0.0

        current_velocity = self.velocity_history[-1]

        if self._velocity_threshold is None:
            self._update_velocity_threshold()

        if self._velocity_threshold is None or self._velocity_threshold <= 0:
            return 0.0

        ratio = current_velocity / self._velocity_threshold
        burst_prob = 1.0 / (1.0 + math.exp(-5.0 * (ratio - 1.0)))

        recent_velocities = list(self.velocity_history)[-10:]
        if len(recent_velocities) >= 3:
            increasing_count = sum(
                1 for i in range(1, len(recent_velocities))
                if recent_velocities[i] > recent_velocities[i - 1]
            )
            trend_factor = increasing_count / (len(recent_velocities) - 1)
            burst_prob = 0.6 * burst_prob + 0.4 * trend_factor

        return float(np.clip(burst_prob, 0.0, 1.0))

    def _compute_time_encoding(self) -> Tuple[float, float]:
        if len(self.buffer) == 0:
            return 0.0, 1.0

        ts = self.buffer[-1].timestamp
        import datetime
        dt = datetime.datetime.fromtimestamp(ts)
        hour_fraction = dt.hour + dt.minute / 60.0

        hour_sin = math.sin(2 * math.pi * hour_fraction / 24.0)
        hour_cos = math.cos(2 * math.pi * hour_fraction / 24.0)

        return hour_sin, hour_cos

    def _update_velocity_threshold(self):
        if len(self.velocity_history) < self.min_samples:
            return

        velocities = np.array(list(self.velocity_history))
        self._velocity_threshold = float(
            np.percentile(velocities, self.burst_velocity_percentile)
        )

    def get_diagnostics(self) -> dict:
        if len(self.buffer) == 0:
            return {"status": "no_data", "samples": 0}

        features = self.extract_features()
        return {
            "samples": len(self.buffer),
            "velocity_samples": len(self.velocity_history),
            "velocity_threshold": self._velocity_threshold,
            "current_features": {
                "traffic_velocity": features[0],
                "traffic_acceleration": features[1],
                "rolling_mean_5min": features[2],
                "rolling_std_5min": features[3],
                "burst_probability": features[4],
                "hour_sin": features[5],
                "hour_cos": features[6]
            },
            "buffer_window_seconds": (
                self.buffer[-1].timestamp - self.buffer[0].timestamp
                if len(self.buffer) > 1 else 0
            )
        }

    def reset(self):
        self.buffer.clear()
        self.velocity_history.clear()
        self._velocity_threshold = None
        self._recalc_counter = 0


def demonstrate_pattern_extractor():
    print("\n" + "=" * 60)
    print("IoT PATTERN FEATURE EXTRACTOR DEMO")
    print("=" * 60)
    print("""
    Simulates IoT traffic patterns and shows how the extractor
    detects trends, velocity changes, and burst probability.

    Data flow:
    IoT Sensors → MQTT → Kafka → [Pattern Extractor taps here]
                                         ↓
                                  7 features → Phase-1 RL
    """)

    extractor = PatternFeatureExtractor(window_size=100, min_samples_for_stats=3)

    base_time = time.time()
    print("\n--- Phase 1: Stable Low Traffic (Night) ---")
    for i in range(20):
        t = base_time + i
        workload = 20 + np.random.normal(0, 3)
        extractor.update(workload_rate=max(5, workload), timestamp=t)

    features = extractor.extract_features()
    print(f"  Velocity:       {features[0]:+.2f} msg/s^2")
    print(f"  Acceleration:   {features[1]:+.2f}")
    print(f"  Rolling Mean:   {features[2]:.1f} msg/s")
    print(f"  Rolling Std:    {features[3]:.1f}")
    print(f"  Burst Prob:     {features[4]:.2f}")

    print("\n--- Phase 2: Traffic Ramp-Up (Morning) ---")
    for i in range(20):
        t = base_time + 20 + i
        workload = 20 + (i * 15) + np.random.normal(0, 5)
        extractor.update(workload_rate=max(5, workload), timestamp=t)

    features = extractor.extract_features()
    print(f"  Velocity:       {features[0]:+.2f} msg/s^2 (RISING)")
    print(f"  Acceleration:   {features[1]:+.2f}")
    print(f"  Rolling Mean:   {features[2]:.1f} msg/s")
    print(f"  Rolling Std:    {features[3]:.1f} (HIGH - volatile)")
    print(f"  Burst Prob:     {features[4]:.2f} (ELEVATED)")

    print("\n--- Phase 3: Sudden Spike (Burst!) ---")
    for i in range(10):
        t = base_time + 40 + i
        workload = 300 + (i * 50) + np.random.normal(0, 10)
        extractor.update(workload_rate=max(5, workload), timestamp=t)

    features = extractor.extract_features()
    print(f"  Velocity:       {features[0]:+.2f} msg/s^2 (SPIKE!)")
    print(f"  Acceleration:   {features[1]:+.2f}")
    print(f"  Rolling Mean:   {features[2]:.1f} msg/s")
    print(f"  Rolling Std:    {features[3]:.1f} (VERY HIGH)")
    print(f"  Burst Prob:     {features[4]:.2f} (HIGH - pre_warm)")

    print("\n--- Phase 4: Traffic Settling (Afternoon) ---")
    for i in range(20):
        t = base_time + 50 + i
        workload = 200 + np.random.normal(0, 10)
        extractor.update(workload_rate=max(5, workload), timestamp=t)

    features = extractor.extract_features()
    print(f"  Velocity:       {features[0]:+.2f} msg/s^2 (stable)")
    print(f"  Acceleration:   {features[1]:+.2f}")
    print(f"  Rolling Mean:   {features[2]:.1f} msg/s")
    print(f"  Rolling Std:    {features[3]:.1f} (settling)")
    print(f"  Burst Prob:     {features[4]:.2f} (low)")

    print("\n" + "=" * 60)
    diag = extractor.get_diagnostics()
    print(f"Diagnostics: {diag['samples']} samples, "
          f"velocity_threshold={diag['velocity_threshold']:.2f}")
    print("=" * 60)

    return True


if __name__ == "__main__":
    demonstrate_pattern_extractor()
