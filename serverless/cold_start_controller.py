"""
Cold Start Mitigation Controller
=================================

Orchestrates the Pattern Feature Extractor and serverless executor manager
to proactively pre-warm Spark executors before IoT traffic spikes arrive.

Data Flow:
    IoT Metrics Stream
         |
    PatternFeatureExtractor (extract 7 features)
         |
    Phase-1 RL / OpenFaaS (get scaling decision)
         |
    If action == 'pre_warm':
         |
    Serverless Platform -> Spin up containers ahead of spike
         |
    Track outcome: cold start avoided? containers wasted?
"""

import time
import threading
import logging
import json
from dataclasses import dataclass, field
from typing import Dict, Optional, List
from collections import deque
import numpy as np

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rl_core.pattern_feature_extractor import PatternFeatureExtractor

logger = logging.getLogger(__name__)


@dataclass
class ColdStartMetrics:
    cold_starts_avoided: int = 0
    cold_starts_occurred: int = 0
    wasted_prewarms: int = 0
    total_prewarms: int = 0
    avg_cold_start_time_ms: float = 0.0
    cost_overhead_pct: float = 0.0
    prediction_accuracy: float = 0.0

    def update_accuracy(self):
        total = self.cold_starts_avoided + self.wasted_prewarms
        if total > 0:
            self.prediction_accuracy = self.cold_starts_avoided / total

    def summary(self) -> str:
        self.update_accuracy()
        return (
            f"Cold Starts Avoided: {self.cold_starts_avoided} | "
            f"Occurred: {self.cold_starts_occurred} | "
            f"Wasted Pre-warms: {self.wasted_prewarms} | "
            f"Accuracy: {self.prediction_accuracy:.1%} | "
            f"Cost Overhead: {self.cost_overhead_pct:.1f}%"
        )


class ColdStartMitigationController:
    """
    Orchestrates pattern detection and pre-warming for cold start mitigation.

    Pipeline:
        1. Collect IoT metrics (from Kafka/MQTT stream)
        2. Feed to PatternFeatureExtractor -> get 7 features
        3. Pass features to RL agent (or OpenFaaS function)
        4. If action == 'pre_warm': spin up containers early
        5. Track outcome and feed back to RL for learning
    """

    def __init__(self,
                 poll_interval_seconds: float = 30.0,
                 openfaas_gateway: str = "http://127.0.0.1:8080",
                 cold_start_time_ms: float = 15000.0):
        self.poll_interval = poll_interval_seconds
        self.cold_start_time_ms = cold_start_time_ms

        self.pattern_extractor = PatternFeatureExtractor(
            window_size=300,
            burst_velocity_percentile=90.0
        )

        self.metrics = ColdStartMetrics()

        self.current_executors = 0
        self.warm_executors = 0
        self.base_cost = 0.0
        self.prewarm_cost = 0.0

        self.decision_log: List[Dict] = []

        self.is_running = False
        self._thread = None

        self._openfaas_gateway = openfaas_gateway

    def process_metrics(self, iot_metrics: Dict) -> Dict:
        """
        Process incoming IoT metrics and decide whether to pre-warm.

        Returns the scaling decision dict.
        """
        workload_rate = iot_metrics.get('workload_rate', 0)
        cpu_util = iot_metrics.get('cpu_util', 0)
        latency_ms = iot_metrics.get('latency_ms', 0)
        current_executors = iot_metrics.get('current_executors', self.current_executors)

        self.pattern_extractor.update(
            workload_rate=workload_rate,
            cpu_util=cpu_util,
            latency_ms=latency_ms
        )

        features = self.pattern_extractor.extract_features()

        enriched_metrics = {
            **iot_metrics,
            'burst_probability': float(features[4]),
            'traffic_velocity': float(features[0]),
            'traffic_acceleration': float(features[1]),
            'rolling_mean': float(features[2]),
            'rolling_std': float(features[3]),
            'current_executors': current_executors
        }

        decision = self._get_scaling_decision(enriched_metrics)

        self._track_outcome(decision, workload_rate, current_executors)

        self.decision_log.append({
            'timestamp': time.time(),
            'workload': workload_rate,
            'burst_probability': float(features[4]),
            'action': decision['action'],
            'target_executors': decision['target_executors']
        })

        return decision

    def _get_scaling_decision(self, metrics: Dict) -> Dict:
        """Get scaling decision - tries OpenFaaS, falls back to local heuristic."""
        try:
            from serverless.openfaas.rl_scaling_function.handler import handle
            result = handle(json.dumps(metrics))
            return json.loads(result)
        except Exception:
            pass

        return self._local_heuristic(metrics)

    def _local_heuristic(self, metrics: Dict) -> Dict:
        """Local heuristic fallback when OpenFaaS is unavailable."""
        burst_prob = metrics.get('burst_probability', 0.0)
        velocity = metrics.get('traffic_velocity', 0.0)
        workload = metrics.get('workload_rate', 0)
        current = metrics.get('current_executors', 0)

        if burst_prob > 0.6 and velocity > 5.0:
            target = min(20, current + 4)
            return {
                'action': 'pre_warm',
                'target_executors': target,
                'executors_to_add': target - current,
                'pre_warm_count': target - current,
                'reason': f'Pattern: burst_prob={burst_prob:.2f}, velocity={velocity:.1f}',
                'confidence': 0.85
            }
        elif workload > 200 or metrics.get('cpu_util', 0) > 80:
            target = min(20, current + 2)
            return {
                'action': 'scale_up',
                'target_executors': target,
                'executors_to_add': target - current,
                'reason': f'High load: {workload:.0f} msg/s',
                'confidence': 0.80
            }
        elif workload < 20 and current > 1:
            return {
                'action': 'scale_down',
                'target_executors': max(0, current - 2),
                'executors_to_kill': list(range(max(0, current - 2), current)),
                'reason': f'Low load: {workload:.0f} msg/s',
                'confidence': 0.90
            }
        else:
            return {
                'action': 'maintain',
                'target_executors': current,
                'reason': 'Normal load',
                'confidence': 0.75
            }

    def _track_outcome(self, decision: Dict, workload: float, current_executors: int):
        """Track whether pre-warm decision was correct."""
        if decision['action'] == 'pre_warm':
            self.metrics.total_prewarms += 1
            self.warm_executors = decision.get('pre_warm_count', 0)
            self.prewarm_cost += self.warm_executors * 0.5

            if workload > 100 or self.pattern_extractor.extract_features()[4] > 0.6:
                self.metrics.cold_starts_avoided += 1
            else:
                self.metrics.wasted_prewarms += 1

        if (current_executors <= 1 and workload > 100 and
                decision['action'] != 'pre_warm'):
            self.metrics.cold_starts_occurred += 1

        if self.base_cost > 0:
            self.metrics.cost_overhead_pct = (self.prewarm_cost / self.base_cost) * 100

        self.base_cost += current_executors * 0.5

    def get_metrics(self) -> ColdStartMetrics:
        self.metrics.update_accuracy()
        return self.metrics

    def start_background(self, metrics_source=None):
        """Start background monitoring loop."""
        if self.is_running:
            return

        self.is_running = True
        self._thread = threading.Thread(
            target=self._background_loop,
            args=(metrics_source,),
            daemon=True
        )
        self._thread.start()

    def _background_loop(self, metrics_source):
        """Background loop: poll metrics and make decisions."""
        while self.is_running:
            if metrics_source:
                metrics = metrics_source()
                self.process_metrics(metrics)
            time.sleep(self.poll_interval)

    def stop(self):
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=5)


def demonstrate_cold_start_controller():
    """Demo the cold start controller with simulated IoT traffic."""
    print("\n" + "=" * 60)
    print("COLD START MITIGATION CONTROLLER DEMO")
    print("=" * 60)

    controller = ColdStartMitigationController(poll_interval_seconds=1.0)

    print("\n--- Simulating 24-hour IoT Traffic ---\n")

    np.random.seed(42)

    for hour in range(24):
        if 6 <= hour <= 8:
            base_workload = 50 + (hour - 6) * 100
        elif 9 <= hour <= 17:
            base_workload = 200 + np.random.normal(0, 30)
        elif 18 <= hour <= 20:
            base_workload = 200 - (hour - 18) * 70
        else:
            base_workload = 20 + np.random.normal(0, 5)

        for minute in range(0, 60, 10):
            workload = max(5, base_workload + np.random.normal(0, 15))

            if hour == 7 and minute == 30:
                workload = 500

            metrics = {
                'workload_rate': workload,
                'cpu_util': min(100, 20 + workload * 0.15),
                'latency_ms': max(50, workload * 1.5),
                'current_executors': controller.current_executors
            }

            decision = controller.process_metrics(metrics)

            controller.current_executors = decision['target_executors']

            action_str = decision['action'].upper()
            print(f"  {hour:02d}:{minute:02d} | "
                  f"Workload: {workload:6.0f} msg/s | "
                  f"Action: {action_str:>9s} | "
                  f"Executors: {decision['target_executors']:2d}")

    print("\n" + "=" * 60)
    print("COLD START MITIGATION RESULTS")
    print("=" * 60)
    results = controller.get_metrics()
    print(f"  {results.summary()}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    demonstrate_cold_start_controller()
