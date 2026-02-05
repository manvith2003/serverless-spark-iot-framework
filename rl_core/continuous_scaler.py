"""
Continuous RL Scaling Controller
================================
This module implements a CONTINUOUS scaling loop that:
1. Collects metrics every N seconds
2. Gets RL recommendation
3. Applies scaling via YARN/K8s API
4. Observes result and provides feedback to RL

This is the BRIDGE between RL Agent and Resource Manager.
"""

import time
import threading
import logging
from typing import Dict, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ResourceManagerType(Enum):
    """Supported resource managers"""
    YARN = "yarn"
    KUBERNETES = "kubernetes"
    LOCAL = "local"  # For testing


@dataclass
class ScalingDecision:
    """Represents an RL scaling decision"""
    target_executors: int
    memory_per_executor: int
    action: str  # scale_up, scale_down, maintain
    reason: str
    confidence: float


class ResourceManagerClient:
    """
    Abstract interface to Resource Manager (YARN/K8s).
    RL Agent sends decisions HERE, and this class talks to the cluster.
    """
    
    def __init__(self, rm_type: ResourceManagerType = ResourceManagerType.LOCAL):
        self.rm_type = rm_type
        self.current_executors = 2
        self.min_executors = 0
        self.max_executors = 50
        
    def request_executors(self, target: int) -> Dict:
        """
        REQUEST executors from Resource Manager.
        
        This is where RL → Resource Manager communication happens!
        """
        target = max(self.min_executors, min(self.max_executors, target))
        
        if self.rm_type == ResourceManagerType.YARN:
            return self._request_yarn(target)
        elif self.rm_type == ResourceManagerType.KUBERNETES:
            return self._request_k8s(target)
        else:
            return self._request_local(target)
    
    def _request_yarn(self, target: int) -> Dict:
        """
        Call YARN ResourceManager API.
        
        In production, this would call:
        - yarn.client.api.requestContainers()
        - or REST API: POST http://rm-host:8088/ws/v1/cluster/apps/{appId}
        """
        logger.info(f"[YARN] Requesting {target} executors...")
        
        # Simulated YARN response
        # In production: response = requests.post(yarn_api_url, json={...})
        allocated = target  # YARN can give less if resources unavailable
        
        self.current_executors = allocated
        
        return {
            "requested": target,
            "allocated": allocated,
            "pending": 0,
            "status": "success"
        }
    
    def _request_k8s(self, target: int) -> Dict:
        """
        Call Kubernetes API to scale Spark executor pods.
        
        In production, this would call:
        - kubectl scale deployment spark-executors --replicas=N
        - or K8s API: PATCH /apis/apps/v1/namespaces/{ns}/deployments/{name}/scale
        """
        logger.info(f"[K8s] Scaling executor deployment to {target} replicas...")
        
        # Simulated K8s response
        # In production: 
        # from kubernetes import client
        # apps_v1 = client.AppsV1Api()
        # apps_v1.patch_namespaced_deployment_scale(name, namespace, {"spec": {"replicas": target}})
        
        self.current_executors = target
        
        return {
            "requested": target,
            "replicas": target,
            "status": "scaled"
        }
    
    def _request_local(self, target: int) -> Dict:
        """Local simulation for testing"""
        logger.info(f"[LOCAL] Simulating scale to {target} executors")
        self.current_executors = target
        return {"requested": target, "allocated": target, "status": "simulated"}
    
    def get_current_executors(self) -> int:
        """Get current executor count from Resource Manager"""
        return self.current_executors


class ContinuousRLScaler:
    """
    CONTINUOUS SCALING LOOP
    
    This is the core component that runs continuously and:
    1. Collects metrics
    2. Gets RL decision
    3. Tells Resource Manager to scale
    4. Observes result
    5. Repeats
    """
    
    def __init__(self, 
                 rl_scheduler,  # Your MultiObjectiveRLScheduler
                 resource_manager: ResourceManagerClient,
                 poll_interval_seconds: int = 10):
        
        self.rl_scheduler = rl_scheduler
        self.rm_client = resource_manager
        self.poll_interval = poll_interval_seconds
        
        self.is_running = False
        self._thread = None
        
        # Metrics collector (in production, this reads from Spark metrics)
        self.metrics_collector = None
        
        # History for learning
        self.decision_history = []
        
    def set_metrics_collector(self, collector: Callable[[], Dict]):
        """Set the function that collects current metrics"""
        self.metrics_collector = collector
        
    def _get_current_metrics(self) -> Dict:
        """Get current Spark/Kafka metrics"""
        if self.metrics_collector:
            return self.metrics_collector()
        
        # Default simulated metrics
        return {
            'workload_rate': np.random.uniform(50, 300),
            'cpu_util': np.random.uniform(30, 90),
            'latency_ms': np.random.uniform(50, 500),
            'kafka_lag': np.random.randint(0, 1000)
        }
    
    def _scaling_loop(self):
        """
        THE CONTINUOUS LOOP
        
        This runs every poll_interval seconds.
        """
        logger.info("=" * 60)
        logger.info("🔄 CONTINUOUS RL SCALING LOOP STARTED")
        logger.info("=" * 60)
        
        iteration = 0
        
        while self.is_running:
            iteration += 1
            
            # ─────────────────────────────────────────────────────────
            # STEP 1: COLLECT METRICS
            # ─────────────────────────────────────────────────────────
            metrics = self._get_current_metrics()
            
            logger.info(f"\n📊 Iteration {iteration}")
            logger.info(f"   Metrics: workload={metrics['workload_rate']:.0f} msg/s, "
                       f"cpu={metrics['cpu_util']:.0f}%, latency={metrics['latency_ms']:.0f}ms")
            
            # ─────────────────────────────────────────────────────────
            # STEP 2: GET RL DECISION
            # ─────────────────────────────────────────────────────────
            decision = self.rl_scheduler.get_scaling_decision(metrics)
            
            logger.info(f"   🤖 RL Decision: {decision['action'].upper()} → "
                       f"{decision['target_executors']} executors")
            logger.info(f"   Reason: {decision['reason']}")
            
            # ─────────────────────────────────────────────────────────
            # STEP 3: APPLY TO RESOURCE MANAGER
            # ─────────────────────────────────────────────────────────
            current = self.rm_client.get_current_executors()
            target = decision['target_executors']
            
            if target != current:
                logger.info(f"   🔧 Calling Resource Manager: {current} → {target} executors")
                result = self.rm_client.request_executors(target)
                logger.info(f"   ✅ RM Response: {result['status']}, allocated={result['allocated']}")
            else:
                logger.info(f"   ⏸️  No change needed (already at {current} executors)")
            
            # ─────────────────────────────────────────────────────────
            # STEP 4: RECORD FOR LEARNING
            # ─────────────────────────────────────────────────────────
            self.decision_history.append({
                'iteration': iteration,
                'metrics': metrics.copy(),
                'decision': decision.copy(),
                'executors_before': current,
                'executors_after': self.rm_client.get_current_executors()
            })
            
            # ─────────────────────────────────────────────────────────
            # STEP 5: WAIT FOR NEXT ITERATION
            # ─────────────────────────────────────────────────────────
            time.sleep(self.poll_interval)
        
        logger.info("\n🛑 SCALING LOOP STOPPED")
    
    def start(self):
        """Start the continuous scaling loop in background thread"""
        if self.is_running:
            logger.warning("Scaler already running")
            return
        
        self.is_running = True
        self._thread = threading.Thread(target=self._scaling_loop, daemon=True)
        self._thread.start()
        logger.info("Continuous RL Scaler started (background thread)")
    
    def stop(self):
        """Stop the scaling loop"""
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Continuous RL Scaler stopped")
    
    def run_once(self):
        """Run one scaling iteration (for testing)"""
        metrics = self._get_current_metrics()
        decision = self.rl_scheduler.get_scaling_decision(metrics)
        result = self.rm_client.request_executors(decision['target_executors'])
        return {"metrics": metrics, "decision": decision, "rm_result": result}


def demonstrate_continuous_scaling():
    """Demo the continuous scaling loop"""
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    
    from rl_core.multi_objective_scheduler import MultiObjectiveRLScheduler
    
    print("\n" + "=" * 70)
    print("🔄 CONTINUOUS RL SCALING DEMONSTRATION")
    print("=" * 70)
    print("""
    This shows the CONTINUOUS LOOP where:
    
    ┌─────────────────────────────────────────────────────────────────┐
    │  Every 5 seconds:                                               │
    │                                                                 │
    │  1. Collect metrics (workload, CPU, latency)                    │
    │  2. RL Agent decides: SCALE_UP / SCALE_DOWN / MAINTAIN          │
    │  3. Tell Resource Manager (YARN/K8s) to adjust executors        │
    │  4. Observe result                                              │
    │  5. Repeat                                                      │
    └─────────────────────────────────────────────────────────────────┘
    """)
    
    # Create components
    rl_scheduler = MultiObjectiveRLScheduler(mode='balanced')
    rm_client = ResourceManagerClient(rm_type=ResourceManagerType.LOCAL)
    
    # Create scaler with 5-second interval
    scaler = ContinuousRLScaler(
        rl_scheduler=rl_scheduler,
        resource_manager=rm_client,
        poll_interval_seconds=3  # Fast for demo
    )
    
    # Run for 5 iterations
    print("\n🚀 Running 5 scaling iterations...\n")
    
    for i in range(5):
        result = scaler.run_once()
        print(f"\nIteration {i+1}:")
        print(f"  Workload: {result['metrics']['workload_rate']:.0f} msg/s")
        print(f"  RL Decision: {result['decision']['action'].upper()}")
        print(f"  Target Executors: {result['decision']['target_executors']}")
        print(f"  RM Allocated: {result['rm_result']['allocated']}")
        time.sleep(1)
    
    print("\n" + "=" * 70)
    print("✅ CONTINUOUS SCALING DEMO COMPLETE")
    print("=" * 70)
    print("""
    In production, call:
    
        scaler.start()   # Runs in background thread
        # ... your Spark job runs ...
        scaler.stop()    # Stop when done
    """)
    
    return True


if __name__ == "__main__":
    demonstrate_continuous_scaling()
