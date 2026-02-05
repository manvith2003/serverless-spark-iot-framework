"""
OpenFaaS Client for Spark Driver
Calls the serverless RL function for scaling decisions
"""

import requests
import json
import time
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class ScalingDecision:
    """Result from OpenFaaS RL scaling function"""
    action: str  # 'scale_up', 'scale_down', 'maintain'
    target_executors: int
    executors_to_kill: List[int]
    executors_to_add: int
    reason: str
    confidence: float
    rl_suggestion: Optional[dict] = None


class OpenFaaSClient:
    """Client for calling OpenFaaS RL scaling function"""
    
    def __init__(self, gateway_url: str = "http://127.0.0.1:8080",
                 function_name: str = "rl-scaling"):
        self.gateway_url = gateway_url.rstrip('/')
        self.function_name = function_name
        self.function_url = f"{self.gateway_url}/function/{function_name}"
        self.timeout = 10  # seconds
        self.retry_count = 3
        self.retry_delay = 1.0
        
    def is_available(self) -> bool:
        """Check if OpenFaaS gateway is available"""
        try:
            response = requests.get(f"{self.gateway_url}/healthz", timeout=5)
            return response.status_code == 200
        except Exception:
            return False
    
    def get_scaling_decision(self, metrics: Dict) -> ScalingDecision:
        """
        Call OpenFaaS function to get scaling decision
        
        Args:
            metrics: Dictionary with current system metrics
                - workload_rate: Current message rate (msg/s)
                - cpu_util: CPU utilization (0-100)
                - mem_util: Memory utilization (0-100)
                - current_executors: Number of current executors
                - latency_ms: Current processing latency
                - alpha, beta, gamma: Scenario weights
                - edge_burst_signal: Edge burst prediction (0-1)
                - edge_predicted_workload: Predicted future workload
        
        Returns:
            ScalingDecision with action to take
        """
        
        for attempt in range(self.retry_count):
            try:
                response = requests.post(
                    self.function_url,
                    json=metrics,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return ScalingDecision(
                        action=data.get('action', 'maintain'),
                        target_executors=data.get('target_executors', 2),
                        executors_to_kill=data.get('executors_to_kill', []),
                        executors_to_add=data.get('executors_to_add', 0),
                        reason=data.get('reason', ''),
                        confidence=data.get('confidence', 0.0),
                        rl_suggestion=data.get('rl_suggestion')
                    )
                else:
                    print(f"⚠️  OpenFaaS returned status {response.status_code}")
                    
            except requests.exceptions.Timeout:
                print(f"⏱️  Timeout calling OpenFaaS (attempt {attempt + 1})")
            except requests.exceptions.ConnectionError:
                print(f"🔌 Connection error to OpenFaaS (attempt {attempt + 1})")
            except Exception as e:
                print(f"❌ Error calling OpenFaaS: {e}")
            
            if attempt < self.retry_count - 1:
                time.sleep(self.retry_delay)
        
        # Return default decision on failure
        return ScalingDecision(
            action='maintain',
            target_executors=metrics.get('current_executors', 2),
            executors_to_kill=[],
            executors_to_add=0,
            reason='OpenFaaS unavailable, maintaining current state',
            confidence=0.0
        )


class SparkExecutorManager:
    """Manages Spark executor scaling based on OpenFaaS decisions"""
    
    def __init__(self, spark_context=None, openfaas_gateway: str = "http://127.0.0.1:8080"):
        self.spark_context = spark_context
        self.openfaas_client = OpenFaaSClient(gateway_url=openfaas_gateway)
        
        # Current state
        self.current_executors = 2
        self.executor_ids = []
        
        # Monitoring metrics
        self.metrics_history = []
        self.decision_history = []
        
    def collect_metrics(self) -> Dict:
        """Collect current metrics from Spark and system"""
        
        # In production, these would come from Spark's metrics system
        # and monitoring tools like Prometheus
        metrics = {
            'workload_rate': 0,
            'cpu_util': 50,
            'mem_util': 50,
            'current_executors': self.current_executors,
            'latency_ms': 500,
            'cost_rate': self.current_executors * 2.0,  # $2 per executor per hour
            'alpha': 0.33,
            'beta': 0.33,
            'gamma': 0.33,
            'shuffle_size': 0,
            'data_temperature': 0.5,
            'edge_predicted_workload': 0,
            'edge_burst_signal': 0.0
        }
        
        if self.spark_context:
            try:
                # Get actual Spark metrics
                status = self.spark_context.statusTracker()
                active_jobs = status.getActiveJobIds()
                metrics['active_jobs'] = len(active_jobs)
                
                # Get executor info
                executor_ids = self.spark_context._jsc.sc().getExecutorIds()
                self.executor_ids = list(executor_ids)
                self.current_executors = len(self.executor_ids)
                metrics['current_executors'] = self.current_executors
            except Exception as e:
                print(f"⚠️  Could not get Spark metrics: {e}")
        
        return metrics
    
    def apply_scaling_decision(self, decision: ScalingDecision) -> bool:
        """Apply the scaling decision to Spark cluster"""
        
        print(f"\n🎯 Applying Scaling Decision: {decision.action}")
        print(f"   Target: {decision.target_executors} executors")
        print(f"   Reason: {decision.reason}")
        print(f"   Confidence: {decision.confidence:.0%}")
        
        if decision.action == 'maintain':
            print("   ✅ No changes needed")
            return True
            
        if decision.action == 'scale_down':
            return self._scale_down(decision)
            
        if decision.action == 'scale_up':
            return self._scale_up(decision)
        
        return False
    
    def _scale_down(self, decision: ScalingDecision) -> bool:
        """Scale down by killing executors"""
        
        if not decision.executors_to_kill:
            print("   ⚠️  No executors specified to kill")
            return False
        
        killed = 0
        for executor_idx in decision.executors_to_kill:
            if executor_idx < len(self.executor_ids):
                executor_id = self.executor_ids[executor_idx]
                if self._kill_executor(executor_id):
                    killed += 1
        
        self.current_executors = decision.target_executors
        print(f"   🔪 Killed {killed} executors")
        print(f"   📊 Current executors: {self.current_executors}")
        
        return killed > 0 or decision.target_executors == 0
    
    def _kill_executor(self, executor_id: str) -> bool:
        """Kill a specific executor"""
        
        if self.spark_context:
            try:
                # Spark Dynamic Allocation API to kill executor
                # In production with YARN/K8s
                self.spark_context._jsc.sc().killExecutor(executor_id)
                print(f"   ✂️  Killed executor: {executor_id}")
                return True
            except Exception as e:
                print(f"   ❌ Failed to kill executor {executor_id}: {e}")
                return False
        else:
            # Simulation mode
            print(f"   ✂️  [SIMULATED] Killed executor: {executor_id}")
            return True
    
    def _scale_up(self, decision: ScalingDecision) -> bool:
        """Scale up by requesting more executors"""
        
        if decision.executors_to_add <= 0:
            print("   ⚠️  No executors to add")
            return False
        
        if self.spark_context:
            try:
                # Request executors through Dynamic Allocation
                # This works with YARN and Kubernetes
                self.spark_context._jsc.sc().requestExecutors(decision.executors_to_add)
                print(f"   ➕ Requested {decision.executors_to_add} new executors")
                self.current_executors = decision.target_executors
                return True
            except Exception as e:
                print(f"   ❌ Failed to request executors: {e}")
                return False
        else:
            # Simulation mode
            print(f"   ➕ [SIMULATED] Requested {decision.executors_to_add} new executors")
            self.current_executors = decision.target_executors
            return True
    
    def run_scaling_loop(self, interval_seconds: int = 30):
        """Run continuous scaling loop"""
        
        print("\n🔄 Starting Serverless Scaling Loop")
        print(f"   Interval: {interval_seconds} seconds")
        print(f"   OpenFaaS Gateway: {self.openfaas_client.gateway_url}")
        print("-" * 60)
        
        try:
            while True:
                # Collect current metrics
                metrics = self.collect_metrics()
                self.metrics_history.append(metrics)
                
                print(f"\n📊 Current Metrics:")
                print(f"   Workload: {metrics['workload_rate']:.1f} msg/s")
                print(f"   CPU: {metrics['cpu_util']:.0f}%")
                print(f"   Executors: {self.current_executors}")
                
                # Get scaling decision from OpenFaaS
                print(f"\n📞 Calling OpenFaaS RL Function...")
                decision = self.openfaas_client.get_scaling_decision(metrics)
                self.decision_history.append(decision)
                
                # Apply the decision
                self.apply_scaling_decision(decision)
                
                print("-" * 60)
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n🛑 Scaling loop stopped")


def simulate_workflow():
    """Simulate the complete workflow"""
    
    print("=" * 60)
    print("🚀 Serverless Spark Scaling Workflow Demo")
    print("=" * 60)
    print("""
    Workflow:
    1. IoT traffic low
    2. Spark Driver sees low load
    3. Driver calls OpenFaaS (RL)
    4. RL says: scale_down = ALL
    5. Driver calls: killExecutor(...)
    6. YARN removes Spark containers
    7. Spark has zero or minimal executors
    """)
    
    # Initialize manager
    manager = SparkExecutorManager()
    manager.current_executors = 4
    manager.executor_ids = ['exec-001', 'exec-002', 'exec-003', 'exec-004']
    
    # Simulate different scenarios
    scenarios = [
        {
            'name': 'Normal Traffic',
            'metrics': {
                'workload_rate': 100.0,
                'cpu_util': 50.0,
                'mem_util': 45.0,
                'current_executors': 4,
                'latency_ms': 300,
            }
        },
        {
            'name': 'LOW TRAFFIC (Scale to Zero)',
            'metrics': {
                'workload_rate': 5.0,
                'cpu_util': 10.0,
                'mem_util': 20.0,
                'current_executors': 4,
                'latency_ms': 100,
            }
        },
        {
            'name': 'HIGH TRAFFIC (Scale Up)',
            'metrics': {
                'workload_rate': 400.0,
                'cpu_util': 90.0,
                'mem_util': 75.0,
                'current_executors': 2,
                'latency_ms': 2000,
            }
        },
        {
            'name': 'EDGE BURST PREDICTION',
            'metrics': {
                'workload_rate': 100.0,
                'cpu_util': 50.0,
                'mem_util': 45.0,
                'current_executors': 4,
                'latency_ms': 300,
                'edge_burst_signal': 1.0,
                'edge_predicted_workload': 500.0,
            }
        }
    ]
    
    # Import the handler for local testing
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/rl-scaling-function')
    from handler import handle
    
    for scenario in scenarios:
        print(f"\n{'='*60}")
        print(f"📡 Scenario: {scenario['name']}")
        print(f"{'='*60}")
        
        metrics = scenario['metrics']
        print(f"\n📊 Metrics: workload={metrics['workload_rate']} msg/s, CPU={metrics['cpu_util']}%")
        
        # Simulate calling OpenFaaS
        print("\n📞 Calling OpenFaaS RL Function...")
        result = handle(json.dumps(metrics))
        decision_data = json.loads(result)
        
        decision = ScalingDecision(
            action=decision_data['action'],
            target_executors=decision_data['target_executors'],
            executors_to_kill=decision_data.get('executors_to_kill', []),
            executors_to_add=decision_data.get('executors_to_add', 0),
            reason=decision_data['reason'],
            confidence=decision_data['confidence']
        )
        
        # Apply the decision
        manager.apply_scaling_decision(decision)
        
        # Update manager state for next iteration
        manager.current_executors = decision.target_executors
        manager.executor_ids = [f'exec-{i:03d}' for i in range(decision.target_executors)]
        
        print(f"\n✅ Current State: {manager.current_executors} executors")


if __name__ == "__main__":
    simulate_workflow()
