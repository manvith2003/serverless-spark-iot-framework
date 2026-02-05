"""
OpenFaaS RL Scaling Function
Serverless function that hosts the RL agent for Spark scaling decisions
"""

import json
import numpy as np
import os
import sys

# Try to import the RL model
try:
    from stable_baselines3 import PPO
    HAS_RL = True
except ImportError:
    HAS_RL = False


class RLScalingHandler:
    """Handler for RL-based scaling decisions"""
    
    def __init__(self):
        self.model = None
        self.model_path = os.environ.get('MODEL_PATH', '/home/app/function/models/rl_optimizer')
        
        # Scaling thresholds
        self.LOW_LOAD_THRESHOLD = 20.0
        self.HIGH_LOAD_THRESHOLD = 200.0
        self.MIN_EXECUTORS = 0
        self.MAX_EXECUTORS = 50
        
    def load_model(self):
        """Load the trained RL model"""
        if not HAS_RL:
            return False
            
        try:
            if os.path.exists(self.model_path + ".zip"):
                self.model = PPO.load(self.model_path)
                return True
        except Exception as e:
            print(f"Could not load model: {e}")
        return False
    
    def get_scaling_decision(self, metrics: dict) -> dict:
        """
        Get scaling decision based on current metrics
        
        Workflow:
        1. IoT traffic low → scale_down = ALL
        2. IoT traffic high → scale_up
        3. Normal traffic → maintain
        """
        
        workload_rate = metrics.get('workload_rate', 0)
        cpu_util = metrics.get('cpu_util', 50)
        mem_util = metrics.get('mem_util', 50)
        current_executors = metrics.get('current_executors', 2)
        latency_ms = metrics.get('latency_ms', 500)
        
        # Scenario weights (from context)
        alpha = metrics.get('alpha', 0.33)  # Cost weight
        beta = metrics.get('beta', 0.33)    # Latency weight
        gamma = metrics.get('gamma', 0.33)  # Throughput weight
        
        # Edge signals
        edge_burst_signal = metrics.get('edge_burst_signal', 0.0)
        edge_predicted_workload = metrics.get('edge_predicted_workload', workload_rate)
        
        # Decision logic
        decision = {
            'action': 'maintain',
            'target_executors': current_executors,
            'executors_to_kill': [],
            'executors_to_add': 0,
            'reason': '',
            'confidence': 0.0
        }
        
        # Case 1: Very low load - scale to zero (serverless idle)
        if workload_rate < self.LOW_LOAD_THRESHOLD and cpu_util < 20:
            decision['action'] = 'scale_down'
            decision['target_executors'] = self.MIN_EXECUTORS
            decision['executors_to_kill'] = list(range(current_executors))  # Kill all
            decision['reason'] = f'Very low load ({workload_rate:.1f} msg/s). Scaling to zero for cost savings.'
            decision['confidence'] = 0.95
            
        # Case 2: Low load - reduce executors
        elif workload_rate < self.LOW_LOAD_THRESHOLD * 2:
            target = max(1, current_executors // 2)
            decision['action'] = 'scale_down'
            decision['target_executors'] = target
            decision['executors_to_kill'] = list(range(target, current_executors))
            decision['reason'] = f'Low load ({workload_rate:.1f} msg/s). Reducing executors.'
            decision['confidence'] = 0.85
            
        # Case 3: Edge burst predicted - pre-scale up
        elif edge_burst_signal > 0.5:
            target = min(self.MAX_EXECUTORS, int(current_executors * 2.5))
            decision['action'] = 'scale_up'
            decision['target_executors'] = target
            decision['executors_to_add'] = target - current_executors
            decision['reason'] = f'Edge burst predicted! Pre-scaling to {target} executors.'
            decision['confidence'] = 0.90
            
        # Case 4: High load - scale up
        elif workload_rate > self.HIGH_LOAD_THRESHOLD or cpu_util > 80:
            # Calculate required executors based on workload
            required = int(workload_rate / 50)  # ~50 msg/s per executor
            target = min(self.MAX_EXECUTORS, max(required, current_executors + 2))
            decision['action'] = 'scale_up'
            decision['target_executors'] = target
            decision['executors_to_add'] = target - current_executors
            decision['reason'] = f'High load ({workload_rate:.1f} msg/s, CPU: {cpu_util:.0f}%). Scaling up.'
            decision['confidence'] = 0.88
            
        # Case 5: Latency-sensitive scenario (high beta weight)
        elif beta > 0.5 and latency_ms > 1000:
            target = min(self.MAX_EXECUTORS, current_executors + 3)
            decision['action'] = 'scale_up'
            decision['target_executors'] = target
            decision['executors_to_add'] = target - current_executors
            decision['reason'] = f'High latency ({latency_ms}ms) in latency-sensitive scenario. Scaling up.'
            decision['confidence'] = 0.85
            
        # Case 6: Cost-sensitive scenario (high alpha weight) and moderate load
        elif alpha > 0.5 and workload_rate < self.HIGH_LOAD_THRESHOLD * 0.7:
            target = max(1, current_executors - 1)
            if target < current_executors:
                decision['action'] = 'scale_down'
                decision['target_executors'] = target
                decision['executors_to_kill'] = [current_executors - 1]
                decision['reason'] = f'Cost optimization: reducing executors in cost-sensitive mode.'
                decision['confidence'] = 0.75
                
        # Case 7: Normal operation - maintain
        else:
            decision['action'] = 'maintain'
            decision['target_executors'] = current_executors
            decision['reason'] = f'Normal load ({workload_rate:.1f} msg/s). Maintaining current capacity.'
            decision['confidence'] = 0.80
        
        # Add RL model prediction if available
        if self.model is not None:
            try:
                state = self._build_state(metrics)
                action, _ = self.model.predict(state, deterministic=True)
                rl_executors = int(action[0] * 10) + 2  # Scale action to executor range
                decision['rl_suggestion'] = {
                    'executors': rl_executors,
                    'raw_action': action.tolist()
                }
            except Exception as e:
                decision['rl_error'] = str(e)
        
        return decision
    
    def _build_state(self, metrics: dict) -> np.ndarray:
        """Build state vector for RL model"""
        return np.array([
            metrics.get('workload_rate', 0) / 500.0,
            metrics.get('data_volume', 0) / 500.0,
            metrics.get('cpu_util', 50) / 100.0,
            metrics.get('mem_util', 50) / 100.0,
            metrics.get('latency_ms', 500) / 1000.0,
            metrics.get('cost_rate', 5) / 10.0,
            metrics.get('alpha', 0.33),
            metrics.get('beta', 0.33),
            metrics.get('gamma', 0.33),
            metrics.get('shuffle_size', 0) / 200.0,
            metrics.get('data_temperature', 0.5),
            metrics.get('edge_predicted_workload', 0) / 500.0,
            metrics.get('edge_burst_signal', 0.0)
        ], dtype=np.float32)


# Global handler instance
handler = RLScalingHandler()


def handle(req):
    """
    OpenFaaS function handler
    
    Expected request format:
    {
        "workload_rate": 150.0,
        "cpu_util": 65.0,
        "mem_util": 45.0,
        "current_executors": 4,
        "latency_ms": 500,
        "alpha": 0.33,
        "beta": 0.33,
        "gamma": 0.33,
        "edge_burst_signal": 0.0,
        "edge_predicted_workload": 150.0
    }
    
    Response format:
    {
        "action": "scale_up" | "scale_down" | "maintain",
        "target_executors": 6,
        "executors_to_kill": [],
        "executors_to_add": 2,
        "reason": "High load...",
        "confidence": 0.88
    }
    """
    
    try:
        # Parse request
        if isinstance(req, str):
            metrics = json.loads(req)
        else:
            metrics = req
            
        # Get scaling decision
        decision = handler.get_scaling_decision(metrics)
        
        return json.dumps(decision, indent=2)
        
    except Exception as e:
        error_response = {
            "error": str(e),
            "action": "maintain",
            "target_executors": 2,
            "reason": "Error in scaling function, maintaining default"
        }
        return json.dumps(error_response, indent=2)


# For local testing
if __name__ == "__main__":
    # Test scenarios
    test_cases = [
        # Low load scenario
        {
            "workload_rate": 10.0,
            "cpu_util": 15.0,
            "current_executors": 4,
            "description": "Very low load - should scale to zero"
        },
        # High load scenario  
        {
            "workload_rate": 300.0,
            "cpu_util": 85.0,
            "current_executors": 4,
            "description": "High load - should scale up"
        },
        # Edge burst scenario
        {
            "workload_rate": 100.0,
            "cpu_util": 50.0,
            "current_executors": 4,
            "edge_burst_signal": 1.0,
            "description": "Edge burst predicted - should pre-scale"
        },
        # Cost-sensitive scenario
        {
            "workload_rate": 80.0,
            "cpu_util": 40.0,
            "current_executors": 4,
            "alpha": 0.9,
            "beta": 0.05,
            "gamma": 0.05,
            "description": "Cost-sensitive, moderate load - should scale down"
        }
    ]
    
    print("🧪 Testing RL Scaling Function\n")
    print("=" * 60)
    
    for i, test in enumerate(test_cases, 1):
        desc = test.pop('description')
        print(f"\n📊 Test Case {i}: {desc}")
        print(f"   Input: {test}")
        
        result = handle(json.dumps(test))
        decision = json.loads(result)
        
        print(f"   ✅ Action: {decision['action']}")
        print(f"   📈 Target Executors: {decision['target_executors']}")
        print(f"   💬 Reason: {decision['reason']}")
        print(f"   🎯 Confidence: {decision['confidence']:.0%}")
        
        if decision.get('executors_to_kill'):
            print(f"   🔪 Kill Executors: {decision['executors_to_kill']}")
        if decision.get('executors_to_add'):
            print(f"   ➕ Add Executors: {decision['executors_to_add']}")
    
    print("\n" + "=" * 60)
    print("✅ All test cases completed!")
