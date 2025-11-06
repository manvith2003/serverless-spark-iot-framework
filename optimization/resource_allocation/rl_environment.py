"""
Reinforcement Learning Environment for Spark Resource Allocation
Simulates Spark cluster dynamics for training RL agent
"""

import numpy as np
import gymnasium as gym
from gymnasium import spaces
from typing import Tuple, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkResourceEnv(gym.Env):
    """
    Custom Gym environment for Spark resource allocation
    
    State Space:
    - Workload rate (messages/second)
    - Data volume (MB/batch)
    - Current CPU utilization (%)
    - Current memory utilization (%)
    - Average latency (ms)
    - Cost rate ($/hour)
    
    Action Space:
    - Number of executors (1-20)
    - Memory per executor (1-8 GB)
    - Storage tier (0=Redis, 1=NVMe, 2=S3, 3=Glacier)
    
    Reward:
    - Multi-objective: -α*cost - β*latency + γ*throughput
    """
    
    metadata = {'render_modes': ['human']}
    
    def __init__(self, 
                 alpha: float = 0.4,  # Cost weight
                 beta: float = 0.4,   # Latency weight
                 gamma: float = 0.2,  # Throughput weight
                 max_steps: int = 1000):
        
        super().__init__()
        
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.max_steps = max_steps
        
        # State space: [workload_rate, data_volume, cpu_util, mem_util, latency, cost_rate]
        self.observation_space = spaces.Box(
            low=np.array([0, 0, 0, 0, 0, 0], dtype=np.float32),
            high=np.array([1000, 1000, 100, 100, 10000, 100], dtype=np.float32),
            dtype=np.float32
        )
        
        # Action space: [num_executors, memory_per_executor, storage_tier]
        # Discrete for simplicity
        self.action_space = spaces.MultiDiscrete([20, 8, 4])
        
        # Cost models ($/hour)
        self.executor_cost_per_hour = 0.1
        self.memory_cost_per_gb_hour = 0.05
        self.storage_costs = {
            0: 0.10,  # Redis (in-memory)
            1: 0.05,  # NVMe SSD
            2: 0.01,  # S3 Standard
            3: 0.001  # S3 Glacier
        }
        
        # Performance models (simplified)
        self.base_latency_ms = 100
        self.base_throughput = 100
        
        self.current_step = 0
        self.state = None
        
        self.reset()
    
    def reset(self, seed=None, options=None) -> Tuple[np.ndarray, Dict]:
        """Reset environment to initial state"""
        super().reset(seed=seed)
        
        # Generate random workload
        self.workload_rate = np.random.uniform(10, 500)
        self.data_volume = np.random.uniform(10, 500)
        
        # Initial state
        self.state = np.array([
            self.workload_rate,
            self.data_volume,
            50.0,  # cpu_util
            50.0,  # mem_util
            500.0,  # latency
            5.0    # cost_rate
        ], dtype=np.float32)
        
        self.current_step = 0
        
        return self.state, {}
    
    def step(self, action: np.ndarray) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """
        Execute action and return next state, reward, done, info
        
        Args:
            action: [num_executors, memory_per_executor, storage_tier]
        """
        num_executors = int(action[0]) + 1  # 1-20
        memory_per_executor = int(action[1]) + 1  # 1-8 GB
        storage_tier = int(action[2])  # 0-3
        
        # Calculate cost
        executor_cost = num_executors * self.executor_cost_per_hour
        memory_cost = num_executors * memory_per_executor * self.memory_cost_per_gb_hour
        storage_cost = self.storage_costs[storage_tier]
        total_cost = executor_cost + memory_cost + storage_cost
        
        # Calculate latency (simplified model)
        # More resources = lower latency, but diminishing returns
        resource_factor = num_executors * memory_per_executor
        latency = self.base_latency_ms * (self.workload_rate / resource_factor)
        latency = max(latency, 10)  # Minimum latency
        
        # Storage tier affects latency
        storage_latency_multipliers = {0: 1.0, 1: 1.2, 2: 1.5, 3: 3.0}
        latency *= storage_latency_multipliers[storage_tier]
        
        # Calculate throughput
        throughput = resource_factor * (self.base_throughput / latency)
        
        # Update state
        cpu_util = min(100, (self.workload_rate / resource_factor) * 100)
        mem_util = min(100, (self.data_volume / (num_executors * memory_per_executor)) * 100)
        
        self.state = np.array([
            self.workload_rate,
            self.data_volume,
            cpu_util,
            mem_util,
            latency,
            total_cost
        ], dtype=np.float32)
        
        # Multi-objective reward
        # Normalize values for fair comparison
        normalized_cost = total_cost / 10.0  # Assume max cost ~10
        normalized_latency = latency / 1000.0  # Assume max latency ~1000ms
        normalized_throughput = throughput / 1000.0  # Assume max throughput ~1000
        
        reward = (
            -self.alpha * normalized_cost -
            self.beta * normalized_latency +
            self.gamma * normalized_throughput
        )
        
        # Check if done
        self.current_step += 1
        done = self.current_step >= self.max_steps
        
        # Info dict
        info = {
            'cost': total_cost,
            'latency': latency,
            'throughput': throughput,
            'num_executors': num_executors,
            'memory_per_executor': memory_per_executor,
            'storage_tier': storage_tier,
            'cpu_util': cpu_util,
            'mem_util': mem_util
        }
        
        # Randomly change workload (simulate real-world dynamics)
        if np.random.random() < 0.1:  # 10% chance
            self.workload_rate = np.random.uniform(10, 500)
            self.data_volume = np.random.uniform(10, 500)
        
        return self.state, reward, done, False, info
    
    def render(self, mode='human'):
        """Render current state"""
        if mode == 'human':
            print(f"\nStep: {self.current_step}")
            print(f"State: {self.state}")
            print(f"Workload: {self.workload_rate:.2f} msg/s, {self.data_volume:.2f} MB")


# Test environment
if __name__ == "__main__":
    env = SparkResourceEnv()
    
    print("🧪 Testing RL Environment\n")
    
    obs, info = env.reset()
    print(f"Initial State: {obs}")
    
    print("\nTaking 5 random actions:")
    for i in range(5):
        action = env.action_space.sample()
        obs, reward, done, truncated, info = env.step(action)
        
        print(f"\nStep {i+1}:")
        print(f"  Action: executors={action[0]+1}, memory={action[1]+1}GB, tier={action[2]}")
        print(f"  Reward: {reward:.3f}")
        print(f"  Cost: ${info['cost']:.2f}/hr")
        print(f"  Latency: {info['latency']:.2f}ms")
        print(f"  Throughput: {info['throughput']:.2f}")
    
    print("\n✅ Environment test complete!")
