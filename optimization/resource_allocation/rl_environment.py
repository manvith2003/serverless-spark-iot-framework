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

import yaml
import os

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
    - Alpha (Cost weight)
    - Beta (Latency weight)
    - Gamma (Throughput weight)

    Action Space:
    - Number of executors (1-20)
    - Memory per executor (1-8 GB)
    - Storage tier (0=Redis, 1=NVMe, 2=S3, 3=Glacier)

    Reward:
    - Multi-objective: -*cost - *latency + *throughput
    """

    metadata = {'render_modes': ['human']}

    def __init__(self, 
                 iot_scenario: str = 'balanced',
                 config_path: str = "configs/iot_scenarios.yaml",
                 max_steps: int = 1000):

        super().__init__()

        self.max_steps = max_steps

        try:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            full_path = os.path.join(base_dir, config_path)

            with open(full_path, 'r') as f:
                self.scenarios = yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Could not load scenarios from {config_path}: {e}")
            logger.info("Using default balanced scenario")
            self.scenarios = {
                'balanced': {'alpha': 0.33, 'beta': 0.33, 'gamma': 0.33}
            }

        self.current_scenario_name = iot_scenario
        self.set_scenario(iot_scenario)

        self.observation_space = spaces.Box(
            low=np.array([0] * 13, dtype=np.float32),
            high=np.array([1000, 1000, 100, 100, 10000, 100, 1, 1, 1, 10000, 1, 1000, 1], dtype=np.float32),
            dtype=np.float32
        )

        self.action_space = spaces.MultiDiscrete([20, 8, 4, 3])

        self.executor_cost_per_hour = 0.1
        self.memory_cost_per_gb_hour = 0.05
        self.storage_costs = {
            0: 0.10,
            1: 0.05,
            2: 0.02,
            3: 0.005
        }

        self.base_latency_ms = 100
        self.base_throughput = 100

        self.current_step = 0
        self.state = None

        self.next_workload_rate = 100.0
        self.edge_burst_signal = 0.0

        self.reset()

    def set_scenario(self, scenario_name: str):
        """Set the current IoT scenario and update weights"""
        if scenario_name not in self.scenarios:
            logger.warning(f"Scenario {scenario_name} not found, using 'balanced'")
            scenario_name = 'balanced'

        self.current_scenario_name = scenario_name
        weights = self.scenarios[scenario_name]
        self.alpha = weights['alpha']
        self.beta = weights['beta']
        self.gamma = weights['gamma']
        logger.info(f"Switched to scenario: {scenario_name} (={self.alpha}, ={self.beta}, ={self.gamma})")

    def reset(self, seed=None, options=None) -> Tuple[np.ndarray, Dict]:
        """Reset environment to initial state"""
        super().reset(seed=seed)

        if options and 'scenario' in options:
            self.set_scenario(options['scenario'])
        else:
            available_scenarios = list(self.scenarios.keys())
            random_scenario = np.random.choice(available_scenarios)
            self.set_scenario(random_scenario)

        self.workload_rate = np.random.uniform(10, 200)
        self.next_workload_rate = self.workload_rate
        self.data_volume = np.random.uniform(10, 500)

        self.shuffle_size = self.data_volume * np.random.uniform(0.1, 0.5)
        self.data_temperature = np.random.random()

        self.edge_burst_signal = 0.0

        self.state = np.array([
            self.workload_rate,
            self.data_volume,
            50.0, 50.0, 500.0, 5.0,
            self.alpha, self.beta, self.gamma,
            self.shuffle_size, self.data_temperature,
            self.next_workload_rate,
            self.edge_burst_signal
        ], dtype=np.float32)

        self.current_step = 0

        return self.state, {}

    def step(self, action: np.ndarray) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """
        Execute action using logical 'Cloud Lag'.
        The Agent sees 'next_workload' in the state.
        The Action taken NOW affects performance when 'next_workload' becomes 'current'.
        """
        self.workload_rate = self.next_workload_rate

        num_executors = int(action[0]) + 1
        memory_per_executor = int(action[1]) + 1
        storage_tier = int(action[2])
        compression_level = int(action[3])

        executor_cost = num_executors * self.executor_cost_per_hour
        memory_cost = num_executors * memory_per_executor * self.memory_cost_per_gb_hour

        compression_ratios = {0: 1.0, 1: 0.6, 2: 0.3}
        compressed_shuffle_size = self.shuffle_size * compression_ratios[compression_level]
        storage_cost = (compressed_shuffle_size / 1024.0) * self.storage_costs[storage_tier]
        total_cost = executor_cost + memory_cost + storage_cost

        resource_factor = num_executors * memory_per_executor

        compute_latency = self.base_latency_ms * (self.workload_rate / resource_factor)

        tier_latency_map = {0: 5, 1: 20, 2: 100, 3: 5000} 
        access_latency = tier_latency_map[storage_tier] * (compressed_shuffle_size / 100.0)

        comp_latency_map = {0: 0, 1: 10, 2: 50}
        decompression_latency = comp_latency_map[compression_level] * (self.shuffle_size / 100.0)

        total_latency = compute_latency + access_latency + decompression_latency
        total_latency = max(total_latency, 10)

        throughput = resource_factor * (1000.0 / total_latency)

        stability_penalty = 0
        if self.data_temperature > 0.7 and storage_tier > 1:
            stability_penalty = 0.5

        if np.random.random() < 0.2:
            burst_factor = np.random.uniform(2.0, 4.0)
            self.next_workload_rate = self.workload_rate * burst_factor
            if self.next_workload_rate > 900: self.next_workload_rate = 900
            self.edge_burst_signal = 1.0
        else:
            self.next_workload_rate = self.workload_rate * np.random.uniform(0.8, 1.2)
            self.next_workload_rate = max(10, min(900, self.next_workload_rate))
            self.edge_burst_signal = 0.0

        cpu_util = min(100, (self.workload_rate / resource_factor) * 100)
        mem_util = min(100, (self.data_volume / (num_executors * memory_per_executor)) * 100)

        self.state = np.array([
            self.workload_rate,
            self.data_volume,
            cpu_util,
            mem_util,
            total_latency,
            total_cost,
            self.alpha,
            self.beta,
            self.gamma,
            self.shuffle_size,
            self.data_temperature,
            self.next_workload_rate,
            self.edge_burst_signal
        ], dtype=np.float32)

        normalized_cost = total_cost / 10.0
        normalized_latency = total_latency / 1000.0
        normalized_throughput = throughput / 1000.0

        reward = (
            -self.alpha * normalized_cost -
            self.beta * normalized_latency +
            self.gamma * normalized_throughput -
            stability_penalty
        )

        self.current_step += 1
        done = self.current_step >= self.max_steps

        info = {
            'cost': total_cost,
            'latency': total_latency,
            'throughput': throughput,
            'tier': storage_tier,
            'burst_signal': self.edge_burst_signal,
            'predicted_load': self.next_workload_rate
        }

        self.data_volume = np.random.uniform(10, 500)
        self.shuffle_size = self.data_volume * np.random.uniform(0.1, 0.5)
        self.data_temperature = np.random.random()

        return self.state, reward, done, False, info

    def render(self, mode='human'):
        pass

if __name__ == "__main__":
    env = SparkResourceEnv()
    print(" Testing Edge-Aware Environment\n")

    obs, info = env.reset()
    print(f"State Dim: {len(obs)} (Expected 13)")
    print(f"Initial State: {list(obs)}")
    print(f"Initial Prediction: {obs[-2]}")

    print("\nRunning 5 Steps to verify Forecast/Arrival Flow:")
    for i in range(5):
        action = env.action_space.sample()
        obs, reward, done, truncated, info = env.step(action)

        print(f"\nStep {i+1}:")
        print(f"  Current Load (Arrived): {obs[0]:.1f}")
        print(f"  Predicted Next Load: {obs[-2]:.1f} (Burst Signal: {obs[-1]})")
        print(f"  Action: Exec={action[0]+1}")
        print(f"  Latency: {info['latency']:.1f}ms")

    print("\n Environment test complete!")
