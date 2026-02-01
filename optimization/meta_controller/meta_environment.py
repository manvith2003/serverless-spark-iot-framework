import gymnasium as gym
from gymnasium import spaces
import numpy as np
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../resource_allocation'))

from rl_environment import SparkResourceEnv
from ppo_agent import ResourceAllocator

class MetaSparkEnv(gym.Env):
    """
    Meta-Environment for Hierarchical RL.

    High-Level Agent (Meta-Controller):
    - Action: [alpha, beta, gamma] (Weights for reward function)
    - Observation: [avg_workload, std_workload, avg_latency] (Long-term context)
    - Reward: Global User Satisfaction (e.g. 1.0 - normalized_latency - normalized_cost)

    The step() function runs the Lower-Level PPO Agent for 'N' steps (or 1 episode)
    using the weights provided by the Meta-Controller.
    """

    def __init__(self, lower_level_model_path="data/models/ppo_resource_allocator/final_model", steps_per_meta_step=1000):
        super(MetaSparkEnv, self).__init__()

        self.action_space = spaces.Box(low=0.0, high=1.0, shape=(3,), dtype=np.float32)

        self.observation_space = spaces.Box(low=0, high=10000, shape=(3,), dtype=np.float32)

        self.steps_per_meta_step = steps_per_meta_step

        self.env = SparkResourceEnv()
        self.agent = ResourceAllocator()

        try:
            self.agent.load("final_model") 
            self.inner_model = self.agent.model
            print(f" MetaEnv: Loaded lower-level agent 'final_model'")
        except Exception as e:
            print(f" MetaEnv Error loading agent: {e}")
            self.inner_model = None

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)

        obs, _ = self.env.reset(seed=seed)

        meta_obs = np.array([
            obs[0],
            0.0,
            obs[4]
        ], dtype=np.float32)

        return meta_obs, {}

    def step(self, action):
        """
        Run one Meta-Step.
        1. Update Reward Weights (from Action)
        2. Run Inner Agent for N steps
        3. Calculate Global Reward
        """
        weights = np.abs(action)
        if np.sum(weights) > 0:
            weights = weights / np.sum(weights)
        else:
            weights = np.array([0.33, 0.33, 0.33])

        alpha, beta, gamma = weights[0], weights[1], weights[2]

        self.env.alpha = alpha
        self.env.beta = beta
        self.env.gamma = gamma

        total_latency = 0
        total_cost = 0
        workload_history = []

        obs = self.env.state

        for _ in range(self.steps_per_meta_step):

            obs[6] = alpha
            obs[7] = beta
            obs[8] = gamma

            if self.inner_model:
                inner_action, _ = self.inner_model.predict(obs, deterministic=True)
            else:
                inner_action = self.env.action_space.sample()

            obs, reward, terminated, truncated, info = self.env.step(inner_action)
            done = terminated or truncated

            info_dict = info if isinstance(info, dict) else info[0]
            total_latency += info_dict['latency']
            total_cost += info_dict['cost']
            workload_history.append(obs[0])

            if done:
                obs, _ = self.env.reset()
                self.env.alpha = alpha
                self.env.beta = beta
                self.env.gamma = gamma

        avg_latency = total_latency / self.steps_per_meta_step
        avg_cost = total_cost / self.steps_per_meta_step

        meta_reward = (1000.0 / (avg_latency + 1.0)) - (avg_cost * 0.1)

        avg_workload = np.mean(workload_history)
        std_workload = np.std(workload_history)

        next_obs = np.array([
            avg_workload,
            std_workload,
            avg_latency
        ], dtype=np.float32)

        done = False

        return next_obs, meta_reward, done, False, {
            'avg_latency': avg_latency,
            'avg_cost': avg_cost,
            'weights': weights
        }
