"""
Deep Q-Network (DQN) Agent for Resource Allocation
Uses Stable-Baselines3 for training
"""

import os
import numpy as np
from stable_baselines3 import DQN
from stable_baselines3.common.callbacks import EvalCallback, CheckpointCallback
from stable_baselines3.common.vec_env import DummyVecEnv
from stable_baselines3.common.monitor import Monitor
import torch

from rl_environment import SparkResourceEnv


class ResourceAllocator:
    """
    RL-based resource allocator for Spark
    """
    
    def __init__(self, 
                 model_path: str = "data/models/dqn_resource_allocator",
                 tensorboard_log: str = "./data/logs/tensorboard"):
        
        self.model_path = model_path
        self.tensorboard_log = tensorboard_log
        self.model = None
        self.env = None
        
        # Create directories
        os.makedirs(model_path, exist_ok=True)
        os.makedirs(tensorboard_log, exist_ok=True)
    
    def create_env(self, alpha=0.4, beta=0.4, gamma=0.2):
        """Create and wrap environment"""
        env = SparkResourceEnv(alpha=alpha, beta=beta, gamma=gamma)
        env = Monitor(env)
        self.env = DummyVecEnv([lambda: env])
        return self.env
    
    def train(self, total_timesteps: int = 100000, save_freq: int = 10000):
        """Train DQN agent"""
        print(f"🎓 Training DQN Agent for {total_timesteps} timesteps\n")
        
        # Create environment if not exists
        if self.env is None:
            self.create_env()
        
        # Create DQN model
        self.model = DQN(
            "MlpPolicy",
            self.env,
            learning_rate=1e-3,
            buffer_size=50000,
            learning_starts=1000,
            batch_size=32,
            tau=0.005,
            gamma=0.99,
            train_freq=4,
            gradient_steps=1,
            target_update_interval=1000,
            exploration_fraction=0.1,
            exploration_initial_eps=1.0,
            exploration_final_eps=0.05,
            verbose=1,
            tensorboard_log=self.tensorboard_log,
            device='cpu'  # Use 'mps' for M2 Mac, 'cuda' for GPU
        )
        
        # Callbacks
        eval_callback = EvalCallback(
            self.env,
            best_model_save_path=f"{self.model_path}/best_model",
            log_path=f"{self.model_path}/eval_logs",
            eval_freq=5000,
            deterministic=True,
            render=False
        )
        
        checkpoint_callback = CheckpointCallback(
            save_freq=save_freq,
            save_path=f"{self.model_path}/checkpoints",
            name_prefix="dqn_resource"
        )
        
        # Train
        self.model.learn(
            total_timesteps=total_timesteps,
            callback=[eval_callback, checkpoint_callback],
            progress_bar=True
        )
        
        # Save final model
        self.model.save(f"{self.model_path}/final_model")
        print(f"\n✅ Training complete! Model saved to {self.model_path}")
    
    def load(self, model_name: str = "final_model"):
        """Load trained model"""
        model_file = f"{self.model_path}/{model_name}"
        if os.path.exists(f"{model_file}.zip"):
            self.model = DQN.load(model_file)
            print(f"✅ Model loaded from {model_file}")
        else:
            raise FileNotFoundError(f"Model not found: {model_file}")
    
    def predict(self, state: np.ndarray) -> tuple[np.ndarray, dict]:
        """
        Predict optimal resource allocation for given state
        
        Args:
            state: [workload_rate, data_volume, cpu_util, mem_util, latency, cost_rate]
        
        Returns:
            action: [num_executors, memory_per_executor, storage_tier]
            info: Additional information
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call load() or train() first.")
        
        action, _ = self.model.predict(state, deterministic=True)
        
        # Decode action
        num_executors = int(action[0]) + 1
        memory_per_executor = int(action[1]) + 1
        storage_tier = int(action[2])
        
        info = {
            'num_executors': num_executors,
            'memory_per_executor_gb': memory_per_executor,
            'storage_tier': storage_tier,
            'storage_tier_name': ['Redis', 'NVMe', 'S3', 'Glacier'][storage_tier]
        }
        
        return action, info
    
    def evaluate(self, num_episodes: int = 10):
        """Evaluate trained model"""
        if self.model is None:
            raise ValueError("Model not loaded")
        
        if self.env is None:
            self.create_env()
        
        print(f"📊 Evaluating model for {num_episodes} episodes\n")
        
        episode_rewards = []
        episode_costs = []
        episode_latencies = []
        
        for episode in range(num_episodes):
            obs = self.env.reset()
            done = False
            episode_reward = 0
            episode_cost = 0
            episode_latency = 0
            steps = 0
            
            while not done:
                action, _ = self.model.predict(obs, deterministic=True)
                obs, reward, done, info = self.env.step(action)
                
                episode_reward += reward[0]
                episode_cost += info[0]['cost']
                episode_latency += info[0]['latency']
                steps += 1
            
            avg_cost = episode_cost / steps
            avg_latency = episode_latency / steps
            
            episode_rewards.append(episode_reward)
            episode_costs.append(avg_cost)
            episode_latencies.append(avg_latency)
            
            print(f"Episode {episode+1}: Reward={episode_reward:.2f}, "
                  f"Avg Cost=${avg_cost:.2f}/hr, Avg Latency={avg_latency:.2f}ms")
        
        print(f"\n📈 Evaluation Results:")
        print(f"  Average Reward: {np.mean(episode_rewards):.2f} ± {np.std(episode_rewards):.2f}")
        print(f"  Average Cost: ${np.mean(episode_costs):.2f}/hr ± ${np.std(episode_costs):.2f}")
        print(f"  Average Latency: {np.mean(episode_latencies):.2f}ms ± {np.std(episode_latencies):.2f}ms")


# CLI for training
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Train RL Resource Allocator")
    parser.add_argument("--train", action="store_true", help="Train new model")
    parser.add_argument("--timesteps", type=int, default=50000, help="Training timesteps")
    parser.add_argument("--evaluate", action="store_true", help="Evaluate model")
    parser.add_argument("--load", type=str, default="final_model", help="Model to load")
    
    args = parser.parse_args()
    
    allocator = ResourceAllocator()
    
    if args.train:
        allocator.create_env()
        allocator.train(total_timesteps=args.timesteps)
    
    if args.evaluate:
        try:
            allocator.load(args.load)
            allocator.evaluate(num_episodes=10)
        except FileNotFoundError:
            print("❌ No trained model found. Train first with --train")
