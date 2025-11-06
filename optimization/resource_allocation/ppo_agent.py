"""
Proximal Policy Optimization (PPO) Agent for Resource Allocation
Supports MultiDiscrete action spaces
"""

import os
import numpy as np
from stable_baselines3 import PPO
from stable_baselines3.common.callbacks import EvalCallback, CheckpointCallback
from stable_baselines3.common.vec_env import DummyVecEnv
from stable_baselines3.common.monitor import Monitor
import torch
from typing import Tuple, Dict

from rl_environment import SparkResourceEnv


class ResourceAllocator:
    """
    RL-based resource allocator for Spark using PPO
    """
    
    def __init__(self, 
                 model_path: str = "data/models/ppo_resource_allocator",
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
        """Train PPO agent"""
        print(f"🎓 Training PPO Agent for {total_timesteps} timesteps\n")
        
        # Create environment if not exists
        if self.env is None:
            self.create_env()
        
        # Create PPO model (supports MultiDiscrete!)
        self.model = PPO(
            "MlpPolicy",
            self.env,
            learning_rate=3e-4,
            n_steps=2048,
            batch_size=64,
            n_epochs=10,
            gamma=0.99,
            gae_lambda=0.95,
            clip_range=0.2,
            ent_coef=0.01,
            vf_coef=0.5,
            max_grad_norm=0.5,
            verbose=1,
            tensorboard_log=self.tensorboard_log,
            device='cpu'  # Change to 'mps' for M2 GPU
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
            name_prefix="ppo_resource"
        )
        
        # Train
        print("Starting training...")
        self.model.learn(
            total_timesteps=total_timesteps,
            callback=[eval_callback, checkpoint_callback],
            progress_bar=True
        )
        
        # Save final model
        final_path = f"{self.model_path}/final_model"
        self.model.save(final_path)
        print(f"\n✅ Training complete! Model saved to {final_path}")
    
    def load(self, model_name: str = "final_model"):
        """Load trained model"""
        model_file = f"{self.model_path}/{model_name}"
        if os.path.exists(f"{model_file}.zip"):
            self.model = PPO.load(model_file)
            print(f"✅ Model loaded from {model_file}")
        else:
            raise FileNotFoundError(f"Model not found: {model_file}.zip")
    
    def predict(self, state: np.ndarray) -> Tuple[np.ndarray, Dict]:
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
        
        storage_names = ['Redis (Hot)', 'NVMe (Warm)', 'S3 (Cold)', 'Glacier (Archive)']
        
        info = {
            'num_executors': num_executors,
            'memory_per_executor_gb': memory_per_executor,
            'storage_tier': storage_tier,
            'storage_tier_name': storage_names[storage_tier],
            'total_memory_gb': num_executors * memory_per_executor
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
        episode_throughputs = []
        
        for episode in range(num_episodes):
            obs = self.env.reset()
            done = False
            episode_reward = 0
            episode_cost = 0
            episode_latency = 0
            episode_throughput = 0
            steps = 0
            
            while not done:
                action, _ = self.model.predict(obs, deterministic=True)
                obs, reward, done, info = self.env.step(action)
                
                episode_reward += reward[0]
                episode_cost += info[0]['cost']
                episode_latency += info[0]['latency']
                episode_throughput += info[0]['throughput']
                steps += 1
            
            avg_cost = episode_cost / steps
            avg_latency = episode_latency / steps
            avg_throughput = episode_throughput / steps
            
            episode_rewards.append(episode_reward)
            episode_costs.append(avg_cost)
            episode_latencies.append(avg_latency)
            episode_throughputs.append(avg_throughput)
            
            print(f"Episode {episode+1}: "
                  f"Reward={episode_reward:.2f}, "
                  f"Cost=${avg_cost:.2f}/hr, "
                  f"Latency={avg_latency:.0f}ms, "
                  f"Throughput={avg_throughput:.0f}")
        
        print(f"\n📈 Evaluation Results (Mean ± Std):")
        print(f"  Reward:     {np.mean(episode_rewards):.2f} ± {np.std(episode_rewards):.2f}")
        print(f"  Cost:       ${np.mean(episode_costs):.2f}/hr ± ${np.std(episode_costs):.2f}")
        print(f"  Latency:    {np.mean(episode_latencies):.0f}ms ± {np.std(episode_latencies):.0f}")
        print(f"  Throughput: {np.mean(episode_throughputs):.0f} ± {np.std(episode_throughputs):.0f}")
        
        return {
            'rewards': episode_rewards,
            'costs': episode_costs,
            'latencies': episode_latencies,
            'throughputs': episode_throughputs
        }


# CLI for training
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Train PPO Resource Allocator")
    parser.add_argument("--train", action="store_true", help="Train new model")
    parser.add_argument("--timesteps", type=int, default=50000, help="Training timesteps")
    parser.add_argument("--evaluate", action="store_true", help="Evaluate model")
    parser.add_argument("--load", type=str, default="final_model", help="Model to load")
    parser.add_argument("--test", action="store_true", help="Quick test")
    
    args = parser.parse_args()
    
    allocator = ResourceAllocator()
    
    if args.test:
        # Quick test
        print("🧪 Quick Test Mode\n")
        allocator.create_env()
        env = SparkResourceEnv()
        
        obs, _ = env.reset()
        print(f"Initial state: {obs}\n")
        
        for i in range(3):
            action = env.action_space.sample()
            obs, reward, done, _, info = env.step(action)
            print(f"Step {i+1}: Reward={reward:.3f}, Cost=${info['cost']:.2f}/hr, "
                  f"Latency={info['latency']:.0f}ms")
        
        print("\n✅ Environment working!")
    
    if args.train:
        allocator.create_env()
        allocator.train(total_timesteps=args.timesteps)
    
    if args.evaluate:
        try:
            allocator.load(args.load)
            allocator.evaluate(num_episodes=10)
        except FileNotFoundError:
            print("❌ No trained model found. Train first with --train")
