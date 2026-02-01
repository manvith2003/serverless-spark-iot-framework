import sys
import os
import numpy as np
from stable_baselines3 import TD3
from stable_baselines3.common.noise import NormalActionNoise
from stable_baselines3.common.evaluation import evaluate_policy

from meta_environment import MetaSparkEnv

def train_meta_controller():
    print(" Initializing Meta-Controller Training...")

    env = MetaSparkEnv(steps_per_meta_step=200)

    n_actions = env.action_space.shape[-1]
    action_noise = NormalActionNoise(mean=np.zeros(n_actions), sigma=0.1 * np.ones(n_actions))

    model = TD3(
        "MlpPolicy",
        env,
        action_noise=action_noise,
        verbose=1,
        learning_rate=1e-3,
        buffer_size=1000,
        batch_size=32,
        tau=0.005,
        gamma=0.99,
        train_freq=(1, "episode"),
        gradient_steps=1,
        tensorboard_log="./data/logs/meta_controller/"
    )

    print(" Starting Meta-Training (Learning optimal Reward Weights)...")
    model.learn(total_timesteps=10, log_interval=1)

    print(" Meta-Training Complete")

    os.makedirs("data/models/meta_controller", exist_ok=True)
    model.save("data/models/meta_controller/td3_meta_agent")

    print("\n Verifying Learned Policy:")
    obs, _ = env.reset()
    for _ in range(3):
        action, _ = model.predict(obs)
        weights = np.abs(action) / np.sum(np.abs(action))
        print(f"   Observation (Workload Stats): {obs}")
        print(f"   Predicted Optimal Weights: ={weights[0]:.2f}, ={weights[1]:.2f}, ={weights[2]:.2f}")
        obs, reward, done, _, _ = env.step(action)
        print(f"   Resulting Meta-Reward: {reward:.2f}\n")

if __name__ == "__main__":
    train_meta_controller()
