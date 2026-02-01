import numpy as np
import sys
import os
import matplotlib.pyplot as plt
from stable_baselines3 import TD3

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from meta_controller.meta_environment import MetaSparkEnv

def run_ablation_study():
    print(" Starting Ablation Study: Meta-Controller vs Fixed Weights\n")

    env = MetaSparkEnv(steps_per_meta_step=500)

    print("1  Testing Benchmark: Fixed Weights (Balanced)")
    fixed_rewards = []
    obs, _ = env.reset()

    for i in range(5):
        action = np.array([0.33, 0.33, 0.33])
        obs, reward, done, _, info = env.step(action)
        fixed_rewards.append(reward)
        print(f"   Episode {i+1}: Global Reward = {reward:.4f}")

    avg_fixed = np.mean(fixed_rewards)
    print(f"    Average Fixed Reward: {avg_fixed:.4f}\n")

    print("2  Testing Meta-Controller (Dynamic Weights)")
    try:
        model = TD3.load("data/models/meta_controller/td3_meta_agent")
        meta_rewards = []
        obs, _ = env.reset()

        for i in range(5):
            action, _ = model.predict(obs)
            obs, reward, done, _, info = env.step(action)
            meta_rewards.append(reward)

            w = np.abs(action) / np.sum(np.abs(action))
            print(f"   Episode {i+1}: Global Reward = {reward:.4f} (Weights: ={w[0]:.2f}, ={w[1]:.2f}, ={w[2]:.2f})")

        avg_meta = np.mean(meta_rewards)
        print(f"    Average Meta Reward: {avg_meta:.4f}\n")

        improvement = ((avg_meta - avg_fixed) / abs(avg_fixed)) * 100
        print(f" Improvement: {improvement:.2f}%")

    except FileNotFoundError:
        print(" Meta-Controller model not found. Train it first!")

if __name__ == "__main__":
    run_ablation_study()
