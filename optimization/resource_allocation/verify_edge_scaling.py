import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '__init__.py'))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from rl_environment import SparkResourceEnv
from ppo_agent import ResourceAllocator

def verify_proactive_scaling():
    print(" Verifying Edge-Cloud Proactive Scaling...")

    agent = ResourceAllocator()
    try:
        agent.load("final_model")
        print(" Model loaded.")
    except Exception as e:
        print(f" Model load failed: {e}")
        return

    env = SparkResourceEnv(iot_scenario='healthcare_critical') # High beta (Latency Sensitive)
    obs, info = env.reset()

    history = {
        'step': [],
        'workload': [],
        'prediction': [],
        'executors': [],
        'latency': [],
        'burst_signal': []
    }

    print("Running simulation...")

    for i in range(50):
        action, agent_info = agent.predict(obs)

        current_load = obs[0]
        predicted_load = obs[-2]
        burst_signal = obs[-1]
        executors = agent_info['num_executors']

        obs, reward, done, truncated, info = env.step(action)

        history['step'].append(i)
        history['workload'].append(current_load)
        history['prediction'].append(predicted_load)
        history['executors'].append(executors)
        history['latency'].append(info['latency'])
        history['burst_signal'].append(burst_signal)

        if i == 18:
            env.next_workload_rate = 800.0 
            env.edge_burst_signal = 1.0
            pass # The step() we just called generated random next. Let's force override it.

            obs[-2] = 800.0
            obs[-1] = 1.0

        elif i == 19:
            pass

    print(f"\n{'Step':<5} | {'Load':<10} | {'Pred':<10} | {'Signal':<6} | {'Execs':<5} | {'Latency':<8}")
    print("-" * 60)
    for i in range(15, 25):
        print(f"{history['step'][i]:<5} | "
              f"{history['workload'][i]:<10.1f} | "
              f"{history['prediction'][i]:<10.1f} | "
              f"{history['burst_signal'][i]:<6.0f} | "
              f"{history['executors'][i]:<5} | "
              f"{history['latency'][i]:<8.1f}")

    exec_at_18 = history['executors'][18] # Saw signal
    exec_at_19 = history['executors'][19] # Burst arrived

    print("\nanalysis:")
    if exec_at_18 > history['executors'][15] + 5:
        print(" PROACTIVE SCALING DETECTED! Agent scaled up BEFORE load arrived.")
    else:
        print(" Agent was reactive (or burst wasn't strong enough).")

if __name__ == "__main__":
    verify_proactive_scaling()
