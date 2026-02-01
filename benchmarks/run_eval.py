import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), '../optimization/resource_allocation'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from rl_environment import SparkResourceEnv
from ppo_agent import ResourceAllocator
from policies import FixedPolicy, DexterPolicy, SeerPolicy

def run_policy_eval(policy, env, steps=1000, seed=42):
    """Run a single policy on the environment"""
    print(f"Running Policy: {policy.name}...")
    obs, info = env.reset(seed=seed)

    metrics = {
        'cost': [],
        'latency': [],
        'executors': [],
        'tier': [],
        'sla_violations': 0
    }

    for _ in tqdm(range(steps)):
        if hasattr(policy, 'predict'):
            if isinstance(policy, ResourceAllocator):
                action, _ = policy.predict(obs)
            else:
                action, _ = policy.predict(obs)
        else:
            action = env.action_space.sample()

        obs, reward, done, truncated, info = env.step(action)

        metrics['cost'].append(info['cost'])
        metrics['latency'].append(info['latency'])
        metrics['executors'].append(action[0]+1) # 0-indexed to 1-20
        metrics['tier'].append(action[2])

        if info['latency'] > 1000:
            metrics['sla_violations'] += 1

    return metrics

def main():
    env = SparkResourceEnv(iot_scenario='balanced', max_steps=1000)
    policies = []

    policies.append(FixedPolicy())
    policies.append(DexterPolicy())
    policies.append(SeerPolicy())

    rl_agent = ResourceAllocator()
    try:
        rl_agent.load("final_model")
        rl_agent.name = "Edge-Aware RL"
        policies.append(rl_agent)
    except Exception as e:
        print(f"Skipping RL Agent (Load Failed): {e}")

    results = {}

    for pol in policies:
        results[pol.name] = run_policy_eval(pol, env, steps=500, seed=123)

    print("\n" + "="*60)
    print(f"{'POLICY':<20} | {'COST ($)':<10} | {'P95 LAT (ms)':<12} | {'SLA VIOLATION %':<15}")
    print("="*60)

    summary_data = []

    for name, m in results.items():
        total_cost = sum(m['cost'])
        p95_lat = np.percentile(m['latency'], 95)
        sla_pct = (m['sla_violations'] / 500) * 100
        stability = np.std(m['executors'])

        print(f"{name:<20} | {total_cost:<10.2f} | {p95_lat:<12.1f} | {sla_pct:<15.1f}%")

        summary_data.append({
            'Policy': name,
            'Total Cost': total_cost,
            'P95 Latency': p95_lat,
            'SLA (%)': sla_pct,
            'Stability': stability
        })

    df = pd.DataFrame(summary_data)
    df.to_csv("benchmarks/results_summary.csv", index=False)
    print("\n Results saved to benchmarks/results_summary.csv")

    plot_comparison(df)

def plot_comparison(df):
    """Generate comparative plots"""
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    axes[0].bar(df['Policy'], df['Total Cost'], color=['gray', 'orange', 'blue', 'green'])
    axes[0].set_title("Total Cost ($)")
    axes[0].set_ylabel("Dollars")

    axes[1].bar(df['Policy'], df['P95 Latency'], color=['gray', 'orange', 'blue', 'green'])
    axes[1].set_title("P95 Latency (ms)")
    axes[1].axhline(y=1000, color='r', linestyle='--', label='SLA')
    axes[1].legend()

    axes[2].bar(df['Policy'], df['SLA (%)'], color=['gray', 'orange', 'blue', 'green'])
    axes[2].set_title("SLA Violations (%)")
    axes[2].set_ylabel("Percent")

    plt.tight_layout()
    plt.savefig("benchmarks/benchmark_plots.png")
    print(" Plots saved to benchmarks/benchmark_plots.png")

if __name__ == "__main__":
    main()
