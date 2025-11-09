"""
Comprehensive visualization and reporting for RL Optimizer results
Shows performance in console with tables, charts, and comparisons
"""

import sys
import os
sys.path.append('optimization/resource_allocation')

from ppo_agent import ResourceAllocator
from rl_environment import SparkResourceEnv
import numpy as np
from typing import Dict, List
import time


class ResultsVisualizer:
    """Display RL optimizer results in beautiful console output"""
    
    def __init__(self):
        self.allocator = ResourceAllocator()
        self.env = SparkResourceEnv()
    
    def print_header(self, text: str):
        """Print a formatted header"""
        print("\n" + "="*70)
        print(f"  {text}")
        print("="*70)
    
    def print_section(self, text: str):
        """Print a section divider"""
        print("\n" + "-"*70)
        print(f"  {text}")
        print("-"*70)
    
    def demonstrate_prediction(self):
        """Show how the RL agent makes decisions"""
        self.print_header("🤖 RL OPTIMIZER DECISION-MAKING DEMO")
        
        print("\nLoading trained model...")
        try:
            self.allocator.load("final_model")
            print("✅ Model loaded successfully!\n")
        except FileNotFoundError:
            print("❌ No trained model found. Train first with:")
            print("   python optimization/resource_allocation/ppo_agent.py --train --timesteps 50000")
            return
        
        # Test scenarios
        scenarios = [
            {
                "name": "🔥 HIGH LOAD Scenario",
                "state": np.array([450.0, 380.0, 85.0, 75.0, 800.0, 8.0], dtype=np.float32),
                "description": "Heavy IoT traffic (450 msg/s), Large data volume (380 MB)"
            },
            {
                "name": "🌊 MODERATE LOAD Scenario", 
                "state": np.array([150.0, 120.0, 50.0, 45.0, 400.0, 5.0], dtype=np.float32),
                "description": "Normal IoT traffic (150 msg/s), Medium data (120 MB)"
            },
            {
                "name": "💤 LOW LOAD Scenario",
                "state": np.array([30.0, 25.0, 20.0, 15.0, 200.0, 2.0], dtype=np.float32),
                "description": "Light IoT traffic (30 msg/s), Small data (25 MB)"
            }
        ]
        
        for i, scenario in enumerate(scenarios, 1):
            self.print_section(f"Scenario {i}: {scenario['name']}")
            
            print(f"\n📊 Input State:")
            print(f"   Workload Rate:  {scenario['state'][0]:.0f} messages/second")
            print(f"   Data Volume:    {scenario['state'][1]:.0f} MB")
            print(f"   CPU Usage:      {scenario['state'][2]:.0f}%")
            print(f"   Memory Usage:   {scenario['state'][3]:.0f}%")
            print(f"   Current Latency: {scenario['state'][4]:.0f} ms")
            print(f"   Current Cost:   ${scenario['state'][5]:.2f}/hr")
            print(f"\n   📝 {scenario['description']}")
            
            # Get RL recommendation
            action, info = self.allocator.predict(scenario['state'])
            
            print(f"\n🎯 RL Optimizer Recommendation:")
            print(f"   ┌─────────────────────────────────────┐")
            print(f"   │ Spark Executors:     {info['num_executors']:2d}             │")
            print(f"   │ Memory per Executor: {info['memory_per_executor_gb']:2d} GB          │")
            print(f"   │ Total Memory:        {info['total_memory_gb']:2d} GB          │")
            print(f"   │ Storage Tier:        {info['storage_tier_name']:<15s}│")
            print(f"   └─────────────────────────────────────┘")
            
            # Simulate outcome
            test_env = SparkResourceEnv()
            test_env.state = scenario['state']
            next_state, reward, _, _, result_info = test_env.step(action)
            
            print(f"\n📈 Expected Performance:")
            print(f"   Cost:       ${result_info['cost']:.2f}/hr")
            print(f"   Latency:    {result_info['latency']:.0f} ms")
            print(f"   Throughput: {result_info['throughput']:.0f} units/sec")
            print(f"   CPU Usage:  {result_info['cpu_util']:.1f}%")
            print(f"   Memory:     {result_info['mem_util']:.1f}%")
            
            time.sleep(0.5)  # Pause for readability
    
    def evaluate_performance(self, num_episodes: int = 20):
        """Comprehensive performance evaluation"""
        self.print_header("📊 COMPREHENSIVE PERFORMANCE EVALUATION")
        
        print("\nLoading model...")
        try:
            self.allocator.load("final_model")
        except FileNotFoundError:
            print("❌ No trained model found")
            return
        
        print(f"Running {num_episodes} evaluation episodes...")
        
        results = self.allocator.evaluate(num_episodes=num_episodes)
        
        self.print_section("Statistical Summary")
        
        metrics = {
            'Cost ($/hr)': results['costs'],
            'Latency (ms)': results['latencies'],
            'Throughput': results['throughputs'],
            'Reward': results['rewards']
        }
        
        print(f"\n{'Metric':<20} {'Mean':>12} {'Std Dev':>12} {'Min':>12} {'Max':>12}")
        print("-"*70)
        
        for name, values in metrics.items():
            mean_val = np.mean(values)
            std_val = np.std(values)
            min_val = np.min(values)
            max_val = np.max(values)
            
            print(f"{name:<20} {mean_val:>12.2f} {std_val:>12.2f} {min_val:>12.2f} {max_val:>12.2f}")
        
        # Performance distribution
        self.print_section("Performance Distribution")
        
        def print_histogram(values, name, bins=10):
            print(f"\n{name} Distribution:")
            hist, edges = np.histogram(values, bins=bins)
            max_count = max(hist)
            
            for i, count in enumerate(hist):
                bar_length = int((count / max_count) * 40)
                bar = "█" * bar_length
                print(f"  {edges[i]:6.1f} - {edges[i+1]:6.1f} │{bar} {count}")
        
        print_histogram(results['costs'], "Cost ($/hr)")
        print_histogram(results['latencies'], "Latency (ms)")
        print_histogram(results['throughputs'], "Throughput")
    
    def compare_with_baseline(self, num_episodes: int = 20):
        """Compare RL vs simple baseline"""
        self.print_header("🏆 RL OPTIMIZER vs BASELINE COMPARISON")
        
        print("\nLoading RL model...")
        try:
            self.allocator.load("final_model")
        except FileNotFoundError:
            print("❌ No trained model found")
            return
        
        # Simple baseline (like Dexter's approach)
        def baseline_policy(state):
            """Rule-based baseline"""
            workload = state[0]
            if workload > 300:
                return np.array([14, 5, 2])  # 15 executors, 6GB, S3
            elif workload > 150:
                return np.array([9, 3, 2])   # 10 executors, 4GB, S3
            else:
                return np.array([4, 1, 2])   # 5 executors, 2GB, S3
        
        print("Evaluating RL Optimizer...")
        rl_costs = []
        rl_latencies = []
        rl_throughputs = []
        
        print("Evaluating Baseline (Rule-based)...")
        baseline_costs = []
        baseline_latencies = []
        baseline_throughputs = []
        
        for episode in range(num_episodes):
            # RL evaluation
            env = SparkResourceEnv()
            obs, _ = env.reset()
            done = False
            ep_cost, ep_lat, ep_thr = 0, 0, 0
            steps = 0
            
            while not done:
                action, _ = self.allocator.predict(obs)
                obs, reward, done, _, info = env.step(action)
                ep_cost += info['cost']
                ep_lat += info['latency']
                ep_thr += info['throughput']
                steps += 1
            
            rl_costs.append(ep_cost / steps)
            rl_latencies.append(ep_lat / steps)
            rl_throughputs.append(ep_thr / steps)
            
            # Baseline evaluation
            env = SparkResourceEnv()
            obs, _ = env.reset()
            done = False
            ep_cost, ep_lat, ep_thr = 0, 0, 0
            steps = 0
            
            while not done:
                action = baseline_policy(obs)
                obs, reward, done, _, info = env.step(action)
                ep_cost += info['cost']
                ep_lat += info['latency']
                ep_thr += info['throughput']
                steps += 1
            
            baseline_costs.append(ep_cost / steps)
            baseline_latencies.append(ep_lat / steps)
            baseline_throughputs.append(ep_thr / steps)
            
            if (episode + 1) % 5 == 0:
                print(f"  Completed {episode + 1}/{num_episodes} episodes...")
        
        # Calculate improvements
        self.print_section("Results Comparison")
        
        rl_cost_mean = np.mean(rl_costs)
        baseline_cost_mean = np.mean(baseline_costs)
        cost_improvement = ((baseline_cost_mean - rl_cost_mean) / baseline_cost_mean) * 100
        
        rl_lat_mean = np.mean(rl_latencies)
        baseline_lat_mean = np.mean(baseline_latencies)
        lat_improvement = ((baseline_lat_mean - rl_lat_mean) / baseline_lat_mean) * 100
        
        rl_thr_mean = np.mean(rl_throughputs)
        baseline_thr_mean = np.mean(baseline_throughputs)
        thr_improvement = ((rl_thr_mean - baseline_thr_mean) / baseline_thr_mean) * 100
        
        print(f"\n{'Metric':<20} {'Baseline':>15} {'RL Optimizer':>15} {'Improvement':>15}")
        print("="*70)
        print(f"{'Cost ($/hr)':<20} ${baseline_cost_mean:>14.2f} ${rl_cost_mean:>14.2f} {cost_improvement:>13.1f}%")
        print(f"{'Latency (ms)':<20} {baseline_lat_mean:>14.0f} {rl_lat_mean:>14.0f} {lat_improvement:>13.1f}%")
        print(f"{'Throughput':<20} {baseline_thr_mean:>14.0f} {rl_thr_mean:>14.0f} {thr_improvement:>13.1f}%")
        
        print("\n" + "="*70)
        if cost_improvement > 0 and lat_improvement > 0:
            print("✅ RL Optimizer outperforms baseline on ALL metrics!")
        else:
            print("⚠️  Mixed results - some metrics improved")
        print("="*70)
        
        # Winner summary
        self.print_section("Performance Winner")
        
        winners = []
        if cost_improvement > 0:
            winners.append(f"💰 Cost: RL wins by {cost_improvement:.1f}%")
        if lat_improvement > 0:
            winners.append(f"⚡ Latency: RL wins by {lat_improvement:.1f}%")
        if thr_improvement > 0:
            winners.append(f"🚀 Throughput: RL wins by {thr_improvement:.1f}%")
        
        print()
        for winner in winners:
            print(f"  {winner}")
        
        return {
            'cost_improvement': cost_improvement,
            'latency_improvement': lat_improvement,
            'throughput_improvement': thr_improvement
        }


def main():
    """Run comprehensive results visualization"""
    
    print("\n" + "🎯"*35)
    print("  SERVERLESS SPARK IoT - RL OPTIMIZER RESULTS")
    print("🎯"*35)
    
    visualizer = ResultsVisualizer()
    
    # Menu
    print("\n📋 Select what to display:")
    print("  1. Show RL Decision-Making Demo")
    print("  2. Performance Evaluation")
    print("  3. Comparison with Baseline")
    print("  4. ALL (Complete Report)")
    print("  0. Exit")
    
    choice = input("\nEnter choice (0-4): ").strip()
    
    if choice == "1":
        visualizer.demonstrate_prediction()
    elif choice == "2":
        visualizer.evaluate_performance()
    elif choice == "3":
        visualizer.compare_with_baseline()
    elif choice == "4":
        visualizer.demonstrate_prediction()
        visualizer.evaluate_performance()
        visualizer.compare_with_baseline()
        
        print("\n" + "="*70)
        print("  🎉 COMPLETE EVALUATION FINISHED!")
        print("="*70)
    else:
        print("Exiting...")


if __name__ == "__main__":
    main()
