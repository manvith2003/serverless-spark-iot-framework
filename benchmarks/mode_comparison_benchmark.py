"""
Multi-Objective RL Mode Comparison Benchmark
=============================================
Compares LOW_LATENCY, COST_SAVING, and BALANCED modes
"""

import numpy as np
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from rl_core.multi_objective_scheduler import MultiObjectiveRLScheduler


def run_mode_benchmark():
    """Benchmark all three modes with same workload"""
    
    print("\n" + "="*70)
    print("📊 MULTI-OBJECTIVE RL MODE BENCHMARK")
    print("="*70)
    
    # Workload: 240 seconds, daily pattern
    np.random.seed(42)
    workload = []
    for t in range(240):
        hour = (t / 10) % 24
        if 0 <= hour < 6:
            rate = 10
        elif 6 <= hour < 9:
            rate = 50 + (hour - 6) * 50
        elif 9 <= hour < 10:
            rate = 500  # 9 AM burst
        elif 10 <= hour < 17:
            rate = 150
        else:
            rate = 50
        workload.append(rate)
    
    results = {}
    
    for mode in ['low_latency', 'cost_saving', 'balanced']:
        scheduler = MultiObjectiveRLScheduler(mode=mode)
        
        total_executors = 0
        max_executors = 0
        scale_actions = 0
        prev_executors = 2
        
        for rate in workload:
            metrics = {
                'workload_rate': rate,
                'cpu_util': min(100, rate / 5),
                'latency_ms': 100 + rate * 0.5
            }
            
            decision = scheduler.get_scaling_decision(metrics)
            
            total_executors += decision['target_executors']
            max_executors = max(max_executors, decision['target_executors'])
            
            if decision['action'] != 'maintain':
                scale_actions += 1
            
            prev_executors = decision['target_executors']
        
        avg_executors = total_executors / len(workload)
        cost = total_executors * 0.01  # $0.01 per executor-second
        
        results[mode] = {
            'avg_executors': avg_executors,
            'max_executors': max_executors,
            'cost': cost,
            'scale_actions': scale_actions
        }
    
    # Print comparison table
    print("\n┌───────────────┬─────────────┬─────────────┬───────────────┬──────────┐")
    print("│ Mode          │ Avg Exec.   │ Max Exec.   │ Cost (units)  │ Actions  │")
    print("├───────────────┼─────────────┼─────────────┼───────────────┼──────────┤")
    
    for mode, r in results.items():
        print(f"│ {mode.upper():13} │ {r['avg_executors']:>10.1f} │ {r['max_executors']:>10d} │ {r['cost']:>12.2f} │ {r['scale_actions']:>7d} │")
    
    print("└───────────────┴─────────────┴─────────────┴───────────────┴──────────┘")
    
    # Analysis
    print("\n📈 ANALYSIS:")
    
    lat = results['low_latency']
    cost = results['cost_saving']
    bal = results['balanced']
    
    cost_savings = ((lat['cost'] - cost['cost']) / lat['cost']) * 100
    print(f"   💰 COST_SAVING saves {cost_savings:.1f}% vs LOW_LATENCY")
    
    executor_diff = lat['avg_executors'] - cost['avg_executors']
    print(f"   📉 COST_SAVING uses {executor_diff:.1f} fewer executors on avg")
    
    print(f"   ⚖️  BALANCED provides middle ground")
    
    print("""
    ┌─────────────────────────────────────────────────────────────────┐
    │                    MODE RECOMMENDATIONS                        │
    ├─────────────────────────────────────────────────────────────────┤
    │  🔥 LOW_LATENCY  → Critical alerts, healthcare, emergencies    │
    │  🌙 COST_SAVING  → Night processing, batch jobs, non-critical  │
    │  ⚖️  BALANCED    → Normal operations, default mode             │
    └─────────────────────────────────────────────────────────────────┘
    """)
    
    return results


if __name__ == "__main__":
    run_mode_benchmark()
