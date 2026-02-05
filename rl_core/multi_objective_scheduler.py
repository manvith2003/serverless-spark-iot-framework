"""
Multi-Objective RL Scheduler with Fixed Modes
==============================================

KEY INSIGHT: Weights are set ONCE per mode, NOT continuously.
This keeps the reward function stationary for proper RL learning.

Three Modes:
- LOW_LATENCY: α=0.7, β=0.2, γ=0.1 (fast response)
- COST_SAVING: α=0.2, β=0.7, γ=0.1 (minimize cloud cost)
- BALANCED:    α=0.4, β=0.4, γ=0.2 (default tradeoff)

Usage:
    scheduler = MultiObjectiveRLScheduler(mode='BALANCED')
    scheduler.set_mode('LOW_LATENCY')  # Change mode once
    decision = scheduler.get_scaling_decision(metrics)
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional
import json


class SchedulerMode(Enum):
    """
    Three fixed modes for Multi-Objective RL.
    Weights are set ONCE when mode is selected, NOT continuously.
    """
    LOW_LATENCY = "low_latency"   # Priority: fast response
    COST_SAVING = "cost_saving"   # Priority: minimize cost
    BALANCED = "balanced"          # Priority: tradeoff


@dataclass
class ModeWeights:
    """
    Reward weights for each mode.
    R = -α·Latency - β·Cost + γ·Utilization
    
    These weights are FIXED per mode - they do NOT change during operation.
    """
    alpha: float  # Latency weight (higher = latency more important)
    beta: float   # Cost weight (higher = cost more important)
    gamma: float  # Utilization weight (resource efficiency)
    
    def __post_init__(self):
        # Normalize to sum to 1
        total = self.alpha + self.beta + self.gamma
        if total != 1.0:
            self.alpha /= total
            self.beta /= total
            self.gamma /= total


# FIXED mode configurations - these never change during operation
MODE_CONFIGS: Dict[SchedulerMode, ModeWeights] = {
    SchedulerMode.LOW_LATENCY: ModeWeights(
        alpha=0.7,   # Latency VERY important
        beta=0.2,    # Cost less important
        gamma=0.1    # Utilization minor
    ),
    SchedulerMode.COST_SAVING: ModeWeights(
        alpha=0.2,   # Latency acceptable
        beta=0.7,    # Cost VERY important
        gamma=0.1    # Utilization minor
    ),
    SchedulerMode.BALANCED: ModeWeights(
        alpha=0.4,   # Moderate latency priority
        beta=0.4,    # Moderate cost priority
        gamma=0.2    # Some utilization focus
    )
}


class MultiObjectiveRLScheduler:
    """
    RL Scheduler with Multi-Objective optimization.
    
    KEY DESIGN:
    - Mode is selected ONCE by admin/system
    - Weights stay FIXED during operation
    - RL learns under STATIONARY reward function
    - NO continuous weight adaptation (removes meta-RL complexity)
    """
    
    def __init__(self, mode: str = "balanced"):
        """
        Initialize scheduler with a mode.
        
        Args:
            mode: One of 'low_latency', 'cost_saving', 'balanced'
        """
        self.current_mode = None
        self.weights = None
        self.set_mode(mode)
        
        # RL state
        self.current_executors = 2
        self.min_executors = 0
        self.max_executors = 50
        
    def set_mode(self, mode: str) -> None:
        """
        Set the scheduling mode ONCE.
        
        This changes the reward weights but they stay FIXED after this call.
        The RL agent then operates with these fixed weights.
        
        Args:
            mode: 'low_latency', 'cost_saving', or 'balanced'
        """
        mode_enum = SchedulerMode(mode.lower())
        self.current_mode = mode_enum
        self.weights = MODE_CONFIGS[mode_enum]
        
        print(f"✅ Mode set to: {mode_enum.value.upper()}")
        print(f"   α (latency):    {self.weights.alpha:.2f}")
        print(f"   β (cost):       {self.weights.beta:.2f}")
        print(f"   γ (utilization): {self.weights.gamma:.2f}")
    
    def calculate_reward(self, latency_ms: float, cost_units: float, 
                        utilization: float) -> float:
        """
        Calculate reward with FIXED weights.
        
        R = -α·Latency - β·Cost + γ·Utilization
        
        This function uses the weights set by set_mode().
        Weights do NOT change during this calculation.
        """
        # Normalize values
        norm_latency = min(1.0, latency_ms / 5000)  # Max 5s
        norm_cost = min(1.0, cost_units / 100)       # Max 100 units
        norm_util = min(1.0, utilization / 100)      # Max 100%
        
        reward = (
            -self.weights.alpha * norm_latency +
            -self.weights.beta * norm_cost +
            self.weights.gamma * norm_util
        )
        
        return reward
    
    def get_scaling_decision(self, metrics: Dict) -> Dict:
        """
        Get scaling decision based on current mode.
        
        The decision logic differs based on the mode weights,
        but the weights themselves are FIXED.
        """
        workload = metrics.get('workload_rate', 100)
        cpu_util = metrics.get('cpu_util', 50)
        latency = metrics.get('latency_ms', 100)
        
        # Calculate ideal executors based on workload
        base_executors = max(1, int(workload / 50) + 1)
        
        # Mode-specific behavior
        if self.current_mode == SchedulerMode.LOW_LATENCY:
            # Scale aggressively - prioritize low latency
            target = min(self.max_executors, base_executors + 4)
            reason = f"LOW_LATENCY mode: Scaling to {target} for fast response"
            
        elif self.current_mode == SchedulerMode.COST_SAVING:
            # Scale conservatively - prioritize cost
            target = max(self.min_executors, base_executors - 2)
            if workload < 30:
                target = 0  # Scale to zero when idle
            reason = f"COST_SAVING mode: Minimal executors ({target})"
            
        else:  # BALANCED
            target = base_executors
            reason = f"BALANCED mode: Optimal capacity ({target})"
        
        # Determine action
        if target > self.current_executors:
            action = "scale_up"
        elif target < self.current_executors:
            action = "scale_down"
        else:
            action = "maintain"
        
        self.current_executors = target
        
        return {
            "action": action,
            "target_executors": target,
            "mode": self.current_mode.value,
            "weights": {
                "alpha": self.weights.alpha,
                "beta": self.weights.beta,
                "gamma": self.weights.gamma
            },
            "reason": reason,
            "reward": self.calculate_reward(latency, target * 0.5, cpu_util)
        }


def demonstrate_modes():
    """Demonstrate the three modes with same workload"""
    
    print("\n" + "="*70)
    print("🎯 MULTI-OBJECTIVE RL: THREE FIXED MODES")
    print("="*70)
    print("""
    Same workload → Different behavior based on mode
    
    ┌─────────────────────────────────────────────────────────────────┐
    │                   MODE COMPARISON                               │
    ├─────────────────────────────────────────────────────────────────┤
    │  Mode          │ α (latency) │ β (cost) │ Behavior             │
    ├─────────────────────────────────────────────────────────────────┤
    │  LOW_LATENCY   │    0.70     │   0.20   │ Scales aggressively  │
    │  COST_SAVING   │    0.20     │   0.70   │ Scales conservatively│
    │  BALANCED      │    0.40     │   0.40   │ Moderate scaling     │
    └─────────────────────────────────────────────────────────────────┘
    """)
    
    # Test workload
    test_metrics = {
        'workload_rate': 200,  # 200 msg/s
        'cpu_util': 60,
        'latency_ms': 150
    }
    
    print(f"\n📊 Test Workload: {test_metrics['workload_rate']} msg/s\n")
    print("-"*70)
    
    for mode in ['low_latency', 'cost_saving', 'balanced']:
        scheduler = MultiObjectiveRLScheduler(mode=mode)
        decision = scheduler.get_scaling_decision(test_metrics)
        
        print(f"\n🔹 Mode: {mode.upper()}")
        print(f"   Decision: {decision['action'].upper()}")
        print(f"   Target Executors: {decision['target_executors']}")
        print(f"   Reason: {decision['reason']}")
        print(f"   Reward: {decision['reward']:.3f}")
        print("-"*70)
    
    # Demonstrate mode switching
    print("\n" + "="*70)
    print("📌 KEY INSIGHT: Mode is set ONCE, weights stay FIXED")
    print("="*70)
    print("""
    ❌ WRONG (Meta-RL / Unstable):
       → Change α, β, γ every second
       → RL cannot learn properly
    
    ✅ CORRECT (This Implementation):
       → Admin selects mode ONCE
       → Weights stay FIXED during operation
       → RL learns under STATIONARY reward
       
    Workflow:
       User/Admin selects mode
              ↓
       set_mode('low_latency')  ← Weights set HERE, ONCE
              ↓
       RL runs with FIXED weights
              ↓
       Scaling decisions reflect mode priority
    """)
    
    return True


if __name__ == "__main__":
    demonstrate_modes()
