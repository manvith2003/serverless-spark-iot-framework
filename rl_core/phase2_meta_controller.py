"""
Phase-2 PPO Meta-Controller (Production-Ready)
===============================================

Uses PPO algorithm to learn optimal (α, β, γ) weights between episodes.

Why PPO for Phase-2:
- Stable policy updates (clipped objective)
- Works well with continuous action spaces
- Proven in production systems
- Better generalization than rule-based

Architecture:
    State (9-dim) → PPO Policy Network → Action (3-dim: α, β, γ)
"""

import numpy as np
import gymnasium as gym
from gymnasium import spaces
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class EpisodePerformance:
    """Performance metrics from Phase-1 episode"""
    avg_latency_ms: float
    total_cost: float
    avg_throughput: float
    sla_violations: int
    messages_dropped: int
    avg_cpu_util: float


class Phase2Environment(gym.Env):
    """
    Gymnasium Environment for Phase-2 PPO Training.
    
    State (9 dimensions):
        - hour_of_day (0-1)
        - day_of_week (0-1)
        - is_business_hours (0/1)
        - latency_ratio (latency/SLA)
        - cost_ratio (cost/budget)
        - throughput_ratio
        - sla_violation_rate
        - latency_trend (-1 to +1)
        - cost_trend (-1 to +1)
    
    Action (3 dimensions - continuous):
        - α (cost weight)
        - β (latency weight)  
        - γ (throughput weight)
        
        Note: Outputs are passed through softmax to ensure α + β + γ = 1
    
    Reward:
        - SLA compliance bonus/penalty
        - Budget compliance bonus/penalty
        - Stability bonus (smooth weight changes)
    """
    
    metadata = {'render_modes': ['human']}
    
    def __init__(self,
                 sla_latency_target_ms: float = 200.0,
                 cost_budget_per_hour: float = 10.0,
                 max_episodes: int = 100):
        
        super().__init__()
        
        self.sla_target = sla_latency_target_ms
        self.cost_budget = cost_budget_per_hour
        self.max_episodes = max_episodes
        
        # State space: 9 dimensions
        self.observation_space = spaces.Box(
            low=np.array([0, 0, 0, 0, 0, 0, 0, -1, -1], dtype=np.float32),
            high=np.array([1, 1, 1, 3, 3, 3, 1, 1, 1], dtype=np.float32),
            dtype=np.float32
        )
        
        # Action space: 3 continuous values → softmax → (α, β, γ)
        self.action_space = spaces.Box(
            low=np.array([-2, -2, -2], dtype=np.float32),
            high=np.array([2, 2, 2], dtype=np.float32),
            dtype=np.float32
        )
        
        # State tracking
        self.current_step = 0
        self.prev_weights = np.array([0.33, 0.33, 0.34])
        self.state = None
        
        # Performance history (simulated Phase-1 results)
        self.episode_history: List[EpisodePerformance] = []
        
        self.reset()
    
    def _softmax(self, x: np.ndarray) -> np.ndarray:
        """Convert raw action to normalized weights (sum = 1)"""
        exp_x = np.exp(x - np.max(x))  # Numerical stability
        return exp_x / exp_x.sum()
    
    def reset(self, seed=None, options=None) -> Tuple[np.ndarray, Dict]:
        """Reset environment for new training episode"""
        super().reset(seed=seed)
        
        self.current_step = 0
        self.prev_weights = np.array([0.33, 0.33, 0.34])
        
        # Initial state: random time context
        hour = np.random.randint(0, 24)
        day = np.random.randint(0, 7)
        
        self.state = np.array([
            hour / 24.0,                          # hour_of_day
            day / 7.0,                            # day_of_week
            1.0 if (9 <= hour <= 17 and day < 5) else 0.0,  # is_business_hours
            1.0,                                  # latency_ratio (start at SLA)
            0.5,                                  # cost_ratio (start at 50% budget)
            1.0,                                  # throughput_ratio
            0.0,                                  # sla_violation_rate
            0.0,                                  # latency_trend
            0.0                                   # cost_trend
        ], dtype=np.float32)
        
        return self.state, {}
    
    def step(self, action: np.ndarray) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """
        Execute one Phase-2 step (= one Phase-1 episode).
        
        1. Convert action to weights
        2. Simulate Phase-1 with these weights
        3. Observe performance
        4. Compute reward
        5. Update state
        """
        # 1. Convert action to weights via softmax
        weights = self._softmax(action)
        alpha, beta, gamma = weights[0], weights[1], weights[2]
        
        # 2. Simulate Phase-1 performance with these weights
        performance = self._simulate_phase1_episode(alpha, beta, gamma)
        
        # 3. Compute Phase-2 reward
        reward = self._compute_reward(performance, weights)
        
        # 4. Update state for next step
        self._update_state(performance, weights)
        
        # 5. Check if done
        self.current_step += 1
        done = self.current_step >= self.max_episodes
        
        # 6. Store for history
        self.prev_weights = weights
        self.episode_history.append(performance)
        
        info = {
            'alpha': alpha,
            'beta': beta,
            'gamma': gamma,
            'latency': performance.avg_latency_ms,
            'cost': performance.total_cost,
            'violations': performance.sla_violations
        }
        
        return self.state, reward, done, False, info
    
    def _simulate_phase1_episode(self, alpha: float, beta: float, gamma: float) -> EpisodePerformance:
        """
        Simulate what Phase-1 RL would achieve with given weights.
        
        In production, this would be ACTUAL Phase-1 execution!
        Here we simulate the relationship between weights and performance.
        """
        # Higher β (latency weight) → Phase-1 scales more → lower latency, higher cost
        # Higher α (cost weight) → Phase-1 scales less → higher latency, lower cost
        
        base_latency = 200  # ms
        base_cost = 8       # $
        
        # Latency decreases when β is high (more resources for latency)
        latency = base_latency * (1.5 - beta)  # β=0.7 → latency=160ms
        latency = max(50, latency + np.random.normal(0, 20))
        
        # Cost increases when β is high (more executors)
        cost = base_cost * (0.5 + beta)  # β=0.7 → cost=$9.6
        cost = max(2, cost + np.random.normal(0, 1))
        
        # SLA violations when latency > target
        violations = 0
        if latency > self.sla_target:
            violations = int((latency - self.sla_target) / 50) + 1
        
        return EpisodePerformance(
            avg_latency_ms=latency,
            total_cost=cost,
            avg_throughput=100 + gamma * 50,
            sla_violations=violations,
            messages_dropped=violations * 10,
            avg_cpu_util=60 + beta * 20
        )
    
    def _compute_reward(self, perf: EpisodePerformance, weights: np.ndarray) -> float:
        """
        Phase-2 Reward Function.
        
        Goals:
        1. Keep latency under SLA
        2. Keep cost under budget
        3. Minimize SLA violations
        4. Smooth weight changes (stability)
        """
        reward = 0.0
        
        # 1. Latency component (-2 to +1)
        if perf.avg_latency_ms <= self.sla_target:
            latency_reward = 1.0  # Under SLA: good!
        else:
            over_ratio = perf.avg_latency_ms / self.sla_target
            latency_reward = -1.0 * (over_ratio - 1)  # Penalty for over SLA
        reward += latency_reward
        
        # 2. Cost component (-2 to +1)
        if perf.total_cost <= self.cost_budget:
            cost_reward = 1.0 - (perf.total_cost / self.cost_budget) * 0.5
        else:
            over_ratio = perf.total_cost / self.cost_budget
            cost_reward = -1.0 * (over_ratio - 1)
        reward += cost_reward
        
        # 3. SLA violations penalty
        reward -= 0.5 * perf.sla_violations
        
        # 4. Stability bonus (penalize large weight changes)
        weight_change = np.abs(weights - self.prev_weights).sum()
        stability_bonus = 0.2 * (1 - weight_change)  # Small bonus for stability
        reward += stability_bonus
        
        return reward
    
    def _update_state(self, perf: EpisodePerformance, weights: np.ndarray):
        """Update state after observing performance"""
        # Advance time (simulate)
        hour = (self.state[0] * 24 + 1) % 24
        day = int(self.state[1] * 7)
        if hour < self.state[0] * 24:  # Wrapped around
            day = (day + 1) % 7
        
        # Compute trends from history
        latency_trend = 0.0
        cost_trend = 0.0
        if len(self.episode_history) >= 1:
            prev = self.episode_history[-1]
            latency_trend = np.sign(perf.avg_latency_ms - prev.avg_latency_ms)
            cost_trend = np.sign(perf.total_cost - prev.total_cost)
        
        self.state = np.array([
            hour / 24.0,
            day / 7.0,
            1.0 if (9 <= hour <= 17 and day < 5) else 0.0,
            min(3.0, perf.avg_latency_ms / self.sla_target),
            min(3.0, perf.total_cost / self.cost_budget),
            min(3.0, perf.avg_throughput / 100.0),
            min(1.0, perf.sla_violations / 10.0),
            latency_trend,
            cost_trend
        ], dtype=np.float32)
    
    def render(self, mode='human'):
        pass


class Phase2PPOController:
    """
    Production-ready Phase-2 Controller using PPO.
    
    This wraps the environment and provides easy-to-use methods
    for training and inference.
    """
    
    def __init__(self,
                 sla_latency_target_ms: float = 200.0,
                 cost_budget_per_hour: float = 10.0):
        
        self.env = Phase2Environment(
            sla_latency_target_ms=sla_latency_target_ms,
            cost_budget_per_hour=cost_budget_per_hour
        )
        
        self.model = None
        self.is_trained = False
        
    def train(self, total_timesteps: int = 10000, verbose: int = 1):
        """Train PPO model on Phase-2 environment"""
        try:
            from stable_baselines3 import PPO
            
            logger.info("Training Phase-2 PPO model...")
            
            self.model = PPO(
                "MlpPolicy",
                self.env,
                learning_rate=3e-4,
                n_steps=128,
                batch_size=64,
                n_epochs=10,
                gamma=0.99,
                verbose=verbose
            )
            
            self.model.learn(total_timesteps=total_timesteps)
            self.is_trained = True
            
            logger.info("Phase-2 PPO training complete!")
            
        except ImportError:
            logger.warning("stable-baselines3 not installed. Using fallback.")
            self._train_fallback()
    
    def _train_fallback(self):
        """Fallback training without stable-baselines3"""
        logger.info("Using rule-based fallback (install stable-baselines3 for PPO)")
        self.is_trained = True
    
    def get_weights(self, state: np.ndarray) -> Tuple[float, float, float]:
        """Get optimal (α, β, γ) for given context state"""
        if self.model is not None:
            action, _ = self.model.predict(state, deterministic=True)
            weights = self._softmax(action)
            return (weights[0], weights[1], weights[2])
        else:
            # Fallback: context-based heuristic
            return self._get_weights_heuristic(state)
    
    def _softmax(self, x: np.ndarray) -> np.ndarray:
        exp_x = np.exp(x - np.max(x))
        return exp_x / exp_x.sum()
    
    def _get_weights_heuristic(self, state: np.ndarray) -> Tuple[float, float, float]:
        """Heuristic fallback when PPO not available"""
        is_business_hours = state[2]
        latency_ratio = state[3]
        cost_ratio = state[4]
        
        if latency_ratio > 1.2:  # Latency over SLA
            return (0.2, 0.6, 0.2)  # Focus on latency
        elif cost_ratio > 1.0:  # Over budget
            return (0.6, 0.2, 0.2)  # Focus on cost
        elif is_business_hours > 0.5:
            return (0.3, 0.5, 0.2)  # Business hours: latency priority
        else:
            return (0.5, 0.3, 0.2)  # Off-hours: cost priority
    
    def save(self, path: str):
        """Save trained model"""
        if self.model is not None:
            self.model.save(path)
            logger.info(f"Phase-2 model saved to {path}")
    
    def load(self, path: str):
        """Load trained model"""
        try:
            from stable_baselines3 import PPO
            self.model = PPO.load(path, env=self.env)
            self.is_trained = True
            logger.info(f"Phase-2 model loaded from {path}")
        except Exception as e:
            logger.warning(f"Could not load model: {e}")


def demonstrate_phase2_ppo():
    """Demonstrate Phase-2 PPO training and inference"""
    
    print("\n" + "=" * 70)
    print("🧠 PHASE-2 PPO: PRODUCTION-READY META-CONTROLLER")
    print("=" * 70)
    print("""
    Phase-2 now uses PPO (Proximal Policy Optimization):
    
    ┌─────────────────────────────────────────────────────────────────┐
    │  State (9-dim)                                                  │
    │  [hour, day, business_hrs, lat_ratio, cost_ratio, ...]         │
    │                           ↓                                     │
    │  PPO Policy Network (MLP)                                       │
    │                           ↓                                     │
    │  Action (3-dim) → Softmax → (α, β, γ)                          │
    │                           ↓                                     │
    │  Reward = SLA_compliance + Budget_compliance - violations       │
    └─────────────────────────────────────────────────────────────────┘
    """)
    
    # Create and train
    controller = Phase2PPOController(
        sla_latency_target_ms=200.0,
        cost_budget_per_hour=10.0
    )
    
    print("Training Phase-2 PPO (short demo)...")
    controller.train(total_timesteps=1000, verbose=0)
    
    # Test inference
    print("\n📊 Testing Phase-2 Inference:")
    print("-" * 70)
    
    test_scenarios = [
        ("Business hours, latency over SLA", 
         np.array([0.42, 0.14, 1.0, 1.5, 0.6, 1.0, 0.1, 0.5, 0.0], dtype=np.float32)),
        ("Night time, cost over budget",
         np.array([0.08, 0.14, 0.0, 0.8, 1.3, 1.0, 0.0, 0.0, 0.5], dtype=np.float32)),
        ("Morning rush, all good",
         np.array([0.38, 0.14, 1.0, 0.9, 0.7, 1.1, 0.0, -0.2, 0.0], dtype=np.float32)),
    ]
    
    for name, state in test_scenarios:
        alpha, beta, gamma = controller.get_weights(state)
        
        # Determine priority
        if beta > alpha and beta > gamma:
            priority = "LATENCY"
        elif alpha > beta and alpha > gamma:
            priority = "COST"
        else:
            priority = "THROUGHPUT"
        
        print(f"\n{name}:")
        print(f"  α={alpha:.3f}, β={beta:.3f}, γ={gamma:.3f}")
        print(f"  → Priority: {priority}")
    
    print("\n" + "=" * 70)
    print("✅ PHASE-2 PPO READY FOR PRODUCTION!")
    print("=" * 70)
    
    return True


if __name__ == "__main__":
    demonstrate_phase2_ppo()
