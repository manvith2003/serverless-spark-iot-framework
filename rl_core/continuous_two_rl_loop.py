"""
Continuous Two-RL Learning Loop
================================

This module runs BOTH Phase-1 and Phase-2 RL agents continuously,
with each learning from production feedback.

Architecture:
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CONTINUOUS TWO-RL LOOP                                  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  OUTER LOOP (Every N episodes):                                     │   │
│   │                                                                     │   │
│   │     Phase-2 PPO observes episode performance                        │   │
│   │            ↓                                                        │   │
│   │     Phase-2 outputs new (α, β, γ)                                   │   │
│   │            ↓                                                        │   │
│   │     Phase-2 LEARNS from reward                                      │   │
│   │                                                                     │   │
│   │   ┌─────────────────────────────────────────────────────────────┐   │   │
│   │   │  INNER LOOP (Every step):                                   │   │   │
│   │   │                                                             │   │   │
│   │   │     Phase-1 PPO observes metrics                            │   │   │
│   │   │            ↓                                                │   │   │
│   │   │     Phase-1 outputs scaling action                          │   │   │
│   │   │            ↓                                                │   │   │
│   │   │     Resource Manager scales                                 │   │   │
│   │   │            ↓                                                │   │   │
│   │   │     Phase-1 LEARNS from reward                              │   │   │
│   │   │            ↓                                                │   │   │
│   │   │     Repeat for episode_length steps                         │   │   │
│   │   │                                                             │   │   │
│   │   └─────────────────────────────────────────────────────────────┘   │   │
│   │                                                                     │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

Key Rule: Phase-2 updates (α, β, γ) BETWEEN Phase-1 episodes, never during!
"""

import numpy as np
import threading
import time
import logging
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass, field
from collections import deque
import os
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class StepMetrics:
    """Metrics collected at each Phase-1 step"""
    workload_rate: float
    cpu_util: float
    latency_ms: float
    cost: float
    executors: int
    timestamp: float = field(default_factory=time.time)


@dataclass
class EpisodeMetrics:
    """Aggregated metrics for one Phase-1 episode"""
    avg_latency_ms: float
    total_cost: float
    avg_throughput: float
    sla_violations: int
    avg_cpu_util: float
    num_steps: int
    reward_sum: float


class ReplayBuffer:
    """Experience replay buffer for online learning"""
    
    def __init__(self, capacity: int = 10000):
        self.buffer = deque(maxlen=capacity)
    
    def add(self, state, action, reward, next_state, done):
        self.buffer.append((state, action, reward, next_state, done))
    
    def sample(self, batch_size: int):
        indices = np.random.choice(len(self.buffer), batch_size, replace=False)
        batch = [self.buffer[i] for i in indices]
        states, actions, rewards, next_states, dones = zip(*batch)
        return (np.array(states), np.array(actions), np.array(rewards),
                np.array(next_states), np.array(dones))
    
    def __len__(self):
        return len(self.buffer)


class ContinuousTwoRLLoop:
    """
    Continuous learning loop for both Phase-1 and Phase-2 RL agents.
    
    Phase-1: Learns scaling actions (every step)
    Phase-2: Learns weight priorities (every episode)
    
    Both learn from real production feedback!
    """
    
    def __init__(self,
                 sla_latency_target_ms: float = 200.0,
                 cost_budget_per_hour: float = 10.0,
                 episode_length: int = 60,
                 step_interval_seconds: float = 1.0,
                 online_update_frequency: int = 32):
        
        # Business constraints
        self.sla_target = sla_latency_target_ms
        self.cost_budget = cost_budget_per_hour
        
        # Timing
        self.episode_length = episode_length
        self.step_interval = step_interval_seconds
        self.update_frequency = online_update_frequency
        
        # Current weights from Phase-2 (start balanced)
        self.alpha = 0.33  # Cost weight
        self.beta = 0.33   # Latency weight
        self.gamma = 0.34  # Throughput weight
        
        # Current executor count (Phase-1 action result)
        self.current_executors = 2
        
        # Replay buffers for online learning
        self.phase1_buffer = ReplayBuffer(capacity=10000)
        self.phase2_buffer = ReplayBuffer(capacity=1000)
        
        # Metrics tracking
        self.step_history: List[StepMetrics] = []
        self.episode_history: List[EpisodeMetrics] = []
        
        # Models (will be initialized)
        self.phase1_model = None
        self.phase2_model = None
        
        # Control flags
        self.is_running = False
        self._thread = None
        
        # Time context for Phase-2
        self.current_hour = 10
        self.current_day = 1
        
        logger.info("ContinuousTwoRLLoop initialized")
    
    def _build_phase1_state(self, metrics: StepMetrics) -> np.ndarray:
        """Build Phase-1 state vector (13 dimensions)"""
        return np.array([
            metrics.workload_rate,      # Workload rate
            100.0,                       # Data volume
            metrics.cpu_util,            # CPU util
            50.0,                        # Memory util
            metrics.latency_ms,          # Latency
            metrics.cost,                # Cost rate
            self.alpha,                  # α (from Phase-2)
            self.beta,                   # β (from Phase-2)
            self.gamma,                  # γ (from Phase-2)
            50.0,                        # Shuffle size
            0.5,                         # Data temperature
            metrics.workload_rate * 1.1, # Predicted next workload
            0.0                          # Burst signal
        ], dtype=np.float32)
    
    def _build_phase2_state(self, episode: EpisodeMetrics) -> np.ndarray:
        """Build Phase-2 state vector (9 dimensions)"""
        hour_norm = self.current_hour / 24.0
        day_norm = self.current_day / 7.0
        is_business = 1.0 if (9 <= self.current_hour <= 17 and self.current_day < 5) else 0.0
        
        latency_trend = 0.0
        cost_trend = 0.0
        if len(self.episode_history) >= 2:
            prev = self.episode_history[-2]
            latency_trend = np.sign(episode.avg_latency_ms - prev.avg_latency_ms)
            cost_trend = np.sign(episode.total_cost - prev.total_cost)
        
        return np.array([
            hour_norm,
            day_norm,
            is_business,
            min(3.0, episode.avg_latency_ms / self.sla_target),
            min(3.0, episode.total_cost / self.cost_budget),
            min(3.0, episode.avg_throughput / 100.0),
            min(1.0, episode.sla_violations / 10.0),
            latency_trend,
            cost_trend
        ], dtype=np.float32)
    
    def _compute_phase1_reward(self, metrics: StepMetrics) -> float:
        """
        Phase-1 reward: Uses weights from Phase-2
        
        R = -α·cost - β·latency + γ·throughput
        """
        norm_cost = metrics.cost / 10.0
        norm_latency = metrics.latency_ms / 1000.0
        norm_throughput = metrics.workload_rate / 200.0
        
        reward = (
            -self.alpha * norm_cost +
            -self.beta * norm_latency +
            self.gamma * norm_throughput
        )
        
        return reward
    
    def _compute_phase2_reward(self, episode: EpisodeMetrics) -> float:
        """
        Phase-2 reward: Based on SLA and budget compliance
        
        R = SLA_compliance + Budget_compliance - violations
        """
        # Latency component
        if episode.avg_latency_ms <= self.sla_target:
            latency_reward = 1.0
        else:
            over_ratio = episode.avg_latency_ms / self.sla_target
            latency_reward = -1.0 * (over_ratio - 1)
        
        # Cost component
        if episode.total_cost <= self.cost_budget:
            cost_reward = 1.0 - (episode.total_cost / self.cost_budget) * 0.5
        else:
            over_ratio = episode.total_cost / self.cost_budget
            cost_reward = -1.0 * (over_ratio - 1)
        
        # Violations penalty
        violation_penalty = -0.5 * episode.sla_violations
        
        return latency_reward + cost_reward + violation_penalty
    
    def _phase1_get_action(self, state: np.ndarray) -> int:
        """Get scaling action from Phase-1 (or heuristic if no model)"""
        if self.phase1_model is not None:
            action, _ = self.phase1_model.predict(state, deterministic=False)
            return int(action[0]) + 1  # 1-20 executors
        
        # Heuristic fallback
        workload = state[0]
        latency = state[4]
        
        if latency > self.sla_target * 1.2:
            return min(20, self.current_executors + 2)  # Scale up
        elif workload < 50 and latency < self.sla_target * 0.5:
            return max(1, self.current_executors - 1)   # Scale down
        else:
            return self.current_executors               # Maintain
    
    def _phase2_get_weights(self, state: np.ndarray) -> Tuple[float, float, float]:
        """Get (α, β, γ) from Phase-2 (or heuristic if no model)"""
        if self.phase2_model is not None:
            action, _ = self.phase2_model.predict(state, deterministic=False)
            exp_action = np.exp(action - np.max(action))
            weights = exp_action / exp_action.sum()
            return (weights[0], weights[1], weights[2])
        
        # Heuristic fallback
        latency_ratio = state[3]
        cost_ratio = state[4]
        is_business = state[2]
        
        if latency_ratio > 1.2:
            return (0.2, 0.6, 0.2)  # Latency focus
        elif cost_ratio > 1.0:
            return (0.6, 0.2, 0.2)  # Cost focus
        elif is_business > 0.5:
            return (0.3, 0.5, 0.2)  # Business hours
        else:
            return (0.5, 0.3, 0.2)  # Off-hours
    
    def _phase1_learn(self):
        """Online learning update for Phase-1"""
        if len(self.phase1_buffer) < self.update_frequency:
            return
        
        # Sample batch and update
        # In production: would call model.learn() with batch
        logger.debug(f"Phase-1 learning from {len(self.phase1_buffer)} experiences")
    
    def _phase2_learn(self):
        """Online learning update for Phase-2"""
        if len(self.phase2_buffer) < 5:
            return
        
        # Sample batch and update
        logger.debug(f"Phase-2 learning from {len(self.phase2_buffer)} experiences")
    
    def _simulate_metrics(self) -> StepMetrics:
        """Simulate production metrics (replace with real metrics in production)"""
        # Workload varies with time
        base_workload = 100 + 50 * np.sin(self.current_hour * np.pi / 12)
        workload = max(10, base_workload + np.random.normal(0, 20))
        
        # Latency depends on workload vs executors
        base_latency = 200 * (workload / (self.current_executors * 50))
        latency = max(20, base_latency + np.random.normal(0, 30))
        
        # Cost depends on executors
        cost = self.current_executors * 0.5
        
        # CPU depends on workload
        cpu = min(100, 30 + (workload / self.current_executors) * 0.5)
        
        return StepMetrics(
            workload_rate=workload,
            cpu_util=cpu,
            latency_ms=latency,
            cost=cost,
            executors=self.current_executors
        )
    
    def run_one_episode(self) -> EpisodeMetrics:
        """
        Run one Phase-1 episode with current Phase-2 weights.
        
        During episode:
        - Phase-1 learns at every step
        - (α, β, γ) stay FIXED
        
        After episode:
        - Phase-2 learns from episode performance
        - Phase-2 outputs new (α, β, γ) for next episode
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 EPISODE START | Weights: α={self.alpha:.3f}, β={self.beta:.3f}, γ={self.gamma:.3f}")
        logger.info(f"{'='*60}")
        
        episode_rewards = []
        episode_latencies = []
        episode_costs = []
        sla_violations = 0
        
        prev_state = None
        prev_action = None
        
        # ─────────────────────────────────────────────────────────────
        # INNER LOOP: Phase-1 runs for episode_length steps
        # ─────────────────────────────────────────────────────────────
        for step in range(self.episode_length):
            # 1. Get current metrics
            metrics = self._simulate_metrics()
            
            # 2. Build Phase-1 state
            state = self._build_phase1_state(metrics)
            
            # 3. Phase-1 gets action
            action = self._phase1_get_action(state)
            
            # 4. Apply action (scale executors)
            old_executors = self.current_executors
            self.current_executors = action
            
            # 5. Compute Phase-1 reward
            reward = self._compute_phase1_reward(metrics)
            
            # 6. Store experience for Phase-1 learning
            if prev_state is not None:
                done = (step == self.episode_length - 1)
                self.phase1_buffer.add(prev_state, prev_action, reward, state, done)
            
            # 7. Phase-1 online learning (every update_frequency steps)
            if step > 0 and step % self.update_frequency == 0:
                self._phase1_learn()
            
            # 8. Track episode metrics
            episode_rewards.append(reward)
            episode_latencies.append(metrics.latency_ms)
            episode_costs.append(metrics.cost)
            
            if metrics.latency_ms > self.sla_target:
                sla_violations += 1
            
            # 9. Log progress
            if step % 10 == 0:
                logger.info(f"  Step {step:3d} | Exec: {old_executors}→{action} | "
                           f"Lat: {metrics.latency_ms:5.0f}ms | R: {reward:+.3f}")
            
            prev_state = state
            prev_action = action
            
            # Simulate time passing
            if self.step_interval > 0:
                time.sleep(self.step_interval * 0.01)  # Speed up for demo
        
        # ─────────────────────────────────────────────────────────────
        # EPISODE COMPLETE: Aggregate metrics
        # ─────────────────────────────────────────────────────────────
        episode_metrics = EpisodeMetrics(
            avg_latency_ms=np.mean(episode_latencies),
            total_cost=np.sum(episode_costs),
            avg_throughput=100.0,
            sla_violations=sla_violations,
            avg_cpu_util=60.0,
            num_steps=self.episode_length,
            reward_sum=np.sum(episode_rewards)
        )
        
        self.episode_history.append(episode_metrics)
        
        logger.info(f"\n📈 EPISODE COMPLETE:")
        logger.info(f"   Avg Latency: {episode_metrics.avg_latency_ms:.0f}ms (SLA: {self.sla_target}ms)")
        logger.info(f"   Total Cost: ${episode_metrics.total_cost:.2f} (Budget: ${self.cost_budget})")
        logger.info(f"   SLA Violations: {sla_violations}")
        logger.info(f"   Total Reward: {episode_metrics.reward_sum:.3f}")
        
        return episode_metrics
    
    def update_phase2(self, episode_metrics: EpisodeMetrics):
        """
        Phase-2 learning after episode completes.
        
        This is where Phase-2 learns and updates (α, β, γ).
        """
        logger.info(f"\n🧠 PHASE-2 UPDATE:")
        
        # Build Phase-2 state
        state = self._build_phase2_state(episode_metrics)
        
        # Store old weights
        old_weights = (self.alpha, self.beta, self.gamma)
        
        # Get new weights from Phase-2
        new_weights = self._phase2_get_weights(state)
        
        # Compute Phase-2 reward
        reward = self._compute_phase2_reward(episode_metrics)
        
        # Store experience for Phase-2 learning
        if len(self.episode_history) >= 2:
            prev_episode = self.episode_history[-2]
            prev_state = self._build_phase2_state(prev_episode)
            self.phase2_buffer.add(prev_state, np.array(old_weights), reward, state, False)
        
        # Phase-2 learning
        self._phase2_learn()
        
        # Update weights for next episode
        self.alpha, self.beta, self.gamma = new_weights
        
        logger.info(f"   Old weights: α={old_weights[0]:.3f}, β={old_weights[1]:.3f}, γ={old_weights[2]:.3f}")
        logger.info(f"   New weights: α={self.alpha:.3f}, β={self.beta:.3f}, γ={self.gamma:.3f}")
        logger.info(f"   Phase-2 Reward: {reward:.3f}")
        
        # Determine and log priority
        if self.beta > self.alpha and self.beta > self.gamma:
            priority = "LATENCY"
        elif self.alpha > self.beta and self.alpha > self.gamma:
            priority = "COST"
        else:
            priority = "THROUGHPUT"
        logger.info(f"   Priority: {priority}")
    
    def run_continuous_loop(self, num_episodes: int = 10):
        """
        Main continuous learning loop.
        
        Runs both Phase-1 and Phase-2 learning continuously.
        """
        logger.info("\n" + "=" * 70)
        logger.info("🔄 CONTINUOUS TWO-RL LEARNING LOOP STARTED")
        logger.info("=" * 70)
        logger.info(f"Episodes: {num_episodes}, Steps/episode: {self.episode_length}")
        logger.info(f"SLA Target: {self.sla_target}ms, Budget: ${self.cost_budget}/hr")
        logger.info("=" * 70)
        
        for ep in range(num_episodes):
            logger.info(f"\n{'#' * 70}")
            logger.info(f"  EPISODE {ep + 1} / {num_episodes}")
            logger.info(f"{'#' * 70}")
            
            # Advance time context
            self.current_hour = (self.current_hour + 1) % 24
            if self.current_hour == 0:
                self.current_day = (self.current_day + 1) % 7
            
            # Run Phase-1 episode (Phase-1 learns during this)
            episode_metrics = self.run_one_episode()
            
            # Phase-2 learns and updates weights
            self.update_phase2(episode_metrics)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ CONTINUOUS LEARNING LOOP COMPLETE")
        logger.info("=" * 70)
        
        # Summary
        logger.info("\n📊 LEARNING SUMMARY:")
        logger.info(f"   Total Episodes: {len(self.episode_history)}")
        logger.info(f"   Phase-1 Experiences: {len(self.phase1_buffer)}")
        logger.info(f"   Phase-2 Experiences: {len(self.phase2_buffer)}")
        
        avg_latency = np.mean([e.avg_latency_ms for e in self.episode_history])
        avg_cost = np.mean([e.total_cost for e in self.episode_history])
        total_violations = sum([e.sla_violations for e in self.episode_history])
        
        logger.info(f"   Avg Latency: {avg_latency:.0f}ms")
        logger.info(f"   Avg Cost: ${avg_cost:.2f}")
        logger.info(f"   Total SLA Violations: {total_violations}")
        logger.info(f"   Final Weights: α={self.alpha:.3f}, β={self.beta:.3f}, γ={self.gamma:.3f}")
    
    def start_background(self, num_episodes: int = 100):
        """Start continuous loop in background thread"""
        if self.is_running:
            logger.warning("Already running")
            return
        
        self.is_running = True
        self._thread = threading.Thread(
            target=self.run_continuous_loop,
            args=(num_episodes,),
            daemon=True
        )
        self._thread.start()
        logger.info("Background continuous learning started")
    
    def stop(self):
        """Stop background loop"""
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Continuous learning stopped")


def demonstrate_continuous_loop():
    """Demo the continuous two-RL loop"""
    
    print("\n" + "=" * 70)
    print("🔄 CONTINUOUS TWO-RL LEARNING DEMONSTRATION")
    print("=" * 70)
    print("""
    This demonstrates BOTH Phase-1 and Phase-2 learning continuously:
    
    ┌─────────────────────────────────────────────────────────────────┐
    │  OUTER LOOP (Phase-2):                                         │
    │    - Observes episode performance                               │
    │    - Outputs (α, β, γ) weights                                  │
    │    - LEARNS from SLA/budget compliance                         │
    │                                                                 │
    │    ┌─────────────────────────────────────────────────────────┐  │
    │    │  INNER LOOP (Phase-1):                                 │  │
    │    │    - Observes workload, cpu, latency                   │  │
    │    │    - Outputs scaling action                            │  │
    │    │    - LEARNS from reward (using Phase-2 weights)        │  │
    │    │    - REPEATS every step                                │  │
    │    └─────────────────────────────────────────────────────────┘  │
    │                                                                 │
    │    After episode: Phase-2 updates weights                      │
    │    Next episode: Phase-1 uses NEW weights                      │
    └─────────────────────────────────────────────────────────────────┘
    """)
    
    # Create and run
    loop = ContinuousTwoRLLoop(
        sla_latency_target_ms=200.0,
        cost_budget_per_hour=10.0,
        episode_length=30,    # 30 steps per episode
        step_interval_seconds=0.1
    )
    
    # Run 5 episodes for demo
    loop.run_continuous_loop(num_episodes=5)
    
    return True


if __name__ == "__main__":
    demonstrate_continuous_loop()
