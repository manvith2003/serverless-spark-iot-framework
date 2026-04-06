"""
End-to-End Flow Demo
=====================
Shows the full pipeline running live in the console:

  IoT Devices → Kafka → RL Agent 1 (Meta-Controller)
             ↓                        ↓ α,β,γ weights
        Pattern Extractor → RL Agent 2 (Resource Agent)
                                        ↓ scaling decision
                                  OpenFaaS Function
                                        ↓ scale_up/down/pre_warm
                               Spark Executors
                                        ↓ reward feedback
                             RL Agent 1 ← (loop)
"""

import time
import json
import math
import random
import sys

# ─── ANSI colour helpers ───────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

C_IOT     = "\033[94m"   # bright blue
C_KAFKA   = "\033[95m"   # bright magenta
C_META    = "\033[96m"   # bright cyan
C_RES     = "\033[92m"   # bright green
C_OFAAS   = "\033[93m"   # bright yellow
C_SPARK   = "\033[91m"   # bright red
C_REWARD  = "\033[35m"   # purple
C_OK      = "\033[32m"
C_WARN    = "\033[33m"
C_HEADER  = "\033[97m"   # white

def col(color, text):
    return f"{color}{BOLD}{text}{RESET}"

def label(tag, color, width=24):
    padded = f"[{tag}]".ljust(width)
    return f"{color}{BOLD}{padded}{RESET}"

def arrow(label_text=""):
    return f"  {DIM}{'─'*4}▶{RESET}  {DIM}{label_text}{RESET}"

def section(title):
    bar = "═" * 62
    print(f"\n{C_HEADER}{BOLD}╔{bar}╗")
    print(f"║  {title.center(60)}  ║")
    print(f"╚{bar}╝{RESET}\n")

def pause(seconds=0.4):
    time.sleep(seconds)

def softmax(arr):
    e = [math.exp(x - max(arr)) for x in arr]
    s = sum(e)
    return [round(x / s, 3) for x in e]

# ─── Simulated Components ──────────────────────────────────────────────────────

class IoTSimulator:
    """Generates synthetic IoT telemetry."""
    scenarios = [
        ("🏥 Healthcare Monitor", 280, 78),
        ("🚗 Autonomous Vehicle", 340, 85),
        ("🔋 Smart Grid Sensor",   80, 30),
        ("🌡️  Industrial IoT",     190, 62),
        ("📈 Traffic Spike Burst", 450, 92),
    ]

    def __init__(self):
        self.step = 0

    def emit(self):
        name, base_rate, base_cpu = self.scenarios[self.step % len(self.scenarios)]
        noise = random.randint(-20, 20)
        msg = {
            "scenario":    name,
            "workload_rate": max(10, base_rate + noise),
            "cpu_util":      min(100, base_cpu + random.randint(-5, 5)),
            "mem_util":      random.randint(40, 75),
            "latency_ms":    random.randint(80, 600),
            "timestamp":     round(time.time(), 2),
        }
        self.step += 1
        return msg


class FakeKafka:
    """Simulated Kafka topic."""
    def __init__(self):
        self._queue = []

    def produce(self, msg):
        self._queue.append(msg)

    def consume(self):
        return self._queue.pop(0) if self._queue else None


class PatternExtractor:
    """Extracts 7 temporal features from the workload stream."""
    def __init__(self):
        self._history = []

    def update(self, workload):
        self._history.append(workload)
        if len(self._history) > 20:
            self._history.pop(0)

    def features(self):
        h = self._history
        if len(h) < 3:
            return dict(velocity=0, acceleration=0, rolling_mean=0, rolling_std=0,
                        burst_probability=0, hour_sin=0, hour_cos=1)
        velocity     = h[-1] - h[-2]
        acceleration = (h[-1] - h[-2]) - (h[-2] - h[-3])
        mean         = round(sum(h) / len(h), 1)
        std          = round((sum((x - mean)**2 for x in h) / len(h))**0.5, 1)
        burst_prob   = round(min(1.0, max(0.0, (h[-1] - mean) / (std + 1e-9) * 0.3)), 2)
        t            = time.localtime()
        hour_frac    = (t.tm_hour * 60 + t.tm_min) / 1440
        return {
            "velocity":         round(velocity, 1),
            "acceleration":     round(acceleration, 1),
            "rolling_mean":     mean,
            "rolling_std":      std,
            "burst_probability":burst_prob,
            "hour_sin":         round(math.sin(2 * math.pi * hour_frac), 3),
            "hour_cos":         round(math.cos(2 * math.pi * hour_frac), 3),
        }


class MetaController:
    """RL Agent 1 – adjusts reward weights (α, β, γ) between episodes."""
    def __init__(self):
        self.alpha = 0.33
        self.beta  = 0.33
        self.gamma = 0.34

    def update_weights(self, latency_ms, cost, sla_target=300):
        """Heuristic policy (stands in for trained TD3/PPO)."""
        t = time.localtime()
        is_business = 9 <= t.tm_hour <= 17
        latency_over = latency_ms > sla_target
        cost_over    = cost > 10.0

        if latency_over:
            raw = [0.5, 2.0, 0.5]        # focus: latency
        elif cost_over:
            raw = [2.0, 0.5, 0.5]        # focus: cost
        elif is_business:
            raw = [0.8, 1.6, 0.6]        # business hours: latency priority
        else:
            raw = [1.6, 0.8, 0.6]        # off-hours: cost priority

        w = softmax(raw)
        self.alpha, self.beta, self.gamma = w
        return w

    def receive_reward(self, reward):
        """Phase-2 reward signal (in real system, triggers model.learn())."""
        pass


class ResourceAgent:
    """RL Agent 2 – decides executor count / storage tier."""
    def __init__(self):
        self.current_executors = 2

    def decide(self, metrics, pattern_features, alpha, beta, gamma):
        workload      = metrics["workload_rate"]
        cpu           = metrics["cpu_util"]
        latency       = metrics["latency_ms"]
        burst_prob    = pattern_features["burst_probability"]
        velocity      = pattern_features["velocity"]

        if burst_prob > 0.6 and velocity > 5:
            action = "pre_warm"
            target = min(20, self.current_executors + 4)
        elif workload > 300 or cpu > 80:
            action = "scale_up"
            target = min(20, self.current_executors + 2)
        elif workload < 50 and cpu < 25:
            action = "scale_down"
            target = max(0, self.current_executors - 2)
        elif beta > 0.5 and latency > 400:
            action = "scale_up"
            target = min(20, self.current_executors + 3)
        elif alpha > 0.5 and workload < 150:
            action = "scale_down"
            target = max(1, self.current_executors - 1)
        else:
            action = "maintain"
            target = self.current_executors

        tier = "HOT 🔴" if workload > 250 else ("WARM 🟠" if workload > 100 else "COLD ⚫")
        decision = {"action": action, "target_executors": target, "storage_tier": tier, "confidence": 0.88}
        self.current_executors = target
        return decision


class OpenFaaSFunction:
    """Serverless rl-scaling-function handler."""
    def execute(self, decision):
        return {
            "executed_action": decision["action"],
            "new_executor_count": decision["target_executors"],
            "storage_tier": decision["storage_tier"],
            "status": "✅ success",
            "function": "rl-scaling-function",
        }


class SparkCluster:
    """Simulated Spark cluster that applies scaling and returns reward."""
    def __init__(self):
        self.executors = 2
        self.total_cost = 0.0

    def apply(self, result):
        self.executors = result["new_executor_count"]
        self.total_cost += self.executors * 0.5   # $0.5 per executor per step

    def compute_reward(self, metrics):
        latency   = metrics["latency_ms"]
        sla_ok    = latency < 300
        cost_ok   = self.total_cost < 50
        reward    = (1.0 if sla_ok else -0.5) + (1.0 if cost_ok else -0.5)
        reward   += random.uniform(-0.1, 0.1)
        return round(reward, 3), self.total_cost


# ─── Main Run Loop ─────────────────────────────────────────────────────────────

def run_pipeline(steps=5):

    section("END-TO-END PIPELINE DEMO  |  Serverless Spark IoT Framework")
    print(f"  Running {steps} steps. Each step = one decision cycle.\n")
    print(f"  {DIM}IoT Devices → Kafka → RL-1 (Meta) → RL-2 (Resource) → OpenFaaS → Spark{RESET}\n")
    time.sleep(1)

    # Instantiate all components
    iot        = IoTSimulator()
    kafka      = FakeKafka()
    extractor  = PatternExtractor()
    meta       = MetaController()
    resource   = ResourceAgent()
    openfaas   = OpenFaaSFunction()
    spark      = SparkCluster()

    cumulative_reward = 0.0

    for step in range(1, steps + 1):

        step_bar = f"STEP {step} / {steps}"
        print(f"\n{C_HEADER}{'─'*64}")
        print(f"  {BOLD}{step_bar}{RESET}")
        print(f"{C_HEADER}{'─'*64}{RESET}")
        time.sleep(0.3)

        # ── 1. IoT Devices emit telemetry ──────────────────────────────────────
        msg = iot.emit()
        print(f"\n{label('IoT Devices', C_IOT)}  📡  Scenario: {col(C_IOT, msg['scenario'])}")
        print(f"{'':26}Workload: {col(C_IOT, msg['workload_rate'])} msg/s  "
              f"CPU: {col(C_IOT, msg['cpu_util'])}%  "
              f"Latency: {col(C_IOT, msg['latency_ms'])}ms")
        pause(0.5)

        print(arrow("IoT Telemetry Stream"))

        # ── 2. Kafka ingests the message ───────────────────────────────────────
        kafka.produce(msg)
        consumed = kafka.consume()
        print(f"\n{label('Apache Kafka', C_KAFKA)}  ⚡  Message ingested & consumed from topic")
        print(f"{'':26}{DIM}workload={consumed['workload_rate']} | cpu={consumed['cpu_util']} | ts={consumed['timestamp']}{RESET}")
        pause(0.5)

        extractor.update(consumed["workload_rate"])
        pat = extractor.features()
        print(f"{'':26}Pattern features extracted:")
        print(f"{'':26}  velocity={col(C_KAFKA, pat['velocity'])}  "
              f"burst_prob={col(C_KAFKA, pat['burst_probability'])}  "
              f"rolling_mean={col(C_KAFKA, pat['rolling_mean'])}")
        pause(0.4)

        print(arrow("Metrics + Pattern Features"))

        # ── 3. RL Agent 1: Meta-Controller sets weights ────────────────────────
        weights = meta.update_weights(consumed["latency_ms"], spark.total_cost)
        a, b, g = weights
        print(f"\n{label('RL Agent 1', C_META)}  🧠  Meta-Controller (TD3/PPO) — phase 2")

        # Pick priority label
        if b > a and b > g:
            pri = col(C_WARN, "LATENCY PRIORITY")
        elif a > b and a > g:
            pri = col(C_OK, "COST PRIORITY")
        else:
            pri = col(C_META, "THROUGHPUT PRIORITY")

        print(f"{'':26}α (cost)={col(C_META, a)}  β (latency)={col(C_META, b)}  γ (throughput)={col(C_META, g)}")
        print(f"{'':26}→ Current focus: {pri}")
        pause(0.5)

        print(arrow("α, β, γ  weights"))

        # ── 4. RL Agent 2: Resource Agent decides ─────────────────────────────
        decision = resource.decide(consumed, pat, a, b, g)
        action_col = {
            "pre_warm":   C_META,
            "scale_up":   C_WARN,
            "scale_down": C_OK,
            "maintain":   DIM,
        }.get(decision["action"], C_HEADER)

        print(f"\n{label('RL Agent 2', C_RES)}  🤖  Resource Agent (PPO) — phase 1")
        print(f"{'':26}Action    : {col(action_col, decision['action'].upper())}")
        print(f"{'':26}Executors : {col(C_RES, decision['target_executors'])}  "
              f"Storage: {decision['storage_tier']}  "
              f"Confidence: {col(C_RES, decision['confidence'])}")
        pause(0.5)

        print(arrow("Scaling Decision"))

        # ── 5. OpenFaaS executes the function ─────────────────────────────────
        result = openfaas.execute(decision)
        print(f"\n{label('OpenFaaS', C_OFAAS)}  λ  rl-scaling-function invoked")
        print(f"{'':26}Action executed : {col(C_OFAAS, result['executed_action'])}")
        print(f"{'':26}New executors   : {col(C_OFAAS, result['new_executor_count'])}")
        print(f"{'':26}Status          : {result['status']}")
        pause(0.5)

        print(arrow("scale_up / scale_down / pre_warm"))

        # ── 6. Spark Executors apply the scaling ──────────────────────────────
        spark.apply(result)
        reward, total_cost = spark.compute_reward(consumed)
        cumulative_reward += reward

        sla_status = col(C_OK, "✅ SLA OK") if consumed["latency_ms"] < 300 else col(C_SPARK, "❌ SLA BREACH")
        print(f"\n{label('Spark Executors', C_SPARK)}  🔥  Cluster scaled")
        print(f"{'':26}Active executors: {col(C_SPARK, spark.executors)}")
        print(f"{'':26}SLA status      : {sla_status}  ({consumed['latency_ms']}ms)")
        print(f"{'':26}Total cost      : {col(C_SPARK, f'${total_cost:.2f}')}")
        pause(0.5)

        # ── 7. Reward feedback sent back to Meta-Controller ───────────────────
        meta.receive_reward(reward)
        reward_col = C_OK if reward > 0 else C_SPARK
        print(f"\n{label('Reward Feedback', C_REWARD)}  🔄  Sent back to RL Agent 1")
        print(f"{'':26}Step reward      : {col(reward_col, reward)}")
        print(f"{'':26}Cumulative reward: {col(C_REWARD, round(cumulative_reward, 3))}")

        time.sleep(0.8)

    # ── Summary ───────────────────────────────────────────────────────────────
    section("PIPELINE RUN COMPLETE")
    print(f"  Steps run          : {col(C_HEADER, steps)}")
    print(f"  Final executors    : {col(C_SPARK,  spark.executors)}")
    print(f"  Total cluster cost : {col(C_WARN,   f'${spark.total_cost:.2f}')}")
    print(f"  Cumulative reward  : {col(C_OK if cumulative_reward > 0 else C_SPARK, round(cumulative_reward, 3))}")
    print(f"\n  {DIM}Loop: IoT → Kafka → RL-1 → RL-2 → OpenFaaS → Spark → Reward → RL-1{RESET}\n")


if __name__ == "__main__":
    steps = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    try:
        run_pipeline(steps=steps)
    except KeyboardInterrupt:
        print(f"\n\n{C_WARN}Pipeline interrupted.{RESET}\n")
