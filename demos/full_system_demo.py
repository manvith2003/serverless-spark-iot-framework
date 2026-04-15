#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║            SERVERLESS SPARK IoT FRAMEWORK — FULL SYSTEM DEMO                   ║
║                                                                                  ║
║   IoT Sensors → MQTT → Kafka → Spark → RL-1 (PPO) → RL-2 (TD3 Meta)           ║
║                                    ↓                                             ║
║                              OpenFaaS (rl-scaling-function)                     ║
║                                    ↓                                             ║
║           Spark Executors ← scale_up / scale_down / pre_warm                   ║
║                                    ↓                                             ║
║               🚨 SPIKE ALERT  if burst detected or SLA breached                ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Run:
    python3 demos/full_system_demo.py
    python3 demos/full_system_demo.py --steps 10   (more steps)
    python3 demos/full_system_demo.py --fast        (no sleep delays)
"""

import time
import math
import random
import argparse
import sys
from datetime import datetime

# ─── ANSI colours ──────────────────────────────────────────────────────────────
R = "\033[0m"
BOLD = "\033[1m"
DIM  = "\033[2m"
BLK  = "\033[30m"
RED  = "\033[91m"
GRN  = "\033[92m"
YLW  = "\033[93m"
BLU  = "\033[94m"
MAG  = "\033[95m"
CYN  = "\033[96m"
WHT  = "\033[97m"
BGRED  = "\033[41m"
BGYLW  = "\033[43m"
BGGRN  = "\033[42m"
BGBLU  = "\033[44m"
BGMAG  = "\033[45m"

def c(color, text):       return f"{color}{BOLD}{text}{R}"
def dim(text):            return f"{DIM}{text}{R}"
def lbl(tag, color, w=28): return f"{color}{BOLD}{'['+tag+']':<{w}}{R}"
def arrow(msg=""):        return f"  {DIM}────▶  {msg}{R}"

def header(title):
    bar = "═" * 74
    print(f"\n{WHT}{BOLD}╔{bar}╗")
    print(f"║  {title.center(72)}  ║")
    print(f"╚{bar}╝{R}\n")

def section(title, color=CYN):
    print(f"\n{color}{BOLD}┌{'─'*72}┐")
    print(f"│  {title:<70}│")
    print(f"└{'─'*72}┘{R}")

def alert_box(kind, msg, color=RED, bg=BGRED):
    bar = "▓" * 74
    print(f"\n{bg}{BLK}{BOLD} {bar} {R}")
    print(f"{bg}{BLK}{BOLD}  🚨  {kind.upper():<68} {R}")
    print(f"{bg}{BLK}{BOLD}  ➤   {msg:<68} {R}")
    print(f"{bg}{BLK}{BOLD} {bar} {R}\n")

def info_box(kind, msg):
    print(f"\n{BGGRN}{BLK}{BOLD}  ✅  {kind.upper():<68} {R}")
    print(f"{BGGRN}{BLK}{BOLD}  ➤   {msg:<68} {R}\n")

def metric(name, val, color=BLU):
    print(f"{'':30}{DIM}├─{R} {name}: {color}{BOLD}{val}{R}")

def tick(fast):
    if not fast:
        time.sleep(0.45)

def arrow_anim(label, fast):
    if fast:
        print(arrow(label))
        return
    sys.stdout.write(f"  {DIM}")
    for ch in ["─", "──", "───", "────▶"]:
        sys.stdout.write(f"\r  {DIM}{ch}{R}  {label}")
        sys.stdout.flush()
        time.sleep(0.08)
    print(f"\r{arrow(label)}")

# ─── Simulated IoT Scenarios ───────────────────────────────────────────────────
SCENARIOS = [
    {"name": "🏥 Healthcare Monitor",  "base_rate": 110, "base_cpu": 35, "spike": False},
    {"name": "🚗 Autonomous Vehicle",  "base_rate": 340, "base_cpu": 82, "spike": False},
    {"name": "🔋 Smart Grid Sensor",   "base_rate":  80, "base_cpu": 28, "spike": False},
    {"name": "🌡  Industrial IoT",     "base_rate": 195, "base_cpu": 60, "spike": False},
    {"name": "📈 Traffic Spike Burst", "base_rate": 480, "base_cpu": 95, "spike": True },
    {"name": "🏭 Factory Floor",       "base_rate": 150, "base_cpu": 50, "spike": False},
    {"name": "⚡ Power Grid Surge",    "base_rate": 430, "base_cpu": 91, "spike": True },
]

def softmax(arr):
    e = [math.exp(x - max(arr)) for x in arr]
    s = sum(e)
    return [round(x / s, 3) for x in e]

# ─────────────────────────────────────────────────────────────────────────────
class FullSystemDemo:

    def __init__(self, fast=False):
        self.fast = fast
        # System state
        self.executors      = 2
        self.alpha          = 0.33   # cost weight
        self.beta           = 0.33   # latency weight
        self.gamma          = 0.34   # throughput weight
        self.total_cost     = 0.0
        self.cum_reward     = 0.0
        self.sla_violations = 0
        self.spike_alerts   = 0
        self.messages_total = 0
        self.step_idx       = 0
        # Pattern extractor state
        self._workload_hist = []

    # ── 1. IoT ─────────────────────────────────────────────────────────────────
    def step_iot(self):
        sc = SCENARIOS[self.step_idx % len(SCENARIOS)]
        noise = random.randint(-25, 25)
        rate  = max(10, sc["base_rate"] + noise)
        cpu   = min(99, sc["base_cpu"]  + random.randint(-6, 6))
        lat   = random.randint(60, 700)
        temp  = round(random.uniform(22, 48), 1)
        if sc["spike"]:
            temp = round(random.uniform(82, 145), 1)

        reading = {
            "scenario":    sc["name"],
            "workload_rate": rate,
            "cpu_util":      cpu,
            "latency_ms":    lat,
            "temperature_c": temp,
            "spike":         sc["spike"],
            "sensor_id":     f"sensor_{random.randint(1,200):03d}",
            "timestamp":     datetime.now().isoformat(),
        }
        sensors = random.randint(6, 15)
        self.messages_total += sensors

        print(f"\n{lbl('IoT Devices', BLU)}  📡  Scenario: {c(BLU, sc['name'])}")
        metric("Sensors active",  c(BLU, sensors))
        metric("Workload",        c(BLU, f"{rate} msg/s"))
        metric("CPU util",        c(YLW if cpu > 80 else BLU, f"{cpu}%"))
        metric("Latency",         c(RED if lat > 300 else BLU, f"{lat} ms"))
        metric("Temperature",     c(RED if sc["spike"] else BLU, f"{temp}°C"))
        metric("Timestamp",       dim(reading["timestamp"][:19]))

        if sc["spike"]:
            alert_box(
                "🌡  HEAT SPIKE DETECTED",
                f"Temp {temp}°C >> 80°C threshold on {reading['sensor_id']} | Preparing pre-warm...",
                color=BLK, bg=BGRED
            )
            self.spike_alerts += 1

        tick(self.fast)
        return reading, sensors

    # ── 2. MQTT ────────────────────────────────────────────────────────────────
    def step_mqtt(self, reading, sensors):
        topic = "iot/sensors/data"
        print(f"\n{lbl('MQTT Broker', MAG)}  📡  Publishing to broker")
        metric("Topic",           c(MAG, topic))
        metric("Messages",        c(MAG, f"{sensors} published"))
        metric("QoS",             dim("1 (at-least-once)"))
        metric("Status",          c(GRN, "✅ Delivered"))
        tick(self.fast)

    # ── 3. Kafka ───────────────────────────────────────────────────────────────
    def step_kafka(self, reading, sensors):
        partitions = random.randint(1, 4)
        offset = self.messages_total
        print(f"\n{lbl('Apache Kafka', YLW)}  ⚡  Ingesting stream")
        metric("Topic",           c(YLW, "iot-realtime-data"))
        metric("Partitions used", c(YLW, partitions))
        metric("Latest offset",   c(YLW, offset))
        metric("Msgs this step",  c(YLW, f"{sensors} ingested"))
        metric("Throughput",      c(YLW, f"{reading['workload_rate']} msg/s"))

        # Pattern features
        self._workload_hist.append(reading["workload_rate"])
        if len(self._workload_hist) > 20:
            self._workload_hist.pop(0)
        h = self._workload_hist
        velocity = round(h[-1] - h[-2], 1) if len(h) >= 2 else 0
        mean = round(sum(h) / len(h), 1)
        std  = round((sum((x-mean)**2 for x in h)/max(1,len(h)))**0.5, 1)
        burst_prob = round(min(1.0, max(0.0, (h[-1]-mean)/(std+1e-9)*0.3)), 2)

        pattern = {"velocity": velocity, "burst_prob": burst_prob,
                   "rolling_mean": mean, "rolling_std": std}
        print(f"{'':30}{DIM}  Pattern features extracted:{R}")
        metric("  Velocity",      c(CYN, velocity))
        metric("  Burst prob",    c(RED if burst_prob > 0.5 else CYN, burst_prob))
        metric("  Rolling mean",  c(CYN, mean))

        if burst_prob > 0.5:
            alert_box(
                "⚡ HIGH BURST PROBABILITY",
                f"burst_prob={burst_prob} | velocity={velocity} → Triggering PRE_WARM via OpenFaaS",
                color=BLK, bg=BGYLW
            )
            self.spike_alerts += 1

        tick(self.fast)
        return pattern

    # ── 4. Spark ───────────────────────────────────────────────────────────────
    def step_spark(self, reading):
        workload = reading["workload_rate"]
        base_lat = 200 * (workload / max(1, self.executors * 50))
        latency  = max(30, base_lat + random.uniform(-30, 30))
        cpu      = min(99, 30 + workload / max(1, self.executors) * 0.5)
        cost     = self.executors * 0.5

        sla_ok   = latency <= 300
        if not sla_ok:
            self.sla_violations += 1

        metrics = {"workload": workload, "latency": latency,
                   "cpu": cpu, "cost": cost, "throughput": workload * 0.9}

        print(f"\n{lbl('Apache Spark', RED)}  🔥  Processing stream batch")
        metric("Active executors", c(RED, self.executors))
        metric("Workload",         c(RED, f"{workload:.0f} msg/s"))
        metric("Latency",          c(RED if not sla_ok else GRN, f"{latency:.0f} ms"))
        metric("CPU util",         c(YLW if cpu > 70 else RED, f"{cpu:.0f}%"))
        metric("Cost/step",        c(YLW, f"${cost:.2f}"))
        metric("SLA (≤300ms)",     c(GRN, "✅ OK") if sla_ok else c(RED, "❌ BREACH"))

        if not sla_ok:
            alert_box(
                "🔴 SLA BREACH — LATENCY EXCEEDED",
                f"Latency {latency:.0f}ms > 300ms SLA | RL-2 will scale up immediately",
                color=BLK, bg=BGRED
            )

        tick(self.fast)
        return metrics

    # ── 5. RL-1 (PPO Resource Agent) ──────────────────────────────────────────
    def step_rl1(self, metrics, pattern):
        workload   = metrics["workload"]
        latency    = metrics["latency"]
        cpu        = metrics["cpu"]
        burst_prob = pattern["burst_prob"]
        velocity   = pattern["velocity"]

        # Policy logic
        if burst_prob > 0.5 and velocity > 5:
            action = "pre_warm";   target = min(20, self.executors + 4)
        elif workload > 320 or cpu > 82:
            action = "scale_up";   target = min(20, self.executors + 2)
        elif workload < 60 and cpu < 25:
            action = "scale_down"; target = max(1,  self.executors - 2)
        elif self.beta > 0.5 and latency > 400:
            action = "scale_up";   target = min(20, self.executors + 3)
        elif self.alpha > 0.5 and workload < 120:
            action = "scale_down"; target = max(1,  self.executors - 1)
        else:
            action = "maintain";   target = self.executors

        # Reward computation
        norm_cost     = metrics["cost"] / 10.0
        norm_latency  = metrics["latency"] / 1000.0
        norm_tput     = metrics["throughput"] / 200.0
        reward = (-self.alpha * norm_cost
                  - self.beta  * norm_latency
                  + self.gamma * norm_tput)
        reward = round(reward, 4)
        self.cum_reward += reward

        action_color = {"pre_warm": MAG, "scale_up": YLW,
                        "scale_down": GRN, "maintain": DIM}.get(action, WHT)

        print(f"\n{lbl('RL-1  PPO Agent', GRN)}  🤖  Resource Agent — Phase 1")
        metric("State vector",    dim(f"[load={workload:.0f}, lat={latency:.0f}, cpu={cpu:.0f}]"))
        metric("Action",          c(action_color, action.upper()))
        metric("Target executors",c(GRN, target))
        print(f"\n{'':30}{DIM}Reward breakdown:{R}")
        metric("  -α×cost",       c(GRN if -self.alpha*norm_cost > 0 else RED,
                                    f"{-self.alpha*norm_cost:+.4f}"))
        metric("  -β×latency",    c(GRN if -self.beta*norm_latency > 0 else RED,
                                    f"{-self.beta*norm_latency:+.4f}"))
        metric("  +γ×throughput", c(GRN, f"{self.gamma*norm_tput:+.4f}"))
        metric("  TOTAL reward",  c(GRN if reward > 0 else RED, f"{reward:+.4f}"))
        metric("  Cumulative",    c(CYN, f"{self.cum_reward:+.4f}"))

        tick(self.fast)
        return action, target

    # ── 6. OpenFaaS ────────────────────────────────────────────────────────────
    def step_openfaas(self, action, target):
        tier = ("HOT 🔴" if target > 8 else
                "WARM 🟠" if target > 3 else "COLD ⚫")

        print(f"\n{lbl('OpenFaaS', YLW)}  λ  rl-scaling-function invoked")
        metric("Function",        c(YLW, "rl-scaling-function"))
        metric("HTTP trigger",    dim("POST /function/rl-scaling-function"))
        metric("Action payload",  c(YLW, action))
        metric("Target executors",c(YLW, target))
        metric("Storage tier",    c(YLW, tier))
        metric("Status",          c(GRN, "✅ 200 OK — success"))
        metric("Scale-to-zero",   dim("control plane idle cost: $0.00"))

        tick(self.fast)
        return tier

    # ── 7. Spark applies scaling ───────────────────────────────────────────────
    def step_spark_scale(self, action, target, tier):
        old = self.executors
        self.executors = target
        self.total_cost += self.executors * 0.5

        delta = target - old
        delta_str = (f"+{delta} added 🚀" if delta > 0 else
                     f"{delta} removed 🛑" if delta < 0 else "no change ↔")

        print(f"\n{lbl('Spark Executors', RED)}  🔥  Scaling applied")
        metric("Previous executors", c(RED, old))
        metric("New executors",      c(GRN if delta > 0 else RED, target))
        metric("Delta",              c(YLW, delta_str))
        metric("Storage tier",       c(YLW, tier))
        metric("Cumulative cost",    c(YLW, f"${self.total_cost:.2f}"))

        tick(self.fast)

    # ── 8. RL-2 (TD3 Meta-Controller) ─────────────────────────────────────────
    def step_rl2(self, metrics):
        latency    = metrics["latency"]
        cost       = metrics["cost"]
        sla_target = 300.0

        latency_over = latency > sla_target
        cost_over    = cost > 10.0
        t = time.localtime()
        is_business  = 9 <= t.tm_hour <= 17

        if latency_over:
            raw = [0.5, 2.0, 0.5]
            focus = "LATENCY PRIORITY 🔴"
            fc = RED
        elif cost_over:
            raw = [2.0, 0.5, 0.5]
            focus = "COST PRIORITY 💰"
            fc = YLW
        elif is_business:
            raw = [0.8, 1.6, 0.6]
            focus = "BUSINESS HOURS — latency guard 🕘"
            fc = CYN
        else:
            raw = [1.6, 0.8, 0.6]
            focus = "OFF-PEAK — cost savings 🌙"
            fc = BLU

        old_a, old_b, old_g = self.alpha, self.beta, self.gamma
        self.alpha, self.beta, self.gamma = softmax(raw)

        meta_reward = (
            (1.0 if latency <= sla_target else -1.0*(latency/sla_target - 1))
            + (1.0 - cost/10*0.5 if cost <= 10 else -1.0*(cost/10 - 1))
        )
        meta_reward = round(meta_reward, 4)

        print(f"\n{lbl('RL-2  TD3 Meta', CYN)}  🧠  Meta-Controller — Phase 2")
        metric("Context",          c(fc, focus))
        print(f"\n{'':30}{DIM}Weight update:{R}")
        metric("  Old α,β,γ",      dim(f"{old_a:.3f}, {old_b:.3f}, {old_g:.3f}"))
        metric("  New α (cost)",   c(CYN, f"{self.alpha:.3f}"))
        metric("  New β (latency)",c(CYN, f"{self.beta:.3f}"))
        metric("  New γ (tput)",   c(CYN, f"{self.gamma:.3f}"))
        metric("  Meta-reward",    c(GRN if meta_reward > 0 else RED,
                                     f"{meta_reward:+.4f}"))

        tick(self.fast)

    # ── Step divider ───────────────────────────────────────────────────────────
    def step_banner(self, step, total):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n{WHT}{BOLD}{'─'*76}")
        print(f"  STEP {step}/{total}  ·  {ts}  ·  Executors: {self.executors}  ·  "
              f"Cost so far: ${self.total_cost:.2f}  ·  Alerts: {self.spike_alerts}")
        print(f"{'─'*76}{R}")

    # ── Full pipeline loop ─────────────────────────────────────────────────────
    def run(self, steps=7):
        header("SERVERLESS SPARK IoT FRAMEWORK — FULL SYSTEM DEMO")
        print(f"  {DIM}Pipeline:{R}  "
              f"{c(BLU,'IoT')} → {c(MAG,'MQTT')} → {c(YLW,'Kafka')} → "
              f"{c(RED,'Spark')} → {c(GRN,'RL-1')} → {c(YLW,'OpenFaaS')} → "
              f"{c(CYN,'RL-2')} → {c(RED,'Spark Executors')}\n")
        print(f"  {DIM}Running {steps} decision cycles. Press Ctrl+C to stop.{R}\n")
        if not self.fast:
            time.sleep(1.2)

        for step in range(1, steps + 1):
            self.step_idx = step - 1
            self.step_banner(step, steps)

            # ── IoT
            section(f"[1/7] IoT SENSORS → generating telemetry", BLU)
            reading, sensors = self.step_iot()
            arrow_anim("IoT telemetry stream →", self.fast)

            # ── MQTT
            section(f"[2/7] MQTT BROKER → routing to Kafka", MAG)
            self.step_mqtt(reading, sensors)
            arrow_anim("MQTT publish → Kafka topic →", self.fast)

            # ── Kafka
            section(f"[3/7] APACHE KAFKA → stream ingestion + pattern extraction", YLW)
            pattern = self.step_kafka(reading, sensors)
            arrow_anim("Kafka stream → Spark micro-batch →", self.fast)

            # ── Spark processing
            section(f"[4/7] APACHE SPARK → processing micro-batch", RED)
            metrics = self.step_spark(reading)
            arrow_anim("Spark metrics → RL-1 (PPO) →", self.fast)

            # ── RL-1
            section(f"[5/7] RL-1 (PPO Resource Agent) → scaling decision", GRN)
            action, target = self.step_rl1(metrics, pattern)
            arrow_anim(f"Scaling decision ({action}) → OpenFaaS →", self.fast)

            # ── OpenFaaS
            section(f"[6/7] OpenFaaS → rl-scaling-function invoked", YLW)
            tier = self.step_openfaas(action, target)
            arrow_anim("OpenFaaS response → Spark cluster →", self.fast)

            # ── Spark scaling applied
            section(f"[7/7] SPARK EXECUTORS → cluster rescaled", RED)
            self.step_spark_scale(action, target, tier)
            arrow_anim("Reward signal → RL-2 (TD3 Meta) →", self.fast)

            # ── RL-2
            section(f"[↩]   RL-2 (TD3 Meta-Controller) → weight update", CYN)
            self.step_rl2(metrics)

            if not self.fast:
                time.sleep(0.6)

        # ── Summary ────────────────────────────────────────────────────────────
        header("PIPELINE RUN COMPLETE — SUMMARY")
        sla_pct = round((1 - self.sla_violations / max(1, steps)) * 100, 1)
        print(f"  {'Steps run':<30}: {c(WHT, steps)}")
        print(f"  {'Final executors':<30}: {c(RED, self.executors)}")
        print(f"  {'Total cluster cost':<30}: {c(YLW, f'${self.total_cost:.2f}')}")
        print(f"  {'Cumulative RL-1 reward':<30}: {c(GRN if self.cum_reward > 0 else RED, f'{self.cum_reward:+.4f}')}")
        print(f"  {'SLA violations':<30}: {c(RED if self.sla_violations else GRN, self.sla_violations)}")
        print(f"  {'SLA compliance':<30}: {c(GRN, f'{sla_pct}%')}")
        print(f"  {'Spike / burst alerts fired':<30}: {c(YLW, self.spike_alerts)}")
        print(f"  {'Final α,β,γ weights':<30}: {c(CYN, f'{self.alpha:.3f}, {self.beta:.3f}, {self.gamma:.3f}')}")
        print(f"\n  {DIM}Loop: IoT→MQTT→Kafka→Spark→RL-1→OpenFaaS→Spark→RL-2→(next step){R}\n")

        if self.sla_violations == 0:
            info_box("🏆 ZERO SLA VIOLATIONS", "RL controllers successfully maintained latency ≤ 300ms throughout!")
        else:
            alert_box("⚠  SLA VIOLATIONS DETECTED",
                      f"{self.sla_violations} SLA breaches across {steps} steps — RL-2 has updated weights",
                      color=BLK, bg=BGYLW)


# ─── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full Serverless Spark IoT Demo")
    parser.add_argument("--steps", type=int, default=7,
                        help="Number of decision cycles to run (default: 7)")
    parser.add_argument("--fast",  action="store_true",
                        help="Skip animation delays")
    args = parser.parse_args()

    demo = FullSystemDemo(fast=args.fast)
    try:
        demo.run(steps=args.steps)
    except KeyboardInterrupt:
        print(f"\n\n{YLW}{BOLD}Demo stopped by user.{R}\n")
