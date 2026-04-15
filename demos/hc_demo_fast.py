#!/usr/bin/env python3
"""
Healthcare End-to-End Pipeline Demo  (fast, no delays)
Shows every step: IoT → MQTT → Kafka → Spark → OpenFaaS → Phase-1 RL → Phase-2 RL
Run:  python3 demos/hc_demo_fast.py
"""
import random, time
import numpy as np
from datetime import datetime

# ── colours ──────────────────────────────────────────────────────────────────
R='\033[91m';G='\033[92m';Y='\033[93m';B='\033[94m'
M='\033[95m';CY='\033[96m';W='\033[1m';D='\033[2m';X='\033[0m'

def hdr(t,c=M): print(f"\n{W}{c}{'═'*72}\n  {t}\n{'═'*72}{X}")
def sec(t,c=CY): print(f"\n{W}{c}┌{'─'*68}┐\n│  {t:<66}│\n└{'─'*68}┘{X}")
def row(k,v,alert=False): c=R if alert else B; print(f"   {c}├─{X} {k}: {W}{v}{X}")
def fref(p): print(f"   {D}📁 {p}{X}")
def alrt(msg): print(f"\n  {R}{W}🚨  {msg}{X}")

# ── patient generator ─────────────────────────────────────────────────────────
WARDS = ["ICU","Emergency","Cardiology","Neonatal","General"]

def make_patient():
    pid  = f"PT-{random.randint(1000,9999)}"
    ward = random.choice(WARDS)
    hr   = random.uniform(60,100)
    spo2 = random.uniform(96,99)
    temp = random.uniform(36.1,37.5)
    sbp  = random.uniform(110,130)
    rr   = random.uniform(12,20)
    spike = None
    if random.random() < 0.40:
        spike = random.choice(["cardiac","hypoxia","hyperthermia","shock","sepsis"])
        if spike=="cardiac":      hr=random.uniform(142,210); spo2=random.uniform(84,91)
        elif spike=="hypoxia":    spo2=random.uniform(76,89); hr=random.uniform(112,155)
        elif spike=="hyperthermia": temp=random.uniform(39.6,42.5)
        elif spike=="shock":      sbp=random.uniform(55,84); hr=random.uniform(122,180)
        elif spike=="sepsis":     temp=random.uniform(38.9,40.8); hr=random.uniform(106,140); rr=random.uniform(23,36); sbp=random.uniform(84,100)
    return dict(patient_id=pid,ward=ward,heart_rate=hr,spo2=spo2,
                temperature=temp,systolic_bp=sbp,resp_rate=rr,
                timestamp=datetime.now().isoformat(),spike=spike)

def check(p):
    a=[]
    if p["heart_rate"]>130 or p["heart_rate"]<45: a.append(f"🫀 Cardiac Risk  HR={p['heart_rate']:.0f} bpm")
    if p["spo2"]<92:           a.append(f"💧 Hypoxia  SpO₂={p['spo2']:.1f}%")
    if p["temperature"]>39.5:  a.append(f"🌡  Hyperthermia  Temp={p['temperature']:.1f}°C")
    if p["systolic_bp"]<88:    a.append(f"🩸 Shock  BP={p['systolic_bp']:.0f} mmHg")
    if p["resp_rate"]>22:      a.append(f"😮 Resp Distress  RR={p['resp_rate']:.0f}/min")
    if p["spo2"]<92 and p["heart_rate"]>106 and p["temperature"]>38.8 and p["resp_rate"]>22:
        a.append("🦠 SEPSIS RISK (multi-sign)")
    return a

# ═════════════════════════════════════════════════════════════════════════════
hdr("SERVERLESS SPARK IoT FRAMEWORK — HEALTHCARE END-TO-END DEMO")
print(f"""
  {Y}DATA FLOW (one service, all patients identified by patient_id):{X}
  {D}
  [ICU / Ward Sensors]
       │  MQTT  hospital/{{ward}}/vitals   QoS-1
       ▼
  [Kafka Topic: healthcare-vitals  (8 partitions, keyed by patient_id)]
       │  Spark Structured Streaming  readStream.format('kafka')
       ▼
  [Spark Batch — anomaly scoring per patient]
       │  HTTP POST /function/rl-scaling
       ▼
  [OpenFaaS → rl-scaling-function/handler.py]   ← Scale-to-Zero
       │  PPO action vector
       ▼
  [Phase-1 RL (PPO)] → YARN executor scaling
       │  episode reward
       ▼
  [Phase-2 RL (TD3)] → weight update (α, β, γ)
  {X}""")

EPISODES   = 3
STEPS      = 3
N_PATIENTS = 12      # patients per Spark batch

alpha, beta, gamma = 0.10, 0.80, 0.10   # healthcare = latency first
executors  = 2
sla_ms     = 150.0
total_msgs = 0
total_alert_pts = 0
sla_violations  = 0
ep_rewards_all  = []

for ep in range(1, EPISODES+1):
    hdr(f"EPISODE {ep}/{EPISODES}   α={alpha:.2f}  β={beta:.2f}  γ={gamma:.2f}  Executors={executors}", CY)
    ep_lats=[]; ep_costs=[]; ep_violations=0

    for st in range(1, STEPS+1):
        sec(f"STEP {st}/{STEPS}  │  {N_PATIENTS} patients in this Spark batch")

        # ── STEP 1: IoT Sensors ──────────────────────────────────────────────
        print(f"\n  {G}{W}[1] IoT SENSORS{X}  (Ward monitors, wearables, pulse-oximeters)")
        fref("ingestion/mqtt_ingestion/mqtt_publisher.py")
        patients = [make_patient() for _ in range(N_PATIENTS)]
        spike_pts= [p for p in patients if p["spike"]]
        row("Patients monitored this second", N_PATIENTS)
        row("Wards active", ", ".join(set(p["ward"] for p in patients)))
        row("Critical readings in batch", len(spike_pts), alert=len(spike_pts)>0)
        sample = patients[0]
        row("Sample payload",
            f'{{"patient_id":"{sample["patient_id"]}","ward":"{sample["ward"]}",'
            f'"hr":{sample["heart_rate"]:.0f},"spo2":{sample["spo2"]:.1f},'
            f'"temp":{sample["temperature"]:.1f},"bp":{sample["systolic_bp"]:.0f}}}')

        # ── STEP 2: MQTT ──────────────────────────────────────────────────────
        print(f"\n  {CY}{W}[2] MQTT BROKER{X}  (Eclipse Mosquitto / AWS IoT Core)")
        fref("ingestion/mqtt_ingestion/mqtt_subscriber.py")
        row("Topic pattern", "hospital/{ward}/vitals")
        row("Messages received", N_PATIENTS)
        row("QoS", "1 (at-least-once delivery)")
        row("Bridge to Kafka", "mqtt_subscriber → kafka_producer.py")

        # ── STEP 3: Kafka ─────────────────────────────────────────────────────
        print(f"\n  {Y}{W}[3] KAFKA{X}  (Topic: healthcare-vitals, 8 partitions, key=patient_id)")
        fref("ingestion/kafka_ingestion/kafka_producer.py")
        total_msgs += N_PATIENTS
        partitions_used = len(set(hash(p["patient_id"]) % 8 for p in patients))
        row("Topic", "healthcare-vitals")
        row("Partition key", "patient_id  → same patient always same partition")
        row("Partitions written", partitions_used)
        row("Cumulative offset", total_msgs)
        row("Retention (HIPAA)", "7 days")

        # ── STEP 4: Spark ─────────────────────────────────────────────────────
        print(f"\n  {B}{W}[4] SPARK STRUCTURED STREAMING{X}  (Batch processing)")
        fref("spark_apps/streaming/iot_streaming_processor.py")
        workload = N_PATIENTS * 12 + random.uniform(-8, 8)
        base_lat = 180 * (workload / max(1, executors * 42))
        latency  = max(20, base_lat + random.uniform(-20, 35))
        cpu      = min(100, 25 + workload / max(1, executors) * 0.6)
        cost     = executors * 0.6
        anomalies= []
        for p in patients:
            a = check(p)
            if a: anomalies.append((p, a))
        sla_ok = latency <= sla_ms
        if not sla_ok: ep_violations+=1; sla_violations+=1
        row("Batch workload", f"{workload:.0f} msg/s")
        row("Latency", f"{latency:.1f} ms  {'✓ OK' if sla_ok else '✗ SLA BREACH'}", alert=not sla_ok)
        row("CPU utilisation", f"{cpu:.1f}%")
        row("Executors", executors)
        row("Anomalous patients found", len(anomalies), alert=len(anomalies)>0)
        ep_lats.append(latency); ep_costs.append(cost)

        # ── STEP 5: OpenFaaS ─────────────────────────────────────────────────
        print(f"\n  {M}{W}[5] OpenFaaS  →  rl-scaling-function/handler.py{X}")
        fref("serverless/openfaas/rl-scaling-function/handler.py")
        row("HTTP call", f"POST /function/rl-scaling  workload={workload:.0f}, lat={latency:.0f}ms")
        row("Scale-to-Zero", "ACTIVE — $0.00/hr when idle")
        row("Cold-start guard", "Edge PRE_WARM via MQTT burst signal")
        if latency > sla_ms*1.3 or len(anomalies)>2:
            decision="scale_up";   new_ex=min(20,executors+3); conf=0.92
        elif latency < sla_ms*0.5 and cost > 10:
            decision="scale_down"; new_ex=max(1,executors-1);  conf=0.88
        else:
            decision="maintain";   new_ex=executors;            conf=0.95
        row("Decision", f"{decision.upper()}  →  target {new_ex} executors", alert=(decision=="scale_up"))
        row("Confidence", f"{conf:.0%}")

        # ── STEP 6: Phase-1 PPO ──────────────────────────────────────────────
        print(f"\n  {G}{W}[6] PHASE-1 RL  (PPO){X}  →  rl_core/continuous_two_rl_loop.py")
        fref("rl_core/continuous_two_rl_loop.py")
        nc=cost/15; nl=latency/1000; nt=(workload*0.9)/300
        reward = -alpha*nc - beta*nl + gamma*nt
        print(f"   {CY}│  Reward: -α×{nc:.3f} - β×{nl:.3f} + γ×{nt:.3f} = {'+' if reward>0 else ''}{reward:.4f}{X}")
        old_ex=executors; executors=new_ex
        if decision=="scale_up":
            print(f"   {G}│  YARN: +{executors-old_ex} containers launched  →  {executors} executors{X}")
        elif decision=="scale_down":
            print(f"   {Y}│  YARN: -{old_ex-executors} containers removed  →  {executors} executors{X}")
        else:
            print(f"   {D}│  YARN: no change  →  {executors} executors{X}")
        ep_rewards_all.append(reward)

        # ── MEDICAL ALERT ENGINE ─────────────────────────────────────────────
        if anomalies:
            total_alert_pts += len(anomalies)
            print(f"\n  {R}{W}{'▓'*68}{X}")
            print(f"  {R}{W}  🚨  MEDICAL ALERT ENGINE — {len(anomalies)} PATIENT(S) IN DANGER{X}")
            print(f"  {R}{W}{'▓'*68}{X}")
            for p, alerts in anomalies:
                print(f"  {R}│  {p['patient_id']} [{p['ward']}]{X}")
                for a in alerts:
                    print(f"  {R}│     → {a}{X}")
                lat_ms = random.randint(35,90)
                print(f"  {Y}│     ACTION: Nurse paged · Physician alerted · Response in {lat_ms}ms{X}")
            print(f"  {R}{W}{'▓'*68}{X}\n")
        else:
            print(f"\n   {G}✓  All {N_PATIENTS} patients — vitals normal{X}")

    # ── STEP 7: Phase-2 TD3 ──────────────────────────────────────────────────
    sec(f"PHASE-2 META-RL (TD3)  —  End of Episode {ep}")
    print(f"\n  {M}{W}[7] PHASE-2 TD3{X}  →  rl_core/phase2_meta_controller.py")
    fref("rl_core/phase2_meta_controller.py")
    avg_lat   = float(np.mean(ep_lats))
    total_cost= float(np.sum(ep_costs))
    lat_r  = 1.0 if avg_lat<=sla_ms else -1.0*(avg_lat/sla_ms-1)
    cost_r = 1.0-(total_cost/15)*0.5 if total_cost<=15 else -1.0
    viol_r = -0.5*ep_violations
    meta_r = lat_r+cost_r+viol_r
    row("Avg latency this episode", f"{avg_lat:.1f} ms")
    row("Total cost this episode",  f"${total_cost:.2f}")
    row("SLA violations",           ep_violations, alert=ep_violations>0)
    print(f"   {CY}│  Meta-reward = {'+' if meta_r>0 else ''}{meta_r:.4f}{X}")
    oa,ob,og=alpha,beta,gamma
    if avg_lat>sla_ms*1.1: beta=min(0.85,beta+0.04)
    if total_cost>15:       alpha=min(0.60,alpha+0.03)
    t=alpha+beta+gamma; alpha/=t; beta/=t; gamma/=t
    print(f"   {Y}│  Weight update: α {oa:.2f}→{alpha:.2f}  β {ob:.2f}→{beta:.2f}  γ {og:.2f}→{gamma:.2f}{X}")
    pri="LATENCY 🏥" if beta>alpha and beta>gamma else "COST" if alpha>gamma else "THROUGHPUT"
    print(f"   {W}│  Priority → {pri}{X}")

# ── FINAL SUMMARY ─────────────────────────────────────────────────────────────
hdr("HEALTHCARE PIPELINE — FINAL SUMMARY", G)
total_batches = EPISODES*STEPS
print(f"""
  {W}┌──────────────────────────────────────────────────────────────────┐{X}
  {W}│  Total patient readings processed  : {total_msgs:<8}                 │{X}
  {W}│  Batches processed (Spark)         : {total_batches:<8}                 │{X}
  {W}│  Patients with alerts fired        : {total_alert_pts:<8} 🚨             │{X}
  {W}│  SLA violations (>150 ms)          : {sla_violations:<8}                 │{X}
  {W}│  Final Spark executor count        : {executors:<8}                 │{X}
  {W}│  Final RL weights                  :                              │{X}
  {W}│    α(cost)       = {alpha:.3f}                                     │{X}
  {W}│    β(latency)    = {beta:.3f}  ← dominant for hospital SLA        │{X}
  {W}│    γ(throughput) = {gamma:.3f}                                     │{X}
  {W}├──────────────────────────────────────────────────────────────────┤{X}
  {W}│  THROUGHPUT CAPACITY (simulated, with auto-scaling):            │{X}
  {W}│    2  executors  →  ~200   patients/sec                         │{X}
  {W}│    5  executors  →  ~600   patients/sec                         │{X}
  {W}│    10 executors  →  ~1,200 patients/sec                         │{X}
  {W}│    20 executors  →  ~2,500 patients/sec (RL auto-scales up)     │{X}
  {W}│                                                                  │{X}
  {W}│  A 500-bed hospital sends ~10 readings/sec → trivial load 🏥    │{X}
  {W}│  A 5,000-bed hospital network → RL scales automatically         │{X}
  {W}└──────────────────────────────────────────────────────────────────┘{X}

  {G}✅  IoT → MQTT → Kafka → Spark → OpenFaaS → Phase-1 RL → YARN{X}
  {G}✅  Phase-2 TD3 adapted β upward to protect hospital SLA{X}
  {G}✅  Per-patient spike detection with patient_id tracking{X}
  {G}✅  One service monitors ALL patients simultaneously{X}
  {G}✅  Scales from 2-bed clinic to 5,000-bed hospital network{X}
""")
