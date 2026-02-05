# HIERARCHICAL, EDGE-AWARE REINFORCEMENT LEARNING FOR SERVERLESS SPARK IOT OPTIMIZATION

**Author**: Manvith M  
**Advisor**: Dr. Shajulin Benedict  
**Date**: February 2026

---

# TABLE OF CONTENTS

1.  **Thinking Process: Why We Built This?**
2.  **Chapter 1: The Problem with Traditional Spark**
3.  **Chapter 2: The New Architecture (Edge-to-Cloud)**
4.  **Chapter 3: Contribution 1 - The Meta-Controller (The Manager)**
5.  **Chapter 4: Contribution 2 - Adaptive Shuffle (The Smart Storage)**
6.  **Chapter 5: Contribution 3 - Edge-Cloud State Awareness (The Crystal Ball)**
7.  **Chapter 6: Benchmarking & Final Results**
8.  **Conclusion**

---

# 1. THINKING PROCESS: WHY WE BUILT THIS?

In the world of IoT (Internet of Things), data is like a river. Sometimes it is calm, but sometimes—like when a traffic jam happens or a machine breaks down—it floods.

We use **Apache Spark** to process this river of data. But there is a big problem: Spark is "dumb" about money and time.
*   If we give it too few computers, it crashes during a flood.
*   If we give it too many computers, we waste thousands of dollars when the river is calm.

The goal of this project was to build a **"Brain" (Reinforcement Learning Agent)** that sits on top of Spark. This Brain watches the river (Workload) and automatically decides:
1.  How many computers to buy (Executors).
2.  Where to store the data (SSD vs RAM).
3.  When to panic (Scale up) before the flood hits.

We call this system the **Serverless Spark IoT Framework**.

---

# CHAPTER 1: THE PROBLEM WITH TRADITIONAL SPARK

To understand our solution, we must first understand why the standard way fails.

## 1.1 The "Reactive Lag" Problem
Imagine you are driving a car, but you can only see through the rear-view mirror. You only know you hit a bump *after* you feel it.
*   **Traditional Spark (HPA)** works like this. It looks at CPU usage from 1 minute ago.
*   **The Issue**: By the time Spark sees "90% CPU Utilization", the queue is already full. Latency has already spiked. 
*   **Result**: The application crashes or slows down exactly when it is needed most.

## 1.2 The "One-Size-Fits-All" Shuffle Problem
When Spark sorts data (Shuffle), it has to write temporary files to disk.
*   **Traditional Spark**: It treats all data the same. It writes everything to the default storage (usually a hard disk).
*   **The Issue**: 
    *   Some data is **HOT** (needed immediately). Writing it to a slow disk slows down the whole job.
    *   Some data is **COLD** (archival). Keeping it in expensive RAM wastes money.
*   **Result**: We pay for expensive storage we don't need, or we suffer slow speeds because we are cheap.

## 1.3 The "Conflicting Goals" Problem
Optimization is a tradeoff. 
*   **Scenario A (Healthcare)**: Lives are at stake. We don't care about cost; we need speed.
*   **Scenario B (Budget Monitoring)**: We have a limited budget. A 5-second delay is fine if we save money.
*   **Traditional Spark**: Uses a fixed configuration. It cannot switch modes.

---

# CHAPTER 2: THE NEW ARCHITECTURE

We rebuilt the pipeline from the ground up. Here is how data flows through our system.

## 2.1 Layer 1: The Edge (The Sensors)
*   **What it is**: The cameras and sensors on the street.
*   **Our Innovation**: We don't just send raw data. The Edge runs a small piece of code called the **Burst Predictor**.
*   **How it works**: It looks at the trend. If traffic is rising fast, it sends a "Warning Flag" (`edge_burst_signal`) to the cloud *before* sending the actual heavy video files.

## 2.2 Layer 2: The Ingestion (Kafka)
*   **What it is**: The waiting room for data.
*   **Our Innovation**: We split the traffic into two lanes:
    1.  **Data Plane**: The heavy sensor data (Terabytes).
    2.  **Control Plane (Metadata)**: The lightweight warning flags from the Edge (Kilobytes).
    *   This ensures the "Warning" arrives instantly, even if the "Data" lane is jammed.

## 2.3 Layer 3: The Brain (The RL Optimizer)
*   **What it is**: A Python program running PPO (Proximal Policy Optimization).
*   **Input**: It sees 13 things (The State Vector):
    *   Cloud Metrics: CPU, RAM, Current Lag.
    *   Edge Metrics: **Predicted Load**, **Burst Signal**.
    *   Data Metrics: **Temperature** (How often is this data reused?).
*   **Output**: It turns 3 knobs (The Action):
    1.  **Executors**: "Give me 20 machines."
    2.  **Storage Tier**: "Use Redis (RAM) for this batch."
    3.  **Compression**: "Use LZ4 compression."

---

# CHAPTER 3: INNOVATION 1 - THE META-CONTROLLER (THE MANAGER)

**The Problem**: A single AI agent can get confused. If we tell it "Save Money" AND "Be Fast", it gets stuck in the middle.

**The Solution**: We built a Hierarchy (Boss and Worker).
1.  **The Meta-Controller (The Boss)**:
    *   It looks at the **Global Scenario** (e.g., "Healthcare Critical" or "Smart City Budget").
    *   It sets the **Priorities** (Rewards).
    *   *Example*: In Healthcare mode, it sets $\alpha=0.1$ (Ignore Cost), $\beta=0.9$ (Minimize Latency).

2.  **The Resource Agent (The Worker)**:
    *   It takes the Boss's priorities and drives the car (adjusts executors).
    *   It doesn't ask "Why?", it just optimizes for the reward curve the Boss gave it.

**Result**: The system can switch behavior instantly.
*   **Morning (Traffic Rush)**: The Boss sets "High Performance Mode".
*   **Night (Empty Streets)**: The Boss sets "Budget Mode".

---

# CHAPTER 4: INNOVATION 2 - ADAPTIVE SHUFFLE (THE SMART STORAGE)

**The Problem**: Spark's Shuffle operation is the #1 cause of slowdowns. Writing 100GB of temporary data to a slow hard drive freezes the computation.

**The Solution**: We implemented **Data Temperature Awareness**.
*   We classify every batch of data:
    *   **HOT Data**: Likely to be reused (e.g., Iterative ML training).
    *   **WARM Data**: Standard ETL.
    *   **COLD Data**: Log archiving, read once.

**The Mechanism**:
The RL Agent chooses where to put the shuffle files:
1.  **Tier 0 (HOT)**: **Redis (In-Memory)**. Ultra-fast, very expensive.
2.  **Tier 1 (WARM)**: **NVMe SSD**. Fast, moderate cost.
3.  **Tier 2 (COLD)**: **HDD / S3**. Slow, very cheap.

**Result**:
*   During a burst, the Agent switches to **Redis**. This keeps the CPU fed with data, preventing the "CPU Idle" problem.
*   During quiet times, it switches to **S3** to save 80% on storage bills.

---

# CHAPTER 5: INNOVATION 3 - EDGE-CLOUD STATE AWARENESS (THE CRYSTAL BALL)

**The Problem**: "Cold Start". It takes 45-60 seconds to boot up new Spark executors. If we wait until the load hits, we are already 60 seconds late.

**The Solution**: Trust the Edge.
The Edge sees the traffic *before* it enters the cloud.
*   **Time T (Edge)**: Traffic Camera sees a 50-car pileup. Reference Signal `Burst=1` is sent.
*   **Time T (Cloud)**: The cloud is currently empty.
*   **The Action**: The RL Agent sees `Burst=1`. It knows this means "Flood incoming in 60 seconds".
*   **The Result**: The Agent scales up to **20 Executors** *while the cloud queues are empty*.
*   **Time T+60s**: The massive data flood arrives. The 20 executors are already booted and waiting. Latency stays low.

**Why this is huge**: This solves the "Auto-Scaling Lag" that has plagued cloud computing for 15 years.

---

# CHAPTER 6: BENCHMARKING & FINAL RESULTS

We proved this works by running a **Tournament**. We pitted our AI against the best industry standards.

## The Competitors
1.  **Fixed (The Baseline)**: A cluster with 15 computers, never changes.
2.  **Dexter (The Standard)**: The "Standard" Auto-scaler found in Kubernetes. (Scale up if CPU > 80%).
3.  **Seer (The Linear Predictor)**: A simple math formula ($Computers = Load \times 0.05$).
4.  **Edge-Aware RL (Our Code)**.

## The Results (500-Step Simulation)

### Metric 1: Reliability (SLA Violations)
*How often did the system fail to process data in time (<1 second)?*

*   **Fixed**: 90% Failure Rate. (It choked immediately).
*   **Dexter (Standard)**: 54% Failure Rate. (It reacted too slowly).
*   **Seer**: 100% Failure Rate. (It didn't understand that Shuffle needs fast storage).
*   **RL (Ours)**: **19% Failure Rate**. 🏆
    *   *Conclusion*: Our system is **3x more reliable** than the industry standard.

### Metric 2: Cost
*How much money did we spend?*

*   **Fixed**: $2,251.
*   **Dexter**: $2,993.
*   **RL (Ours)**: **$3,287**.
    *   *Conclusion*: We spent **9% more** than Dexter.
    *   *Why?*: Reliability isn't free. The Agent "bought" extra stability by using expensive Redis storage during critical moments. This is a trade-off any critical business (like a hospital) would happily make.

---

# CONCLUSION

This project moves Serverless Spark from a **Reactive** era to a **Proactive** era.

By combining:
1.  **Edge Signals** (Forecasting),
2.  **Adaptive Storage** (Handling the Shuffle bottleneck), and
3.  **Hierarchical Control** (Understanding Business Goals),

We have created a framework that can handle the chaotic, high-speed nature of modern IoT workloads. We didn't just optimize numbers; we built a system that "understands" the physics of data.
