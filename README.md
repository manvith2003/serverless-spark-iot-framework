# Serverless Spark IoT Framework (RL-Optimized)

## 🎓 Master's Thesis Project
**Subject**: Hierarchical, Edge-Aware Reinforcement Learning for Serverless IoT Resource Optimization.

---

## 🚀 Overview
This framework implements a novel **Reinforcement Learning (RL) Optimizer** for Serverless Spark workloads processing High-Velocity IoT data. Unlike traditional auto-scalers (HPA) or static heuristics, this system uses a **Contextual PPO Agent** to jointly optimize:
1.  **Compute**: Number of Executors & Memory per Executor.
2.  **Storage**: Shuffle Tier Selection (Redis vs NVMe vs S3) & Compression.
3.  **Proactivity**: Reacts to **Edge Signals** (Burst Prediction) before load arrives.

## ✨ Key System Contributions

### 1. Hierarchical Contextual RL (Meta-Controller)
*   **Problem**: Fixed reward weights ($\alpha, \beta, \gamma$) fail when workload priorities shift (e.g., Budget vs Critical Mode).
*   **Solution**: A **Meta-Controller (TD3)** dynamically tunes the lower-level agent's reward function based on the current `IoT_Scenario`.
*   **Report**: [Meta-Controller Report](meta_controller_report.txt)

### 2. Adaptive Shuffle & Storage Tiering (Step 4)
*   **Problem**: Shuffle cost/latency is a bottleneck. "One size fits all" storage fails for variable data temperatures.
*   **Solution**: The RL Agent selects the optimal **Storage Tier** (HOT/WARM/COLD) and **Compression Level** based on `Data_Temperature` (Reuse Probability).
*   **Report**: [Adaptive Shuffle Report](adaptive_shuffle_report.txt)

### 3. Edge-Cloud State Awareness (Step 5)
*   **Problem**: Cloud auto-scaling is reactive (lags behind bursts).
*   **Solution**: Ingests `Burst_Prediction` signals from Edge Gateways into the RL State Vector (Dim=13), enabling **Proactive Scaling**.
*   **Report**: [Edge-Cloud Report](edge_cloud_report.txt)

---

## 🛠️ Architecture & Tech Stack

```
IoT Devices → Edge Processing → MQTT/Kafka → Serverless Spark → Output
              (Burst Pred)     (Ingestion)   (RL Optimizer)    (Dashboard)
                                             (4-Tier Shuffle)
```

- **Processing**: Apache Spark 3.5 (Structured Streaming)
- **RL Framework**: Stable-Baselines3 (PPO, TD3), Gymnasium
- **Cloud/Infra**: Docker, Kubernetes (Simulated for Thesis)
- **State Store**: Redis (Hot), NVMe (Warm), S3 (Cold)

---

## 💻 Installation & Usage

### 1. Prerequisites
*   Python 3.10+
*   Java 11+
*   Docker (Optional for full stack)

### 2. Setup
```bash
git clone https://github.com/manvith2003/serverless-spark-iot-framework.git
cd serverless-spark-iot-framework
pip install -r requirements.txt
```

### 3. Run Experiments
**Train the RL Agent:**
```bash
python optimization/resource_allocation/ppo_agent.py --train --timesteps 100000
```

**Run Benchmark Tournament (Evaluation):**
```bash
python benchmarks/run_eval.py
```
*Output*: Generates `benchmarks/results_summary.csv` and `benchmark_plots.png`.

---

## 📊 Evaluation Results (Step 6)

We conducted a head-to-head tournament over a stochastic 500-step trace.

| Policy | Description | SLA Violations | Verdict |
| :--- | :--- | :--- | :--- |
| **Fixed** | Static 15 Executors | 90.0% | ❌ Failed (Overloaded) |
| **Dexter** | Standard HPA (Reactive) | 54.4% | ❌ Too slow for IoT Bursts |
| **Seer** | Linear Predictive | 100.0% | ❌ Under-provisioned Shuffle |
| **Edge-Aware RL** | **Proposed System** | **19.2%** | ✅ **Superior Stability** |

**Thesis Conclusion**: The Edge-Aware Contextual RL agent incurs a moderate cost premium (~9%) to achieve a **3x improvement in Service Reliability**.

---

## 🎓 Research Information

This work is part of a research project at **Indian Institute of Information Technology Kottayam**.

**Authors**: Manvith M  
**Advisor**: Dr. Shajulin Benedict  
**Contact**: manvith131250@gmail.com

---

## 📂 Project Structure

```text
.
├── benchmarks/                 # Evaluation Scripts & Policies
│   ├── policies.py             # Baseline implementations
│   └── run_eval.py             # Tournament runner
├── configs/                    # Configuration files
├── optimization/               # Core RL Logic
│   ├── meta_controller/        # TD3 Meta-Agent
│   └── resource_allocation/    # PPO Resource Agent
├── spark_core/                 # Spark Integration
└── reports/                    # Generated Technical Reports
```

---

## 📝 License
MIT License.
