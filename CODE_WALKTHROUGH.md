# Code Walkthrough Guide: Defending Your Thesis

During your presentation, your professor/examiner will likely ask you to "show the code" to prove that the architecture you designed actually works the way you claim. 

You do **not** need to show them every single file. You only need to show them the **"Core Intellectual Property"** — the novel files that make your framework different from a standard Spark project.

Here are the 5 most important files to walk them through, end-to-end:

---

## 1. The Real-Time Spark Driver 
**File path:** `serverless/openfaas/spark_driver_client.py`

**The Narrative (What to say):**
> *"This script is the main Spark execution layer. Traditional Spark streaming just runs with a fixed number of executors. Here, we intercept the micro-batches. After every batch, instead of blindly continuing, this script pauses, analyzes the workload size (messages per second), and fires off an HTTP request to our OpenFaaS Serverless RL optimizer asking for scaling instructions. When the RL agent replies with `target_executors`, you can see this script dynamically applying the new configuration via the Spark context APIs."*

**What to point out in the code:**
- Point out the `def foreach_batch(...)` function where the metrics are calculated.
- Highlight the `requests.post("http://<openfaas_url>/function/rl-scaling", ...)` line to prove the decoupling.

---

## 2. The Serverless RL Function (The OpenFaaS Bridge)
**File path:** `serverless/openfaas/rl-scaling-function/handler.py`

**The Narrative (What to say):**
> *"This is the OpenFaaS handler. Instead of embedding our heavy PyTorch reinforcement learning model directly inside Spark, we deployed it as a serverless function here. This handles the request sent by the Spark Driver. The genius of this file is that it implements **Scale-to-Zero**. If no requests come from Spark (e.g. at night when sensors are idle), OpenFaaS kills this container container so we pay nothing. When a request hits, it boots up and runs the `get_scaling_decision()` method."*

**What to point out in the code:**
- Show them the `def handle(req):` function.
- Point out how it extracts `workload_rate` and `latency_ms` and passes it to the `RLScalingHandler` class.
- Point out the `PRE_WARM` logic where if the RL agent outputs `3` (pre_warm), it adds additional executors preemptively.

---

## 3. The PPO Resource Optimizer (Phase 1 RL)
**File path:** `rl_core/continuous_two_rl_loop.py`

**The Narrative (What to say):**
> *"This file defines the Phase-1 PPO Agent and its environment. It controls the tactical, step-by-step scaling decisions. The custom `SparkResourceEnv` environment defined here is what allowed us to train the agent offline. Rather than using simple IF-statements like 'If CPU > 80% scale up', the PPO agent uses a multi-objective reward function to balance Latency, Cost, and Throughput simultaneously."*

**What to point out in the code:**
- Point out the custom Gym environment `class SparkResourceEnv(gym.Env):`
- Show them the `step()` function where the agent applies actions: `SCALE_UP`, `SCALE_DOWN`, `MAINTAIN`, or `PRE_WARM`.
- Specifically show the `_calculate_reward()` method to prove you use a mathematical reward function $R_t = -0.4\,C - 0.4\,L + 0.2\,T$ rather than just basic thresholds.

---

## 4. The TD3 Meta-Controller (Phase 2 RL)
**File path:** `rl_core/phase2_meta_controller.py`

**The Narrative (What to say):**
> *"While PPO does the tactical scaling, it has fixed weights. Over time, workloads change dramatically — a night idle period needs a different strategy than a morning traffic spike. This file introduces the Phase-2 TD3 Meta-Controller. It runs once per episode and observes the overall SLA health. It then continuously outputs new weights ($\alpha$ for Cost, $\beta$ for Latency, $\gamma$ for Throughput), which are fed back down to the PPO agent."*

**What to point out in the code:**
- Show the `Phase2Environment` class.
- Point out that its action space is continuous (a `Box` from 0.0 to 1.0) because it outputs the actual float weights `[alpha, beta, gamma]`.
- Explain how this provides "Context-Aware" shifting where the framework prioritizes cost at night, and latency during the day.

---

## 5. The Cold-Start Mitigation Engine
**File path:** `rl_core/pattern_feature_extractor.py`

**The Narrative (What to say):**
> *"One of the biggest issues with Serverless is the 'Cold Start' problem — the delay it takes to boot a new container during a sudden traffic burst. We solved this with this Pattern Extractor. It runs at the IoT edge and extracts 7 temporal features (like moving averages and velocity) to compute a `burst_probability`. This signal is sent ahead of the data. Because of this file, our RL agent knows a burst is coming 45 seconds in advance and preemptively boots Spark executors using the `PRE_WARM` action, completely eliminating SLA violations."*

**What to point out in the code:**
- Show the `extract_streaming_features(window_data)` method.
- Point out the calculations for `traffic_velocity_msg_sec` and `moving_average_5m`.
- Point out how these form the `burst_probability` score.

---

### Tips for Answering Questions:
If the professor asks:
* **"Why not just use Kubernetes HPA (Horizontal Pod Autoscaler)?"**  
  *Answer:* HPA is reactive. It waits for CPU to cross 80\%, then boots a pod, meaning we suffer a 45+ second cold-start delay where IoT data drops. Our RL agent uses Edge Pattern Extraction to preemptively `PRE_WARM` the cluster. Look at `continuous_two_rl_loop.py` to see the pre-warm action.
* **"How do you know the RL isn't just making random guesses?"**  
  *Answer:* The PPO agent was trained for 50,000 timesteps and we visualized the reward surface using TensorBoard. The convergence charts in Chapter 4 of the thesis prove it found a stable, optimal policy. 
* **"Does it actually scale to zero?"**  
  *Answer:* Yes. By deploying the RL engine inside OpenFaaS (look at `stack.yml` labels `com.openfaas.scale.zero=true`), the HTTP endpoint shuts down completely if there are no IoT events for 5 minutes.
