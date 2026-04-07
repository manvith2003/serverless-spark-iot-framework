# Live Demonstration Guide: Serverless Spark IoT Framework

This guide is designed for your thesis defense or project review. It walks you through starting the entire 5-layer architecture from scratch, demonstrating the data generation, ingestion, Spark processing, and the RL-driven dynamic scaling happening live.

## Prerequisites

Ensure Docker Desktop, Python 3.11, and OpenFaaS `faas-cli` are installed and operational on your machine.
Ensure you are in the project root directory: `serverless-spark-iot-framework`.

## Presentation Flow: "The Live Demo"

You should perform these steps across multiple terminal windows (or split-panes in VSCode/iTerm2) that you can easily switch between during the presentation.

### 🍅 Terminal 1: Spin up the Infrastructure (Layers 2, 4, 5)

This starts Kafka, MQTT, Redis, Spark Master/Worker, and the OpenFaaS Gateway.

```bash
cd serverless/openfaas
docker-compose up -d
```

**What to say during the presentation:** 
*"First, I'm spinning up the core infrastructure using Docker Compose. This boots our Kafka brokers for the data lane, Mosquitto MQTT for the control lane, our Spark Stream processor, Redis state store, and the OpenFaaS serverless gateway."*

---

### 🍅 Terminal 2: Deploy the RL Scaling Function (Layer 3/4)

Now we deploy your trained PPO + TD3 agent as a serverless function.

```bash
cd serverless/openfaas
# Optional: faas-cli build -f stack.yml 
faas-cli up -f stack.yml
```

**What to say during the presentation:**
*"Next, I'm deploying our trained Two-Phase Reinforcement Learning optimizer. We package the PyTorch model inside an OpenFaaS rl-scaling-function. Notice the faas-cli up command output — it registers the function. Currently, it sits at 0 scale until load arrives, ensuring we incur no arbitrary computing costs."*

---

### 🍅 Terminal 3: Start the Spark Streaming Job (Layer 5)

We start the structured streaming job that ingests from Kafka.

```bash
cd rl_core
python spark_driver_client.py
```

**What to say during the presentation:**
*"Here, we initialize the Spark execution layer. This script sets up structured streaming to read the raw data from our Kafka topics in 5-second micro-batches. You won't see much action yet because we haven't turned on the IoT sensors."*

---

### 🍅 Terminal 4: Launch the IoT Generators (Layer 1)

This is the "start" button. It will begin blasting JSON data across Smart City, Healthcare, and Industrial domains.

```bash
cd rl_core
python synthetic_generators.py 
```
*(Wait a few seconds, then manually kill it `Ctrl+C` and run it again with a high load flag to simulate a burst if your script supports parameters, otherwise just let it run).*

**What to say during the presentation:**
*"Now, let's turn on the data. I'm starting our synthetic sensors. We are generating real-time traffic, healthcare vitals, and industrial telemetry. Instantly, this data flows into MQTT, gets bridged to Kafka, and hits our Spark application."*

---

### 🍅 The Reveal: Watching the RL Optimizer Work

Now, switch your screen focus back to **Terminal 3 (Spark Driver)** or open a new terminal to watch **Docker Stats**:

```bash
docker stats
```

**What to say during the presentation:**
*"If we look at the Spark Driver output (or Docker Stats), we can watch the RL engine actively making decisions. 
1. Spark receives a batch of data. 
2. It calls the OpenFaaS HTTP endpoint with metrics like CPU load and message velocity.
3. The RL agent evaluates these metrics and outputs SCALE_UP.
4. You can see OpenFaaS provisioning new container instances to handle the load dynamically!"*

---

### 🍅 Monitoring and Dashboards (Visual Proof)

Have these browser tabs pre-loaded before the presentation starts, and simply refresh them during the demo:

1. **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
   - *Shows executors being added and removed live.*
2. **OpenFaaS Portal:** [http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/)
   - *Shows the invocation count of the `rl-scaling` function spiking up.*

**To show convergence/training proof:**
If you want to show the beautiful training charts live, run:
```bash
tensorboard --logdir=./tboard_logs
```
And open [http://localhost:6006](http://localhost:6006) to show the reward curves going up.

---

### End Demo Teardown

To shut it all down cleanly at the end of the presentation:

```bash
cd serverless/openfaas
docker-compose down
```
