#!/bin/bash

echo "🚀 Starting Spark Structured Streaming for IoT Data"
echo ""

# Activate conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate spark-iot

# Set environment variables
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Create checkpoint directory
mkdir -p data/checkpoints data/processed

# Run Spark job
python spark_core/streaming/spark_streaming_job.py

