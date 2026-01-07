#!/bin/bash
set -e

echo "=== Jupyter Startup Script Starting ==="

# Function to handle shutdown
cleanup() {
    echo "Received shutdown signal, exiting..."
    exit 0
}
trap cleanup SIGTERM SIGINT

# Wait for services with timeout
echo "Waiting for Livy server (max 60 seconds)..."
timeout 60 bash -c 'until nc -z livy-server 8998; do echo "Livy not ready, waiting..."; sleep 2; done' || {
    echo "WARNING: Livy server timeout, continuing anyway..."
}

echo "Waiting for Spark Master (max 60 seconds)..."
timeout 60 bash -c 'until nc -z spark-master 7077; do echo "Spark Master not ready, waiting..."; sleep 2; done' || {
    echo "WARNING: Spark Master timeout, continuing anyway..."
}

# Create config directory and file
echo "Creating SparkMagic config..."
mkdir -p /home/jovyan/.sparkmagic
cat > /home/jovyan/.sparkmagic/config.json << 'CONFIG'
{
  "kernel_python_credentials": {
    "username": "",
    "password": "",
    "url": "http://livy-server:8998",
    "auth": "None"
  },
  "kernel_scala_credentials": {
    "username": "",
    "password": "",
    "url": "http://livy-server:8998",
    "auth": "None"
  },
  "session_configs": {
    "driverMemory": "1g",
    "executorCores": 2,
    "executorMemory": "1g",
    "conf": {
      "spark.master": "spark://spark-master:7077"
    }
  },
  "logging_config": {
    "version": 1
  }
}
CONFIG

# Change to working directory
cd /home/jovyan/work

echo "Starting JupyterLab server..."
echo "Port: ${JUPYTER_PORT:-8888}"
echo "Token: ${JUPYTER_TOKEN:-none}"

# CRITICAL: Use the most basic Jupyter startup possible
# Remove all problematic flags and use only essential ones

jupyter lab \
    --port=${JUPYTER_PORT:-8888} \
    --no-browser \
    --ip=0.0.0.0 \
    --allow-root \
    --ServerApp.token='' \
    --ServerApp.password='' \
    --ServerApp.allow_origin='*' \
    --ServerApp.allow_remote_access=True

# If we get here, Jupyter exited - this should not happen normally
echo "ERROR: Jupyter Lab exited unexpectedly!"
echo "Exit code: $?"
echo "Keeping container alive for debugging (5 minutes)..."
sleep 300
