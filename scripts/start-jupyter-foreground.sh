#!/bin/bash
set -e

# Wait for services
echo "Waiting for Livy server..."
until nc -z livy-server 8998; do
    echo "Livy not ready, waiting..."
    sleep 5
done

echo "Waiting for Spark Master..."
until nc -z spark-master 7077; do
    echo "Spark Master not ready, waiting..."
    sleep 5
done

# Create config
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
  }
}
CONFIG

# Change to working directory
cd /home/jovyan/work

# Try starting Jupyter with explicit foreground options
echo "Starting JupyterLab in foreground..."

# Method 1: Use --ServerApp.open_browser=False and remove collaborative flag
jupyter lab \
    --port=8888 \
    --no-browser \
    --ip=0.0.0.0 \
    --allow-root \
    --ServerApp.token='' \
    --ServerApp.password='' \
    --ServerApp.allow_origin='*' \
    --ServerApp.allow_remote_access=True \
    --ServerApp.notebook_dir=/home/jovyan/work \
    --ServerApp.disable_check_xsrf=True \
    --ServerApp.open_browser=False

# If we get here, Jupyter exited - let's see why
echo "Jupyter exited with code: $?"
echo "Keeping container alive for debugging..."
sleep 300
