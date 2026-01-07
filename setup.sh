#!/bin/bash

# Load environment variables
set -a
source .env
set +a

# Create directories
mkdir -p notebooks data config scripts

# Build and start containers
docker compose down
docker compose build
docker compose up -d

# Check status
echo "Checking services..."
sleep 10

echo "Spark Master UI: http://localhost:8080"
echo "Jupyter Lab: http://localhost:${JUPYTER_PORT:-8888}"
echo "Livy Server: http://localhost:${LIVY_PORT:-8998}"

# Show logs
docker compose logs -f jupyter
