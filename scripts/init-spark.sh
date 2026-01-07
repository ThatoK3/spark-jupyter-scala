#!/bin/bash
set -e

if [ "$1" = "master" ]; then
    echo "Starting Spark Master..."
    $SPARK_HOME/sbin/start-master.sh \
        --host spark-master \
        --port 7077 \
        --webui-port 8080
    
    tail -f $SPARK_HOME/logs/*

elif [ "$1" = "worker" ]; then
    echo "Starting Spark Worker..."
    
    until nc -z spark-master 7077; do
        echo "Waiting for Spark Master..."
        sleep 5
    done
    
    $SPARK_HOME/sbin/start-worker.sh \
        spark://spark-master:7077 \
        --cores 2 \
        --memory 2g \
        --webui-port 8081
    
    tail -f $SPARK_HOME/logs/*

else
    echo "Usage: $0 {master|worker}"
    exit 1
fi
