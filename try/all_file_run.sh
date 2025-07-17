#!/bin/bash

set -e  # Exit immediately if any command fails

echo "=================================="
echo "Step 1: Load HDFS history to RocksDB1"
echo "=================================="
flink run -py history_data.py

echo "=================================="
echo "Step 2: Run Kafka → RocksDB2 and HDFS storage"
echo "=================================="
flink run -py kafka_consumer.py

echo "=================================="
echo "Step 3: Run Union & Aggregate (RocksDB1 + RocksDB2)"
echo "=================================="
flink run -py processor.py

echo "=================================="
echo " All jobs completed successfully!"
echo "=================================="
