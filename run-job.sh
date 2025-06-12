#!/bin/bash

# Start Hadoop cluster
docker-compose up -d

# Wait for cluster to initialize
sleep 30

# Prepare HDFS
docker exec namenode hdfs dfs -mkdir -p /input
docker exec namenode hdfs dfs -mkdir -p /output

# Copy JAR to container
docker cp ./MapReduce_Lab3/Activity3/target/Activity3-1.0-SNAPSHOT.jar namenode:/tmp/

# Run MapReduce job
docker exec namenode \
  bash -c "hadoop jar /tmp/Activity3-1.0-SNAPSHOT.jar Elasfer.RandomPointGeneratorJob /output/points 1000 10"

# Vérification des arguments
echo "=== Vérification des arguments ==="
docker exec namenode \
  bash -c "hdfs dfs -cat /output/points/_logs/history/* | grep 'ARGUMENTS:'"

# View results
echo "Job output:"
docker exec namenode hdfs dfs -cat /output/points/part-r-00000 | head -n 20
