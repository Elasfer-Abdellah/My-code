#!/bin/bash

# Clone repo
git clone https://github.com/Elasfer-Abdellah/My-code.git
cd My-code/MapReduce/Activity3

# Start Hadoop cluster
docker-compose up -d

# Wait for cluster to initialize
sleep 60

# Prepare HDFS
docker exec namenode hdfs dfs -mkdir -p /input
docker exec namenode hdfs dfs -mkdir -p /output

# Copy JAR to container
docker cp target/Activity3-1.0-SNAPSHOT.jar namenode:/tmp/

# Run MapReduce job
docker exec namenode \
  hadoop jar /tmp/Activity3-1.0-SNAPSHOT.jar \
  com.yourpackage.RandomPointGeneratorJob \
  /output/points \
  1000 \
  10

# View results
echo "Job output:"
docker exec namenode hdfs dfs -cat /output/points/part-r-00000 | head -n 20
