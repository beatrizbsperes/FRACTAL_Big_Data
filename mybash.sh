#!/bin/bash

# Run spark for different cores and executors 

echo "TROPA DO CAGAO"
echo "================================"

#  Sampling 0.001 with 4 executors
echo ""
echo "1: sampling=0.001, num-executors=4"
echo "-------------------------------------------------------"
spark-submit \
  --master yarn \
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.27 \
  run.py \
  --sampling 0.001 \
  --num-executors 4 \
  --num-cores-per-executor 2 \
  --executor-mem 2g \
  --driver-mem 2g

echo ""
echo "completed!"

# Sampling 0.001 with 8 executors
echo ""
echo "2: sampling=0.001, num-executors=8"
echo "-------------------------------------------------------"
spark-submit \
  --master yarn \
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.27 \
  run.py \
  --sampling 0.001 \
  --num-executors 8 \
  --num-cores-per-executor 2 \
  --executor-mem 2g \
  --driver-mem 2g

echo ""
echo "2 completed!"

# 3: Sampling 0.005 with 4 executors
echo ""
echo "Running Experiment 3: sampling=0.005, num-executors=4"
echo "-------------------------------------------------------"
spark-submit \
  --master yarn \
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.27 \
  run.py \
  --sampling 0.005 \
  --num-executors 4 \
  --num-cores-per-executor 2 \
  --executor-mem 2g \
  --driver-mem 2g

echo ""
echo "3 completed!"

# Copy results to S3
echo ""
echo "Copying metrics to S3 bucket..."
echo "================================"
aws s3 cp /home/efs/erasmus/emanuel/metrics s3://ubs-homes/erasmus/emanuel/mymetrics --recursive

echo ""
echo "All done!"