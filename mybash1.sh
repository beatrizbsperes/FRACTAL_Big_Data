#!/bin/bash

# Run spark for different cores and executors 

echo "TROPA DO CAGAO"
echo "================================"

echo ""
echo "sampling=1%, num-executors=32"
echo "-------------------------------------------------------"
spark-submit \
  --master yarn \
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.27 \
  run.py \
  --sampling 0.01 \
  --num-executors 32 \
  --num-cores-per-executor 2 \
  --executor-mem 14g \
  --driver-mem 4g

echo ""
echo "completed!"

echo ""
echo "sampling=2%, num-executors=16"
echo "-------------------------------------------------------"
spark-submit \
  --master yarn \
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.27 \
  run.py \
  --sampling 0.01 \
  --num-executors 16 \
  --num-cores-per-executor 3 \
  --executor-mem 14g \
  --driver-mem 4g

echo ""
echo "completed!"


echo ""
echo "sampling=2%, num-executors=12"
echo "-------------------------------------------------------"
spark-submit \
  --master yarn \
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.27 \
  run.py \
  --sampling 0.01 \
  --num-executors 12 \
  --num-cores-per-executor 3 \
  --executor-mem 14g \
  --driver-mem 4g

echo ""
echo "completed!"


echo ""
echo "3: sampling=2%, num-executors=8"
echo "-------------------------------------------------------"
spark-submit \
  --master yarn \
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.27 \
  run.py \
  --sampling 0.01 \
  --num-executors 8 \
  --num-cores-per-executor 5 \
  --executor-mem 14g \
  --driver-mem 4g

echo ""
echo "completed!"


# Copy results to S3
echo ""
echo "Copying metrics to S3 bucket..."
echo "================================"
aws s3 cp /home/efs/erasmus/emanuel/metrics s3://ubs-homes/erasmus/emanuel/metrics1 --recursive

echo ""
echo "All done!"