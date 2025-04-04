#!/bin/bash

SPARK_HOME=/home/tom/app/spark-3.5.1-bin-hadoop3
DS=$1
PY_PATH=$2

echo "DS ==> $DS"
echo "PY_PATH  ==> $PY_PATH"

$SPARK_HOME/bin/spark-submit \
--master spark://spark-tom-1.c.praxis-bond-455400-a4.internal:7077 \
--executor-memory 2G \
--executor-cores 2 \
$PY_PATH $DS