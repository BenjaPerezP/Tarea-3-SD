#!/bin/bash
set -euo pipefail

# Asumimos que los archivos ya fueron generados por export_responses.py
DATA_DIR=/opt/batch/data

hdfs dfs -mkdir -p /input/human
hdfs dfs -mkdir -p /input/llm

hdfs dfs -put -f "$DATA_DIR/human_responses.txt" /input/human/
hdfs dfs -put -f "$DATA_DIR/llm_responses.txt"   /input/llm/

echo "Archivos cargados a HDFS:"
hdfs dfs -ls /input/human
hdfs dfs -ls /input/llm
