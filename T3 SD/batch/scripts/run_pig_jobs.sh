#!/bin/bash
set -euo pipefail

PIG=/opt/pig/bin/pig
PIG_SCRIPT=/opt/batch/pig/wordcount.pig
STOPWORDS=/input/stopwords/stopwords.txt

# Human
$PIG -param INPUT=/input/human/human_responses.txt \
     -param OUTPUT=/output/human_wordcount \
     -param STOPWORDS=$STOPWORDS \
     -param N=100 \
     $PIG_SCRIPT

# LLM
$PIG -param INPUT=/input/llm/llm_responses.txt \
     -param OUTPUT=/output/llm_wordcount \
     -param STOPWORDS=$STOPWORDS \
     -param N=100 \
     $PIG_SCRIPT

echo "Jobs Pig ejecutados. Resultados en /output/human_wordcount y /output/llm_wordcount"
hdfs dfs -ls /output
