#!/bin/bash
set -e

echo "Starting indexing pipeline"

echo "Checking input path"
hdfs dfs -test -d /index/data || hdfs dfs -test -d /data
if [ $? -ne 0 ]; then
  echo "Error: No input data found in /index/data or /data"
  exit 1
fi

if hdfs dfs -test -d /index/data; then
  INPUT_PATH="/index/data"
else
  INPUT_PATH="/data"
fi

echo "Using input path: $INPUT_PATH"


hdfs dfs -rm -r /tmp/term_frequencies /tmp/document_frequencies /tmp/combined_output 2>/dev/null || true

echo "Term Frequency MapReduce"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input /index/data \
    -output /tmp/term_frequencies

echo "Document Frequency MapReduce"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input /tmp/term_frequencies \
    -output /tmp/document_frequencies

echo "Loading data into Cassandra"

python3 /app/app.py create_schema

hdfs dfs -cat /tmp/term_frequencies/part-* /tmp/document_frequencies/part-* | python3 /app/app.py load_data

if [[ "$@" == *"--vector"* ]]; then
    echo "Creating vector representations"
    python3 /app/app.py create_indexes
    spark-submit --master yarn --archives /app/.venv.tar.gz#.venv vector_indexer.py
fi

echo "Indexing completed successfully"
