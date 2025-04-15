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


echo "Term Frequency pipeline"

hdfs dfs -rm -r -f /tmp/term_frequencies 2>/dev/null || true

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output /tmp/term_frequencies


echo "Document Frequency pipeline"

hdfs dfs -rm -r -f /tmp/document_frequencies 2>/dev/null || true

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -files /app/mapreduce/mapper2.py \
    -mapper "python3 mapper2.py" \
    -input /tmp/term_frequencies \
    -output /tmp/document_frequencies


echo "Loading data into Cassandra"

python3 /app/app.py create_schema

echo "Checking output"
hdfs dfs -du -h /tmp/term_frequencies
hdfs dfs -du -h /tmp/document_frequencies

hdfs dfs -cat /tmp/document_frequencies/* | python3 /app/app.py load_vocabulary
hdfs dfs -cat /tmp/term_frequencies/* | python3 /app/app.py load_term_index

echo "Indexing completed successfully"
