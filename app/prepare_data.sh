#!/bin/bash

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

hdfs dfs -mkdir -p /index/data
hdfs dfs -mkdir -p /data

# if [ ! -f "a.parquet" ]; then
#     echo "Downloading sample parquet file..."
#     wget https://www.kaggle.com/datasets/jjinho/wikipedia-20230701/download -O a.parquet
# fi

if [ -f "a.parquet" ]; then
    echo "Processing a.parquet file"
    hdfs dfs -put -f a.parquet / && \
    spark-submit prepare_data.py
else
    echo "a.parquet not found, skipping initial processing"
fi

echo "Converting files to MapReduce input format"
for file in data/*.txt; do
    doc_id=$(basename "$file" | cut -d'_' -f1)
    title=$(basename "$file" | cut -d'_' -f2- | sed 's/\.txt$//')
    content=$(cat "$file" | tr '\n' ' ' | sed 's/\t/ /g')
    echo -e "${doc_id}\t${title}\t${content}" >> /tmp/mr_input.txt
done

hdfs dfs -put -f /tmp/mr_input.txt /index/data/part-00000
rm /tmp/mr_input.txt
