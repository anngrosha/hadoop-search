#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"

if [ $# -eq 0 ]; then
    echo "Usage: search.sh <query>"
    exit 1
fi

QUERY="$*"

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

if [[ "$@" == *"--vector"* ]]; then
    echo "Search with vectors"
    spark-submit --master yarn --archives /app/.venv.tar.gz#.venv --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python query.py "$QUERY" --vector
else
    echo "Search with BM25"
    spark-submit --master yarn --archives /app/.venv.tar.gz#.venv --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python query.py "$QUERY"
fi
