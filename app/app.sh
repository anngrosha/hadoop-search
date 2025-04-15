#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

echo "Started services"

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

echo "Venv done"

echo "Starting prep"

# Collect data
bash prepare_data.sh

echo "Done prep"

echo "Starting indexer"

# Run the indexer
bash index.sh

echo "Done indexer"

echo "data science query running"

# Run the ranker
bash search.sh "data science"

echo "data science query finished"
