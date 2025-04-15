from pyspark import SparkContext
from cassandra.cluster import Cluster
import math
import numpy as np
from collections import defaultdict
import sys

sc = SparkContext(appName="VectorIndexer")
    
# Connects to the cassandra server
cluster = Cluster(['cassandra-server'])

session = cluster.connect()
    
vocabulary = {}
rows = session.execute("SELECT term, term_index FROM vocabulary")
for row in rows:
    vocabulary[row.term] = row.term_index
    

if len(vocabulary) == 0:
    print("Error: vocab empty, run indexer first.")
    sys.exit(1)
    

total_docs_row = session.execute("SELECT COUNT(*) as count FROM document_stats").one()
total_docs = total_docs_row.count if total_docs_row.count else 1
    
documents = []
rows = session.execute("SELECT doc_id, title FROM document_stats")
for row in rows:
    documents.append((row.doc_id, row.title))
    

for doc_id, title in documents:
    vector = [0.0] * len(vocabulary)
    norm = 0.0
        
    term_rows = session.execute(f"""
    SELECT ti.term, ti.tf, v.df 
    FROM term_index ti
    JOIN vocabulary v ON ti.term = v.term
    WHERE ti.doc_id = '{doc_id}'
    """)
        
    for term_row in term_rows:
        term = term_row.term
        tf = term_row.tf
        df = term_row.df
            
        idf = math.log(total_docs / (df + 1))
        tfidf = tf * idf
            
        term_index = vocabulary[term]
        vector[term_index] = tfidf
        norm += tfidf * tfidf
        
        norm = math.sqrt(norm)
        
        session.execute(f"""
        INSERT INTO document_vectors (doc_id, title, vector, norm)
        VALUES ('{doc_id}', '{title}', {vector}, {norm})
        """)
    
print(f"created vectors for {len(documents)} documents")
sc.stop()
