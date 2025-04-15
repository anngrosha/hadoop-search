import sys
from pyspark import SparkContext
from cassandra.cluster import Cluster
import math
import numpy as np
from collections import defaultdict

def safe_divide(numerator, denominator):
    return numerator / denominator if denominator else 0

def calculate_bm25(query, spark_context):
    # cluster = Cluster(['localhost'])
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    print("Connected to cluster for BM25")
    
    avg_dl_row = session.execute("SELECT AVG(length) as avg FROM document_stats").one()
    avg_dl = avg_dl_row.avg if avg_dl_row and avg_dl_row.avg else 1
    print("avg_dl_row, avg_dl_row.avg: ", avg_dl_row, avg_dl_row.avg)
    
    total_docs_row = session.execute("SELECT COUNT(*) as count FROM document_stats").one()
    total_docs = total_docs_row.count if total_docs_row else 1
    
    query_terms = [term.lower() for term in query.split()]
    doc_scores = defaultdict(float)
    
    for term in query_terms:
        df_row = session.execute("SELECT df FROM vocabulary WHERE term = %s", [term]).one()
        df = df_row.df if df_row else 0

        
        idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1)

        
        term_rows = session.execute("SELECT doc_id, tf, doc_length FROM term_index WHERE term = %s", [term])

        
        for doc_id, tf, doc_length in term_rows:
            if doc_length is None:
                doc_row = session.execute("SELECT length FROM document_stats WHERE doc_id = %s", [doc_id]).one()
                doc_length = doc_row.length if doc_row else avg_dl
            
            k1 = 1
            b = 0.75
            
            numerator = tf * (k1 + 1)
            denominator = tf + k1 * (1 - b + b * safe_divide(doc_length, avg_dl))
            score = idf * safe_divide(numerator, denominator)

            
            doc_scores[doc_id] += score
    
    top_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)[:10]
    results = []

    
    for doc_id, score in top_docs:
        title_row = session.execute("SELECT title FROM document_stats WHERE doc_id = %s", [doc_id]).one()
        title = title_row.title if title_row else "Untitled"
        results.append((doc_id, title, score))
    
    return results

def vector_search(query, spark_context):
    # cluster = Cluster(['localhost'])
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    
    vocab = {}
    rows = session.execute("SELECT term, term_index FROM vocabulary")
    for row in rows:
        vocab[row.term] = row.term_index
    
    if not vocab:
        return []
    
    query_terms = [term.lower() for term in query.split()]
    query_vec = [0.0] * len(vocab)
    
    for term in query_terms:
        if term in vocab:
            query_vec[vocab[term]] = 1.0
    
    query_norm = math.sqrt(sum(x*x for x in query_vec)) or 1.0
    norm_query = [x/query_norm for x in query_vec]
    
    rows = session.execute("SELECT doc_id, title, vector FROM document_stats WHERE vector IS NOT NULL")
    
    results = []
    for doc_id, title, doc_vec in rows:
        if not doc_vec or len(doc_vec) != len(vocab):
            continue
            
        dot_product = sum(q*d for q,d in zip(norm_query, doc_vec))
        doc_norm = math.sqrt(sum(d*d for d in doc_vec)) or 1.0
        cosine_sim = dot_product / doc_norm
        
        results.append((doc_id, title, cosine_sim))
    
    return sorted(results, key=lambda x: x[2], reverse=True)[:10]

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: query.py <query> [--vector]")
        sys.exit(1)
    
    query = " ".join(sys.argv[1:-1] if "--vector" in sys.argv else sys.argv[1:])

    print("Query:", query)
    use_vector = "--vector" in sys.argv
    
    sc = SparkContext(appName="SearchEngineQuery")
    
    if use_vector:
        print("Applying vector_search...")
        results = vector_search(query, sc)
        print()
        print("Top 10 relevant documents (Vector Search):", results)
    else:
        print("Applying BM25...")
        results = calculate_bm25(query, sc)
        print()
        print("Top 10 relevant documents (BM25):", results)
    
    for i, (doc_id, title, score) in enumerate(results, 1):
        print(f"{i}. Doc ID: {doc_id} | Title: {title} | Score: {score:.4f}")
    
    sc.stop()
    