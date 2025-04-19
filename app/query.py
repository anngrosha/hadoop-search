import sys
from pyspark import SparkContext
from cassandra.cluster import Cluster
import math
import numpy as np
from collections import defaultdict
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra import ConsistencyLevel
from cassandra.query import named_tuple_factory, tuple_factory

def safe_divide(numerator, denominator):
    return numerator / denominator if denominator else 0

def calculate_bm25(query, spark_context):
    cluster = None
    try:
        cluster = Cluster(['cassandra-server'])
        session = cluster.connect('search_engine')

        stats = session.execute("SELECT avg_doc_length, total_docs FROM corpus_stats WHERE id = 'global'").one()
        avg_dl = float(stats.avg_doc_length) if stats and stats.avg_doc_length else 1.0
        N = int(stats.total_docs) if stats and stats.total_docs else 1

        k1 = 1.2
        b = 0.75

        query_terms = [term.lower() for term in query.split() if term.strip()]
        if not query_terms:
            return []

        doc_scores = defaultdict(float)
        title_cache = {}

        term_dfs = {}
        for term in query_terms:
            df_row = session.execute("SELECT df FROM vocabulary WHERE term = %s", [term]).one()
            term_dfs[term] = int(df_row.df) if df_row else 0

        for term in query_terms:
            df = term_dfs[term]
            if df == 0:
                continue

            idf = max(0, math.log((N - df + 0.5) / (df + 0.5) + 1))

            term_docs = session.execute("""
                SELECT doc_id, tf, doc_length FROM term_index 
                WHERE term = %s
            """, [term])

            for doc in term_docs:
                doc_id = doc.doc_id
                tf = int(doc.tf)
                length = int(doc.doc_length)

                if doc_id not in title_cache:
                    title_row = session.execute("""
                        SELECT title FROM document_metadata 
                        WHERE doc_id = %s
                    """, [doc_id]).one()
                    title_cache[doc_id] = title_row.title if title_row else "Untitled"

                numerator = tf * (k1 + 1)
                denominator = tf + k1 * (1 - b + b * (length / avg_dl))
                doc_scores[(doc_id, title_cache[doc_id])] += idf * safe_divide(numerator, denominator)

        top_docs = sorted(
            ((doc_id, title, score) for (doc_id, title), score in doc_scores.items()),
            key=lambda x: x[2], 
            reverse=True
        )[:10]
        
        return top_docs

    except Exception as e:
        print(f"Error in BM25 calculation: {str(e)}", file=sys.stderr)
        return []
    finally:
        if cluster:
            cluster.shutdown()

def vector_search(query, spark_context):
    cluster = None
    try:
        cluster = Cluster(
            ['cassandra-server'],
            port=9042,
            connect_timeout=60,
            idle_heartbeat_interval=30,
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 30.0),
            protocol_version=4,
            control_connection_timeout=60
        )
        session = cluster.connect('search_engine')
        session.default_consistency_level = ConsistencyLevel.ONE
        session.default_timeout = 60

        session.row_factory = named_tuple_factory
        vocab_rows = session.execute("SELECT term FROM vocabulary")
        
        vocab = {row.term: idx for idx, row in enumerate(vocab_rows)}
    
        print(f"First vocabulary term: {next(iter(vocab)) if vocab else 'Empty'}")

        if not vocab:
            return []
        
        query_terms = [term.lower() for term in query.split() if term.strip()]
        query_vec = np.zeros(len(vocab))
        
        total_docs = session.execute(
            "SELECT total_docs FROM corpus_stats WHERE id = 'global'"
        ).one()[0] or 1

        print(f"Vector total_docs: {total_docs}")

        session.row_factory = tuple_factory

        for term in query_terms:
            if term in vocab:
                df_row = session.execute(
                    "SELECT df FROM vocabulary WHERE term = %s",
                    [term]
                ).one()
                if df_row:
                    idf = math.log(total_docs / (df_row[0] + 1))
                    query_vec[vocab[term]] = idf
        
        query_norm = np.linalg.norm(query_vec)
        if query_norm == 0:
            return []
        norm_query = query_vec / query_norm

        results = []
        for doc in session.execute("SELECT doc_id, title, vector FROM document_metadata"):
            
            if not doc[2] or len(doc[2]) != len(vocab):
                continue
            
            doc_vec = np.array(doc[2])
            doc_norm = np.linalg.norm(doc_vec)
            
            if doc_norm == 0:
                continue
                
            cosine_sim = np.dot(norm_query, doc_vec) / (query_norm * doc_norm)

            results.append((doc[0], doc[1], float(cosine_sim)))
        
        return sorted(results, key=lambda x: x[2], reverse=True)[:10]
        
    except Exception as e:
        print(f"Error in vector search: {str(e)}", file=sys.stderr)
        return []
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: query.py <query> [--vector]")
        sys.exit(1)
    
    query = " ".join(sys.argv[1:-1] if "--vector" in sys.argv else sys.argv[1:])
    use_vector = "--vector" in sys.argv
    
    sc = SparkContext(appName="SearchEngineQuery")
    
    try:
        if use_vector:
            print("Applying vector search...")
            results = vector_search(query, sc)
            print("Top 10 documents (Vector Search):")
        else:
            print("Applying BM25 search")
            results = calculate_bm25(query, sc)
            print("Top 10 documents (BM25):")
        
        if not results:
            print("No results found")
        else:
            for i, (doc_id, title, score) in enumerate(results, 1):
                print(f"{i}. Doc ID: {doc_id} | Title: {title} | Score: {score:.4f}")
                
    except Exception as e:
        print(f"Search failed: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        sc.stop()
