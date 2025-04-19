from cassandra.cluster import Cluster
import numpy as np
import math
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_tfidf_vectors():
    cluster = None
    try:
        cluster = Cluster(['cassandra-server'])
        session = cluster.connect('search_engine')
        
        logger.info("Starting vector indexing process")
        
        vocab_terms = session.execute("SELECT term FROM vocabulary ALLOW FILTERING")
        vocab = {term.term: idx for idx, term in enumerate(vocab_terms)}
        vocab_size = len(vocab)
        
        logger.info(f"Vocabulary size: {vocab_size}")
        
        if vocab_size == 0:
            logger.warning("Empty vocabulary - no vectors will be created")
            return
        
        stats = session.execute("SELECT total_docs FROM corpus_stats WHERE id = 'global'").one()
        total_docs = stats.total_docs if stats else 1
        logger.info(f"Total documents: {total_docs}")
        
        batch_size = 100
        doc_ids = [row.doc_id for row in session.execute(
            "SELECT doc_id FROM document_metadata LIMIT 10000 ALLOW FILTERING"
        )]
        logger.info(f"Found {len(doc_ids)} documents to process")
        
        for i, doc_id in enumerate(doc_ids):
            try:
                vector = np.zeros(vocab_size)
                
                terms = session.execute("""
                    SELECT term, tf FROM term_index 
                    WHERE doc_id = %s
                    ALLOW FILTERING
                """, [doc_id])
                
                term_count = 0
                for term in terms:
                    if term.term in vocab:
                        df_row = session.execute(
                            "SELECT df FROM vocabulary WHERE term = %s ALLOW FILTERING",
                            [term.term]
                        ).one()
                        
                        if df_row:
                            tf = term.tf
                            df = df_row.df
                            idf = math.log(total_docs / (df + 1))
                            vector[vocab[term.term]] = tf * idf
                            term_count += 1
                
                norm = np.linalg.norm(vector)
                if norm > 0:
                    vector = (vector / norm).tolist()
                else:
                    vector = [0.0] * vocab_size
                
                session.execute("""
                    UPDATE document_metadata 
                    SET vector = %s
                    WHERE doc_id = %s
                """, [vector, doc_id])
                
                if (i+1) % batch_size == 0:
                    logger.info(f"Processed {i+1}/{len(doc_ids)} documents (last doc_id: {doc_id}, terms: {term_count})")
                
            except Exception as e:
                logger.error(f"Error processing document {doc_id}: {str(e)}")
                continue
        
        logger.info("Vector indexing completed successfully")
        
    except Exception as e:
        logger.error(f"Vector indexing failed: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    calculate_tfidf_vectors()
    