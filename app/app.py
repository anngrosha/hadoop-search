from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_cassandra_session():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    return Cluster(['cassandra-server'], port=9042, auth_provider=auth_provider, protocol_version=4).connect()

def create_schema():
    session = None
    try:
        session = get_cassandra_session()
        
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        
        session.set_keyspace('search_engine')

        session.execute("""
        CREATE TABLE IF NOT EXISTS document_metadata (
            doc_id text PRIMARY KEY,
            title text,
            length int,
            vector list<float>
        )""")
        
        session.execute("""
        CREATE CUSTOM INDEX IF NOT EXISTS vector_index 
        ON document_metadata (vector) 
        USING 'StorageAttachedIndex'
        """)

        session.execute("""
        CREATE TABLE IF NOT EXISTS term_index (
            term text,
            doc_id text,
            tf int,
            doc_length int,
            PRIMARY KEY (term, doc_id)
        )""")
        
        session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )""")

        session.execute("""
        CREATE TABLE IF NOT EXISTS corpus_stats (
            id text PRIMARY KEY,
            avg_doc_length float,
            total_docs int
        )""")
        
        logger.info("Schema created successfully")

    except Exception as e:
        logger.error(f"Error creating schema: {str(e)}")
        raise

    finally:
        if session:
            session.cluster.shutdown()

def load_data():
    session = None
    try:
        session = get_cassandra_session()
        session.set_keyspace('search_engine')
        
        meta_insert = session.prepare("""
            INSERT INTO document_metadata (doc_id, title, length)
            VALUES (?, ?, ?)
        """)

        term_insert = session.prepare("""
            INSERT INTO term_index (term, doc_id, tf, doc_length)
            VALUES (?, ?, ?, ?)
        """)
        
        vocab_insert = session.prepare("""
            INSERT INTO vocabulary (term, df)
            VALUES (?, ?)
        """)

        stats_insert = session.prepare("""
            INSERT INTO corpus_stats (id, avg_doc_length, total_docs)
            VALUES (?, ?, ?)
        """)
        
        term_count = 0
        vocab_count = 0
        meta_count = 0

        doc_lengths = []
        
        for line in sys.stdin:
            try:
                parts = line.strip().split('\t')
                
                if len(parts) == 4:
                    term, doc_id, tf, length = parts
                    session.execute(term_insert, (term, doc_id, int(tf), int(length)))
                    term_count += 1
                    doc_lengths.append(int(length))

                elif len(parts) == 3 and parts[0] != "!META!":
                    doc_id, title, length = parts
                    session.execute(meta_insert, (doc_id, title, int(length)))
                    meta_count += 1

                elif len(parts) == 2:
                    term, df = parts
                    session.execute(vocab_insert, [term, int(df)])
                    vocab_count += 1
                
            except Exception as e:
                logger.error(f"Skipping malformed line: {line.strip()}. Error: {str(e)}")
                continue

        if doc_lengths:
            avg_length = sum(doc_lengths) / len(doc_lengths)
            session.execute(stats_insert, ['global', float(avg_length), len(doc_lengths)])
        
        logger.info(f"""Loaded: {term_count} term index records {vocab_count} vocabulary terms {meta_count} document metadata entries""")

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

    finally:
        if session:
            session.cluster.shutdown()

def create_indexes():
    session = None
    try:
        session = get_cassandra_session()
        session.set_keyspace('search_engine')
        
        session.execute("""
            CREATE INDEX IF NOT EXISTS term_index_doc_id_idx 
            ON term_index (doc_id)
        """)
        
        logger.info("Secondary index created successfully")
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}")
        raise
    finally:
        if session:
            session.cluster.shutdown()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: app.py [create_schema|load_data]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == "create_schema":
            create_schema()
        elif command == "load_data":
            load_data()
        elif command == "create_indexes":
            create_indexes()
        else:
            print(f"Invalid command: {command}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Command failed: {str(e)}")
        sys.exit(1)
