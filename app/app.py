from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys
import json

def create_schema():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(
            ['cassandra-server'],
            port=9042,
            auth_provider=auth_provider,
            protocol_version=4
        )
        session = cluster.connect()
        
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        
        session.set_keyspace('search_engine')
        
        session.execute("""
        CREATE TABLE IF NOT EXISTS term_index (
            term text,
            doc_id text,
            tf int,
            doc_length int,
            PRIMARY KEY (term, doc_id)
        )""")
        
        session.execute("""
        CREATE TABLE IF NOT EXISTS document_stats (
            doc_id text PRIMARY KEY,
            title text,
            length int
        )""")
        
        session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int,
            term_index int
        )""")
        
        print("Schema created successfully")
    except Exception as e:
        print(f"Error creating schema: {str(e)}", file=sys.stderr)
        raise
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

def load_term_index():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(
            ['cassandra-server'],
            port=9042,
            auth_provider=auth_provider,
            protocol_version=4
        )
        session = cluster.connect('search_engine')
        
        batch_size = 100
        batch = session.prepare("""
            INSERT INTO term_index (term, doc_id, tf, doc_length)
            VALUES (?, ?, ?, ?)
        """)
        
        current_batch = []
        
        count = 0
        for line in sys.stdin:
            try:
                word, doc_id, count, length = line.strip().split('\t')
                current_batch.append((word, doc_id, int(count), int(length)))
                
                if len(current_batch) >= batch_size:
                    session.execute(batch, current_batch)
                    current_batch = []

                count += 1
                if count % 100 == 0:
                    print(f"Processed {count} records...")
                    
            except ValueError as e:
                print(f"Skipping malformed line: {line.strip()}. Error: {str(e)}", file=sys.stderr)
                continue
            
            print(f"Successfully loaded {count} term index records")
        
        if current_batch:
            session.execute(batch, current_batch)
            
        print("Term index loaded successfully")
    except Exception as e:
        print(f"Error loading term index: {str(e)}", file=sys.stderr)
        raise
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

def load_vocabulary():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(
            ['cassandra-server'],
            port=9042,
            auth_provider=auth_provider,
            protocol_version=4
        )
        session = cluster.connect('search_engine')
        
        insert_stmt = session.prepare("""
            INSERT INTO vocabulary (term, df, term_index)
            VALUES (?, ?, ?)
        """)
        
        for line in sys.stdin:
            try:
                term, df, index = line.strip().split('\t')
                session.execute(insert_stmt, (term, int(df), int(index)))
            except ValueError as e:
                print(f"Skipping malformed line: {line.strip()}. Error: {str(e)}", file=sys.stderr)
                continue
        
        print("Vocabulary loaded successfully")
    except Exception as e:
        print(f"Error loading vocabulary: {str(e)}", file=sys.stderr)
        raise
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: app.py [create_schema|load_term_index|load_vocabulary]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == "create_schema":
            create_schema()
        elif command == "load_term_index":
            load_term_index()
        elif command == "load_vocabulary":
            load_vocabulary()
        else:
            print(f"Invalid command: {command}")
            sys.exit(1)
    except Exception as e:
        print(f"Command failed: {str(e)}", file=sys.stderr)
        sys.exit(1)
