import threading
import time
from couchbase.cluster import Cluster, ClusterOptions
import psycopg2
from queue import Queue
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CouchbaseReader(threading.Thread):
    def __init__(self, cluster, bucket_name, collection_name, query, batch_size=1000, timeout=60):
        super().__init__()
        self.cluster = cluster
        self.bucket = cluster.get_bucket(bucket_name)
        self.collection = self.bucket.get_collection(collection_name)
        self.query = query
        self.batch_size = batch_size
        self.timeout = timeout
        self.stop_event = threading.Event()
        self.doc_count = 0

    def run(self):
        print(f"Couchbase reader thread started for collection {self.collection.name()}")
        while not self.stop_event.is_set():
            try:
                # Execute query and process results in batches
                result = self.collection.query(self.query, batch_size=self.batch_size)
                for row in result.rows():
                    if doc := row.document:  # Using walrus operator for concise assignment
                        yield doc
                        self.doc_count += 1
            except Exception as e:
                logging.error(f"Error reading from Couchbase: {e}")
                time.sleep(5)

        print(f"Couchbase reader processed {self.doc_count} documents")

    def stop(self):
        self.stop_event.set()

class PostgreSQLWriter(threading.Thread):
    def __init__(self, conn, table_name, queue):
        super().__init__()
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.queue = queue
        self.stop_event = threading.Event()
        self.row_count = 0

    def run(self):
        print(f"PostgreSQL writer thread started for table {self.table_name}")
        while not self.stop_event.is_set():
            try:
                # Get document from queue with timeout
                doc = self.queue.get(timeout=1)
                if doc:
                    try:
                        # Prepare SQL query (example - adjust based on your schema)
                        cols = ', '.join([f'"{key}"' for key in doc])
                        vals = ', '.join(['%s'] * len(doc))
                        sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({vals})"

                        # Execute the query
                        self.cursor.execute(sql, list(doc.values()))
                        self.row_count += 1
                    except Exception as e:
                        logging.error(f"Error writing to PostgreSQL: {e}")
                else:
                    # Queue empty, check if we should stop
                    if self.stop_event.is_set():
                        break
            except Empty:
                # Queue is empty, continue looping
                pass

        print(f"PostgreSQL writer processed {self.row_count} rows")
        self.cursor.close()

    def stop(self):
        self.stop_event.set()

def etl_data(couchbase_host, couchbase_user, couchbase_password, 
             target_bucket, destination_table, num_threads=4, query="SELECT * FROM `your-collection`"):
    """ETL data from Couchbase to PostgreSQL using multithreading."""
    logging.info(f"Starting ETL process with {num_threads} threads")

    # Create queue for documents
    queue = Queue()

    # Initialize connections
    cluster = Cluster(f"{couchbase_host}:9361", options=ClusterOptions().credentials(
        username=couchbase_user, password=couchbase_password))
    bucket = cluster.get_bucket(target_bucket)
    collection = bucket.get_collection("your-collection")  # Replace with your collection name

    # PostgreSQL connection string (replace placeholders)
    pg_conn_str = "host=your-postgres-host dbname=your-db user=your-user password=your-password"
    pg_conn = psycopg2.connect(pg_conn_str)
    cursor = pg_conn.cursor()

    # Create reader and writer threads
    readers = [CouchbaseReader(cluster, target_bucket, collection.name(), query) for _ in range(num_threads)]
    writers = [PostgreSQLWriter(pg_conn, destination_table, queue) for _ in range(num_threads)]

    # Start threads
    for reader in readers:
        reader.start()
    for writer in writers:
        writer.start()

    # Wait for all readers to finish and signal writers to stop
    logging.info("All readers have finished, signaling writers to stop")
    for reader in readers:
        reader.stop()
        reader.join()
    for writer in writers:
        writer.stop()
        writer.join()

    # Close PostgreSQL connection
    pg_conn.close()
    logging.info("ETL process complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Couchbase to PostgreSQL ETL with multithreading")
    parser.add_argument("--host", required=True, help="Couchbase host address")
    parser.add_argument("--user", required=True, help="Couchbase username")
    parser.add_argument("--password", required=True, help="Couchbase password")
    parser.add_argument("--target_bucket", required=True, help="Target bucket in Couchbase")
    parser.add_argument("--destination_table", required=True, help="Destination table in PostgreSQL")
    parser.add_argument("--num_threads", type=int, default=4, help="Number of threads to use (default: 4)")
    args = parser.parse_args()

    etl_data(args.host, args.user, args.password, args.target_bucket, args.destination_table, args.num_threads)
