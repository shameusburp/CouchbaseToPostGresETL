# Couchbase to PostgreSQL ETL

This application performs an Extract, Transform, Load (ETL) operation from Couchbase to PostgreSQL using a multithreaded architecture for improved performance. It allows you to efficiently transfer data between these two database systems with configurable parameters.

## Features
- Multithreaded design for parallel processing
- Configurable number of threads
- Supports batch processing for efficient data transfer
- Clear separation of reader and writer components
- Error handling with logging
- Command-line interface for easy configuration

## Prerequisites
1. Python 3.7+
2. Couchbase SDK for Python (`couchbase`)
3. PostgreSQL driver for Python (`psycopg2`)

Install required libraries:

pip install couchbase psycopg2

Configuration
The application takes the following input parameters via command line:

--host: Couchbase host address (required)
--user: Couchbase username (required)
--password: Couchbase password (required)
--target_bucket: Target bucket in Couchbase (required)
--destination_table: Destination table in PostgreSQL (required)
--num_threads: Number of threads to use (optional, default: 4)
Usage
To run the ETL process, execute the following command:

python CouchbaseToPostGresETL.py --host <couchbase_host> --user <username> --password <password> --target_bucket <bucket_name> --destination_table <table_name> --num_threads <number_of_threads>
For example:

python CouchbaseToPostGresETL.py --host 192.168.1.100 --user myuser --password securepassword --target_bucket production --destination_table public_data --num_threads 8

Copy
bash
python main.py --host 192.168.1.100 --user myuser --password securepassword --target_bucket production --destination_table public_data --num_threads 8
Architecture
The application follows a queue-based architecture with separate reader and writer threads:

Reader Threads: Read data from Couchbase in batches and place documents into a queue
Queue: Acts as a buffer between readers and writers
Writer Threads: Retrieve documents from the queue and write them to PostgreSQL
This design allows for decoupling of read and write operations, enabling independent scaling and improved resilience.

Additional Notes
The Couchbase query used is SELECT * FROM \your-collection`` by default. Update this in the code or via command-line arguments if needed.
Ensure that your PostgreSQL database schema matches the structure of the documents in Couchbase.
For production environments, consider adding features like data validation, transformation, and error handling to meet specific ETL requirements.
The application currently performs a full load. Implement incremental loading strategies for ongoing data synchronization.

Contributing
Feel free to contribute to this project by submitting pull requests or reporting issues on the GitHub repository.

Create a new branch from main
Write clear, concise code with comments
Add unit tests for any new functionality
Submit a pull request with a detailed description of your changes
License
This project is licensed under the MIT License - see LICENSE file for details

Happy ETLing!
