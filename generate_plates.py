import json
import warnings
import random
import string
import argparse
import logging
import time

from datetime import timedelta
from urllib3.exceptions import NotOpenSSLWarning
from concurrent.futures import ThreadPoolExecutor, as_completed

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions, QueryOptions, InsertOptions
from couchbase.cluster import QueryScanConsistency

# Suppress urllib3 OpenSSL warning
warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load and validate connection details from JSON file
def load_connection_details():
    with open('connection_details.json', 'r') as file:
        details = json.load(file)
    validate_connection_details(details)
    return details

def validate_connection_details(details):
    required_keys = ['local', 'remote', 'local_bucket', 'remote_bucket', 'scope_name', 'collection_name', 'batch_size', 'timeouts']
    for key in required_keys:
        if key not in details:
            raise ValueError(f"Missing required key in connection details: {key}")

# Function to load car models from JSON
def load_car_models():
    json_file = 'car_models.json'
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data.get("cars", [])

# Function to select a random brand and model from car data
def select_random_car(cars_data):
    car_entry = random.choice(cars_data)
    brand = car_entry['brand']
    model = random.choice(car_entry['models'])
    return brand, model

# Generate a random license plate
def generate_license_plate(format_str):
    return ''.join(
        random.choice(string.ascii_uppercase) if char == 'X' else
        random.choice(string.digits) if char == '9' else
        '-' for char in format_str
    )

# Generate the next document key
def generate_document_key(index):
    return f"car_plates_{index:09d}"

# Connect to Couchbase
def connect_to_couchbase(endpoint, username, password, bucket_name, timeout):
    try:
        cluster = Cluster(endpoint, ClusterOptions(PasswordAuthenticator(username, password)))
        bucket = cluster.bucket(bucket_name)
        cluster.wait_until_ready(timedelta(seconds=timeout))
        return cluster, bucket
    except CouchbaseException as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise

# Create scope and collection if needed
def create_scope_and_collection(cluster, bucket, scope_name, collection_name):
    collection_manager = bucket.collections()
    try:
        scopes = collection_manager.get_all_scopes()
        if scope_name not in [scope.name for scope in scopes]:
            logger.info(f"Creating scope: {scope_name}")
            collection_manager.create_scope(scope_name)

        # Refresh scopes after scope creation
        scopes = collection_manager.get_all_scopes()
        collections = next((scope.collections for scope in scopes if scope.name == scope_name), [])
        if collection_name not in [col.name for col in collections]:
            logger.info(f"Creating collection: {collection_name}")
            collection_manager.create_collection(scope_name, collection_name)

        # Ensure the collection is queryable
        for _ in range(10):  # Retry for up to 10 seconds
            try:
                query = f"SELECT RAW 1 FROM `{bucket.name}`.`{scope_name}`.`{collection_name}` LIMIT 1"
                cluster.query(query)
                logger.info("Collection is now available for querying.")
                return True
            except CouchbaseException:
                logger.info("Waiting for collection to be queryable...")
                time.sleep(1)
    except CouchbaseException as e:
        logger.error(f"Error creating scope or collection: {e}")
        return False
    return True

# Get the highest existing document index
def get_highest_existing_index(cluster, bucket_name, scope_name, collection_name, retries=10, delay=1):
    query = f"""
    SELECT MAX(TO_NUMBER(SUBSTR(META().id, 12))) AS max_index
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE META().id LIKE 'car_plates_%'
    """
    for attempt in range(retries):
        try:
            logger.info("Executing query to retrieve the highest existing document index...")
            result = cluster.query(query, QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS))
            for row in result:
                max_index = row.get('max_index')
                if max_index is not None:
                    logger.info(f"Highest existing index found: {int(max_index)}")
                    return int(max_index)
            logger.info("No existing documents found. Starting from index 0.")
            return 0
        except CouchbaseException as e:
            if "Keyspace not found" in str(e) and attempt < retries - 1:
                logger.warning(f"Keyspace not found. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                logger.error(f"Couchbase error while retrieving highest index: {e}")
                return 0  # Return 0 if the retries are exhausted

# Generate license plate data
def generate_license_plate_data(index, cars_data):
    format_str = random.choice([
        'X-999-XX', 
        'XX-99-99', 
        '99-99-XX', 
        'XX-99-XX', 
        '99-XX-99', 
        'XX-XX-99', 
        '99-XX-XX'
    ])
    license_plate = generate_license_plate(format_str)
    brand, model = select_random_car(cars_data)
    document_key = generate_document_key(index)
    document = {"license_plate": license_plate, "brand": brand, "model": model, "isParked": False, "isPaid": False}
    return document_key, document

# Process a batch with exponential backoff
def process_batch(collection, batch, mode, retries=3, operation_timeout=10):
    documents = {key: document for key, document in batch}
    for attempt in range(retries):
        try:
            options = InsertOptions(timeout=timedelta(seconds=operation_timeout))
            if mode == 'insert':
                collection.insert_multi(documents, options)
            elif mode == 'upsert':
                collection.upsert_multi(documents, options)
            return True
        except CouchbaseException as e:
            delay = 2 ** attempt + random.uniform(0, 1)
            logger.warning(f"Batch processing error: {e}, retrying in {delay:.2f}s")
            time.sleep(delay)
    logger.error("Batch processing failed after retries.")
    return False

# Main function
def main():
    parser = argparse.ArgumentParser(description="Generate and insert/upsert car registrations.")
    parser.add_argument('--location', choices=['local', 'remote'], required=True, help="Connection location")
    parser.add_argument('--total', type=int, default=100000, help="Total registrations to generate")
    parser.add_argument('--mode', choices=['insert', 'upsert'], default='upsert', help="Insert or upsert mode")
    parser.add_argument('--threads', type=int, default=4, help="Number of threads to use")
    args = parser.parse_args()

    connection_details = load_connection_details()
    location_info = connection_details[args.location]
    endpoint = location_info['endpoint']
    username = location_info['username']
    password = location_info['password']
    bucket_name = connection_details[f"{args.location}_bucket"]
    scope_name = connection_details["scope_name"]
    collection_name = connection_details["collection_name"]
    batch_size = connection_details["batch_size"]
    timeouts = connection_details["timeouts"]

    start_time = time.time()

    try:
        cluster, bucket = connect_to_couchbase(endpoint, username, password, bucket_name, timeouts["connection_timeout"])
        if not create_scope_and_collection(cluster, bucket, scope_name, collection_name):
            logger.error("Failed to set up scope or collection.")
            return

        collection = bucket.scope(scope_name).collection(collection_name)
        highest_index = get_highest_existing_index(cluster, bucket_name, scope_name, collection_name)
        start_index = highest_index + 1
        total_to_generate = max(0, args.total - highest_index)

        cars_data = load_car_models()
        batches = []
        batch = []
        for i in range(total_to_generate):
            index = start_index + i
            batch.append(generate_license_plate_data(index, cars_data))
            if len(batch) >= batch_size:
                batches.append(batch)
                batch = []
        if batch:
            batches.append(batch)

        def process_batches(batches_slice):
            for batch in batches_slice:
                process_batch(collection, batch, args.mode, operation_timeout=timeouts["operation_timeout"])

        batch_slices = [batches[i::args.threads] for i in range(args.threads)]
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = [executor.submit(process_batches, slice) for slice in batch_slices]
            for future in as_completed(futures):
                future.result()

        logger.info(f"Processed {total_to_generate} car registrations.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        try:
            cluster.close()
            logger.info("Closed Couchbase connection.")
        except NameError:
            logger.warning("Cluster was not initialized; skipping cleanup.")

    end_time = time.time()
    logger.info(f"Execution completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
