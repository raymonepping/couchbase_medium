import json
import warnings
import logging
import argparse
import time
import traceback

from datetime import timedelta
from urllib3.exceptions import NotOpenSSLWarning

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, InsertOptions, UpsertOptions
from couchbase.exceptions import (
    CouchbaseException,
    DocumentExistsException,
    ScopeAlreadyExistsException,
    CollectionAlreadyExistsException,
)
from couchbase.management.options import CreateCollectionOptions

# Suppress urllib3 OpenSSL warning
warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Function to load and validate JSON files
def load_and_validate_json(file_path, required_keys):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        raise ValueError(f"Missing keys in {file_path}: {missing_keys}")
    return data

# Function to ensure the existence of scope and collection
def ensure_scope_and_collection(bucket, scope_name, collection_name):
    collection_manager = bucket.collections()

    try:
        # Ensure the scope exists
        scopes = collection_manager.get_all_scopes()
        if scope_name not in [scope.name for scope in scopes]:
            logger.info(f"Scope '{scope_name}' does not exist. Creating...")
            try:
                collection_manager.create_scope(scope_name)
                logger.info(f"Created scope '{scope_name}'.")
            except ScopeAlreadyExistsException:
                logger.info(f"Scope '{scope_name}' already exists.")

        # Ensure the collection exists
        collections = next((scope.collections for scope in scopes if scope.name == scope_name), [])
        if collection_name not in [col.name for col in collections]:
            logger.info(f"Collection '{collection_name}' does not exist in scope '{scope_name}'. Creating...")
            try:
                collection_manager.create_collection(
                    scope_name=scope_name,
                    collection_name=collection_name,
                    settings=None,  # Optional settings can be specified here
                    options=CreateCollectionOptions(timeout=timedelta(seconds=10))
                )
                logger.info(f"Created collection '{collection_name}' in scope '{scope_name}'.")
            except CollectionAlreadyExistsException:
                logger.info(f"Collection '{collection_name}' already exists in scope '{scope_name}'.")

    except CouchbaseException as e:
        logger.error(f"Error ensuring scope or collection: {e}\n{traceback.format_exc()}")
        return False

    return True

# Function to connect to Couchbase
def connect_to_couchbase(endpoint, username, password, bucket_name):
    try:
        cluster = Cluster(endpoint, ClusterOptions(PasswordAuthenticator(username, password)))
        bucket = cluster.bucket(bucket_name)
        cluster.wait_until_ready(timedelta(seconds=30))
        return bucket
    except CouchbaseException as e:
        logger.error(f"Failed to connect to Couchbase: {e}\n{traceback.format_exc()}")
        raise

# Function to process a batch of car brands
def process_brands_batch(collection, batch, mode, stats):
    try:
        batch_start_time = time.time()
        if mode == "insert":
            for key, value in batch.items():
                try:
                    collection.insert(key, value, InsertOptions(timeout=timedelta(seconds=10)))
                    stats["successful"] += 1
                except DocumentExistsException:
                    stats["skipped"] += 1
        elif mode == "upsert":
            collection.upsert_multi(batch, UpsertOptions(timeout=timedelta(seconds=10)))
            stats["successful"] += len(batch)

        batch_duration = time.time() - batch_start_time
        logger.info(f"Processed batch of size {len(batch)} in {batch_duration:.2f} seconds.")
    except CouchbaseException as e:
        logger.error(f"Failed to process batch: {e}\n{traceback.format_exc()}")
        stats["failed"] += len(batch)

# Function to generate brand data
def generate_brand_data(brands, start_id, batch_size):
    batch = {}
    counter = start_id
    for brand in brands:
        key = f"brand_{counter:03d}"
        batch[key] = brand
        counter += 1
        if len(batch) == batch_size:
            yield batch
            batch = {}
    if batch:
        yield batch

# Main function
def main():
    parser = argparse.ArgumentParser(description="Process car brands data and connect to Couchbase (local or remote).")
    parser.add_argument("--location", choices=["local", "remote"], required=True, help="Specify local or remote connection")
    parser.add_argument("--mode", choices=["insert", "upsert"], default="upsert", help="Specify insert or upsert mode")
    args = parser.parse_args()

    # Load connection details and validate
    connection_details = load_and_validate_json(
        "connection_details.json", required_keys=["local", "remote", "local_bucket", "remote_bucket"]
    )

    # Extract connection and batch details
    location_info = connection_details[args.location]
    endpoint = location_info["endpoint"]
    username = location_info["username"]
    password = location_info["password"]
    bucket_name = connection_details[f"{args.location}_bucket"]
    scope_name = connection_details.get("scope_name", "cars")
    collection_name = connection_details.get("collection_name", "brands")
    batch_size = connection_details.get("batch_size", 100)

    # Connect to Couchbase
    bucket = connect_to_couchbase(endpoint, username, password, bucket_name)

    # Ensure the scope and collection exist
    if not ensure_scope_and_collection(bucket, scope_name, collection_name):
        logger.error("Failed to ensure scope or collection. Exiting.")
        return

    collection = bucket.scope(scope_name).collection(collection_name)

    # Load car brands data
    brands_data = load_and_validate_json("car_brands.json", required_keys=["brands"])
    brands = brands_data.get("brands", [])

    if not brands:
        logger.warning("No car brands found in the input data.")
        return

    # Initialize stats
    stats = {"successful": 0, "skipped": 0, "failed": 0}

    # Process car brands in batches
    start_time = time.time()
    for batch in generate_brand_data(brands, start_id=1, batch_size=batch_size):
        process_brands_batch(collection, batch, mode=args.mode, stats=stats)

    total_duration = time.time() - start_time

    # Detailed summary report
    logger.info("Processing Summary:")
    logger.info(f" - Total brands processed: {len(brands)}")
    logger.info(f" - Successfully processed: {stats['successful']}")
    logger.info(f" - Failed: {stats['failed']}")
    logger.info(f" - Total time taken: {total_duration:.2f} seconds")

if __name__ == "__main__":
    main()
