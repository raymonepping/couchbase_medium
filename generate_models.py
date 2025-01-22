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
from couchbase.exceptions import CouchbaseException, DocumentExistsException, ScopeAlreadyExistsException, CollectionAlreadyExistsException
from couchbase.management.collections import CollectionSpec
from concurrent.futures import ThreadPoolExecutor

# Suppress urllib3 OpenSSL warning
warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to load and validate JSON files
def load_and_validate_json(file_path, required_keys):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        raise ValueError(f"Missing keys in {file_path}: {missing_keys}")
    return data

# Function to validate car models
def validate_car_models(cars):
    if not isinstance(cars, list):
        raise ValueError("The 'cars' key must contain a list of car objects.")
    for car in cars:
        if not isinstance(car, dict) or 'brand' not in car or 'models' not in car:
            raise ValueError("Each car must be a dictionary with 'brand' and 'models' keys.")
        if not isinstance(car['models'], list) or not car['models']:
            raise ValueError(f"Car models must be a non-empty list. Found: {car['models']}")

# Function to measure operation time
def time_operation(operation, *args, **kwargs):
    start_time = time.time()
    result = operation(*args, **kwargs)
    duration = time.time() - start_time
    logger.info(f"Operation '{operation.__name__}' took {duration:.2f} seconds.")
    return result

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
                    collection_name=collection_name
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

# Function to process a batch of car models
def process_car_models(collection, batch, mode):
    try:
        if mode == 'insert':
            failed_documents = []
            for key, value in batch.items():
                try:
                    collection.insert(key, value, InsertOptions(timeout=timedelta(seconds=10)))
                except DocumentExistsException:
                    failed_documents.append(key)
                    logger.error(f"Failed to insert document with key: {key} (document already exists)")
            logger.info(f"Inserted batch of {len(batch) - len(failed_documents)} car models.")
            if failed_documents:
                logger.warning(f"Failed to insert {len(failed_documents)} documents that already existed.")
        elif mode == 'upsert':
            collection.upsert_multi(batch, UpsertOptions(timeout=timedelta(seconds=10)))
            logger.info(f"Upserted batch of {len(batch)} car models.")
    except CouchbaseException as e:
        logger.error(f"Failed to process batch: {e}\n{traceback.format_exc()}")

# Function to generate car model data
def generate_car_model_data(cars, start_id, batch_size):
    batch = {}
    counter = start_id

    for car in cars:
        brand = car["brand"]
        for model in car["models"]:
            key = f"car_models_{counter:03d}"
            car_model_data = {
                "car_model_id": key,
                "brand": brand,
                "model": model
            }
            batch[key] = car_model_data
            counter += 1
            if len(batch) == batch_size:
                yield batch
                batch = {}
    if batch:
        yield batch

# Process batches in parallel
def process_batches_in_parallel(collection, batches, mode):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_car_models, collection, batch, mode) for batch in batches]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in parallel batch processing: {e}\n{traceback.format_exc()}")

# Main function
def main():
    parser = argparse.ArgumentParser(description="Process car models data and connect to Couchbase (local or remote).")
    parser.add_argument('--location', choices=['local', 'remote'], required=True, help="Specify local or remote connection")
    parser.add_argument('--mode', choices=['insert', 'upsert'], default='upsert', help="Specify whether to insert or upsert car models")
    args = parser.parse_args()

    # Load connection details and validate
    connection_details = load_and_validate_json('connection_details.json', required_keys=["local", "remote", "local_bucket", "remote_bucket"])

    # Extract connection and batch details
    location_info = connection_details[args.location]
    endpoint = location_info['endpoint']
    username = location_info['username']
    password = location_info['password']
    bucket_name = connection_details[f"{args.location}_bucket"]
    scope_name = connection_details.get("scope_name", "cars")
    collection_name = connection_details.get("collection_name", "models")
    batch_size = connection_details.get("batch_size", 100)
    timeouts = connection_details.get("timeouts", {})
    connection_timeout = timeouts.get("connection_timeout", 30)
    operation_timeout = timeouts.get("operation_timeout", 10)

    # Connect to Couchbase
    bucket = time_operation(connect_to_couchbase, endpoint, username, password, bucket_name)

    # Ensure the scope and collection exist
    if not time_operation(ensure_scope_and_collection, bucket, scope_name, collection_name):
        logger.error("Failed to ensure scope or collection. Exiting.")
        return

    collection = bucket.scope(scope_name).collection(collection_name)

    # Load car models data
    car_models_data = load_and_validate_json('car_models.json', required_keys=["cars"])
    cars = car_models_data.get("cars", [])

    try:
        validate_car_models(cars)
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return

    if not cars:
        logger.warning("No car models found in the input data.")
        return

    # Process car models in batches
    start_time = time.time()
    total_processed = 0
    for batch in generate_car_model_data(cars, start_id=1, batch_size=batch_size):
        process_car_models(collection, batch, mode=args.mode)
        total_processed += len(batch)
        logger.info(f"Total processed so far: {total_processed}")

    end_time = time.time()
    logger.info(f"Processing Summary:")
    logger.info(f" - Total processed: {total_processed}")
    logger.info(f" - Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
