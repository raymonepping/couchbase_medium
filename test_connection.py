"""
This script connects to a Couchbase cluster, verifies the existence of a bucket,
scope, collection, and a Full Text Search (FTS) index within the specified scope.
"""

import os
import sys
import logging
from datetime import timedelta
from dotenv import load_dotenv
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection details from the .env file
CLUSTER_CONN_STR = f'couchbase://{os.getenv("COUCHBASE_ENDPOINT")}'
USERNAME = os.getenv("COUCHBASE_USERNAME")
PASSWORD = os.getenv("COUCHBASE_PASSWORD")
BUCKET_NAME = os.getenv("COUCHBASE_BUCKET")
SCOPE_NAME = os.getenv("COUCHBASE_SCOPE")
COLLECTION_NAME = os.getenv("COUCHBASE_COLLECTION")
INDEX_NAME = os.getenv("COUCHBASE_INDEX")

def connect_to_cluster():
    """
    Connect to the Couchbase cluster and return the cluster object.
    """
    auth = PasswordAuthenticator(USERNAME, PASSWORD)
    cluster = Cluster(CLUSTER_CONN_STR, ClusterOptions(auth))
    cluster.wait_until_ready(timedelta(seconds=5))
    logger.info("Successfully connected to Couchbase cluster.")
    return cluster

def get_bucket(cluster):
    """
    Get a reference to the specified bucket.
    """
    bucket = cluster.bucket(BUCKET_NAME)
    logger.info('Successfully connected to bucket "%s".', BUCKET_NAME)
    return bucket

def check_scope_exists(bucket):
    """
    Check if the specified scope exists within the bucket.
    """
    collection_manager = bucket.collections()
    scopes = collection_manager.get_all_scopes()
    if any(scope.name == SCOPE_NAME for scope in scopes):
        logger.info('Scope "%s" exists.', SCOPE_NAME)
        return True
    logger.error('Scope "%s" does not exist.', SCOPE_NAME)
    return False

def check_collection_exists(bucket):
    """
    Check if the specified collection exists within the scope.
    """
    collection_manager = bucket.collections()
    scopes = collection_manager.get_all_scopes()
    collections = [
        col.name
        for scope in scopes
        if scope.name == SCOPE_NAME
        for col in scope.collections
    ]
    if COLLECTION_NAME in collections:
        logger.info(
            'Collection "%s" exists within scope "%s".',
            COLLECTION_NAME, SCOPE_NAME
        )
        return True
    logger.error(
        'Collection "%s" does not exist within scope "%s".',
        COLLECTION_NAME, SCOPE_NAME
    )
    return False

def check_index_exists(cluster):
    """
    Check if the specified Full Text Search (FTS) index exists within the scope.
    """
    search_manager = cluster.search_indexes()
    indexes = search_manager.get_all_indexes()
    scoped_index_name = f"{BUCKET_NAME}.{SCOPE_NAME}.{INDEX_NAME}"
    index_exists = any(index.name == scoped_index_name for index in indexes)
    if index_exists:
        logger.info('FTS Index "%s" exists.', scoped_index_name)
        return True
    logger.error('FTS Index "%s" not found', scoped_index_name)
    return False

def main():
    """
    Main function to perform all checks.
    """
    try:
        cluster = connect_to_cluster()
        bucket = get_bucket(cluster)
        if check_scope_exists(bucket):
            if check_collection_exists(bucket):
                if check_index_exists(cluster):
                    logger.info('All checks passed successfully!')
    except CouchbaseException as err:
        logger.error('ERR: %s', str(err))
        sys.exit(1)

if __name__ == "__main__":
    main()
