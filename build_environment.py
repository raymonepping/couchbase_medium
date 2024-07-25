"""
This script connects to a Couchbase cluster, validates the existence of a bucket,
and creates a bucket, scopes, collections, and indexes based on the config.json configuration.
"""

import json
import os
import sys
import logging
import traceback

from datetime import timedelta
from dotenv import load_dotenv

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, WaitUntilReadyOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.diagnostics import ServiceType
from couchbase.management.queries import CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions
from couchbase.management.search import SearchIndexManager
from couchbase.management.buckets import CreateBucketSettings, BucketType, ConflictResolutionType
from couchbase.management.collections import CreateCollectionSettings
from couchbase.exceptions import (
    CouchbaseException,
    CollectionAlreadyExistsException,
    ScopeAlreadyExistsException,
    BucketAlreadyExistsException,
    ScopeNotFoundException,
    QueryIndexAlreadyExistsException
)

# Import the couchbase module directly
import couchbase

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='app.log',
    filemode='w',
    level=logging.DEBUG,
    format='%(levelname)s::%(asctime)s::%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Add console logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(levelname)s::%(asctime)s::%(message)s'))
logger = logging.getLogger()
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

# Suppress warnings from the Couchbase SDK
sdk_logger = logging.getLogger("couchbase")
sdk_logger.setLevel(logging.ERROR)

# Specifically suppress "Operations over threshold" warnings
ops_threshold_logger = logging.getLogger("couchbase.operation_threshold")
ops_threshold_logger.setLevel(logging.CRITICAL)

# Add a filter to suppress specific warnings
class SpecificWarningFilter(logging.Filter):
    def filter(self, record):
        if "Operations over threshold" in record.getMessage():
            return False
        if "requested network \"auto\" is not found, fallback to \"default\" host" in record.getMessage():
            return False
        if "unable to find connected session with GCCCP support, retry in 2500ms" in record.getMessage():
            return False
        return True

logger.addFilter(SpecificWarningFilter())

if hasattr(couchbase, 'configure_logging'):
    couchbase.configure_logging(logger.name, level=logger.level)

# Load configuration from config.json
with open('config.json', encoding='utf-8') as config_file:
    config = json.load(config_file)

CREATE_BUCKET = config.get('createBucket', False)
BUCKET_CONFIG = config.get('bucket', {})
BUCKET_NAME = BUCKET_CONFIG.get('name')
RAM_QUOTA_MB = BUCKET_CONFIG.get('ramQuotaMB', 100)
FLUSH_ENABLED = BUCKET_CONFIG.get('flushEnabled', False)
REPLICA_INDEX = BUCKET_CONFIG.get('replicaIndex', False)
NUM_REPLICAS = BUCKET_CONFIG.get('numReplicas', 1)
BUCKET_TYPE = BUCKET_CONFIG.get('bucketType', 'couchbase').upper()
SCOPES = config.get('scopes', [])
INDEXES = config.get('indexes', {})

def validate_config():
    """
    Validate the configuration loaded from config.json.
    """
    if not BUCKET_NAME:
        logger.error('Bucket name must be specified in config.json.')
        sys.exit(1)
    if not isinstance(SCOPES, list):
        logger.error('Scopes must be a list in config.json.')
        sys.exit(1)
    if not isinstance(INDEXES, dict):
        logger.error('Indexes must be a dictionary in config.json.')
        sys.exit(1)

def connect_to_cluster():
    """
    Connect to the Couchbase cluster and return the cluster object.
    """
    auth = PasswordAuthenticator(os.getenv('COUCHBASE_USERNAME'), os.getenv('COUCHBASE_PASSWORD'))
    cluster = Cluster(
        f'couchbase://{os.getenv("COUCHBASE_ENDPOINT")}', 
        ClusterOptions(auth)
    )
    cluster.wait_until_ready(
        timedelta(seconds=5), 
        WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query])
    )
    logger.info("Successfully connected to Couchbase cluster.")
    return cluster

def create_bucket(cluster):
    """
    Create a bucket if it does not exist.
    """
    bucket_manager = cluster.buckets()
    try:
        bucket_settings = CreateBucketSettings(
            name=BUCKET_NAME,
            flush_enabled=FLUSH_ENABLED,
            ram_quota_mb=RAM_QUOTA_MB,
            num_replicas=NUM_REPLICAS,
            bucket_type=BucketType[BUCKET_TYPE],
            conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER
        )
        bucket_manager.create_bucket(bucket_settings)
        logger.info('Bucket "%s" created with RAM quota %d MB.', BUCKET_NAME, RAM_QUOTA_MB)
    except BucketAlreadyExistsException:
        logger.info('Bucket "%s" already exists.', BUCKET_NAME)

def get_bucket(cluster):
    """
    Get a reference to the specified bucket.
    """
    bucket = cluster.bucket(BUCKET_NAME)
    logger.info('Successfully connected to bucket "%s".', BUCKET_NAME)
    return bucket

def create_scope_and_collections(collection_manager, scope, collections):
    """
    Create a scope and its collections if they do not exist.
    """
    logger.info('Creating scope "%s" with collections: %s', scope, collections)
    try:
        collection_manager.create_scope(scope)
        logger.info('Scope "%s" created.', scope)
    except ScopeAlreadyExistsException:
        logger.info('Scope "%s" already exists.', scope)

    for collection in collections:
        try:
            collection_manager.create_collection(
                scope_name=scope, 
                collection_name=collection,
                settings=CreateCollectionSettings()
            )
            logger.info('Collection "%s" created in scope "%s".', collection, scope)
        except CollectionAlreadyExistsException:
            logger.info('Collection "%s" already exists in scope "%s".', collection, scope)
        except ScopeNotFoundException:
            logger.error('The scope "%s" does not exist', scope)

def create_indexes(cluster):
    """
    Create primary, secondary, and FTS indexes if they do not exist.
    """
    query_index_manager = cluster.query_indexes()
    search_index_manager = SearchIndexManager(cluster)

    for primary in INDEXES.get("primary", []):
        if primary.get("create", False):
            scope = primary.get("scope")
            collection = primary.get("collection")
            try:
                logger.info('Creating primary index on collection "%s" in scope "%s".', collection, scope)
                query_index_manager.create_primary_index(
                    BUCKET_NAME,
                    CreatePrimaryQueryIndexOptions(
                        scope_name=scope,
                        collection_name=collection,
                        ignore_if_exists=True
                    )
                )
                logger.info(
                    'Primary index created on collection "%s" in scope "%s".', 
                    collection, scope
                )
            except ScopeNotFoundException:
                logger.error('Scope "%s" not found for collection "%s".', scope, collection)
            except CouchbaseException as e:
                logger.error('Error creating primary index on collection "%s" in scope "%s": %s', collection, scope, e)

    for index in INDEXES.get("secondary", []):
        if index.get("create", False):
            index_name = index['name']
            fields = index['fields']
            scope = index.get("scope")
            collection = index.get("collection")
            try:
                logger.info('Creating secondary index "%s" on collection "%s" in scope "%s".', index_name, collection, scope)
                query_index_manager.create_index(
                    BUCKET_NAME,
                    index_name,
                    fields,
                    CreateQueryIndexOptions(
                        scope_name=scope,
                        collection_name=collection
                    )
                )
                logger.info(
                    'Secondary index "%s" created on collection "%s" in scope "%s".', 
                    index_name, collection, scope
                )
            except QueryIndexAlreadyExistsException:
                logger.info(
                    'Secondary index "%s" already exists on collection "%s" in scope "%s".', 
                    index_name, collection, scope
                )
            except CouchbaseException as e:
                logger.error('Error creating secondary index "%s" on collection "%s" in scope "%s": %s', index_name, collection, scope, e)

    for fts_index in INDEXES.get("fts", []):
        if fts_index.get("create", False):
            fts_index_name = fts_index['name']
            scope = fts_index.get("scope")
            collection = fts_index.get("collection")
            fts_index_spec = {
                "name": fts_index_name,
                "type": "fulltext-index",
                "sourceName": BUCKET_NAME,
                "sourceType": "couchbase",
                "planParams": {
                    "indexPartitions": 6
                },
                "params": {
                    "doc_config": {
                        "mode": "scope.collection" if scope and collection else "bucket",
                        "scope_name": scope,
                        "collection_name": collection
                    },
                    "mapping": {
                        "default_mapping": {
                            "enabled": True
                        }
                    }
                }
            }
            try:
                logger.info('Creating FTS index "%s" on collection "%s" in scope "%s".', fts_index_name, collection, scope)
                search_index_manager.get_index(fts_index_name)
                logger.info('FTS index "%s" already exists.', fts_index_name)
            except CouchbaseException:
                search_index_manager.upsert_index(fts_index_spec)
                logger.info(
                    'FTS index "%s" created on collection "%s" in scope "%s".', 
                    fts_index_name, collection, scope
                )

def main():
    """
    Main function to perform all checks and create bucket, scopes, collections, and indexes.
    """
    validate_config()

    try:
        cluster = connect_to_cluster()
        if CREATE_BUCKET:
            create_bucket(cluster)
        bucket = get_bucket(cluster)
        collection_manager = bucket.collections()

        for scope_config in SCOPES:
            if scope_config.get("create", False):
                create_scope_and_collections(collection_manager, scope_config["name"], scope_config["collections"])

        create_indexes(cluster)

        logger.info('All bucket, scopes, collections, and indexes created successfully!')
    except CouchbaseException as err:
        logger.error('ERR: %s', str(err))
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
