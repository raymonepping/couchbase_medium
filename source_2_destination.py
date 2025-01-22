import json
import time
import sys
import logging
import warnings

from datetime import timedelta
from collections import defaultdict

# Suppress certain OpenSSL warnings if needed
from urllib3.exceptions import NotOpenSSLWarning
warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

# --- Couchbase Python SDK ---
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (
    ClusterOptions,
    GetOptions,
    UpsertOptions,
    QueryOptions
)
from couchbase.exceptions import (
    CollectionAlreadyExistsException,
    ScopeAlreadyExistsException,
    CouchbaseException
)

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def ensure_target_scope_and_collection(cluster, bucket_name, scope_name, collection_name):
    """
    Ensures the given scope/collection exist on the target cluster & bucket.
    If missing, creates them using the Collections Management API.
    """
    bucket = cluster.bucket(bucket_name)
    cm = bucket.collections()

    # Check if scope exists
    all_scopes = cm.get_all_scopes()
    scope_exists = any(s.name == scope_name for s in all_scopes)
    if not scope_exists:
        logging.info(f"[Target] Scope '{scope_name}' not found in bucket '{bucket_name}'. Creating...")
        try:
            cm.create_scope(scope_name)
            logging.info(f"[Target] Created scope '{scope_name}'.")
        except ScopeAlreadyExistsException:
            logging.info(f"[Target] Scope '{scope_name}' already exists.")

    # Ensure collection exists
    try:
        cm.create_collection(scope_name, collection_name)
        logging.info(f"[Target] Created collection '{collection_name}' in scope '{scope_name}'.")
    except CollectionAlreadyExistsException:
        logging.info(f"[Target] Collection '{collection_name}' in scope '{scope_name}' already exists.")
    except CouchbaseException as e:
        logging.error(f"[Target] Error creating collection '{collection_name}' in scope '{scope_name}': {e}")


def main(config_path: str):
    """
    Reads input_config.json, then for each source collection listed:
      1) Verify/create the target scope+collection.
      2) Query source doc IDs.
      3) KV GET each doc, upsert into target.
      4) Logs progress with detailed metrics at the end.
    """
    # Step 1: Load configuration
    with open(config_path, "r") as f:
        config = json.load(f)

    # Unpack Source config
    src_cfg = config["source"]
    src_conn_str = src_cfg["connection_string"]
    src_username = src_cfg["username"]
    src_password = src_cfg["password"]
    src_bucket_name = src_cfg["bucket"]
    src_scope_name = src_cfg["scope"]
    src_collection_list = src_cfg["collections"]

    # Unpack Target config
    tgt_cfg = config["target"]
    tgt_conn_str = tgt_cfg["connection_string"]
    tgt_username = tgt_cfg["username"]
    tgt_password = tgt_cfg["password"]
    tgt_bucket_name = tgt_cfg["bucket"]
    tgt_scope_name = tgt_cfg["scope"]

    # Metrics data structure
    metrics = defaultdict(lambda: {"transferred": 0, "query_time": 0, "kv_get_time": 0, "kv_upsert_time": 0})

    # Step 2: Connect to Source cluster
    logging.info("\nConnecting to source cluster...")
    source_auth = PasswordAuthenticator(src_username, src_password)
    source_cluster = Cluster(src_conn_str, ClusterOptions(source_auth))
    source_cluster.wait_until_ready(timedelta(seconds=10))
    logging.info(f"Source cluster ready (bucket='{src_bucket_name}', scope='{src_scope_name}')\n")

    # Step 3: Connect to Target cluster
    logging.info("Connecting to target cluster...")
    target_auth = PasswordAuthenticator(tgt_username, tgt_password)
    target_cluster = Cluster(tgt_conn_str, ClusterOptions(target_auth))
    target_cluster.wait_until_ready(timedelta(seconds=10))
    logging.info(f"Target cluster ready (bucket='{tgt_bucket_name}', scope='{tgt_scope_name}')\n")

    # Track total docs & start time
    total_docs_transferred = 0
    overall_start_time = time.time()

    # Replicate each source collection to a matching name on target
    for src_coll_name in src_collection_list:
        tgt_coll_name = src_coll_name
        logging.info(f"\n--- Now replicating from '{src_scope_name}.{src_coll_name}' -> '{tgt_scope_name}.{tgt_coll_name}' ---\n")

        # Ensure the target scope/collection exist
        ensure_target_scope_and_collection(target_cluster, tgt_bucket_name, tgt_scope_name, tgt_coll_name)

        # Query source for doc IDs
        query_str = (
            f"SELECT META().id AS doc_id "
            f"FROM `{src_bucket_name}`.`{src_scope_name}`.`{src_coll_name}`"
        )
        logging.info(f"Querying source doc IDs with: {query_str}\n")

        start_query_time = time.time()
        try:
            rows = source_cluster.query(query_str)
            metrics[src_coll_name]["query_time"] += time.time() - start_query_time
        except CouchbaseException as e:
            logging.error(f"Error querying source collection '{src_coll_name}': {e}")
            continue

        # Get collection objects
        source_bucket = source_cluster.bucket(src_bucket_name)
        source_collection = source_bucket.scope(src_scope_name).collection(src_coll_name)

        target_bucket = target_cluster.bucket(tgt_bucket_name)
        target_collection = target_bucket.scope(tgt_scope_name).collection(tgt_coll_name)

        # Transfer docs (KV get + upsert)
        doc_count_for_collection = 0
        for row in rows:
            doc_id = row.get("doc_id")
            if not doc_id:
                continue

            # KV GET from source
            start_kv_get_time = time.time()
            try:
                get_res = source_collection.get(doc_id, GetOptions(timeout=timedelta(seconds=5)))
                doc_content = get_res.content_as[dict]
                metrics[src_coll_name]["kv_get_time"] += time.time() - start_kv_get_time
            except CouchbaseException as get_err:
                logging.error(f"Error GET doc_id={doc_id} from {src_coll_name}: {get_err}")
                continue

            # KV Upsert to target
            start_kv_upsert_time = time.time()
            try:
                target_collection.upsert(doc_id, doc_content, UpsertOptions(timeout=timedelta(seconds=5)))
                metrics[src_coll_name]["kv_upsert_time"] += time.time() - start_kv_upsert_time
                doc_count_for_collection += 1
                total_docs_transferred += 1
                metrics[src_coll_name]["transferred"] += 1

                if doc_count_for_collection % 100 == 0:
                    logging.info(f"{src_coll_name}: upserted {doc_count_for_collection} docs so far...")

            except CouchbaseException as upsert_err:
                logging.error(f"Error upserting doc_id={doc_id} into target: {upsert_err}")

        logging.info(
            f"\nFinished transferring {doc_count_for_collection} docs from '{src_coll_name}' -> '{tgt_coll_name}'.\n"
        )

    # Final summary
    overall_end_time = time.time()
    elapsed = overall_end_time - overall_start_time
    logging.info("\n=== Replication completed ===\n")
    logging.info(f"Total docs transferred: {total_docs_transferred}")
    logging.info(f"Time taken: {elapsed:.2f} seconds.\n")

    # Detailed Metrics
    logging.info("--- Detailed Metrics ---")
    for collection, data in metrics.items():
        logging.info(f"\nCollection: {collection}")
        logging.info(f"  Documents Transferred: {data['transferred']}")
        logging.info(f"  Query Time: {data['query_time']:.2f} seconds")
        logging.info(f"  KV GET Time: {data['kv_get_time']:.2f} seconds")
        logging.info(f"  KV Upsert Time: {data['kv_upsert_time']:.2f} seconds")

    # Close clusters
    source_cluster.close()
    target_cluster.close()
    logging.info("\nClosed source and target clusters.\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Usage: python source_2_destination.py <path_to_input_config.json>")
        sys.exit(1)

    config_file = sys.argv[1]
    main(config_file)
