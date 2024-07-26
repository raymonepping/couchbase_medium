"""
This script manages Couchbase users by creating, updating, and validating them 
based on a configuration provided in users.json.
It uses the roles defined in roles.json to assign roles to the users.
"""

import json
import os
import sys
import logging
import traceback

from datetime import timedelta
from dotenv import load_dotenv

from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions
from couchbase.management.users import User, Role

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename="app.log",
    filemode="w",
    level=logging.DEBUG,
    format="%(levelname)s::%(asctime)s::%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Add console logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    logging.Formatter("%(levelname)s::%(asctime)s::%(message)s")
)
logger = logging.getLogger()
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

# Suppress warnings from the Couchbase SDK
sdk_logger = logging.getLogger("couchbase")
sdk_logger.setLevel(logging.ERROR)

# Specifically suppress "Operations over threshold" warnings
ops_threshold_logger = logging.getLogger("couchbase.operation_threshold")
ops_threshold_logger.setLevel(logging.CRITICAL)


class SpecificWarningFilter(logging.Filter):
    """
    Filter to suppress specific warnings in the log output.
    """

    def filter(self, record):
        if "Operations over threshold" in record.getMessage():
            return False
        if (
            'requested network "auto" is not found, fallback to "default" host'
            in record.getMessage()
        ):
            return False
        if (
            "unable to find connected session with GCCCP support, retry in 2500ms"
            in record.getMessage()
        ):
            return False
        return True

    def add_to_logger(self, target_logger):
        """
        Add this filter to the specified logger.
        """
        target_logger.addFilter(self)


# Add the specific warning filter to the logger
logger.addFilter(SpecificWarningFilter())

# Load user configuration from users.json
with open("users.json", encoding="utf-8") as user_file:
    user_config = json.load(user_file)

# Load role definitions from roles.json
with open("roles.json", encoding="utf-8") as roles_file:
    role_definitions = json.load(roles_file)["roles"]

# Extract user information and environment variables
users = user_config.get("users", [])
endpoint = os.getenv("COUCHBASE_ENDPOINT")
admin_username = os.getenv("COUCHBASE_USERNAME")
admin_password = os.getenv("COUCHBASE_PASSWORD")
bucket_name = os.getenv("COUCHBASE_BUCKET")


def validate_configs():
    """
    Validate the user and role configurations.
    """
    if not users:
        logger.error("No users found in users.json.")
        sys.exit(1)

    for user in users:
        if "username" not in user or "password" not in user or "roles" not in user:
            logger.error("Invalid user configuration: %s", user)
            sys.exit(1)
        for role in user["roles"]:
            if role["role"] not in role_definitions:
                logger.error(
                    "Role definition for '%s' not found in roles.json", role["role"]
                )
                sys.exit(1)


def connect_to_cluster():
    """
    Connect to the Couchbase cluster and return the cluster object.
    """
    try:
        auth = PasswordAuthenticator(admin_username, admin_password)
        cluster = Cluster(f"couchbase://{endpoint}", ClusterOptions(auth))
        cluster.wait_until_ready(timedelta(seconds=5))
        logger.info("Successfully connected to Couchbase cluster.")
        return cluster
    except CouchbaseException as e:
        logger.error("Error connecting to Couchbase cluster: %s", e)
        logger.error(traceback.format_exc())
        raise


def create_user(user_manager, user):
    """
    Create or update a single user in Couchbase.
    """
    roles = []
    for role in user["roles"]:
        role_def = role_definitions.get(role["role"])
        if role_def is None:
            logger.error(
                "Role definition for '%s' not found in roles.json", role["role"]
            )
            continue
        if role_def.get("type") == "bucket":
            role_obj = Role(name=role_def["role"], bucket=bucket_name)
        else:
            role_obj = Role(name=role_def["role"])
        roles.append(role_obj)

    user_obj = User(
        username=user["username"],
        password=user["password"],
        roles=roles,
        display_name=user.get("display_name", user["username"]),
    )
    user_manager.upsert_user(user_obj)
    logger.info('User "%s" created/updated successfully.', user["username"])


def create_or_update_users(cluster):
    """
    Create or update users in Couchbase.
    """
    try:
        user_manager = cluster.users()
        for user in users:
            if user.get("create", False):
                create_user(user_manager, user)
    except CouchbaseException as e:
        logger.error("Error creating/updating users: %s", e)
        logger.error(traceback.format_exc())
        raise


def validate_users(cluster):
    """
    Validate the creation of users in Couchbase.
    """
    try:
        user_manager = cluster.users()
        for user in users:
            if user.get("create", False):
                retrieved_user = user_manager.get_user(user["username"])
                if retrieved_user:
                    logger.info('User "%s" validated successfully.', user["username"])
                    print(f"User's display name: {retrieved_user.user.display_name}")
                    roles = retrieved_user.user.roles
                    for r in roles:
                        print(
                            f"\tUser has role {r.name}, applicable to bucket {r.bucket}"
                        )
    except CouchbaseException as e:
        logger.error("Error validating users: %s", e)
        logger.error(traceback.format_exc())
        raise


def main():
    """
    Main function to connect to Couchbase, create/update users, and validate them.
    """
    try:
        validate_configs()
        cluster = connect_to_cluster()
        create_or_update_users(cluster)
        validate_users(cluster)
        logger.info("All users created and validated successfully!")
    except CouchbaseException as e:
        logger.error("ERR: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
