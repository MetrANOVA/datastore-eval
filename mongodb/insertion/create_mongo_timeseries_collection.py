# create_mongo_timeseries_collection.py (Corrected Wildcard Index)

import argparse
import sys

from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure


def create_timeseries_collection(mongo_uri, db_name, collection_name):
    """
    Connects to MongoDB and creates a time series collection with appropriate options,
    a specific compound index on device/interfaceName, and a wildcard index
    on the rest of the metadata.

    Args:
        mongo_uri (str): The MongoDB connection string.
        db_name (str): The name of the database.
        collection_name (str): The name for the time series collection.
    """
    print(f"Attempting to connect to MongoDB at: {mongo_uri}")
    try:
        client = MongoClient(mongo_uri)
        client.admin.command("ping")
        print("MongoDB connection successful.")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)

    db = client[db_name]
    print(f"Using database: '{db_name}'")

    timeseries_options = {
        "timeField": "timestamp",
        "metaField": "meta",
        "granularity": "minutes",
    }

    try:
        db.create_collection(collection_name, timeseries=timeseries_options)
        print(f"Successfully created time series collection: '{collection_name}'")
    except CollectionInvalid:
        print(f"Collection '{collection_name}' already exists.")
        try:
            coll_info_list = list(
                db.list_collections(filter={"name": collection_name}, limit=1)
            )
            if not coll_info_list:
                raise LookupError(
                    f"Collection {collection_name} reported existing but not found."
                )
            coll_options = coll_info_list[0].get("options", {})
            if coll_options.get("timeseries"):
                print(f"'{collection_name}' is already a time series collection.")
            else:
                print(
                    f"Error: Collection '{collection_name}' exists but is NOT a time series collection.",
                    file=sys.stderr,
                )
                client.close()
                sys.exit(1)
        except Exception as list_coll_e:
            print(
                f"Error verifying existing collection '{collection_name}': {list_coll_e}",
                file=sys.stderr,
            )
            client.close()
            sys.exit(1)
    except OperationFailure as e:
        print(
            f"Operation Failure during collection creation: {e.details}",
            file=sys.stderr,
        )
        client.close()
        sys.exit(1)
    except Exception as e:
        print(
            f"An unexpected error occurred during collection creation: {e}",
            file=sys.stderr,
        )
        client.close()
        sys.exit(1)

    collection = db[collection_name]

    # --- Attempt to Drop the Unexpected Index ---
    unexpected_index_name = "metadata_1_timestamp_1"
    print(
        f"Attempting to drop unexpected index '{unexpected_index_name}' if it exists..."
    )
    try:
        collection.drop_index(unexpected_index_name)
        print(f"Successfully dropped index '{unexpected_index_name}'.")
    except OperationFailure as e:
        # Error code for "index not found" is 27 for some versions/contexts
        # Check by code_name or common error message components
        if (
            e.code == 27
            or e.code_name == "IndexNotFound"
            or "index not found" in str(e.details).lower()
        ):
            print(f"Index '{unexpected_index_name}' was not found (which is good).")
        else:
            # Some other error occurred trying to drop it
            print(
                f"Warning: Operation Failure while trying to drop index '{unexpected_index_name}': {e.details}",
                file=sys.stderr,
            )
    except Exception as e:
        print(
            f"Warning: An unexpected error occurred while trying to drop index '{unexpected_index_name}': {e}",
            file=sys.stderr,
        )

    # --- 1. Specific Compound Index for Primary Identifiers ---
    index_name_primary = "metadata_device_interface_idx"
    index_keys_primary = [("metadata.device", 1), ("metadata.interfaceName", 1)]
    try:
        collection.create_index(index_keys_primary, name=index_name_primary)
        print(
            f"Successfully created primary compound index '{index_name_primary}' on {index_keys_primary}."
        )
    except OperationFailure as e:
        if (
            e.code in [85, 86]
            or "Index already exists" in str(e.details).lower()
            or "index with matching name" in str(e.details).lower()
        ):
            print(
                f"Primary compound index '{index_name_primary}' likely already exists."
            )
        else:
            print(
                f"Operation Failure during primary index creation: {e.details}",
                file=sys.stderr,
            )
    except Exception as e:
        print(
            f"An unexpected error occurred during primary index creation: {e}",
            file=sys.stderr,
        )

    # --- 2. Wildcard Index for All Other Metadata Fields ---
    # Corrected syntax:
    # index_name_wildcard = "metadata_all_subfields_wildcard_idx"  # More descriptive name
    # index_keys_wildcard = {
    #     "metadata.$**": 1
    # }  # Apply wildcard to all paths under 'metadata'
    # try:
    #     collection.create_index(index_keys_wildcard, name=index_name_wildcard)
    #     print(
    #         f"Successfully created wildcard index '{index_name_wildcard}' on all 'metadata' sub-fields."
    #     )
    # except OperationFailure as e:
    #     if (
    #         e.code in [85, 86]
    #         or "Index already exists" in str(e.details).lower()
    #         or "index with matching name" in str(e.details).lower()
    #     ):
    #         print(f"Wildcard index '{index_name_wildcard}' likely already exists.")
    #     else:
    #         print(
    #             f"Operation Failure during wildcard index creation for key {index_keys_wildcard}: {e.details}",
    #             file=sys.stderr,
    #         )
    # except Exception as e:
    #     print(
    #         f"An unexpected error occurred during wildcard index creation for key {index_keys_wildcard}: {e}",
    #         file=sys.stderr,
    #     )

    print("Setup complete.")
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a MongoDB Time Series Collection with specific and wildcard indexes."
    )
    parser.add_argument(
        "--uri",
        default="mongodb://localhost:27017/",
        help="MongoDB connection URI (default: mongodb://localhost:27017/)",
    )
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument(
        "--collection", required=True, help="Time series collection name"
    )

    args = parser.parse_args()
    create_timeseries_collection(args.uri, args.db, args.collection)
