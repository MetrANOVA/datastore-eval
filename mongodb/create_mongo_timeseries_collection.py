import argparse
import sys

from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure


def create_timeseries_collection(mongo_uri, db_name, collection_name):
    """
    Connects to MongoDB and creates a time series collection with appropriate options
    and a compound index on metadata fields.

    Args:
        mongo_uri (str): The MongoDB connection string.
        db_name (str): The name of the database.
        collection_name (str): The name for the time series collection.
    """
    print(f"Attempting to connect to MongoDB at: {mongo_uri}")
    try:
        client = MongoClient(mongo_uri)
        # The ismaster command is cheap and does not require auth.
        client.admin.command("ismaster")
        print("MongoDB connection successful.")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        sys.exit(1)

    db = client[db_name]
    print(f"Using database: '{db_name}'")

    # --- Time Series Collection Options ---
    # 'timestamp' will be the name of the field holding the time
    # 'metadata' will be the name of the sub-document holding metadata fields
    timeseries_options = {
        "timeField": "timestamp",
        "metaField": "metadata",
        # 'granularity': 'seconds' # Optional: Helps optimize storage if data is regular.
        # Can be 'seconds', 'minutes', 'hours'.
        # Omit if interval varies (e.g., 30s and 60s mixed).
    }

    # --- Create the Time Series Collection ---
    try:
        db.create_collection(collection_name, timeseries=timeseries_options)
        print(f"Successfully created time series collection: '{collection_name}'")
    except CollectionInvalid:
        print(f"Collection '{collection_name}' already exists.")
        # Optionally check if it's actually a time series collection
        coll_options = db.command(
            {"listCollections": 1, "filter": {"name": collection_name}}
        )["cursor"]["firstBatch"][0].get("options", {})
        if coll_options.get("timeseries"):
            print(f"'{collection_name}' is already a time series collection.")
        else:
            print(
                f"Error: Collection '{collection_name}' exists but is NOT a time series collection."
            )
            print("Please drop it manually or choose a different name.")
            client.close()
            sys.exit(1)
    except OperationFailure as e:
        print(f"Operation Failure during collection creation: {e.details}")
        client.close()
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during collection creation: {e}")
        client.close()
        sys.exit(1)

    # --- Create Index on Metadata Fields ---
    # Indexing the metaField components is crucial for performance
    collection = db[collection_name]
    index_name = "metadata_node_interface_idx"
    try:
        # Create a compound index on the required metadata fields within the metaField
        collection.create_index(
            [("metadata.nodeName", 1), ("metadata.interfaceName", 1)], name=index_name
        )
        print(
            f"Successfully created compound index '{index_name}' on metadata.nodeName and metadata.interfaceName."
        )
    except OperationFailure as e:
        # Check if the index already exists (error code 85 for IndexOptionsConflict, 86 for IndexKeySpecsConflict)
        if e.code in [85, 86]:
            print(f"Index '{index_name}' likely already exists.")
            # You could verify by listing indexes if needed: list(collection.list_indexes())
        else:
            print(f"Operation Failure during index creation: {e.details}")
            # Decide if you want to exit here or continue
    except Exception as e:
        print(f"An unexpected error occurred during index creation: {e}")
        # Decide if you want to exit here or continue

    print("Setup complete.")
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a MongoDB Time Series Collection."
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
