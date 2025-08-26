import json
import sys
import time
from datetime import datetime

import pymongo
from bson.objectid import ObjectId


def run_query(ip, db_name, collection_name, query_filename):
    """
    Connects to MongoDB, reads a query pipeline from a file,
    executes it, and returns the result.
    """
    # Read the JSON file and parse it into a Python object
    with open(query_filename, "r") as f:
        pipeline = json.load(f)

    # Convert date strings from the file into proper datetime objects for pymongo
    # This is a simple example; a more complex solution could search recursively
    if pipeline and "$match" in pipeline[0] and "timestamp" in pipeline[0]["$match"]:
        gte_date_str = pipeline[0]["$match"]["timestamp"]["$gte"]
        lte_date_str = pipeline[0]["$match"]["timestamp"]["$lte"]
        pipeline[0]["$match"]["timestamp"]["$gte"] = datetime.fromisoformat(
            gte_date_str.replace("Z", "+00:00")
        )
        pipeline[0]["$match"]["timestamp"]["$lte"] = datetime.fromisoformat(
            lte_date_str.replace("Z", "+00:00")
        )

    client = pymongo.MongoClient(f"mongodb://{ip}:27017")
    db = client[db_name]
    collection = db[collection_name]

    start_time = time.perf_counter()

    # Execute the pipeline using the standard, reliable aggregate method
    query_result = list(collection.aggregate(pipeline))

    end_time = time.perf_counter()
    client.close()

    # Custom serializer to handle datetime and ObjectId for JSON output
    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    return {
        "exec_time": end_time - start_time,
        "result": json.loads(json.dumps(query_result, default=json_serializer)),
    }


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: python mongo_runner.py <ip> <db_name> <collection_name> <query_filename>",
            file=sys.stderr,
        )
        sys.exit(1)

    _, ip, db_name, collection_name, query_filename = sys.argv

    try:
        data = run_query(ip, db_name, collection_name, query_filename)
        print(json.dumps(data))
    except Exception as e:
        print(
            f"Error executing query from file '{query_filename}': {e}", file=sys.stderr
        )
        sys.exit(1)
