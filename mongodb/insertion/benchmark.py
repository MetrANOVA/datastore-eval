# benchmark.py
import argparse
import datetime
import json
import os
import sys
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure

# The only field name we need to know from the JSONL
MONGO_TIME_FIELD = "timestamp"


def parse_iso_timestamp_in_json(doc: dict):
    """Converts a timestamp string in a dict (from JSON) to a datetime object."""
    if MONGO_TIME_FIELD in doc and isinstance(doc[MONGO_TIME_FIELD], str):
        try:
            ts_str = doc[MONGO_TIME_FIELD]
            # fromisoformat in modern Python handles the +00:00 timezone correctly
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1] + "+00:00"
            doc[MONGO_TIME_FIELD] = datetime.datetime.fromisoformat(ts_str)
        except (ValueError, TypeError):
            # If parsing fails, return None to signal the document is invalid
            return None
    return doc


def main_insert_logic(args):
    log_prefix = f"[MongoWorker PID:{os.getpid()}]"
    print(
        f"{log_prefix} Starting. Processing pre-computed file: {args.input_file.name}"
    )

    try:
        client = MongoClient(args.uri, w=1)
        db = client[args.db]
        collection = db[args.collection]
        client.admin.command("ping")
        print(f"{log_prefix} Connected to MongoDB.")
    except Exception as e:
        print(f"{log_prefix} ERROR: Connecting to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)

    docs_processed = 0
    docs_sent = 0
    batches_sent = 0

    try:
        with open(args.input_file, "r", encoding="utf-8") as jsonl_file:
            batch = []

            for line_num, line in enumerate(jsonl_file):
                if args.limit >= 0 and docs_processed >= args.limit:
                    print(
                        f"{log_prefix} Reached processing limit of {args.limit} records."
                    )
                    break

                try:
                    doc = json.loads(line)
                    doc = parse_iso_timestamp_in_json(doc)
                    if doc is None:
                        print(
                            f"{log_prefix} WARN Line {line_num+1}: Invalid timestamp in JSON. Skipping.",
                            file=sys.stderr,
                        )
                        continue

                    batch.append(doc)
                    docs_processed += 1
                except json.JSONDecodeError:
                    print(
                        f"{log_prefix} WARN Line {line_num+1}: Invalid JSON. Skipping: {line.strip()[:100]}...",
                        file=sys.stderr,
                    )
                    continue

                if len(batch) >= args.batch_size:
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        docs_sent += len(result.inserted_ids)
                        batches_sent += 1
                        if batches_sent % 100 == 0:
                            print(f"{log_prefix} Sent {batches_sent} batches...")
                    except Exception as e:
                        print(
                            f"{log_prefix} ERROR inserting batch: {e}", file=sys.stderr
                        )
                    finally:
                        batch = []

            if batch:  # Final batch
                try:
                    result = collection.insert_many(batch, ordered=False)
                    docs_sent += len(result.inserted_ids)
                    batches_sent += 1
                except Exception as e:
                    print(
                        f"{log_prefix} ERROR inserting final batch: {e}",
                        file=sys.stderr,
                    )

    except Exception as e:
        print(f"{log_prefix} FATAL ERROR: {e}", file=sys.stderr)

    print(f"\n--- {log_prefix} Summary ---")
    print(
        f"{log_prefix} Processed {docs_processed} records from {args.input_file.name}."
    )
    print(
        f"{log_prefix} Attempted to send {docs_sent} documents in {batches_sent} batches."
    )

    if "client" in locals() and client:
        try:
            client.close()
        except:
            pass
    print(f"{log_prefix} Finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Lean MongoDB Inserter from Pre-computed JSON-Lines File."
    )
    parser.add_argument(
        "--uri", default="mongodb://localhost:27017/", help="MongoDB connection URI."
    )
    parser.add_argument("--db", required=True, help="MongoDB Database name.")
    parser.add_argument("--collection", required=True, help="MongoDB Collection name.")
    parser.add_argument(
        "--input_file",
        required=True,
        type=Path,
        help="Path to the pre-computed input JSON-Lines (.jsonl) file for this worker.",
    )
    parser.add_argument(
        "--batch_size", type=int, default=5000, help="Documents per insert batch."
    )
    parser.add_argument(
        "--limit", type=int, default=-1, help="Max records to process from this file."
    )

    args = parser.parse_args()
    if args.batch_size < 1:
        print("Error: --batch_size must be > 0.", file=sys.stderr)
        sys.exit(1)

    main_insert_logic(args)
