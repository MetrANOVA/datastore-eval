import argparse
import csv
import datetime
import os
import statistics
import sys
import time
from pathlib import Path

import bson
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure

# --- Standardized Configuration ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.device"  # Mapped to 'device' in MongoDB metadata
TSV_INTERFACE_COL = "meta.name"  # Mapped to 'interfaceName' in MongoDB metadata
REQUIRED_TSV_COLS = [TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTERFACE_COL]
FIXED_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
DISCARD_TSV_FIELDS = {"@collect_time_min", "@exit_time", "@processing_time"}

# --- !!! USER CONFIGURATION: Define TSV fields to go into MongoDB 'metadata' field !!! ---
# Add the original TSV header names of fields you want to ensure are in the 'metadata' sub-document.
# 'meta.device' and 'meta.name' are already handled.
# If a field here starts with 'meta.', the prefix will be stripped for the MongoDB key.
EXPLICIT_METADATA_FIELDS_TSV_NAMES = {
    "meta.device_info.loc_name",
    "meta.device_info.loc_type",
    "meta.device_info.role",
    "meta.ifindex",
}
# --- End User Configuration ---


# --- Helper Function (parse_timestamp - using fixed format version) ---
def parse_timestamp(ts_string):
    fmt = FIXED_TIMESTAMP_FORMAT
    try:
        ts_string_adj, fmt_adj = ts_string, fmt
        if fmt.endswith("Z") and ts_string.endswith("Z"):
            ts_string_adj, fmt_adj = ts_string[:-1] + "+0000", fmt[:-1] + "%z"
        if "." not in ts_string_adj and "%f" in fmt_adj:
            if (
                "%z" in fmt_adj
                and len(ts_string_adj) > 6
                and ts_string_adj[-6] in ("+", "-")
            ):
                offset_part = ts_string_adj[-6:]
                base_ts_part = ts_string_adj[:-6]
                ts_string_adj = base_ts_part + ".000" + offset_part
            elif "%z" not in fmt_adj:
                ts_string_adj += ".000"
        dt = datetime.datetime.strptime(ts_string_adj, fmt_adj)
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt
    except Exception:
        return None


# --- Main Function ---
def insert_data(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    tsv_file: Path,
    batch_size: int,
    worker_num: int,
    total_workers: int,
    limit: int,
):
    worker_log_prefix = f"[Worker {worker_num+1}/{total_workers}]"
    print(f"{worker_log_prefix} Starting. PID: {os.getpid()}")
    print(f"{worker_log_prefix} Processing TSV File: {tsv_file}")
    print(
        f"{worker_log_prefix} Explicit metadata TSV fields: {EXPLICIT_METADATA_FIELDS_TSV_NAMES if EXPLICIT_METADATA_FIELDS_TSV_NAMES else 'None (beyond device/interfaceName)'}"
    )
    # ... (other initial print statements from previous script) ...
    print(f"{worker_log_prefix} Target DB: {db_name}, Collection: {collection_name}")
    print(f"{worker_log_prefix} Document Batch Size: {batch_size}")
    print(
        f"{worker_log_prefix} Max docs to process by this worker: {'No limit' if limit < 0 else limit}"
    )
    print(f"{worker_log_prefix} Using fixed timestamp format: {FIXED_TIMESTAMP_FORMAT}")
    print(f"{worker_log_prefix} Discarding TSV fields: {DISCARD_TSV_FIELDS}")
    print(
        f"{worker_log_prefix} Prefixes 'values.' and 'meta.' (for non-explicit meta) will be stripped for MongoDB field names."
    )
    print(f"{worker_log_prefix} Connecting to MongoDB: {mongo_uri} (Write Concern w=1)")

    if not tsv_file.is_file():
        print(
            f"{worker_log_prefix} Error: Input TSV file not found: '{tsv_file}'",
            file=sys.stderr,
        )
        sys.exit(1)
    if worker_num < 0 or worker_num >= total_workers:
        print(
            f"{worker_log_prefix} Error: worker_num ({worker_num}) out of range for total_workers ({total_workers}).",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        client = MongoClient(mongo_uri, w=1)
        db = client[db_name]
        collection = db[collection_name]
        client.admin.command("ping")
        print(f"{worker_log_prefix} Connected to MongoDB.")
    except Exception as e:
        print(f"{worker_log_prefix} Error connecting to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)

    lines_processed_by_this_worker = 0
    docs_sent_by_this_worker = 0
    batches_sent_by_this_worker = 0
    file_processing_error = False

    try:
        print(f"{worker_log_prefix} Opening file: {tsv_file.name}...")
        with open(tsv_file, "r", newline="", encoding="utf-8") as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter="\t")
            tsv_header = reader.fieldnames
            if not tsv_header:
                raise ValueError("Header row is empty or could not be read.")
            missing_req = [col for col in REQUIRED_TSV_COLS if col not in tsv_header]
            if missing_req:
                raise ValueError(f"File missing required TSV headers: {missing_req}")
            print(f"{worker_log_prefix} Header verified. Starting data processing...")

            batch = []
            file_line_index = 0

            for row_data_dict in reader:
                if (file_line_index % total_workers) == worker_num:
                    if limit >= 0 and lines_processed_by_this_worker >= limit:
                        print(
                            f"{worker_log_prefix} Reached processing limit of {limit} assigned documents."
                        )
                        break

                    lines_processed_by_this_worker += 1
                    valid_doc = True
                    parse_errors = []
                    mongo_doc = {}
                    metadata_subdoc = {}
                    top_level_fields = {}

                    # 1. Timestamp (Required for mongo_doc['timestamp'])
                    ts_val = row_data_dict.get(TSV_TIMESTAMP_COL)
                    if ts_val:
                        parsed_datetime = parse_timestamp(ts_val)
                        if parsed_datetime:
                            mongo_doc["timestamp"] = parsed_datetime
                        else:
                            parse_errors.append(f"Invalid TS '{ts_val}'")
                            valid_doc = False
                    else:
                        parse_errors.append(f"Missing TS ('{TSV_TIMESTAMP_COL}')")
                        valid_doc = False

                    if (
                        not valid_doc
                    ):  # If timestamp fails, don't process rest of row for this doc
                        print(
                            f"{worker_log_prefix} WARN DataRowIndex {lines_processed_by_this_worker} (file line ~{file_line_index+2}): Skipping due to TS error: {'; '.join(parse_errors)}",
                            file=sys.stderr,
                        )
                        file_line_index += 1
                        continue

                    # 2. Handle specific mappings for node and interface (always go to metadata)
                    node_val = row_data_dict.get(TSV_NODE_COL)
                    intf_val = row_data_dict.get(TSV_INTERFACE_COL)
                    if node_val:
                        metadata_subdoc["device"] = node_val
                    else:
                        parse_errors.append(f"Missing/Empty '{TSV_NODE_COL}'")
                        valid_doc = False
                    if intf_val:
                        metadata_subdoc["interfaceName"] = intf_val
                    else:
                        parse_errors.append(f"Missing/Empty '{TSV_INTERFACE_COL}'")
                        valid_doc = False

                    # If required device/interfaceName are missing, the document is invalid
                    if not valid_doc:
                        print(
                            f"{worker_log_prefix} WARN DataRowIndex {lines_processed_by_this_worker} (file line ~{file_line_index+2}): Skipping due to missing device/interface: {'; '.join(parse_errors)}",
                            file=sys.stderr,
                        )
                        file_line_index += 1
                        continue

                    # 3. Process all other fields from the TSV row
                    for key, value in row_data_dict.items():
                        if (
                            key == TSV_TIMESTAMP_COL
                            or key == TSV_NODE_COL
                            or key == TSV_INTERFACE_COL
                        ):
                            continue  # Already handled

                        if key in DISCARD_TSV_FIELDS:
                            continue  # Explicitly discard

                        is_empty_value = value is None or value == ""

                        # measurements from 'values.*'
                        if key.startswith("values."):
                            new_key = key.replace("values.", "", 1)
                            if is_empty_value:
                                top_level_fields[new_key] = None
                            else:
                                try:
                                    top_level_fields[new_key] = float(value)
                                except (ValueError, TypeError):
                                    parse_errors.append(
                                        f"Non-numeric for measurement '{key}'->'{new_key}': '{value}'. Storing as null."
                                    )
                                    top_level_fields[new_key] = None
                        # Explicitly designated metadata fields
                        elif key in EXPLICIT_METADATA_FIELDS_TSV_NAMES:
                            new_meta_key = (
                                key.replace("meta.", "", 1)
                                if key.startswith("meta.")
                                else key
                            )
                            if not is_empty_value:
                                metadata_subdoc[new_meta_key] = value
                            # else: if empty, it's omitted from metadata
                        # Other 'meta.*' fields not in explicit list also go to metadata
                        elif key.startswith("meta."):
                            new_meta_key = key.replace("meta.", "", 1)
                            if not is_empty_value:
                                metadata_subdoc[new_meta_key] = value
                        # All other fields become top-level fields
                        else:
                            if not is_empty_value:
                                top_level_fields[key] = value

                    # Document assembly (valid_doc should still be true here if required fields were present)
                    mongo_doc["metadata"] = metadata_subdoc
                    mongo_doc.update(top_level_fields)
                    batch.append(mongo_doc)

                    if len(batch) >= batch_size:
                        if batch:
                            try:
                                collection.insert_many(batch, ordered=False)
                                docs_sent_by_this_worker += len(batch)
                                batches_sent_by_this_worker += 1
                                if batches_sent_by_this_worker % 10 == 0:
                                    print(
                                        f"{worker_log_prefix} Sent batch {batches_sent_by_this_worker}. Total sent: {docs_sent_by_this_worker}"
                                    )
                            except (
                                Exception
                            ) as e:  # Catching broader errors for insert_many
                                print(
                                    f"{worker_log_prefix} ERROR inserting batch {batches_sent_by_this_worker+1}: {e}",
                                    file=sys.stderr,
                                )
                                # Decide if you want to stop or continue on batch insert error
                            finally:
                                batch = []

                file_line_index += 1

            if batch:  # Final batch
                try:
                    collection.insert_many(batch, ordered=False)
                    docs_sent_by_this_worker += len(batch)
                    batches_sent_by_this_worker += 1
                    print(
                        f"{worker_log_prefix} Sent final batch {batches_sent_by_this_worker} ({len(batch)} docs)."
                    )
                except Exception as e:
                    print(
                        f"{worker_log_prefix} ERROR inserting final batch: {e}",
                        file=sys.stderr,
                    )

    except FileNotFoundError:
        print(f"{worker_log_prefix} Error: Input TSV file not found", file=sys.stderr)
        file_processing_error = True
    except ValueError as ve:
        print(f"{worker_log_prefix} Error processing TSV: {ve}", file=sys.stderr)
        file_processing_error = True
    except Exception as e:
        print(f"{worker_log_prefix} An unexpected error: {e}", file=sys.stderr)
        file_processing_error = True

    print(f"\n--- {worker_log_prefix} Summary ---")
    if file_processing_error:
        print(f"{worker_log_prefix} Processing may have stopped prematurely.")
    print(
        f"{worker_log_prefix} Processed {lines_processed_by_this_worker} assigned lines from {tsv_file.name}."
    )
    print(
        f"{worker_log_prefix} Sent {docs_sent_by_this_worker} documents to MongoDB in {batches_sent_by_this_worker} batches."
    )

    if client:
        try:
            client.close()
            print(f"{worker_log_prefix} MongoDB connection closed.")
        except Exception as e:
            print(
                f"{worker_log_prefix} Error closing MongoDB connection: {e}",
                file=sys.stderr,
            )
    print(f"{worker_log_prefix} Finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MongoDB TSV Inserter (Worker for Strided Reads - Selective Metadata)"
    )
    parser.add_argument(
        "--uri", default="mongodb://localhost:27017/", help="MongoDB connection URI."
    )
    parser.add_argument("--db", required=True, help="MongoDB Database name.")
    parser.add_argument(
        "--collection", required=True, help="MongoDB Time series collection name."
    )
    parser.add_argument(
        "--tsv_file", required=True, type=Path, help="Path to the input TSV file."
    )
    parser.add_argument(
        "--batch_size", type=int, default=5000, help="Documents per insert batch."
    )
    parser.add_argument(
        "--worker_num",
        type=int,
        required=True,
        help="Current worker number (0-indexed).",
    )
    parser.add_argument(
        "--total_workers",
        type=int,
        required=True,
        help="Total number of workers in this run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Max data lines this worker processes from its stride.",
    )
    # No --output for detailed CSV, measurement is external

    args = parser.parse_args()

    if args.batch_size < 1:
        print("Error: --batch_size must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.total_workers < 1:
        print("Error: --total_workers must be > 0.", file=sys.stderr)
        sys.exit(1)
    if not (0 <= args.worker_num < args.total_workers):
        print(f"Error: --worker_num ({args.worker_num}) out of range.", file=sys.stderr)
        sys.exit(1)

    insert_data(
        mongo_uri=args.uri,
        db_name=args.db,
        collection_name=args.collection,
        tsv_file=args.tsv_file,
        batch_size=args.batch_size,
        worker_num=args.worker_num,
        total_workers=args.total_workers,
        limit=args.limit,
    )
