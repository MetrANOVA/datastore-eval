# insert_tsv_to_mongo_benchmark.py (Updated: Add Batch Start Wall-Clock Time)

import argparse
import csv
import datetime  # Ensure datetime is imported
import os
import statistics
import sys
import time
from pathlib import Path

import bson
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure

# --- Configuration (remains the same) ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.device"
TSV_INTERFACE_COL = "meta.name"
REQUIRED_TSV_COLS = [TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTERFACE_COL]
FIXED_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
DISCARD_TSV_FIELDS = {"@collect_time_min", "@exit_time", "@processing_time"}


# --- Helper Function (parse_timestamp - remains the same) ---
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
def insert_data_and_benchmark(
    mongo_uri,
    db_name,
    collection_name,
    tsv_file: Path,
    batch_size: int,
    offset: int,
    limit: int,
    output_file: str,
):
    # ... (initial print statements and connection logic remain the same) ...
    print(f"Starting data insertion process for MongoDB...")
    print(f"Processing file: {tsv_file}")
    print(f"Row Offset: {offset}, Row Limit: {'No limit' if limit < 0 else limit}")
    print(f"Batch size (documents): {batch_size}")
    print(f"Using fixed timestamp format: {FIXED_TIMESTAMP_FORMAT}")
    print(f"Discarding TSV fields: {DISCARD_TSV_FIELDS}")
    print("Prefixes 'values.' and 'meta.' will be stripped from field names.")
    print(f"Connecting to MongoDB: {mongo_uri}")
    print(
        f"!!! IMPORTANT: Output file is '{output_file}'. Ensure uniqueness if running in parallel. !!!"
    )

    if not tsv_file.is_file():
        print(f"Error: Input TSV file not found at '{tsv_file}'", file=sys.stderr)
        sys.exit(1)
    try:
        client = MongoClient(mongo_uri, w=1)
        db = client[db_name]
        collection = db[collection_name]  # Explicit w=1
        client.admin.command("ping")
        print(
            f"Connected to DB '{db_name}', Collection '{collection_name}'. Write concern w=1."
        )
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)

    benchmark_results = []
    total_docs_attempted_segment = 0
    total_docs_parsed_segment = 0
    total_docs_inserted_segment = 0
    total_insertion_time_segment = 0.0
    all_batch_durations = []
    total_batches_processed = 0
    file_processing_error = False
    script_start_wall_time = datetime.datetime.now(
        datetime.timezone.utc
    )  # Overall script start

    try:
        # ... (file opening, header processing, offset skipping, pause point logic remains the same) ...
        print(f"Opening file: {tsv_file.name}")
        with open(tsv_file, "r", newline="", encoding="utf-8") as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter="\t")
            tsv_header = reader.fieldnames
            if not tsv_header:
                raise ValueError("Header row is empty")
            print(
                f"  TSV Headers ({len(tsv_header)}): {str(tsv_header[:5]) + ('...' if len(tsv_header) > 5 else '')}"
            )
            missing_req = [col for col in REQUIRED_TSV_COLS if col not in tsv_header]
            if missing_req:
                raise ValueError(f"File missing required TSV headers: {missing_req}")

            print(f"Skipping {offset} data rows...")
            rows_processed_for_limit_check = 0
            rows_skipped_count = 0
            if offset > 0:
                try:
                    for _ in range(offset):
                        next(reader)
                        rows_skipped_count += 1
                except StopIteration:
                    print(
                        f"Warning: Offset ({offset}) exceeded total data rows.",
                        file=sys.stderr,
                    )
                    file_processing_error = True
            print(f"Finished skipping {rows_skipped_count} data rows.")

            if not file_processing_error:
                try:
                    limit_str = (
                        "None (full file after offset)" if limit < 0 else str(limit)
                    )
                    input(
                        f"Offset {offset} reached for {tsv_file.name}. Press Enter to start processing (limit: {limit_str})..."
                    )
                    print(
                        f"Resuming... Processing rows for offset {offset}, limit {limit_str}"
                    )
                except KeyboardInterrupt:
                    print("\nCtrl+C detected. Exiting.")
                    sys.exit(1)

            if not file_processing_error:
                batch = []
                rows_attempted_in_current_batch = 0
                rows_parsed_in_current_batch = 0
                # --- Process Data Rows in Batches ---
                for i, row_data_dict in enumerate(reader):
                    if limit >= 0 and rows_processed_for_limit_check >= limit:
                        print(f"Reached processing limit of {limit} data rows.")
                        break

                    current_data_row_index = i + 1
                    total_docs_attempted_segment += 1
                    rows_attempted_in_current_batch += 1
                    valid_doc = True
                    parse_errors = []
                    mongo_doc = {}
                    metadata_subdoc = {}
                    measurements = {}

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

                    if valid_doc:
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

                        for key, value in row_data_dict.items():
                            if (
                                key
                                in {TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTERFACE_COL}
                                or key in DISCARD_TSV_FIELDS
                            ):
                                continue
                            if key.startswith("values."):
                                new_key = key.replace("values.", "", 1)
                                if value is None or value == "":
                                    measurements[new_key] = None
                                else:
                                    try:
                                        measurements[new_key] = float(value)
                                    except (ValueError, TypeError):
                                        parse_errors.append(
                                            f"Non-numeric for '{key}'->'{new_key}': '{value}'. Null."
                                        )
                                        measurements[new_key] = None
                            elif key.startswith("meta."):
                                new_key = key.replace("meta.", "", 1)
                                if value is not None and value != "":
                                    metadata_subdoc[new_key] = value
                            else:
                                if value is not None and value != "":
                                    metadata_subdoc[key] = value

                    if valid_doc:
                        mongo_doc["metadata"] = metadata_subdoc
                        mongo_doc.update(measurements)
                        batch.append(mongo_doc)
                        rows_parsed_in_current_batch += 1
                        total_docs_parsed_segment += 1
                    else:
                        print(
                            f"  WARN Row {current_data_row_index} (file line ~{rows_skipped_count+i+2}): Skipping: {'; '.join(parse_errors)}",
                            file=sys.stderr,
                        )

                    rows_processed_for_limit_check += 1

                    # --- Check if Batch is Full and Insert ---
                    if len(batch) >= batch_size:
                        total_batches_processed += 1
                        approx_batch_bytes = 0
                        if batch:
                            try:
                                for doc_to_encode in batch:
                                    approx_batch_bytes += len(
                                        bson.encode(doc_to_encode)
                                    )
                            except Exception as e:
                                print(f"  Warning: Could not estimate BSON size: {e}")
                                approx_batch_bytes = -1

                        print(
                            f"  Inserting batch {total_batches_processed} ({len(batch)} docs, approx {approx_batch_bytes} bytes)..."
                        )

                        # Record wall-clock time just before insert_many
                        batch_start_wall_time = datetime.datetime.now(
                            datetime.timezone.utc
                        )

                        op_start_time = time.monotonic()
                        inserted_count = 0
                        error_msg = ""
                        try:
                            result = collection.insert_many(batch, ordered=False)
                            duration = time.monotonic() - op_start_time
                            inserted_count = len(result.inserted_ids)
                            all_batch_durations.append(duration)
                            total_docs_inserted_segment += inserted_count
                            total_insertion_time_segment += duration
                            print(
                                f"    Batch {total_batches_processed} OK ({inserted_count} docs) in {duration:.4f}s"
                            )
                        except BulkWriteError as bwe:
                            duration = time.monotonic() - op_start_time
                            inserted_count = bwe.details.get("nInserted", 0)
                            if inserted_count > 0:
                                all_batch_durations.append(duration)
                            total_docs_inserted_segment += inserted_count
                            total_insertion_time_segment += duration
                            error_msg = f"BulkWriteError ({len(bwe.details.get('writeErrors',[]))} errors)"
                            print(
                                f"  ERROR: BW Err batch {total_batches_processed}: {error_msg}",
                                file=sys.stderr,
                            )
                        except Exception as e:
                            duration = time.monotonic() - op_start_time
                            total_insertion_time_segment += duration
                            error_msg = f"Unexpected Python error: {e}"
                            print(
                                f"  ERROR: Unexpected Err batch {total_batches_processed}: {error_msg}",
                                file=sys.stderr,
                            )

                        benchmark_results.append(
                            {
                                "file": tsv_file.name,
                                "batch_number": total_batches_processed,
                                "batch_start_wall_time": batch_start_wall_time.isoformat(),  # ADDED
                                "docs_attempted_batch": rows_attempted_in_current_batch,
                                "docs_parsed_batch": rows_parsed_in_current_batch,
                                "docs_inserted_batch": inserted_count,
                                "approx_batch_bytes": approx_batch_bytes,
                                "time_seconds": duration,
                                "error": error_msg,
                            }
                        )
                        batch = []
                        rows_attempted_in_current_batch = 0
                        rows_parsed_in_current_batch = 0

                # --- Insert Final Partial Batch ---
                if batch:
                    total_batches_processed += 1
                    approx_batch_bytes = 0
                    try:
                        for doc_to_encode in batch:
                            approx_batch_bytes += len(bson.encode(doc_to_encode))
                    except Exception as e:
                        print(
                            f"  Warning: Could not estimate BSON size for final batch: {e}"
                        )
                        approx_batch_bytes = -1

                    print(
                        f"  Inserting final batch {total_batches_processed} ({len(batch)} docs, approx {approx_batch_bytes} bytes)..."
                    )

                    # Record wall-clock time just before insert_many
                    batch_start_wall_time = datetime.datetime.now(datetime.timezone.utc)

                    op_start_time = time.monotonic()
                    inserted_count = 0
                    error_msg = ""
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        duration = time.monotonic() - op_start_time
                        inserted_count = len(result.inserted_ids)
                        all_batch_durations.append(duration)
                        total_docs_inserted_segment += inserted_count
                        total_insertion_time_segment += duration
                        print(
                            f"    Final Batch {total_batches_processed} OK ({inserted_count} docs) in {duration:.4f}s"
                        )
                    except BulkWriteError as bwe:
                        duration = time.monotonic() - op_start_time
                        inserted_count = bwe.details.get("nInserted", 0)
                        if inserted_count > 0:
                            all_batch_durations.append(duration)
                        total_docs_inserted_segment += inserted_count
                        total_insertion_time_segment += duration
                        error_msg = f"BulkWriteError ({len(bwe.details.get('writeErrors',[]))} errors)"
                        print(
                            f"  ERROR: BW Err final batch {total_batches_processed}: {error_msg}",
                            file=sys.stderr,
                        )
                    except Exception as e:
                        duration = time.monotonic() - op_start_time
                        total_insertion_time_segment += duration
                        error_msg = f"Unexpected Python error: {e}"
                        print(
                            f"  ERROR: Unexpected Err final batch {total_batches_processed}: {error_msg}",
                            file=sys.stderr,
                        )

                    benchmark_results.append(
                        {
                            "file": tsv_file.name,
                            "batch_number": total_batches_processed,
                            "batch_start_wall_time": batch_start_wall_time.isoformat(),  # ADDED
                            "docs_attempted_batch": rows_attempted_in_current_batch,
                            "docs_parsed_batch": rows_parsed_in_current_batch,
                            "docs_inserted_batch": inserted_count,
                            "approx_batch_bytes": approx_batch_bytes,
                            "time_seconds": duration,
                            "error": error_msg,
                        }
                    )

    except FileNotFoundError:
        print(f"Error: Input TSV file not found", file=sys.stderr)
        file_processing_error = True
    except ValueError as ve:
        print(f"Error processing TSV: {ve}", file=sys.stderr)
        file_processing_error = True
    except Exception as e:
        print(f"An unexpected error: {e}", file=sys.stderr)
        file_processing_error = True

    # --- Final Summary & Output ---
    # ... (summary print logic remains the same) ...
    print("\n--- MongoDB Insertion Summary ---")
    print(
        f"Overall script start wall time (UTC): {script_start_wall_time.isoformat()}"
    )  # Added overall start time
    print(f"Processed file: {tsv_file.name}")
    if file_processing_error:
        print("Processing may have stopped prematurely.")
    print(f"Specified Offset: {offset}, Limit: {'No limit' if limit < 0 else limit}")
    print(f"Total documents attempted in segment: {total_docs_attempted_segment}")
    print(f"Total documents successfully parsed: {total_docs_parsed_segment}")
    print(f"Total documents inserted successfully: {total_docs_inserted_segment}")
    print(f"Total batches processed: {total_batches_processed}")
    print(
        f"Total insertion time (sum of batch inserts): {total_insertion_time_segment:.4f} seconds"
    )
    if total_insertion_time_segment > 0 and total_docs_inserted_segment > 0:
        avg_docs_per_sec = total_docs_inserted_segment / total_insertion_time_segment
        print(f"Average insertion rate for segment: {avg_docs_per_sec:.2f} docs/second")
    else:
        print("Average insertion rate: N/A")
    if all_batch_durations:
        print("\nBatch Performance Statistics (inserts in segment):")
        # ... (statistics printing remains same) ...
    else:
        print("\nNo successful batch performance stats for this segment.")

    try:
        output_file_path = Path(output_file)
        print(
            f"\nWriting detailed benchmark results for this segment to: {output_file_path}"
        )
        with open(output_file_path, "w", newline="", encoding="utf-8") as outfile_csv:
            if benchmark_results:
                # Add 'batch_start_wall_time' to fieldnames
                fieldnames = [
                    "file",
                    "batch_number",
                    "batch_start_wall_time",
                    "docs_attempted_batch",
                    "docs_parsed_batch",
                    "docs_inserted_batch",
                    "approx_batch_bytes",
                    "time_seconds",
                    "error",
                ]
                writer = csv.DictWriter(
                    outfile_csv, fieldnames=fieldnames, extrasaction="ignore"
                )
                writer.writeheader()
                writer.writerows(benchmark_results)
            else:
                outfile_csv.write("No benchmark data recorded.\n")
                print(f"Benchmark file created, no data.")
    except IOError as e:
        print(f"\nError writing benchmark results: {e}", file=sys.stderr)
    except Exception as e:
        print(f"\nUnexpected error writing benchmark results: {e}", file=sys.stderr)
    finally:
        if "client" in locals() and client:
            try:
                client.close()
                print("MongoDB connection closed.")
            except Exception as e:
                print(f"Error closing MongoDB connection: {e}", file=sys.stderr)
    print("Benchmarking complete for this segment.")


if __name__ == "__main__":
    # ... (argparse setup remains the same) ...
    parser = argparse.ArgumentParser(
        description="Insert segment of large TSV into MongoDB Time Series and benchmark."
    )
    parser.add_argument(
        "--uri", default="mongodb://localhost:27017/", help="MongoDB connection URI"
    )
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument(
        "--collection", required=True, help="Time series collection name"
    )
    parser.add_argument(
        "--tsv_file",
        required=True,
        type=Path,
        help="Path to the single large input TSV file.",
    )
    parser.add_argument(
        "--batch_size", type=int, default=5000, help="Docs per insert batch"
    )
    parser.add_argument("--offset", type=int, default=0, help="Data rows to skip")
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Max data rows to process after offset (-1 for no limit)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV file for benchmark (MUST be unique for parallel runs)",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        print("Error: --batch_size must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.offset < 0:
        print("Error: --offset must be >= 0.", file=sys.stderr)
        sys.exit(1)

    insert_data_and_benchmark(
        mongo_uri=args.uri,
        db_name=args.db,
        collection_name=args.collection,
        tsv_file=args.tsv_file,
        batch_size=args.batch_size,
        offset=args.offset,
        limit=args.limit,
        output_file=args.output,
    )
