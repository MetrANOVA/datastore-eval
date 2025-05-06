# insert_tsv_to_mongo_benchmark.py (Updated to discard specific fields)

import argparse
import csv
import datetime
import os
import statistics
import sys
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure

# --- Configuration ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.device"
TSV_INTERFACE_COL = "meta.name"
REQUIRED_TSV_COLS = [TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTERFACE_COL]
FIXED_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# --- Fields to explicitly discard from TSV ---
DISCARD_TSV_FIELDS = {"@collect_time_min", "@exit_time", "@processing_time"}


# --- Helper Function (parse_timestamp) ---
def parse_timestamp(ts_string):
    """Parses timestamp string using the FIXED_TIMESTAMP_FORMAT."""
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
    """
    Reads a segment of a large TSV, inserts into MongoDB Time Series,
    and benchmarks. Pauses after offset skipping.
    Ensures all 'values.*' fields are handled, storing null if empty/invalid.
    Discards specified fields.
    """
    print(f"Starting data insertion process for MongoDB...")
    print(f"Processing file: {tsv_file}")
    print(f"Row Offset: {offset}, Row Limit: {'No limit' if limit < 0 else limit}")
    print(f"Batch size: {batch_size}")
    print(f"Using fixed timestamp format: {FIXED_TIMESTAMP_FORMAT}")
    print(f"Discarding TSV fields: {DISCARD_TSV_FIELDS}")
    print(f"Connecting to MongoDB: {mongo_uri}")
    print(
        f"!!! IMPORTANT: Output file is '{output_file}'. Ensure uniqueness if running in parallel. !!!"
    )

    if not tsv_file.is_file():
        print(f"Error: Input TSV file not found at '{tsv_file}'", file=sys.stderr)
        sys.exit(1)
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        client.admin.command("ping")
        print(f"Connected to DB '{db_name}', Collection '{collection_name}'.")
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

    try:
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
                            # 1. Skip already processed special fields
                            if (
                                key == TSV_TIMESTAMP_COL
                                or key == TSV_NODE_COL
                                or key == TSV_INTERFACE_COL
                            ):
                                continue

                            # 2. Skip explicitly discarded fields
                            if key in DISCARD_TSV_FIELDS:  # New logic to discard
                                # print(f"  DEBUG Line ~{rows_skipped_count+i+2}: Discarding field '{key}'") # Optional debug
                                continue

                            # 3. Process 'values.*' fields
                            if key.startswith("values."):
                                if value is None or value == "":
                                    measurements[key] = None
                                else:
                                    try:
                                        measurements[key] = float(value)
                                    except (ValueError, TypeError):
                                        parse_errors.append(
                                            f"Non-numeric for '{key}': '{value}'. Storing as null."
                                        )
                                        measurements[key] = None
                            # 4. Process other fields as general metadata
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

                    if len(batch) >= batch_size:
                        total_batches_processed += 1
                        # (Batch insertion logic remains the same here and for final batch)
                        print(
                            f"  Inserting batch {total_batches_processed} ({len(batch)} docs)..."
                        )
                        start_time = time.monotonic()
                        inserted_count = 0
                        error_msg = ""
                        try:
                            result = collection.insert_many(batch, ordered=False)
                            duration = time.monotonic() - start_time
                            inserted_count = len(result.inserted_ids)
                            all_batch_durations.append(duration)
                            total_docs_inserted_segment += inserted_count
                            total_insertion_time_segment += duration
                            print(
                                f"    Batch {total_batches_processed} OK ({inserted_count} docs) in {duration:.4f}s"
                            )
                        except BulkWriteError as bwe:
                            duration = time.monotonic() - start_time
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
                            duration = time.monotonic() - start_time
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
                                "docs_attempted_batch": rows_attempted_in_current_batch,
                                "docs_parsed_batch": rows_parsed_in_current_batch,
                                "docs_inserted_batch": inserted_count,
                                "time_seconds": duration,
                                "error": error_msg,
                            }
                        )
                        batch = []
                        rows_attempted_in_current_batch = 0
                        rows_parsed_in_current_batch = 0

                if batch:  # Final batch
                    total_batches_processed += 1
                    print(
                        f"  Inserting final batch {total_batches_processed} ({len(batch)} docs)..."
                    )
                    start_time = time.monotonic()
                    inserted_count = 0
                    error_msg = ""
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        duration = time.monotonic() - start_time
                        inserted_count = len(result.inserted_ids)
                        all_batch_durations.append(duration)
                        total_docs_inserted_segment += inserted_count
                        total_insertion_time_segment += duration
                        print(
                            f"    Final Batch {total_batches_processed} OK ({inserted_count} docs) in {duration:.4f}s"
                        )
                    except BulkWriteError as bwe:
                        duration = time.monotonic() - start_time
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
                        duration = time.monotonic() - start_time
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
                            "docs_attempted_batch": rows_attempted_in_current_batch,
                            "docs_parsed_batch": rows_parsed_in_current_batch,
                            "docs_inserted_batch": inserted_count,
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

    # --- Final Summary & Output (same as before) ---
    print("\n--- MongoDB Insertion Summary ---")
    print(f"Processed file: {tsv_file.name}")
    if file_processing_error:
        print("Processing may have stopped prematurely.")
    print(f"Specified Offset: {offset}, Limit: {'No limit' if limit < 0 else limit}")
    print(f"Total documents attempted in segment: {total_docs_attempted_segment}")
    print(f"Total documents successfully parsed: {total_docs_parsed_segment}")
    print(f"Total documents inserted successfully: {total_docs_inserted_segment}")
    print(f"Total batches processed: {total_batches_processed}")
    print(f"Total insertion time: {total_insertion_time_segment:.4f} seconds")
    if total_insertion_time_segment > 0 and total_docs_inserted_segment > 0:
        avg_docs_per_sec = total_docs_inserted_segment / total_insertion_time_segment
        print(f"Average insertion rate for segment: {avg_docs_per_sec:.2f} docs/second")
    else:
        print("Average insertion rate: N/A")
    if all_batch_durations:
        print("\nBatch Performance Statistics (inserts in segment):")
        print(f"  Total batches with inserts: {len(all_batch_durations)}")
        try:
            print(
                f"  Average batch insert time: {statistics.mean(all_batch_durations):.6f}s"
            )
            print(
                f"  Median batch insert time: {statistics.median(all_batch_durations):.6f}s"
            )
            print(f"  Min batch insert time: {min(all_batch_durations):.6f}s")
            print(f"  Max batch insert time: {max(all_batch_durations):.6f}s")
            if len(all_batch_durations) > 1:
                print(f"  Std Dev: {statistics.stdev(all_batch_durations):.6f}s")
        except statistics.StatisticsError as e:
            print(f"Could not calculate stats: {e}")
    else:
        print("\nNo successful batch performance stats for this segment.")
    try:
        output_file_path = Path(output_file)
        print(
            f"\nWriting detailed benchmark results for this segment to: {output_file_path}"
        )
        with open(output_file_path, "w", newline="", encoding="utf-8") as outfile_csv:
            if benchmark_results:
                fieldnames = [
                    "file",
                    "batch_number",
                    "docs_attempted_batch",
                    "docs_parsed_batch",
                    "docs_inserted_batch",
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
