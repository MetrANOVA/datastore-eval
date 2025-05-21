# insert_tsv_to_mongo_benchmark.py (Standardized Version)

import argparse
import csv
import datetime
import statistics
import sys
import time
from pathlib import Path

import bson  # For BSON size calculation
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure

# --- Standardized Configuration ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.device"  # Mapped to 'device' in MongoDB metadata
TSV_INTERFACE_COL = "meta.name"  # Mapped to 'interfaceName' in MongoDB metadata
REQUIRED_TSV_COLS = [TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTERFACE_COL]
FIXED_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
DISCARD_TSV_FIELDS = {"@collect_time_min", "@exit_time", "@processing_time"}


# --- Helper Function (parse_timestamp) ---
def parse_timestamp(ts_string):
    """Parses timestamp string using the FIXED_TIMESTAMP_FORMAT."""
    fmt = FIXED_TIMESTAMP_FORMAT
    try:
        ts_string_adj, fmt_adj = ts_string, fmt
        # Handle Z for UTC
        if fmt.endswith("Z") and ts_string.endswith("Z"):
            ts_string_adj, fmt_adj = ts_string[:-1] + "+0000", fmt[:-1] + "%z"

        # Handle missing fractional seconds if format expects them
        # This part can be tricky; relies on format consistency or more robust parsing
        if "." not in ts_string_adj and "%f" in fmt_adj:
            # Attempt to add dummy milliseconds
            if (
                "%z" in fmt_adj
                and len(ts_string_adj) > 6
                and ts_string_adj[-6] in ("+", "-")
            ):  # Check for timezone offset
                offset_part = ts_string_adj[-6:]
                base_ts_part = ts_string_adj[:-6]
                ts_string_adj = base_ts_part + ".000" + offset_part  # Add dummy millis
            elif "%z" not in fmt_adj:  # No timezone in format string
                ts_string_adj += ".000"  # Add dummy millis

        dt = datetime.datetime.strptime(ts_string_adj, fmt_adj)

        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt
    except Exception as e:
        # print(f"DEBUG Timestamp parse error: {e} for '{ts_string}' with format '{fmt}'", file=sys.stderr)
        return None


# --- Main Function ---
def insert_data_and_benchmark(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    tsv_file: Path,
    batch_size: int,
    offset: int,
    limit: int,
    output_file: str,
    no_pause_manual: bool,
):
    """
    Reads a segment of a large TSV, inserts into MongoDB Time Series,
    and benchmarks. Pauses after offset skipping unless no_pause_manual is True.
    Strips 'values.' and 'meta.' prefixes for field names.
    """
    print(f"--- MongoDB Insertion Benchmark ---")
    print(f"Processing TSV File: {tsv_file}")
    print(f"Row Offset: {offset}, Row Limit: {'No limit' if limit < 0 else limit}")
    print(f"Document Batch Size: {batch_size}")
    print(f"Output CSV: {output_file}")
    print(f"Using fixed timestamp format: {FIXED_TIMESTAMP_FORMAT}")
    print(f"Discarding TSV fields: {DISCARD_TSV_FIELDS}")
    print("Prefixes 'values.' and 'meta.' will be stripped for MongoDB field names.")
    print(f"Connecting to MongoDB: {mongo_uri} (Write Concern w=1)")

    if not tsv_file.is_file():
        print(f"Error: Input TSV file not found at '{tsv_file}'", file=sys.stderr)
        sys.exit(1)
    try:
        client = MongoClient(mongo_uri, w=1)
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
    script_start_wall_time = datetime.datetime.now(datetime.timezone.utc)

    try:
        print(f"\nOpening file: {tsv_file.name}...")
        with open(tsv_file, "r", newline="", encoding="utf-8") as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter="\t")
            tsv_header = reader.fieldnames
            if not tsv_header:
                raise ValueError("Header row is empty or could not be read.")
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

            # --- PAUSE POINT ---
            if not file_processing_error and not no_pause_manual:
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
                    print("\nCtrl+C detected during pause. Exiting.")
                    sys.exit(1)
            elif not file_processing_error and no_pause_manual:
                print(
                    f"Auto-resuming (no_pause_manual set). Processing rows for offset {offset}, limit {'None' if limit < 0 else str(limit)}"
                )
            # --- End PAUSE POINT ---

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
                            if (
                                key
                                in {TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTERFACE_COL}
                                or key in DISCARD_TSV_FIELDS
                            ):
                                continue

                            new_key = key
                            if key.startswith("values."):
                                new_key = key.replace("values.", "", 1)
                                if value is None or value == "":
                                    measurements[new_key] = None
                                else:
                                    try:
                                        measurements[new_key] = float(value)
                                    except (ValueError, TypeError):
                                        parse_errors.append(
                                            f"Non-numeric for measurement '{key}'->'{new_key}': '{value}'. Null."
                                        )
                                        measurements[new_key] = None
                            elif key.startswith("meta."):  # For other meta fields
                                new_key = key.replace("meta.", "", 1)
                                if value is not None and value != "":
                                    metadata_subdoc[new_key] = value
                            else:  # General metadata not matching other patterns
                                if value is not None and value != "":
                                    metadata_subdoc[new_key] = (
                                        value  # new_key is key here
                                    )

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
                                "batch_start_wall_time": batch_start_wall_time.isoformat(),
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

                if batch:  # Final batch
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
                            "batch_start_wall_time": batch_start_wall_time.isoformat(),
                            "docs_attempted_batch": rows_attempted_in_current_batch,
                            "docs_parsed_batch": rows_parsed_in_current_batch,
                            "docs_inserted_batch": inserted_count,
                            "approx_batch_bytes": approx_batch_bytes,
                            "time_seconds": duration,
                            "error": error_msg,
                        }
                    )

    except FileNotFoundError:
        print(f"Error: Input TSV file not found at '{tsv_file}'", file=sys.stderr)
        file_processing_error = True
    except ValueError as ve:
        print(f"Error processing TSV: {ve}", file=sys.stderr)
        file_processing_error = True
    except Exception as e:
        print(f"An unexpected error during file processing: {e}", file=sys.stderr)
        file_processing_error = True

    # --- Final Summary & Output ---
    print("\n--- MongoDB Insertion Summary ---")
    print(f"Overall script start wall time (UTC): {script_start_wall_time.isoformat()}")
    print(f"Processed file: {tsv_file.name}")
    if file_processing_error:
        print(
            "Processing may have stopped prematurely due to critical file/header errors."
        )
    print(f"Specified Offset: {offset}, Limit: {'No limit' if limit < 0 else limit}")
    print(f"Total documents attempted in segment: {total_docs_attempted_segment}")
    print(
        f"Total documents successfully parsed in segment: {total_docs_parsed_segment}"
    )
    print(
        f"Total documents inserted successfully in segment: {total_docs_inserted_segment}"
    )
    print(f"Total batches processed for segment: {total_batches_processed}")
    print(
        f"Total insertion time (sum of batch inserts): {total_insertion_time_segment:.4f} seconds"
    )

    if total_insertion_time_segment > 0 and total_docs_inserted_segment > 0:
        avg_docs_per_sec_segment = (
            total_docs_inserted_segment / total_insertion_time_segment
        )
        print(
            f"Average insertion rate for segment: {avg_docs_per_sec_segment:.2f} docs/second"
        )
    else:
        print("Average insertion rate for segment: N/A")

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
            print(f"Could not calculate some statistics: {e}")
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
    parser = argparse.ArgumentParser(
        description="Insert segment of large TSV into MongoDB Time Series and benchmark."
    )
    # MongoDB Connection
    parser.add_argument(
        "--uri", default="mongodb://localhost:27017/", help="MongoDB connection URI."
    )
    parser.add_argument("--db", required=True, help="MongoDB Database name.")
    parser.add_argument(
        "--collection", required=True, help="MongoDB Time series collection name."
    )
    # Common Benchmark Args
    parser.add_argument(
        "--tsv_file", required=True, type=Path, help="Path to the input TSV file."
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=5000,
        help="Number of documents per insert batch (Default: 5000 for Mongo).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Number of data rows to skip from the beginning of the TSV file (Default: 0).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Maximum number of data rows to process after offset (-1 for no limit, Default: -1).",
    )
    parser.add_argument(
        "--output",
        required=True,  # Made required
        help="Output CSV file for batch benchmark results (MUST be unique if running in parallel).",
    )
    parser.add_argument(
        "--no_pause_manual",
        action="store_true",
        help="Disable interactive pause after offset skipping (for non-orchestrated test runs).",
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
        no_pause_manual=args.no_pause_manual,
    )
