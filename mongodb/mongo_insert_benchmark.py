import argparse
import csv
import datetime
import os
import statistics
import sys
import time

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure

# --- Configuration ---
# Adjust these column names based on your exact TSV header
TIMESTAMP_COL = "timestamp"
INPUT_COL = "aggregate(values.input, 60, average)"  # Example name, change to your actual column name
OUTPUT_COL = "aggregate(values.output, 60, average)"  # Example name, change to your actual column name
NODE_COL = "node"
INTERFACE_COL = "intf"

# List required columns for validation
REQUIRED_COLS = [TIMESTAMP_COL, INPUT_COL, OUTPUT_COL, NODE_COL, INTERFACE_COL]


def parse_timestamp(ts_string, fmt="%Y-%m-%dT%H:%M:%S.%fZ"):
    """
    Parses a timestamp string into a datetime object.
    Handles potential timezone info (assumes UTC if Z present).
    Modify the 'fmt' string if your timestamp format is different.
    Common formats:
    ISO 8601 with Z: '%Y-%m-%dT%H:%M:%S.%fZ' or '%Y-%m-%dT%H:%M:%SZ'
    ISO 8601 with offset: '%Y-%m-%dT%H:%M:%S.%f%z'
    Unix timestamp (seconds): handle with datetime.datetime.fromtimestamp(float(ts_string), tz=datetime.timezone.utc)
    """
    try:
        # Handle Unix timestamps separately if needed
        if ts_string.isdigit() or (
            "." in ts_string and ts_string.replace(".", "", 1).isdigit()
        ):
            return datetime.datetime.fromtimestamp(
                float(ts_string), tz=datetime.timezone.utc
            )

        # Attempt parsing with specified format
        # Handle potential 'Z' indicating UTC
        if fmt.endswith("Z") and ts_string.endswith("Z"):
            ts_string = ts_string[:-1] + "+0000"
            fmt = fmt[:-1] + "%z"  # Python needs numeric offset

        dt = datetime.datetime.strptime(ts_string, fmt)

        # If timezone is naive, assume UTC (MongoDB stores in UTC)
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            # Convert aware datetime to UTC
            dt = dt.astimezone(datetime.timezone.utc)
        return dt
    except ValueError as e:
        print(
            f"Error parsing timestamp '{ts_string}' with format '{fmt}': {e}",
            file=sys.stderr,
        )
        return None
    except Exception as e:
        print(f"Unexpected error parsing timestamp '{ts_string}': {e}", file=sys.stderr)
        return None


def insert_data_and_benchmark(
    mongo_uri, db_name, collection_name, data_dir, batch_size, ts_format, output_file
):
    """
    Reads TSV files, inserts data into MongoDB Time Series collection,
    and benchmarks the process.

    Args:
        mongo_uri (str): MongoDB connection URI.
        db_name (str): Database name.
        collection_name (str): Time series collection name.
        data_dir (str): Directory containing the TSV files.
        batch_size (int): Number of documents to insert per batch.
        ts_format (str): The format string for parsing timestamps (strptime format).
        output_file (str): Path to write benchmark results (CSV).
    """
    print(f"Starting data insertion process...")
    print(f"Connecting to MongoDB: {mongo_uri}")
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        # Verify connection
        client.admin.command("ismaster")
        print(f"Connected to DB '{db_name}', Collection '{collection_name}'.")
    except ConnectionFailure as e:
        print(f"Error connecting to MongoDB: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during connection: {e}")
        sys.exit(1)

    if not os.path.isdir(data_dir):
        print(f"Error: Data directory '{data_dir}' not found.")
        sys.exit(1)

    benchmark_results = []
    total_docs_inserted = 0
    total_insertion_time = 0.0
    batch_durations = []
    files_processed = 0
    files_skipped = 0

    print(f"Scanning directory: {data_dir}")
    for filename in os.listdir(data_dir):
        if filename.lower().endswith(".tsv"):
            filepath = os.path.join(data_dir, filename)
            print(f"\nProcessing file: {filename}")
            files_processed += 1
            docs_in_file = 0
            batches_in_file = 0
            file_insertion_time = 0.0
            batch = []
            first_row = True  # Flag to check header

            try:
                with open(filepath, "r", newline="", encoding="utf-8") as tsvfile:
                    reader = csv.reader(tsvfile, delimiter="\t")
                    header = []

                    for i, row in enumerate(reader):
                        if not row:  # Skip empty rows
                            continue

                        if first_row:
                            header = [h.strip() for h in row]
                            # --- Header Validation ---
                            missing_cols = [
                                col for col in REQUIRED_COLS if col not in header
                            ]
                            if missing_cols:
                                print(
                                    f"  ERROR: File '{filename}' is missing required columns: {missing_cols}. Skipping file."
                                )
                                files_skipped += 1
                                files_processed -= (
                                    1  # Don't count skipped file in processed total
                                )
                                break  # Stop processing this file

                            print(f"  Detected Headers: {header}")
                            optional_meta_cols = [
                                h for h in header if h not in REQUIRED_COLS
                            ]
                            print(f"  Optional Metadata Fields: {optional_meta_cols}")
                            first_row = False
                            continue  # Skip header row from data processing

                        # --- Data Row Processing ---
                        if len(row) != len(header):
                            print(
                                f"  WARNING: Row {i+1} has {len(row)} columns, expected {len(header)}. Skipping row: {row}",
                                file=sys.stderr,
                            )
                            continue

                        row_data = dict(zip(header, row))

                        # Parse timestamp
                        timestamp = parse_timestamp(
                            row_data.get(TIMESTAMP_COL, ""), ts_format
                        )
                        if timestamp is None:
                            print(
                                f"  WARNING: Row {i+1}: Could not parse timestamp '{row_data.get(TIMESTAMP_COL)}'. Skipping row.",
                                file=sys.stderr,
                            )
                            continue

                        # Parse measurements (add more specific error handling if needed)
                        try:
                            input_val = float(row_data[INPUT_COL])
                            output_val = float(row_data[OUTPUT_COL])
                        except (ValueError, TypeError) as e:
                            print(
                                f"  WARNING: Row {i+1}: Error parsing numeric data ({INPUT_COL} or {OUTPUT_COL}). Skipping row: {e} - Row content: {row_data}",
                                file=sys.stderr,
                            )
                            continue

                        # --- Construct Metadata Sub-document ---
                        metadata = {
                            NODE_COL: row_data.get(NODE_COL),
                            INTERFACE_COL: row_data.get(INTERFACE_COL),
                        }
                        # Add optional metadata fields found in the header
                        for meta_col in optional_meta_cols:
                            if (
                                meta_col in row_data and row_data[meta_col]
                            ):  # Add if present and not empty
                                metadata[meta_col] = row_data[meta_col]

                        # --- Construct MongoDB Document ---
                        document = {
                            "timestamp": timestamp,
                            "metadata": metadata,
                            # Add measurements directly (not nested under a sub-field usually)
                            "input": input_val,
                            "output": output_val,
                            # Add any other top-level fields if needed from row_data
                        }
                        batch.append(document)

                        # --- Insert Batch when Full ---
                        if len(batch) >= batch_size:
                            start_time = time.monotonic()
                            try:
                                result = collection.insert_many(
                                    batch, ordered=False
                                )  # ordered=False can be faster if failures are okay
                                duration = time.monotonic() - start_time
                                inserted_count = len(result.inserted_ids)
                                batch_durations.append(duration)
                                benchmark_results.append(
                                    {
                                        "file": filename,
                                        "batch_number": batches_in_file + 1,
                                        "docs_in_batch": len(batch),
                                        "docs_inserted": inserted_count,
                                        "time_seconds": duration,
                                    }
                                )
                                total_docs_inserted += inserted_count
                                docs_in_file += inserted_count
                                total_insertion_time += duration
                                file_insertion_time += duration
                                batches_in_file += 1
                                # print(f"    Inserted batch {batches_in_file} ({inserted_count} docs) in {duration:.4f}s")

                            except BulkWriteError as bwe:
                                duration = time.monotonic() - start_time
                                print(
                                    f"  ERROR: Bulk write error in batch {batches_in_file + 1}: {len(bwe.details.get('writeErrors', []))} errors."
                                )
                                # Handle partial success if needed
                                inserted_count = bwe.details.get("nInserted", 0)
                                batch_durations.append(
                                    duration
                                )  # Still record time taken
                                benchmark_results.append(
                                    {
                                        "file": filename,
                                        "batch_number": batches_in_file + 1,
                                        "docs_in_batch": len(batch),
                                        "docs_inserted": inserted_count,  # Record actual insertions
                                        "time_seconds": duration,
                                        "error": "BulkWriteError",
                                    }
                                )
                                total_docs_inserted += inserted_count
                                docs_in_file += inserted_count
                                total_insertion_time += duration
                                file_insertion_time += duration
                                batches_in_file += 1
                                # Log more details if necessary: print(bwe.details)
                            except Exception as e:
                                duration = time.monotonic() - start_time
                                print(
                                    f"  ERROR: Unexpected error inserting batch {batches_in_file + 1}: {e}"
                                )
                                benchmark_results.append(
                                    {
                                        "file": filename,
                                        "batch_number": batches_in_file + 1,
                                        "docs_in_batch": len(batch),
                                        "docs_inserted": 0,
                                        "time_seconds": duration,
                                        "error": str(e),
                                    }
                                )
                                # Decide whether to stop or continue

                            batch = []  # Reset batch

            except FileNotFoundError:
                print(
                    f"  ERROR: File '{filename}' not found during processing (unexpected). Skipping."
                )
                files_skipped += 1
                files_processed -= 1
                continue
            except Exception as e:
                print(
                    f"  ERROR: Failed to process file '{filename}' due to unexpected error: {e}. Skipping file."
                )
                files_skipped += 1
                files_processed -= 1
                # Ensure batch is cleared if error occurred mid-file
                batch = []
                continue

            # --- Insert Final Partial Batch (if any) ---
            if batch:
                start_time = time.monotonic()
                try:
                    result = collection.insert_many(batch, ordered=False)
                    duration = time.monotonic() - start_time
                    inserted_count = len(result.inserted_ids)
                    batch_durations.append(duration)
                    benchmark_results.append(
                        {
                            "file": filename,
                            "batch_number": batches_in_file + 1,
                            "docs_in_batch": len(batch),
                            "docs_inserted": inserted_count,
                            "time_seconds": duration,
                        }
                    )
                    total_docs_inserted += inserted_count
                    docs_in_file += inserted_count
                    total_insertion_time += duration
                    file_insertion_time += duration
                    batches_in_file += 1
                    # print(f"    Inserted final batch {batches_in_file} ({inserted_count} docs) in {duration:.4f}s")

                except BulkWriteError as bwe:
                    duration = time.monotonic() - start_time
                    print(
                        f"  ERROR: Bulk write error in final batch: {len(bwe.details.get('writeErrors', []))} errors."
                    )
                    inserted_count = bwe.details.get("nInserted", 0)
                    batch_durations.append(duration)
                    benchmark_results.append(
                        {
                            "file": filename,
                            "batch_number": batches_in_file + 1,
                            "docs_in_batch": len(batch),
                            "docs_inserted": inserted_count,
                            "time_seconds": duration,
                            "error": "BulkWriteError",
                        }
                    )
                    total_docs_inserted += inserted_count
                    docs_in_file += inserted_count
                    total_insertion_time += duration
                    file_insertion_time += duration
                    batches_in_file += 1
                except Exception as e:
                    duration = time.monotonic() - start_time
                    print(f"  ERROR: Unexpected error inserting final batch: {e}")
                    batch_durations.append(duration)
                    benchmark_results.append(
                        {
                            "file": filename,
                            "batch_number": batches_in_file + 1,
                            "docs_in_batch": len(batch),
                            "docs_inserted": 0,
                            "time_seconds": duration,
                            "error": str(e),
                        }
                    )

            print(
                f"  Finished processing file: {filename}. Inserted {docs_in_file} documents in {batches_in_file} batches. Total time for file: {file_insertion_time:.4f}s"
            )

    print("\n--- Insertion Summary ---")
    print(f"Processed {files_processed} files.")
    if files_skipped > 0:
        print(f"Skipped {files_skipped} files due to errors.")
    print(f"Total documents inserted: {total_docs_inserted}")
    print(f"Total insertion time: {total_insertion_time:.4f} seconds")

    if total_insertion_time > 0:
        avg_docs_per_sec = total_docs_inserted / total_insertion_time
        print(f"Average insertion rate: {avg_docs_per_sec:.2f} docs/second")
    else:
        print("Average insertion rate: N/A (no insertion time recorded)")

    if batch_durations:
        print("\nBatch Performance Statistics:")
        print(f"  Total batches: {len(batch_durations)}")
        print(
            f"  Average batch insert time: {statistics.mean(batch_durations):.6f} seconds"
        )
        print(
            f"  Median batch insert time: {statistics.median(batch_durations):.6f} seconds"
        )
        print(f"  Min batch insert time: {min(batch_durations):.6f} seconds")
        print(f"  Max batch insert time: {max(batch_durations):.6f} seconds")
        if len(batch_durations) > 1:
            print(
                f"  Std Dev batch insert time: {statistics.stdev(batch_durations):.6f} seconds"
            )
    else:
        print("\nNo batch performance statistics available.")

    # --- Write Benchmark Results to CSV ---
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as outfile:
            if benchmark_results:
                # Use the keys from the first result dict as header, ensuring consistency
                fieldnames = benchmark_results[0].keys()
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(benchmark_results)
                print(f"\nDetailed benchmark results written to: {output_file}")
            else:
                outfile.write("No benchmark data recorded.\n")
                print(
                    f"\nBenchmark output file created, but no data was recorded: {output_file}"
                )

    except IOError as e:
        print(f"\nError writing benchmark results to '{output_file}': {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred writing benchmark results: {e}")

    client.close()
    print("MongoDB connection closed.")
    print("Benchmarking complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert TSV data into MongoDB Time Series and benchmark."
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
    parser.add_argument("--dir", required=True, help="Directory containing TSV files")
    parser.add_argument(
        "--batchsize",
        type=int,
        default=1000,
        help="Number of documents per insert batch (default: 1000)",
    )
    # IMPORTANT: Provide the correct format string for your timestamps
    parser.add_argument(
        "--tsformat",
        default="%Y-%m-%dT%H:%M:%S.%fZ",  # Example: ISO 8601 UTC
        help="Timestamp format string (strptime format, e.g., '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ' or '%%s.%%f' for Unix epoch)",
    )
    parser.add_argument(
        "--output",
        default="mongo_insert_benchmark.csv",
        help="Output CSV file for benchmark results (default: mongo_insert_benchmark.csv)",
    )

    args = parser.parse_args()

    # Validate timestamp format basic check (doesn't guarantee correctness for data)
    if "%" not in args.tsformat:
        print(
            f"Warning: Timestamp format '{args.tsformat}' does not contain '%'. Ensure it's correct."
        )
        # Add specific checks if known non-% formats like 'epoch' are expected

    insert_data_and_benchmark(
        mongo_uri=args.uri,
        db_name=args.db,
        collection_name=args.collection,
        data_dir=args.dir,
        batch_size=args.batchsize,
        ts_format=args.tsformat,
        output_file=args.output,
    )
