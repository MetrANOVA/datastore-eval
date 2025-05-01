import argparse
import csv
import datetime
import os
import statistics
import sys
import time
from pathlib import Path

import clickhouse_connect

# --- Configuration ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.device"
TSV_INTF_COL = "meta.name"
REQUIRED_TSV_COLS = [TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTF_COL]
FIXED_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"  # Using fixed format


# --- Helper Functions (parse_timestamp, get_clickhouse_schema, create_tsv_to_ch_mapping) ---
def parse_timestamp(ts_string):
    """Parses timestamp string using FIXED_TIMESTAMP_FORMAT."""
    fmt = FIXED_TIMESTAMP_FORMAT
    try:
        if fmt.endswith("Z") and ts_string.endswith("Z"):
            ts_string = ts_string[:-1] + "+0000"
            fmt = fmt[:-1] + "%z"
        elif "." not in ts_string and "%f" in fmt:
            ts_string += ".000"  # Add dummy millisec
        dt = datetime.datetime.strptime(ts_string, fmt)
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt
    except Exception:
        return None


def get_clickhouse_schema(client, db_name, table_name):
    """Gets column names and types from a ClickHouse table."""
    try:
        fq_table_name = f"`{db_name}`.`{table_name}`"
        query = f"DESCRIBE TABLE {fq_table_name}"
        result = client.query(query)
        schema = {
            row[0]: {"type": row[1], "nullable": "Nullable" in row[1]}
            for row in result.result_rows
        }
        print(f"Fetched schema for {fq_table_name}. Columns: {list(schema.keys())}")
        if not schema:
            print(f"Error: Fetched schema is empty.", file=sys.stderr)
            return None
        return schema
    except Exception as e:
        print(f"Error getting table schema: {e}", file=sys.stderr)
        return None


def create_tsv_to_ch_mapping(tsv_headers, ch_schema):
    """Creates mapping from TSV header names to ClickHouse column names."""
    mapping = {}
    ch_columns_set = set(ch_schema.keys())
    successfully_mapped_tsv = set()
    for tsv_col in tsv_headers:
        if not tsv_col:
            continue
            found_mapping = False
        if tsv_col in ch_columns_set:
            mapping[tsv_col] = tsv_col
            successfully_mapped_tsv.add(tsv_col)
            found_mapping = True
        if not found_mapping:
            ch_col_quoted = f"`{tsv_col}`"
            if ch_col_quoted in ch_columns_set:
                mapping[tsv_col] = ch_col_quoted
                successfully_mapped_tsv.add(tsv_col)
                found_mapping = True
    missing_mappings = [req for req in REQUIRED_TSV_COLS if req not in mapping]
    if missing_mappings:
        print(
            f"CRITICAL ERROR: Required TSV columns not mapped: {missing_mappings}",
            file=sys.stderr,
        )
        return None
    ignored_cols = [h for h in tsv_headers if h and h not in successfully_mapped_tsv]
    if ignored_cols:
        print(
            f"INFO: Ignoring TSV columns not in CH schema: {ignored_cols}",
            file=sys.stderr,
        )
    print(f"Successfully created mapping for {len(mapping)} TSV columns.")
    return mapping


# --- Main Function ---
def insert_data_and_benchmark(
    host,
    port,
    user,
    password,
    db_name,
    table_name,
    tsv_file: Path,
    batch_size: int,
    offset: int,
    limit: int,
    output_file: str,
):
    """
    Reads a segment of a large TSV file (using offset/limit), inserts data
    into ClickHouse table in batches, and benchmarks the process.
    """
    print(f"Starting data insertion process for ClickHouse...")
    print(f"Processing file: {tsv_file}")
    print(f"Row Offset: {offset}, Row Limit: {'No limit' if limit < 0 else limit}")
    print(f"Batch size: {batch_size}")
    print(f"Connecting to ClickHouse: {host}:{port}")
    print("!!! IMPORTANT: Ensure --output file is unique if running in parallel !!!")

    if not tsv_file.is_file():
        print(f"Error: Input TSV file not found", file=sys.stderr)
        sys.exit(1)

    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, user=user, password=password, database=db_name
        )
        client.ping()
        print(f"Connected to DB '{db_name}', targeting Table '{table_name}'.")
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}", file=sys.stderr)
        sys.exit(1)

    # --- Get Target Table Schema ---
    table_schema = get_clickhouse_schema(client, db_name, table_name)
    if not table_schema:
        print(f"Cannot proceed without table schema.", file=sys.stderr)
        if "client" in locals() and client:
            client.close()
        sys.exit(1)
    target_columns_ordered = list(table_schema.keys())

    # --- Benchmarking Setup ---
    benchmark_results = []
    total_rows_attempted_segment = 0
    total_rows_parsed_segment = 0
    total_rows_inserted_segment = 0
    total_insertion_time_segment = 0.0
    all_batch_durations = []
    total_batches_processed = 0
    file_read_error = False

    # --- Process the Single File ---
    try:
        print(f"Opening file: {tsv_file.name}")
        with open(tsv_file, "r", newline="", encoding="utf-8") as tsvfile:
            # Use standard csv.reader, will use DictReader logic later
            reader = csv.reader(tsvfile, delimiter="\t")
            tsv_header = []
            tsv_ch_map = None

            # --- Process Header ---
            try:
                tsv_header = [h.strip() for h in next(reader)]
                if not tsv_header:
                    raise ValueError("Header empty")
                print(f"  TSV Headers ({len(tsv_header)}): {tsv_header[:5]}...")
            except StopIteration:
                print(
                    f"  ERROR: File '{tsv_file.name}' is empty. Exiting.",
                    file=sys.stderr,
                )
                file_read_error = True
            except Exception as e:
                print(f"  ERROR reading header: {e}. Exiting.", file=sys.stderr)
                file_read_error = True

            # --- Validate Header/Mapping ---
            if not file_read_error:
                missing_req = [c for c in REQUIRED_TSV_COLS if c not in tsv_header]
                if missing_req:
                    print(
                        f"  ERROR: Missing required TSV headers: {missing_req}. Exiting.",
                        file=sys.stderr,
                    )
                    file_read_error = True
                else:
                    tsv_ch_map = create_tsv_to_ch_mapping(tsv_header, table_schema)
                    if tsv_ch_map is None:
                        print(f"  ERROR creating mapping. Exiting.", file=sys.stderr)
                        file_read_error = True

            # --- Skip Offset Rows ---
            if not file_read_error:
                print(f"Skipping {offset} rows...")
                rows_processed_count = 0  # Counter for limit check
                rows_skipped_count = 0
                if offset > 0:
                    try:
                        for _ in range(offset):
                            next(reader)  # Read and discard row
                            rows_skipped_count += 1
                    except StopIteration:
                        print(
                            f"Warning: Offset ({offset}) exceeded total rows in file after header.",
                            file=sys.stderr,
                        )
                        file_read_error = True  # Mark to prevent further processing
                print(f"Finished skipping {rows_skipped_count} rows.")

            if (
                not file_read_error
            ):  # Only pause if offset skipping didn't hit end of file
                input(
                    f"Offset {offset} reached for {tsv_file.name}. Press Enter to start processing (limit: {'None' if limit < 0 else limit})..."
                )
                print("Starting processing and benchmarking...")

            # --- Process Data Rows within Limit ---
            if not file_read_error:
                batch_data = []
                rows_attempted_in_current_batch = 0
                rows_parsed_in_current_batch = 0

                for i, row in enumerate(reader):
                    # Check limit BEFORE processing the row
                    if limit >= 0 and rows_processed_count >= limit:
                        print(f"Reached processing limit of {limit} rows after offset.")
                        break  # Stop reading the file

                    line_number = i + 2 + offset  # Actual line number in file
                    total_rows_attempted_segment += 1
                    rows_attempted_in_current_batch += 1

                    if not row:
                        total_rows_attempted_segment -= 1
                        rows_attempted_in_current_batch -= 1
                        continue  # Skip empty
                    if len(row) != len(tsv_header):
                        print(
                            f"  WARN Line {line_number}: Col count mismatch. Skipping.",
                            file=sys.stderr,
                        )
                        continue

                    row_data_dict = dict(zip(tsv_header, row))
                    ordered_row_values = [None] * len(target_columns_ordered)
                    valid_row = True
                    parse_errors = []

                    # Parse/Validate/Convert data based on TSV column mapping
                    for tsv_col_name, ch_col_name in tsv_ch_map.items():
                        raw_value = row_data_dict.get(tsv_col_name)
                        col_schema = table_schema[ch_col_name]
                        is_nullable = col_schema["nullable"]
                        col_index = target_columns_ordered.index(ch_col_name)
                        parsed_value = None

                        if raw_value is None or raw_value == "":
                            if is_nullable:
                                parsed_value = None
                            else:
                                if "String" in col_schema["type"]:
                                    parsed_value = ""
                                else:
                                    parse_errors.append(
                                        f"Empty non-null col '{ch_col_name}'"
                                    )
                                    valid_row = False
                        else:
                            try:
                                if tsv_col_name == TSV_TIMESTAMP_COL:
                                    parsed_value = parse_timestamp(raw_value)
                                    if parsed_value is None:
                                        raise ValueError(f"TS parsing failed")
                                elif tsv_col_name in REQUIRED_TSV_COLS:
                                    parsed_value = str(raw_value)
                                elif tsv_col_name.startswith("values."):
                                    parsed_value = float(raw_value)
                                else:
                                    parsed_value = str(raw_value)
                            except (ValueError, TypeError) as e:
                                parse_errors.append(
                                    f"Conv. error TSV '{tsv_col_name}': {e}"
                                )
                                valid_row = False

                        if valid_row:
                            ordered_row_values[col_index] = parsed_value
                        else:
                            break

                    # Add valid row to batch
                    if valid_row:
                        batch_data.append(ordered_row_values)
                        rows_parsed_in_current_batch += 1
                        total_rows_parsed_segment += 1
                    else:
                        print(
                            f"  WARN Line {line_number}: Skipping: {'; '.join(parse_errors)}",
                            file=sys.stderr,
                        )

                    rows_processed_count += 1  # Increment AFTER attempting row

                    # --- Check if Batch is Full and Insert ---
                    if len(batch_data) >= batch_size:
                        total_batches_processed += 1
                        print(
                            f"  Inserting batch {total_batches_processed} ({len(batch_data)} rows)..."
                        )
                        start_time = time.monotonic()
                        rows_inserted_batch = 0
                        error_msg = ""
                        try:
                            client.insert(
                                table=table_name,
                                data=batch_data,
                                column_names=target_columns_ordered,
                                database=db_name,
                            )
                            duration = time.monotonic() - start_time
                            rows_inserted_batch = len(batch_data)
                            all_batch_durations.append(duration)
                            total_rows_inserted_segment += rows_inserted_batch
                            total_insertion_time_segment += duration
                            print(
                                f"    Batch {total_batches_processed} OK ({rows_inserted_batch} rows) in {duration:.4f}s"
                            )
                        except Exception as e:
                            duration = time.monotonic() - start_time
                            error_msg = f"Batch {total_batches_processed} FAIL: {e}"
                            print(f"  ERROR: {error_msg}", file=sys.stderr)
                        benchmark_results.append(
                            {
                                "file": tsv_file.name,
                                "batch_number": total_batches_processed,
                                "rows_attempted_batch": rows_attempted_in_current_batch,
                                "rows_parsed_batch": rows_parsed_in_current_batch,
                                "rows_inserted_batch": rows_inserted_batch,
                                "time_seconds": duration,
                                "error": error_msg,
                            }
                        )
                        batch_data = []
                        rows_attempted_in_current_batch = 0
                        rows_parsed_in_current_batch = 0  # Reset batch

                # --- End of File Loop / Limit Reached ---

                # --- Insert Final Partial Batch ---
                if batch_data:
                    total_batches_processed += 1
                    print(
                        f"  Inserting final batch {total_batches_processed} ({len(batch_data)} rows)..."
                    )
                    start_time = time.monotonic()
                    rows_inserted_batch = 0
                    error_msg = ""
                    try:
                        client.insert(
                            table=table_name,
                            data=batch_data,
                            column_names=target_columns_ordered,
                            database=db_name,
                        )
                        duration = time.monotonic() - start_time
                        rows_inserted_batch = len(batch_data)
                        all_batch_durations.append(duration)
                        total_rows_inserted_segment += rows_inserted_batch
                        total_insertion_time_segment += duration
                        print(
                            f"    Final Batch {total_batches_processed} OK ({rows_inserted_batch} rows) in {duration:.4f}s"
                        )
                    except Exception as e:
                        duration = time.monotonic() - start_time
                        error_msg = f"Final Batch {total_batches_processed} FAIL: {e}"
                        print(f"  ERROR: {error_msg}", file=sys.stderr)
                    benchmark_results.append(
                        {
                            "file": tsv_file.name,
                            "batch_number": total_batches_processed,
                            "rows_attempted_batch": rows_attempted_in_current_batch,
                            "rows_parsed_batch": rows_parsed_in_current_batch,
                            "rows_inserted_batch": rows_inserted_batch,
                            "time_seconds": duration,
                            "error": error_msg,
                        }
                    )

    except FileNotFoundError:
        print(f"Error: Input TSV file not found", file=sys.stderr)
        file_read_error = True
    except ValueError as ve:
        print(f"Error processing TSV header: {ve}", file=sys.stderr)
        file_read_error = True
    except Exception as e:
        print(f"An unexpected error during file processing: {e}", file=sys.stderr)
        file_read_error = True

    # --- Final Summary & Output ---
    print("\n--- ClickHouse Insertion Summary ---")
    print(f"Processed file: {tsv_file.name}")
    if file_read_error:
        print("Processing stopped due to critical file read or header errors.")
    print(f"Specified Offset: {offset}, Limit: {'No limit' if limit < 0 else limit}")
    print(f"Total rows attempted in segment: {total_rows_attempted_segment}")
    print(f"Total rows successfully parsed in segment: {total_rows_parsed_segment}")
    print(
        f"Total rows inserted successfully across all batches: {total_rows_inserted_segment}"
    )
    print(f"Total batches processed for segment: {total_batches_processed}")
    print(
        f"Total insertion time (sum of successful batch inserts): {total_insertion_time_segment:.4f} seconds"
    )

    if total_insertion_time_segment > 0 and total_rows_inserted_segment > 0:
        avg_rows_per_sec = total_rows_inserted_segment / total_insertion_time_segment
        print(f"Average insertion rate for segment: {avg_rows_per_sec:.2f} rows/second")
    else:
        print("Average insertion rate: N/A")

    if all_batch_durations:
        print("\nBatch Performance Statistics (inserts in segment):")
        # (Statistics printing same as before)
        print(f"  Total successful batches: {len(all_batch_durations)}")
        try:
            print(
                f"  Average batch insert time: {statistics.mean(all_batch_durations):.6f} seconds"
            )
            print(
                f"  Median batch insert time: {statistics.median(all_batch_durations):.6f} seconds"
            )
            print(f"  Min batch insert time: {min(all_batch_durations):.6f} seconds")
            print(f"  Max batch insert time: {max(all_batch_durations):.6f} seconds")
            if len(all_batch_durations) > 1:
                print(
                    f"  Std Dev batch insert time: {statistics.stdev(all_batch_durations):.6f} seconds"
                )
        except statistics.StatisticsError as stat_err:
            print(f"Could not calculate some statistics: {stat_err}")

    else:
        print(
            "\nNo successful batch performance statistics available for this segment."
        )

    # --- Write Benchmark Results to CSV ---
    # (CSV Writing logic remains the same, writes results for this segment)
    try:
        output_file_path = Path(output_file)
        print(
            f"\nWriting detailed benchmark results for this segment to: {output_file_path}"
        )
        with open(output_file_path, "w", newline="", encoding="utf-8") as outfile:
            if benchmark_results:
                fieldnames = [
                    "file",
                    "batch_number",
                    "rows_attempted_batch",
                    "rows_parsed_batch",
                    "rows_inserted_batch",
                    "time_seconds",
                    "error",
                ]
                writer = csv.DictWriter(
                    outfile, fieldnames=fieldnames, extrasaction="ignore"
                )
                writer.writeheader()
                writer.writerows(benchmark_results)
            else:
                outfile.write("No benchmark data recorded.\n")
                print(f"Benchmark output file created, but no data was recorded.")
    except IOError as e:
        print(f"\nError writing benchmark results: {e}", file=sys.stderr)
    except Exception as e:
        print(
            f"\nAn unexpected error occurred writing benchmark results: {e}",
            file=sys.stderr,
        )

    # --- Close Connection ---
    finally:
        if "client" in locals() and client:
            try:
                client.close()
                print("ClickHouse connection closed.")
            except Exception as e:
                print(f"Error during ClickHouse connection close: {e}", file=sys.stderr)
    print("Benchmarking complete for this segment.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert segment of large TSV file into ClickHouse and benchmark."
    )
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port")
    parser.add_argument("--user", default="default", help="ClickHouse user")
    parser.add_argument("--password", default="", help="ClickHouse password")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument(
        "--tsv_file",
        required=True,
        type=Path,
        help="Path to the single large input TSV file.",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=50000,
        help="Number of rows per insertion batch",
    )
    # Added offset and limit
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Number of data rows to skip from the beginning",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Maximum number of data rows to process after offset (-1 for no limit)",
    )
    # ts_format removed
    parser.add_argument(
        "--output",
        required=True,  # Make output required for parallel runs
        help="Output CSV file for batch benchmark results (MUST be unique for parallel runs)",
    )

    args = parser.parse_args()

    if args.batch_size < 1:
        print("Error: --batch_size must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.offset < 0:
        print("Error: --offset must be >= 0.", file=sys.stderr)
        sys.exit(1)

    insert_data_and_benchmark(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        db_name=args.db,
        table_name=args.table,
        tsv_file=args.tsv_file,
        batch_size=args.batch_size,
        offset=args.offset,  # Pass offset
        limit=args.limit,  # Pass limit
        # ts_format removed
        output_file=args.output,
    )
