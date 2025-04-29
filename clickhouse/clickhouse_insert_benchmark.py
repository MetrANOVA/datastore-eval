import argparse
import csv
import datetime
import os
import statistics
import sys
import time

import clickhouse_connect

# --- Configuration ---
# These keys should match the actual headers in your TSV files.
TSV_TIMESTAMP_COL = "timestamp"
TSV_INPUT_COL = "aggregate(values.input, 60, average)"
TSV_OUTPUT_COL = "aggregate(values.output, 60, average)"
TSV_NODE_COL = "node"
TSV_INTF_COL = "intf"

# All potential metadata keys expected in TSV headers
TSV_METADATA_COLS = [
    "alternate_intf",
    "circuit.carrier",
    "circuit.carrier_id",
    "circuit.carrier_type",
    "circuit.circuit_id",
    "circuit.customer",
    "circuit.customer_id",
    "circuit.customer_type",
    "circuit.description",
    "circuit.name",
    "circuit.owner",
    "circuit.owner_id",
    "circuit.role",
    "circuit.speed",
    "circuit.type",
    "contracted_bandwidth",
    "description",
    "entity.contracted_bandwidth",
    "entity.id",
    "entity.type",
    "entity.name",
    "interface_address",
    "interface_id",
    "max_bandwidth",
    "network",
    "node_id",
    "node_management_address",
    "node_role",
    "node_type",
    "parent_interface",
    "pop.id",
    "pop.locality",
    "pop.name",
    "pop.type",
    "service.description",
    "service.direction",
    "service.contracted_bandwidth",
    "service.entity",
    "service.entity_id",
    "service.entity_roles",
    "service.entity_type",
    "service.name",
    "service.service_id",
    "service.type",
    "type",
]

# List required TSV columns for basic validation
REQUIRED_TSV_COLS = [
    TSV_TIMESTAMP_COL,
    TSV_INPUT_COL,
    TSV_OUTPUT_COL,
    TSV_NODE_COL,
    TSV_INTF_COL,
]


def parse_timestamp(ts_string, fmt="%Y-%m-%dT%H:%M:%S.%fZ"):
    """Parses timestamp string to timezone-aware UTC datetime."""
    # (Function remains the same as previous version)
    try:
        if ts_string.isdigit() or ("." in ts_string and ts_string.replace(".", "", 1).isdigit()):
            return datetime.datetime.fromtimestamp(float(ts_string), tz=datetime.timezone.utc
                                                            )
        if fmt.endswith("Z") and ts_string.endswith("Z"):
            ts_string = ts_string[:-1] + "+0000"
            fmt = fmt[:-1] + "%z"
        dt = datetime.datetime.strptime(ts_string, fmt)
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt
    except Exception as e:
        # Reduced verbosity on parse failure, handle higher up
        # print(f"Error parsing timestamp '{ts_string}' with format '{fmt}': {e}", file=sys.stderr)
        return None


def get_clickhouse_schema(client, db_name, table_name):
    """Gets column names and types from a ClickHouse table."""
    try:
        query = f"DESCRIBE TABLE {db_name}.{table_name}"
        result = client.query(query)
        # Returns map like {'col_name': {'type': 'String', 'nullable': True}, ...}
        # Note: Column names in the map keys will include backticks if needed.
        schema = {
            row[0]: {"type": row[1], "nullable": "Nullable" in row[1]}
            for row in result.result_rows
        }
        print(
            f"Fetched schema for {db_name}.{table_name}. Columns: {list(schema.keys())}"
        )
        return schema
    except Exception as e:
        print(
            f"Error getting table schema for {db_name}.{table_name}: {e}",
            file=sys.stderr,
        )
        return None


def create_tsv_to_ch_mapping(tsv_headers, ch_schema):
    """Creates a mapping from TSV header names to ClickHouse column names."""
    mapping = {}
    ch_columns_set = set(ch_schema.keys())

    # Direct mappings for required fields (assuming TSV names match sample keys)
    direct_map = {
        TSV_TIMESTAMP_COL: "timestamp",  # CH name is simple
        TSV_INPUT_COL: "aggregate(values.input, 60, average)",
        TSV_OUTPUT_COL: "aggregate(values.output, 60, average)",
        TSV_NODE_COL: "node",
        TSV_INTF_COL: "intf",
    }

    for tsv_col in tsv_headers:
        tsv_col = tsv_col.strip()
        found = False

        # Check direct map first
        if tsv_col in direct_map:
            ch_col = f"`{direct_map[tsv_col]}`"  # Assume simple names need quoting too for consistency
            if ch_col in ch_columns_set:
                mapping[tsv_col] = ch_col
                found = True
            # Check without backticks if direct map value was simple
            elif direct_map[tsv_col] in ch_columns_set:
                mapping[tsv_col] = direct_map[tsv_col]
                found = True

        # Check metadata columns (need quoting for CH name)
        elif tsv_col in TSV_METADATA_COLS:
            # Construct the expected ClickHouse column name with backticks
            ch_col = f"{tsv_col}"
            if ch_col in ch_columns_set:
                mapping[tsv_col] = ch_col
                found = True

        # If no match found after checking possibilities
        if not found:
            print(
                f"  INFO: TSV Header '{tsv_col}' does not map to any known ClickHouse column. It will be ignored.",
                file=sys.stderr,
            )
            print(ch_columns_set)

    # Check if all required TSV columns were successfully mapped
    for req_col in REQUIRED_TSV_COLS:
        if req_col not in mapping:
            print(
                f"  CRITICAL ERROR: Required TSV column '{req_col}' could not be mapped to the ClickHouse schema. Check TSV headers and ClickHouse table definition.",
                file=sys.stderr,
            )
            return None  # Indicate fatal error

    return mapping


def insert_data_and_benchmark(
    host, port, user, password, db_name, table_name, data_dir, ts_format, output_file
):
    """Reads TSV files, inserts data into ClickHouse table, and benchmarks."""
    print(f"Starting data insertion process for ClickHouse...")
    print(f"Connecting to ClickHouse: {host}:{port}")
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db_name,
            # Consider increasing settings for large inserts if timeouts occur
            # settings={'send_timeout': 600, 'receive_timeout': 600}
        )
        client.ping()
        print(f"Connected to DB '{db_name}', Table '{table_name}'.")
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}", file=sys.stderr)
        sys.exit(1)

    # --- Get Target Table Schema ---
    table_schema = get_clickhouse_schema(client, db_name, table_name)
    if not table_schema:
        print(f"Cannot proceed without table schema.", file=sys.stderr)
        client.close()
        sys.exit(1)
    # Get the exact column names *in the order ClickHouse expects*
    target_columns_ordered = list(table_schema.keys())

    if not os.path.isdir(data_dir):
        print(f"Error: Data directory '{data_dir}' not found.", file=sys.stderr)
        sys.exit(1)

    benchmark_results = []
    total_rows_inserted = 0
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
            rows_in_file_attempted = 0
            rows_in_file_inserted = 0
            file_insertion_time = 0.0
            batch_data = []  # List to hold rows for insertion (list of lists)
            tsv_header = []
            header_processed = False
            tsv_ch_map = None

            try:
                with open(filepath, "r", newline="", encoding="utf-8") as tsvfile:
                    reader = csv.reader(tsvfile, delimiter="\t")

                    for i, row in enumerate(reader):
                        if not row:
                            continue  # Skip empty

                        if not header_processed:
                            tsv_header = [h.strip() for h in row]
                            # --- Validate Header and Create Mapping ---
                            missing_req = [
                                col
                                for col in REQUIRED_TSV_COLS
                                if col not in tsv_header
                            ]
                            if missing_req:
                                print(
                                    f"  ERROR: File '{filename}' is missing required TSV headers: {missing_req}. Skipping file.",
                                    file=sys.stderr,
                                )
                                files_skipped += 1
                                files_processed -= 1
                                tsv_header = []  # Mark as invalid header
                                break  # Stop processing this file

                            print(f"  TSV Headers: {tsv_header}")
                            tsv_ch_map = create_tsv_to_ch_mapping(
                                tsv_header, table_schema
                            )
                            if tsv_ch_map is None:  # Critical mapping error
                                print(
                                    f"  ERROR: Failed to create valid mapping for TSV headers in {filename}. Skipping file.",
                                    file=sys.stderr,
                                )
                                files_skipped += 1
                                files_processed -= 1
                                tsv_header = []  # Mark as invalid header
                                break

                            print(f"  TSV -> ClickHouse Mapping: {tsv_ch_map}")
                            header_processed = True
                            continue  # Skip header row from data processing

                        # --- Data Row Processing ---
                        rows_in_file_attempted += 1
                        if len(row) != len(tsv_header):
                            # Allow more columns, but log if fewer (might indicate parsing issue)
                            if len(row) < len(tsv_header):
                                print(
                                    f"  WARNING: Row {i+1}: Expected {len(tsv_header)} columns based on header, found {len(row)}. Skipping row: {row}",
                                    file=sys.stderr,
                                )
                                continue
                            # Silently ignore extra columns in the row

                        row_data_dict = dict(
                            zip(tsv_header, row)
                        )  # Dict from this row's data

                        # --- Parse, Validate, and Order Data ---
                        ordered_row_values = [None] * len(
                            target_columns_ordered
                        )  # Initialize with None
                        valid_row = True
                        parse_errors = []

                        # Iterate through the TSV columns that have a mapping to ClickHouse
                        for tsv_col_name, ch_col_name in tsv_ch_map.items():
                            raw_value = row_data_dict.get(tsv_col_name)
                            target_type = table_schema[ch_col_name]["type"]
                            is_nullable = table_schema[ch_col_name]["nullable"]
                            col_index = target_columns_ordered.index(
                                ch_col_name
                            )  # Get position for insertion list

                            parsed_value = None
                            if raw_value is None or raw_value == "":
                                if is_nullable:
                                    parsed_value = None
                                elif "String" in target_type:
                                    parsed_value = (
                                        ""  # Empty string for non-null strings
                                    )
                                else:
                                    parse_errors.append(
                                        f"Empty value for non-nullable column '{ch_col_name}'"
                                    )
                                    valid_row = False
                                    # Don't break, collect all errors for the row
                            else:
                                # --- Type Conversion ---
                                try:
                                    if tsv_col_name == TSV_TIMESTAMP_COL:
                                        ts = parse_timestamp(raw_value, ts_format)
                                        if ts is None:
                                            raise ValueError("Timestamp parsing failed")
                                        parsed_value = ts
                                    elif "Float" in target_type:
                                        parsed_value = float(raw_value)
                                    elif "Int" in target_type:
                                        parsed_value = int(raw_value)
                                    # elif 'DateTime' in target_type and isinstance(raw_value, (int, float)):
                                    #    # Handle epoch numbers if needed and column type matches
                                    #    parsed_value = raw_value
                                    else:  # Assume String
                                        parsed_value = str(raw_value)
                                except (ValueError, TypeError) as e:
                                    parse_errors.append(
                                        f"Conversion error for column '{ch_col_name}' (value: '{raw_value}', type: {target_type}): {e}"
                                    )
                                    valid_row = False

                            if valid_row:
                                ordered_row_values[col_index] = parsed_value
                            # else: value remains None, error will be reported below

                        # --- Row Validation Result ---
                        if valid_row:
                            # Final check: ensure non-nullable CH columns got a value
                            for idx, ch_col_name in enumerate(target_columns_ordered):
                                if (
                                    not table_schema[ch_col_name]["nullable"]
                                    and ordered_row_values[idx] is None
                                ):
                                    # This implies a required TSV column was missing or failed parsing
                                    # Error should have been caught earlier, but double-check.
                                    if ch_col_name not in [
                                        m for m in tsv_ch_map.values()
                                    ]:  # Check if it was expected from TSV
                                        parse_errors.append(
                                            f"Non-nullable ClickHouse column '{ch_col_name}' has no corresponding value from TSV."
                                        )
                                        valid_row = False
                                    elif (
                                        not parse_errors
                                    ):  # If no previous error, maybe logic flaw
                                        parse_errors.append(
                                            f"Non-nullable ClickHouse column '{ch_col_name}' ended up with None despite being mapped."
                                        )
                                        valid_row = False

                            if valid_row:
                                batch_data.append(ordered_row_values)
                            else:
                                print(
                                    f"  WARNING: Row {i+1}: Skipping due to errors: {'; '.join(parse_errors)} - Row content: {row_data_dict}",
                                    file=sys.stderr,
                                )

                        else:  # Row already invalid from parsing/conversion
                            print(
                                f"  WARNING: Row {i+1}: Skipping due to errors: {'; '.join(parse_errors)} - Row content: {row_data_dict}",
                                file=sys.stderr,
                            )

                    # --- End of File Read ---
                    if not header_processed:  # File skipped due to header issues
                        continue

            except FileNotFoundError:
                print(
                    f"  ERROR: File '{filename}' not found during processing (unexpected). Skipping.",
                    file=sys.stderr,
                )
                files_skipped += 1
                files_processed -= 1
                continue
            except Exception as e:
                print(
                    f"  ERROR: Failed to read or parse file '{filename}': {e}. Skipping file.",
                    file=sys.stderr,
                )
                files_skipped += 1
                files_processed -= 1
                batch_data = []  # Clear any partial batch
                continue

            # --- Insert Batch for the File ---
            if batch_data:
                print(
                    f"  Attempting to insert {len(batch_data)} valid rows from {filename} (attempted {rows_in_file_attempted} rows)..."
                )
                start_time = time.monotonic()
                try:
                    # Insert using list of lists and specifying column names from DESCRIBE
                    client.insert(
                        table=table_name,
                        data=batch_data,
                        column_names=target_columns_ordered,  # Must match table schema order
                    )
                    duration = time.monotonic() - start_time
                    inserted_count = len(batch_data)  # Assume success if no exception
                    batch_durations.append(duration)
                    benchmark_results.append(
                        {
                            "file": filename,
                            "rows_attempted": rows_in_file_attempted,
                            "rows_inserted": inserted_count,
                            "time_seconds": duration,
                        }
                    )
                    total_rows_inserted += inserted_count
                    rows_in_file_inserted = inserted_count
                    total_insertion_time += duration
                    file_insertion_time = duration
                    print(
                        f"    Successfully inserted {inserted_count} rows in {duration:.4f}s"
                    )

                except clickhouse_connect.driver.exceptions.Error as ch_err:
                    duration = (
                        time.monotonic() - start_time
                    )  # Record time even on failure
                    print(
                        f"  ERROR: ClickHouse insertion FAILED for {filename}: {ch_err}",
                        file=sys.stderr,
                    )
                    print(
                        f"    Failed batch contained {len(batch_data)} rows.",
                        file=sys.stderr,
                    )
                    # Log more details if needed, e.g., error code, message
                    if hasattr(ch_err, "code"):
                        print(f"    Error Code: {ch_err.code}", file=sys.stderr)
                    benchmark_results.append(
                        {
                            "file": filename,
                            "rows_attempted": rows_in_file_attempted,
                            "rows_inserted": 0,
                            "time_seconds": duration,
                            "error": f"ClickHouse Error Code {getattr(ch_err, 'code', 'N/A')}: {ch_err}",
                        }
                    )
                except Exception as e:
                    duration = time.monotonic() - start_time
                    print(
                        f"  ERROR: Unexpected error during insertion for {filename}: {e}",
                        file=sys.stderr,
                    )
                    benchmark_results.append(
                        {
                            "file": filename,
                            "rows_attempted": rows_in_file_attempted,
                            "rows_inserted": 0,
                            "time_seconds": duration,
                            "error": str(e),
                        }
                    )
            else:
                print(f"  No valid data parsed to insert from {filename}.")

            print(
                f"  Finished processing file: {filename}. Inserted {rows_in_file_inserted} rows. Time for file insert: {file_insertion_time:.4f}s"
            )

    # --- Final Summary & Output ---
    print("\n--- ClickHouse Insertion Summary ---")
    print(f"Processed {files_processed} files.")
    if files_skipped > 0:
        print(f"Skipped {files_skipped} files due to header/read/mapping errors.")
    print(f"Total rows inserted successfully: {total_rows_inserted}")
    print(
        f"Total insertion time (sum of successful batch inserts): {total_insertion_time:.4f} seconds"
    )

    if total_insertion_time > 0 and total_rows_inserted > 0:
        avg_rows_per_sec = total_rows_inserted / total_insertion_time
        print(f"Average insertion rate: {avg_rows_per_sec:.2f} rows/second")
    else:
        print("Average insertion rate: N/A")

    if batch_durations:
        print("\nFile/Batch Performance Statistics (each batch = one file):")
        print(f"  Total successful batches (files inserted): {len(batch_durations)}")
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
        print("\nNo successful batch performance statistics available.")

    # --- Write Benchmark Results to CSV ---
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as outfile:
            if benchmark_results:
                # Ensure all possible keys are included in header, even if some runs had errors
                fieldnames = [
                    "file",
                    "rows_attempted",
                    "rows_inserted",
                    "time_seconds",
                    "error",
                ]
                present_keys = set().union(*(d.keys() for d in benchmark_results))
                final_fieldnames = [f for f in fieldnames if f in present_keys]
                # Add any unexpected keys found
                for key in present_keys:
                    if key not in final_fieldnames:
                        final_fieldnames.append(key)

                writer = csv.DictWriter(outfile, fieldnames=final_fieldnames)
                writer.writeheader()
                writer.writerows(benchmark_results)
                print(f"\nDetailed benchmark results written to: {output_file}")
            else:
                outfile.write("No benchmark data recorded.\n")
                print(
                    f"\nBenchmark output file created, but no data was recorded: {output_file}"
                )
    except IOError as e:
        print(
            f"\nError writing benchmark results to '{output_file}': {e}",
            file=sys.stderr,
        )
    except Exception as e:
        print(
            f"\nAn unexpected error occurred writing benchmark results: {e}",
            file=sys.stderr,
        )

    if "client" in locals() and client:
        client.close()
        print("ClickHouse connection closed.")
    print("Benchmarking complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert TSV data into ClickHouse and benchmark."
    )
    parser.add_argument(
        "--host", default="localhost", help="ClickHouse host (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=8123, help="ClickHouse HTTP port (default: 8123)"
    )
    parser.add_argument(
        "--user", default="default", help="ClickHouse user (default: default)"
    )
    parser.add_argument(
        "--password", default="", help="ClickHouse password (default: empty)"
    )
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--dir", required=True, help="Directory containing TSV files")
    parser.add_argument(
        "--tsformat",
        default="%Y-%m-%dT%H:%M:%S.%fZ",  # Example: ISO 8601 UTC
        help="Timestamp format string (strptime format, e.g., '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ')",
    )
    parser.add_argument(
        "--output",
        default="clickhouse_insert_benchmark.csv",
        help="Output CSV file for benchmark results (default: clickhouse_insert_benchmark.csv)",
    )

    args = parser.parse_args()

    insert_data_and_benchmark(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        db_name=args.db,
        table_name=args.table,
        data_dir=args.dir,
        ts_format=args.tsformat,
        output_file=args.output,
    )
