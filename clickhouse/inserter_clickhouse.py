# inserter_clickhouse.py
import argparse
import csv
import datetime
import os
import sys
import zlib
from pathlib import Path

import clickhouse_connect

# --- Standardized Configuration ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.iface_in.device"
TSV_INTERFACE_COL = "meta.iface_in.name"
REQUIRED_TSV_COLS = [TSV_TIMESTAMP_COL, TSV_NODE_COL, TSV_INTERFACE_COL]
DISCARD_TSV_FIELDS = {"@collect_time_min", "@exit_time", "@processing_time", "@start_time"}
SCOREBOARD_TABLE = "scoreboard"


# --- Helper Functions ---
def parse_timestamp(ts_string):
    """
    Parses ISO 8601-like timestamp strings, accommodating optional fractional seconds
    and 'Z' or +HH:MM timezone offset. Returns a timezone-aware UTC datetime object or None.
    """
    if not ts_string:
        return None
    try:
        # fromisoformat in Python 3.11+ is very flexible.
        # For older versions, this replacement is a robust way to handle 'Z'.
        if ts_string.endswith("Z"):
            ts_string = ts_string[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(ts_string)
    except Exception:
        return None


def get_clickhouse_schema(client, db_name, table_name):
    """Gets column names and their nullability from a ClickHouse table."""
    try:
        fq_table_name = f"`{db_name}`.`{table_name}`"
        query = f"DESCRIBE TABLE {fq_table_name}"
        result = client.query(query)
        schema = {
            row[0]: {"type": row[1], "nullable": "Nullable" in row[1]}
            for row in result.result_rows
        }
        if not schema:
            print(
                f"Error: Fetched schema is empty for {fq_table_name}. Table must exist with columns.",
                file=sys.stderr,
            )
            return None
        return schema
    except Exception as e:
        print(
            f"Error getting table schema for {db_name}.{table_name}: {e}",
            file=sys.stderr,
        )
        if hasattr(e, "message") and e.message:
            print(f"ClickHouse Error Details: {e.message}", file=sys.stderr)
        return None


def create_tsv_to_ch_column_mapping(tsv_header_names: list, ch_schema_map: dict):
    """
    Maps original TSV header names to actual ClickHouse column names found in the schema,
    after applying transformations (like prefix stripping) to TSV names to find a conceptual match.
    """
    mapping = {}
    ch_column_names_in_schema = set(ch_schema_map.keys())
    successfully_mapped_tsv = set()

    for tsv_col_name in tsv_header_names:
        if tsv_col_name in DISCARD_TSV_FIELDS:
            continue

        target_ch_key_unquoted = tsv_col_name
        if tsv_col_name == TSV_TIMESTAMP_COL:
            target_ch_key_unquoted = "@timestamp"
        elif tsv_col_name == TSV_NODE_COL:
            target_ch_key_unquoted = "meta.iface_in.device"
        elif tsv_col_name == TSV_INTERFACE_COL:
            target_ch_key_unquoted = "meta.iface_in.name"
        elif tsv_col_name.startswith("values."):
            target_ch_key_unquoted = tsv_col_name.replace("values.", "", 1)
        elif tsv_col_name.startswith("meta."):
            target_ch_key_unquoted = tsv_col_name.replace("meta.", "", 1)

        found_ch_col_name = None
        if target_ch_key_unquoted in ch_column_names_in_schema:
            found_ch_col_name = target_ch_key_unquoted
        else:
            quoted_target_key = f"`{target_ch_key_unquoted}`"
            if quoted_target_key in ch_column_names_in_schema:
                found_ch_col_name = quoted_target_key
        # if we *still* haven't found a column name after munging,
        # try 'no munging'
        if not found_ch_col_name:
            if tsv_col_name in ch_column_names_in_schema:
                found_ch_col_name = tsv_col_name

        if found_ch_col_name:
            mapping[tsv_col_name] = found_ch_col_name
            successfully_mapped_tsv.add(tsv_col_name)

    missing_mappings = [req for req in REQUIRED_TSV_COLS if req not in mapping]
    if missing_mappings:
        print(
            f"CRITICAL ERROR: Required TSV columns not mapped to CH schema: {missing_mappings}",
            file=sys.stderr,
        )
        print(
            f"  > This means the inserter expected to find ClickHouse columns like 'timestamp', 'device', 'interfaceName', but they were not found in the table schema.",
            file=sys.stderr,
        )
        print(
            f"  > Please run 'DESCRIBE {list(ch_schema_map.keys())[0].split('.')[0] if ch_schema_map else 'your_table'};' in clickhouse-client to see the actual column names.",
            file=sys.stderr,
        )
        return None

    return mapping


# --- Main Function ---
def main_insert_logic(args):
    # Determine mode and set up logging prefix
    is_strided_mode = args.worker_num is not None and args.total_workers is not None
    worker_id_str = (
        f"{args.worker_num+1}/{args.total_workers}"
        if is_strided_mode
        else f"PID:{os.getpid()}"
    )
    log_prefix = f"[CH_Worker_{worker_id_str}]"

    print(f"{log_prefix} Starting.")
    if is_strided_mode:
        print(f"{log_prefix} Mode: Strided Read from single large file.")
    else:
        print(f"{log_prefix} Mode: Processing single pre-split file.")
    print(f"{log_prefix} Reading from: {args.tsv_file}")

    try:
        client = clickhouse_connect.get_client(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.db,
        )
        client.ping()
        print(f"{log_prefix} Connected to ClickHouse.")
    except Exception as e:
        print(f"{log_prefix} ERROR: Connecting to ClickHouse: {e}", file=sys.stderr)
        sys.exit(1)

    ch_schema_map = get_clickhouse_schema(client, args.db, args.table)
    if not ch_schema_map:
        if client:
            client.close()
            sys.exit(1)

    target_ch_columns_ordered = list(ch_schema_map.keys())

    lines_processed_by_this_worker = 0
    rows_sent = 0
    batches_sent = 0

    try:
        with open(args.tsv_file, "r", newline="", encoding="utf-8") as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter="\t")
            tsv_header_from_file = reader.fieldnames
            if not tsv_header_from_file:
                raise ValueError("Header row is empty")

            missing_req = [
                col for col in REQUIRED_TSV_COLS if col not in tsv_header_from_file
            ]
            if missing_req:
                raise ValueError(f"File missing required TSV headers: {missing_req}")

            print(f"{log_prefix} Header verified. Creating TSV-to-CH mapping...")
            tsv_to_actual_ch_col_map = create_tsv_to_ch_column_mapping(
                tsv_header_from_file, ch_schema_map
            )
            if tsv_to_actual_ch_col_map is None:
                raise ValueError(
                    "Failed to create valid mapping between TSV and ClickHouse schema."
                )
            print(f"{log_prefix} Mapping created. Starting data insertion...")

            batch_data = []
            file_line_index = 0  # Only used for strided mode

            for row_data_dict in reader:
                if is_strided_mode:
                    if file_line_index % args.total_workers != args.worker_num:
                        file_line_index += 1
                        continue  # Skip line, it's not for this worker

                if args.limit >= 0 and lines_processed_by_this_worker >= args.limit:
                    print(
                        f"{log_prefix} Reached processing limit of {args.limit} rows."
                    )
                    break

                lines_processed_by_this_worker += 1
                parsed_row = [None] * len(target_ch_columns_ordered)
                valid_row = True
                parse_errors = []

                for tsv_col_name, raw_value in row_data_dict.items():
                    if tsv_col_name not in tsv_to_actual_ch_col_map:
                        continue

                    ch_key = tsv_to_actual_ch_col_map[tsv_col_name]
                    ch_idx = target_ch_columns_ordered.index(ch_key)
                    is_nullable = ch_schema_map[ch_key]["nullable"]
                    parsed_value = None

                    if raw_value is None or raw_value == "":
                        if is_nullable:
                            parsed_value = None
                        elif "String" in ch_schema_map[ch_key]["type"]:
                            parsed_value = ""
                        else:
                            valid_row = False
                            parse_errors.append(f"Empty for non-null CH '{ch_key}'")
                            break
                    else:
                        try:
                            if tsv_col_name == TSV_TIMESTAMP_COL:
                                parsed_value = parse_timestamp(raw_value)
                                if not parsed_value:
                                    raise ValueError("TS parsing failed")
                            elif tsv_col_name.startswith("values."):
                                parsed_value = float(raw_value)
                            else:
                                ch_type = ch_schema_map[ch_key]["type"].lower()
                                if "int" in ch_type:
                                    parsed_value = int(raw_value)
                                elif "float" in ch_type:
                                    parsed_value = float(raw_value)
                                else:
                                    parsed_value = str(raw_value)
                        except (ValueError, TypeError) as e:
                            valid_row = False
                            parse_errors.append(
                                f"Conv. error for TSV '{tsv_col_name}': {e}"
                            )
                            break

                    parsed_row[ch_idx] = parsed_value

                if valid_row:
                    batch_data.append(parsed_row)
                else:
                    print(
                        f"{log_prefix} WARN Row {lines_processed_by_this_worker}: Skipping: {'; '.join(parse_errors)}",
                        file=sys.stderr,
                    )

                if len(batch_data) >= args.batch_size:
                    try:
                        start_time = datetime.datetime.now()
                        client.insert(
                            table=args.table,
                            data=batch_data,
                            column_names=target_ch_columns_ordered,
                            database=args.db,
                        )
                        end_time = datetime.datetime.now()
                        rows_sent += len(batch_data)
                        batches_sent += 1
                        if batches_sent % 1 == 0:
                            print(
                                f"{log_prefix} Sent batch {batches_sent}. Total rows sent: {rows_sent}"
                            )
                        scoreboard_row = [
                            args.table,
                            len(batch_data),
                            start_time,
                            end_time,
                        ]
                        scoreboard_columns = [
                            'table_name',
                            'batch_size',
                            'start_time',
                            'end_time'
                        ]
                        client.insert(
                            table=SCOREBOARD_TABLE,
                            data=[scoreboard_row],
                            column_names=scoreboard_columns,
                            database=args.db,
                        )
                    except Exception as e:
                        print(
                            f"{log_prefix} ERROR inserting batch {batches_sent+1}: {e}",
                            file=sys.stderr,
                        )
                    finally:
                        batch_data = []

                if is_strided_mode:
                    file_line_index += 1

            if batch_data:  # Final batch
                try:
                    client.insert(
                        table=args.table,
                        data=batch_data,
                        column_names=target_ch_columns_ordered,
                        database=args.db,
                    )
                    rows_sent += len(batch_data)
                    batches_sent += 1
                    print(
                        f"{log_prefix} Sent final batch {batches_sent} ({len(batch_data)} rows)."
                    )
                except Exception as e:
                    print(
                        f"{log_prefix} ERROR inserting final batch: {e}",
                        file=sys.stderr,
                    )

    except Exception as e:
        print(f"{log_prefix} ERROR during file processing: {e}", file=sys.stderr)

    print(f"\n--- {log_prefix} Summary ---")
    print(f"{log_prefix} Processed {lines_processed_by_this_worker} assigned rows.")
    print(
        f"{log_prefix} Attempted to send {rows_sent} rows to ClickHouse in {batches_sent} batches."
    )

    if client:
        try:
            client.close()
            print(f"{log_prefix} ClickHouse connection closed.")
        except:
            pass
    print(f"{log_prefix} Finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ClickHouse Lean Inserter with multiple data source modes."
    )
    # Connection Args
    parser.add_argument("--host", default="localhost", help="ClickHouse host.")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port.")
    parser.add_argument("--user", default="default", help="ClickHouse user.")
    parser.add_argument("--password", default="", help="ClickHouse password.")
    parser.add_argument("--db", required=True, help="ClickHouse Database name.")
    parser.add_argument("--table", required=True, help="ClickHouse Table name.")
    # Data & Mode Args
    parser.add_argument(
        "--tsv_file", required=True, type=Path, help="Path to the input TSV file."
    )
    parser.add_argument(
        "--batch_size", type=int, default=50000, help="Rows per insert batch."
    )
    # Strided Mode Args (Optional: if not provided, script processes the whole file)
    parser.add_argument(
        "--worker_num",
        type=int,
        default=None,
        help="Current worker number (0-indexed) for strided reads.",
    )
    parser.add_argument(
        "--total_workers",
        type=int,
        default=None,
        help="Total number of workers for strided reads.",
    )
    # General Args
    parser.add_argument(
        "--limit", type=int, default=-1, help="Max data rows this worker processes."
    )

    args = parser.parse_args()

    if (args.worker_num is not None and args.total_workers is None) or (
        args.worker_num is None and args.total_workers is not None
    ):
        print(
            "Error: For strided mode, both --worker_num and --total_workers must be provided.",
            file=sys.stderr,
        )
        sys.exit(1)

    main_insert_logic(args)
