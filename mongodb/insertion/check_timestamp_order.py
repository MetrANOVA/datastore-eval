# check_timestamp_order_per_series.py
import argparse
import csv
import datetime
import sys
from pathlib import Path


def parse_timestamp(ts_string: str):
    """Parses ISO 8601-like timestamp strings."""
    if not ts_string:
        return None
    try:
        if ts_string.endswith("Z"):
            ts_string = ts_string[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(ts_string)
    except Exception:
        return None


def check_sort_order_per_series(
    tsv_filepath: Path,
    timestamp_col: str,
    node_col: str,
    intf_col: str,
    limit: int,
    count_all: bool,
):
    """
    Reads a TSV file and checks if the timestamp column is in ascending order
    for each unique node/interface combination.
    """
    print(
        f"Checking PER-SERIES timestamp order for column '{timestamp_col}' in file: {tsv_filepath}"
    )
    print(f"Series identified by: ('{node_col}', '{intf_col}')")
    if count_all:
        print("Mode: Counting all out-of-order entries.")
    else:
        print("Mode: Exiting on first out-of-order entry found.")

    if not tsv_filepath.is_file():
        print(f"Error: File not found at '{tsv_filepath}'", file=sys.stderr)
        sys.exit(1)

    # Dictionary to store the last seen timestamp and line number for each series
    # Key: tuple (node_val, intf_val)
    # Value: tuple (datetime_object, line_number)
    last_timestamp_per_series = {}

    processed_lines = 0
    error_count = 0
    last_progress_len = 0

    try:
        with open(tsv_filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")

            required_headers = {timestamp_col, node_col, intf_col}
            if not required_headers.issubset(reader.fieldnames):
                print(
                    f"Error: One or more required columns not found in TSV header.",
                    file=sys.stderr,
                )
                print(f"Required: {required_headers}", file=sys.stderr)
                print(f"Found: {reader.fieldnames}", file=sys.stderr)
                sys.exit(1)

            for line_num, row in enumerate(reader, 2):  # Start from line 2
                processed_lines += 1

                if limit >= 0 and processed_lines > limit:
                    print(f"\nReached check limit of {limit} lines.")
                    break

                # Progress update
                if processed_lines % 50000 == 0:
                    status_message = f"Checked {processed_lines:,} lines... Found {error_count} error(s) so far."
                    sys.stdout.write("\r" + " " * last_progress_len + "\r")
                    sys.stdout.write(status_message)
                    sys.stdout.flush()
                    last_progress_len = len(status_message)

                node_val = row.get(node_col)
                intf_val = row.get(intf_col)
                timestamp_str = row.get(timestamp_col)

                if not node_val or not intf_val or not timestamp_str:
                    continue  # Skip rows missing key information

                current_timestamp = parse_timestamp(timestamp_str)
                if current_timestamp is None:
                    continue  # Skip rows with unparseable timestamps

                series_key = (node_val, intf_val)

                if series_key in last_timestamp_per_series:
                    # We've seen this series before, compare timestamps
                    previous_timestamp, last_good_line = last_timestamp_per_series[
                        series_key
                    ]

                    if current_timestamp < previous_timestamp:
                        error_count += 1
                        sys.stdout.write(
                            "\r" + " " * last_progress_len + "\r"
                        )  # Clear progress line
                        print(
                            "\n--- ERROR: Out-of-order timestamp found FOR SERIES! ---",
                            file=sys.stderr,
                        )
                        print(f"  > Series Identifier: {series_key}", file=sys.stderr)
                        print(
                            f"  > Problem found on data line {processed_lines} (file line {line_num}):",
                            file=sys.stderr,
                        )
                        print(
                            f"  > Previous Timestamp for this series (from line {last_good_line}): {previous_timestamp.isoformat()}",
                            file=sys.stderr,
                        )
                        print(
                            f"  > Current Timestamp for this series (from line {line_num}):  {current_timestamp.isoformat()}",
                            file=sys.stderr,
                        )
                        last_progress_len = 0

                        if not count_all:
                            sys.exit(1)

                # Update the last seen timestamp for this series
                last_timestamp_per_series[series_key] = (current_timestamp, line_num)

        sys.stdout.write("\r" + " " * last_progress_len + "\r")

        if error_count == 0:
            print("\n--- SUCCESS ---")
            print(f"Scan complete. Checked {processed_lines:,} data lines.")
            print(f"Found {len(last_timestamp_per_series)} unique series.")
            print("No out-of-order timestamps were found within any individual series.")
        else:
            print("\n--- FAILURE ---")
            print(
                f"Scan complete. Found a total of {error_count} out-of-order timestamp(s) within their respective series."
            )
            sys.exit(1)

    except Exception as e:
        sys.stdout.write("\n")
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check if a large TSV file is sorted by timestamp in ascending order on a per-series basis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "tsv_file", type=Path, help="Path to the input TSV file to check."
    )
    parser.add_argument(
        "--timestamp_col",
        type=str,
        default="@timestamp",
        help="Name of the column containing the timestamp.",
    )
    parser.add_argument(
        "--node_col",
        type=str,
        default="meta.device",
        help="Name of the column representing the node/device.",
    )
    parser.add_argument(
        "--intf_col",
        type=str,
        default="meta.name",
        help="Name of the column representing the interface/name.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Check only the first N data lines of the file (-1 for all).",
    )
    parser.add_argument(
        "--count_all",
        action="store_true",
        help="Scan the entire file and count all errors instead of exiting on the first one.",
    )

    args = parser.parse_args()

    check_sort_order_per_series(
        args.tsv_file,
        args.timestamp_col,
        args.node_col,
        args.intf_col,
        args.limit,
        args.count_all,
    )
